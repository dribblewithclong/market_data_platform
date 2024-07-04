import sys
import re
import warnings
import datetime
import pytz
from io import BytesIO
from minio import Minio
import pandas as pd

sys.path.append(
    re.search(
        f'.*{re.escape("market_data_platform")}',
        __file__,
    ).group()
)

warnings.filterwarnings("ignore")

from scripts.utils.logger import Logger     # noqa: E402


class MinioUtils:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret: str,
    ) -> None:
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret,
            secure=False,
        )
        self.sandbox_prefix = {
            True: 'sandbox/',
            False: '',
        }
        self.logging = Logger(
            name=__name__,
        )

    def gen_rundate_path(self) -> str:
        today = datetime.datetime.now(
            pytz.timezone('Asia/Ho_Chi_Minh')
        )

        return today.strftime('%Y/%m/%d')

    def list_all_objects(
        self,
        file_path: str,
        sandbox_env: bool = False,
        bucket_name: str = 'sandbox',
    ) -> list:
        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=f'{self.sandbox_prefix[sandbox_env]}{file_path}',
            recursive=True,
        )
        objects = [
            i.object_name for i in objects
        ]

        return objects

    def count_data_rows(
        self,
        file_path: str,
        sandbox_env: bool = False,
        bucket_name: str = 'sandbox',
    ) -> int:
        rows = 0

        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=f'{self.sandbox_prefix[sandbox_env]}{file_path}',
            recursive=True,
        )

        num_objects = 0
        for obj in objects:
            if '.parquet' in obj.object_name:
                file_name = obj.object_name.replace(
                    '.parquet', ''
                ).split('/')[-1]
                file_path = '/'.join(
                    obj.object_name.split(
                        '/'
                    )[:-1]
                ).split('sandbox/')[-1]
                df = self.get_data(
                    file_path=file_path,
                    file_name=file_name,
                    sandbox_env=sandbox_env,
                    bucket_name=bucket_name,
                )
                rows += len(df)
                num_objects += 1
                if num_objects % 100 == 0:
                    self.logging.info(f'Got {num_objects} objects')

        return rows

    def data_exist(
        self,
        file_path: str,
        file_name: str,
        sandbox_env: bool = False,
        bucket_name: str = 'sandbox',
    ) -> bool:
        """
        Check if the data file exists in a directory

        :param file_path: the directory contains file to check
        :param file_name: file name to check
            notes that file_name remove the ".parquet" extension
        :param sandbox_env: whether to look up file
            in sandbox directory (dev mode)
            defaults to False
        :param bucket_name: the name of the bucket to check
            defaults to 'sandbox'

        :return: True if the data file exists
            otherwise False
        """

        obj = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=f'{self.sandbox_prefix[sandbox_env]}'
                   f'{file_path}/{file_name}.parquet'
        )
        obj = [*obj]

        if len(obj) == 0:
            return False
        return True

    def load_data(
        self,
        data: pd.DataFrame,
        file_path: str,
        file_name: str,
        sandbox_env: bool = False,
        bucket_name: str = 'sandbox',
    ) -> None:
        """
        Load data from dataframe to storage

        :param data: dataframe contains data to load
        :param file_path: the directory contains file to load the data
        :param file_name: file name contains data to load
            notes that file_name remove the ".parquet" extension
        :param sandbox_env: whether to look up file
            in sandbox directory (dev mode)
            defaults to False
        :param bucket_name: the name of the bucket
            to load the data
            defaults to 'sandbox'
        """

        parquet_data = data.to_parquet(index=False)
        bytes_data = BytesIO(parquet_data)

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=f'{self.sandbox_prefix[sandbox_env]}'
                        f'{file_path}/{file_name}.parquet',
            data=bytes_data,
            length=bytes_data.getbuffer().nbytes,
            content_type=f'{self.sandbox_prefix[sandbox_env]}parquet',
        )

        self.logging.info(
            f'Done loading data with {len(data)} rows to Minio storage'
        )

    def get_data(
        self,
        file_path: str,
        file_name: str,
        sandbox_env: bool = False,
        bucket_name: str = 'sandbox',
    ) -> pd.DataFrame:
        """
        Get data from a single file of directory of storage

        :param file_path: the directory contains file to get the data
        :param file_name: file name contains data to get
            notes that file_name remove the ".parquet" extension
        :param sandbox_env: whether to look up file
            in sandbox directory (dev mode)
            defaults to False
        :param bucket_name: the name of the bucket
            to get the data
            defaults to 'sandbox'

        :return: dataframe contains data to get
        """

        parquet_data = BytesIO(
            self.client.get_object(
                bucket_name=bucket_name,
                object_name=f'{self.sandbox_prefix[sandbox_env]}'
                            f'{file_path}/{file_name}.parquet',
            ).data
        )
        df = pd.read_parquet(parquet_data)

        return df

    def get_data_wildcard(
        self,
        file_path: str,
        sandbox_env: bool = False,
        bucket_name: str = 'sandbox',
    ) -> pd.DataFrame:
        """
        Get data from multiple files of directories of storage

        :param file_path: the directory contains files to get the data
        :param sandbox_env: whether to look up file
            in sandbox directory (dev mode)
            defaults to False
        :param bucket_name: the name of the bucket
            to get the data
            defaults to 'sandbox'

        :return: dataframe contains data to get
        """

        data = list()

        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=f'{self.sandbox_prefix[sandbox_env]}{file_path}',
            recursive=True,
        )

        num_objects = 0
        for obj in objects:
            if '.parquet' in obj.object_name:
                file_name = obj.object_name.replace(
                    '.parquet', ''
                ).split('/')[-1]
                file_path = '/'.join(
                    obj.object_name.split(
                        '/'
                    )[:-1]
                ).split('sandbox/')[-1]
                data.append(
                    self.get_data(
                        file_path=file_path,
                        file_name=file_name,
                        sandbox_env=sandbox_env,
                        bucket_name=bucket_name,
                    )
                )
                num_objects += 1
                if num_objects % 100 == 0:
                    self.logging.info(f'Got {num_objects} objects')

        if len(data) > 0:
            df = pd.concat(data).reset_index(drop=True)
            return df

        return pd.DataFrame()

    def get_distinct_values(
        self,
        columns: list,
        file_path: str,
        sandbox_env: bool = False,
        bucket_name: str = 'sandbox',
    ) -> tuple:
        distinct_val = tuple()

        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=f'{self.sandbox_prefix[sandbox_env]}{file_path}',
            recursive=True,
        )

        num_objects = 0
        for obj in objects:
            if '.parquet' in obj.object_name:
                file_name = obj.object_name.replace(
                    '.parquet', ''
                ).split('/')[-1]
                file_path = '/'.join(
                    obj.object_name.split(
                        '/'
                    )[:-1]
                ).split('sandbox/')[-1]
                df = self.get_data(
                    file_path=file_path,
                    file_name=file_name,
                    sandbox_env=sandbox_env,
                    bucket_name=bucket_name,
                )[columns]
                distinct_val += tuple(
                    df.apply(
                        lambda row: ' | '.join(
                            row.values.astype(str)
                        ),
                        axis=1,
                    ).tolist()
                )
                num_objects += 1
                if num_objects % 100 == 0:
                    self.logging.info(f'Got {num_objects} objects')

        return distinct_val
