import os
import sys
import re
import json
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

warnings.filterwarnings('ignore')

from scripts.utils.logger \
    import Logger             # noqa: E402
from scripts.utils.auto_retry \
    import retry_on_error   # noqa: E402


class MinioUtils:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret: str,
    ) -> None:
        self.current_dir = os.path.dirname(__file__)

        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret,
            secure=False,
        )

        self.logging = Logger()

    def gen_rundate_path(
        self,
        include_hours: bool = False
    ) -> str:
        today = datetime.datetime.now(
            pytz.timezone('Asia/Ho_Chi_Minh')
        )
        hour = f'/{today.hour}' if include_hours else ''
        return today.strftime('%Y/%m/%d') + hour

    def list_all_objects(
        self,
        file_path: str,
        only_filename: bool = False,
        bucket_name: str = 'lakehouse',
    ) -> list:
        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=file_path,
            recursive=True,
        )
        if only_filename:
            objects = [
                i.object_name.split(
                    '/'
                )[-1].split(
                    '.'
                )[0]
                for i in objects
            ]
        else:
            objects = [
                i.object_name for i in objects
            ]

        return objects

    def truncate_folder(
        self,
        file_path: str,
        bucket_name: str = 'lakehouse',
    ) -> None:
        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=file_path,
            recursive=True,
        )
        for obj in objects:
            self.client.remove_object(
                bucket_name=bucket_name,
                object_name=obj.object_name,
            )

    def count_data_rows(
        self,
        file_path: str,
        bucket_name: str = 'lakehouse',
    ) -> int:
        rows = 0

        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=file_path,
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
        bucket_name: str = 'lakehouse',
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
            defaults to 'lakehouse'

        :return: True if the data file exists
            otherwise False
        """

        obj = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=f'{file_path}/{file_name}.parquet'
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
        bucket_name: str = 'lakehouse',
        hide_log: bool = False,
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
            defaults to 'lakehouse'
        :param hide_log: whether to hide log
            defaults to False
        """

        parquet_data = data.to_parquet(index=False)
        bytes_data = BytesIO(parquet_data)

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=f'{file_path}/{file_name}.parquet',
            data=bytes_data,
            length=bytes_data.getbuffer().nbytes,
            content_type='parquet',
        )

        if not hide_log:
            self.logging.info(
                f'Done loading data with {len(data)} rows to Minio storage'
            )

    @retry_on_error(max_retries=4)
    def load_data_html(
        self,
        data: str,
        file_path: str,
        file_name: str,
        bucket_name: str = 'lakehouse',
    ) -> None:
        html_bytes = data.encode('utf-8')

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=f'{file_path}/{file_name}.html',
            data=BytesIO(html_bytes),
            length=len(html_bytes),
            content_type='html',
        )

    def load_data_json(
        self,
        data: dict,
        file_path: str,
        file_name: str,
        bucket_name: str = 'lakehouse',
    ) -> None:
        data_str = json.dumps(data)
        html_bytes = data_str.encode('utf-8')

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=f'{file_path}/{file_name}.json',
            data=BytesIO(html_bytes),
            length=len(html_bytes),
            content_type='html',
        )

    def get_data_json(
        self,
        file_path: str,
        file_name: str,
        bucket_name: str = 'lakehouse',
    ) -> dict:
        data = json.loads(
            self.client.get_object(
                bucket_name=bucket_name,
                object_name=f'{file_path}/{file_name}.json',
            ).data.decode('utf-8')
        )

        return data

    def get_data(
        self,
        file_path: str,
        file_name: str,
        bucket_name: str = 'lakehouse',
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
            defaults to 'lakehouse'

        :return: dataframe contains data to get
        """

        parquet_data = BytesIO(
            self.client.get_object(
                bucket_name=bucket_name,
                object_name=f'{file_path}/{file_name}.parquet',
            ).data
        )
        df = pd.read_parquet(parquet_data)

        return df

    def get_data_wildcard(
        self,
        file_path: str,
        bucket_name: str = 'lakehouse',
        batch_files_size: int = None,
    ) -> callable:
        """
        Get data from multiple files of directories of storage

        :param file_path: the directory contains files to get the data
        :param sandbox_env: whether to look up file
            in sandbox directory (dev mode)
            defaults to False
        :param bucket_name: the name of the bucket
            to get the data
            defaults to 'lakehouse'
        :param batch_files_size: number of files included
            when return data at once
            default to None
        :return: Generator contains dataframe
        """

        objects = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=file_path,
            recursive=True,
        )
        files = [
            i.object_name for i in objects
            if '.parquet' in i.object_name
        ]
        if len(files) == 0:
            yield pd.DataFrame()

        if batch_files_size:
            file_batches = [
                files[i:i+batch_files_size] for i in range(
                    0,
                    len(files),
                    batch_files_size,
                )
            ]
            self.logging.info(
                f'Total iterations: {len(file_batches)}'
            )
            for batch in file_batches:
                data = list()
                for file in batch:
                    if '/' not in file:
                        parent_dir = ''
                    else:
                        parent_dir = '/'.join(
                            file.split(
                                '/'
                            )[:-1]
                        )
                    filename = file.split(
                        '/'
                    )[-1].replace(
                        '.parquet',
                        '',
                    )
                    data.append(
                        self.get_data(
                            file_path=parent_dir,
                            file_name=filename,
                        )
                    )
                yield pd.concat(data).reset_index(
                    drop=True
                )
                del data
                self.logging.info('Done an interations')
        else:
            data = list()
            num_file = 0
            for file in files:
                if '/' not in file:
                    parent_dir = ''
                else:
                    parent_dir = '/'.join(
                        file.split(
                            '/'
                        )[:-1]
                    )
                filename = file.split(
                    '/'
                )[-1].replace(
                    '.parquet',
                    '',
                )
                data.append(
                    self.get_data(
                        file_path=parent_dir,
                        file_name=filename,
                    )
                )
                num_file += 1
                if num_file % 100 == 0:
                    self.logging.info(
                        f'Got {num_file} objects'
                    )
            if len(data) > 0:
                yield pd.concat(data).reset_index(drop=True)
                del data
            else:
                yield pd.DataFrame()
