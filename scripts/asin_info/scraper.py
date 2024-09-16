import os
import glob
import re
import sys
import warnings
import asyncio
import hashlib
import random
from dotenv import load_dotenv
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.errors import RequestsError
from asynciolimiter import Limiter
from bs4 import BeautifulSoup
import pandas as pd

sys.path.append(
    re.search(
        f'.*{re.escape("market_data_platform")}',
        __file__,
    ).group()
)

warnings.filterwarnings('ignore')

from scripts.utils.logger \
    import Logger                                  # noqa: E402
from scripts.utils.minio_pd \
    import MinioUtils                              # noqa: E402
from scripts.utils.retrieve_proxies \
    import generate_proxy_html                     # noqa: E402
from scripts.utils.amz_captcha_solver \
    import async_solve_captcha_cffi                # noqa: E402
from scripts.utils.retrieve_cookies \
    import _executor as gen_cookies_by_zipcode     # noqa: E402


class AsinInfoScraper:
    def __init__(
        self,
        input_li: list,
        rundate_path: str,
        info_type: str,
        info_validate: dict = {
            'name': '',
            'attrs': '',
        },
        limit_rate: float = 8/1,
        export_details: bool = False,
        export_details_size: int = 64,
        local_storage: bool = False,
        zipcode: str = '10001',
        country: str = 'USA',
    ) -> None:
        load_dotenv(
            re.search(
                f'.*{re.escape("market_data_platform")}',
                __file__,
            ).group() + '/.env'
        )

        self.limit_rate = limit_rate
        self.limiter = Limiter(self.limit_rate)
        self.rundate_path = rundate_path
        self.input_li = input_li
        self.succeed_code = [200]
        self.logging = Logger()
        self.to_retries_request = []
        self.req_made = 0
        self.resp_received = 0
        self.num_error_asin = 0
        self.batch_data_process = []
        self.info_type = info_type
        self.info_validate = info_validate
        self.dev_dir = os.path.dirname(__file__) + f'/data/{self.info_type}'
        self.prod_dir = f'bronze/amazon/{self.info_type}'
        self.export_details = export_details
        self.export_details_size = export_details_size
        self.details_data = list()
        self.minio_u = MinioUtils(
            endpoint=os.getenv(
                'MINIO_HOST',
            ),
            access_key=os.getenv(
                'MINIO_ACCESS_KEY',
            ),
            secret=os.getenv(
                'MINIO_SECRET_KEY',
            ),
        )
        self.local_storage = local_storage
        self.zipcode = zipcode
        self.country = country
        self.browser = [
            'chrome110', 'chrome116', 'chrome119', 'chrome120',
            'chrome123', 'chrome124', 'safari17_0', 'edge101',
        ]

    def get_asins_already(self) -> list:
        if self.local_storage:
            file_dir = (
                f'{self.dev_dir}/raw/{self.rundate_path}'
                f'/**/*'
            )
            return [
                i.split(
                    '/'
                )[-1].split(
                    '.'
                )[0] for i in glob.glob(
                    file_dir,
                    recursive=True,
                )
            ]
        else:
            return self.minio_u.list_all_objects(
                file_path=f'{self.prod_dir}/raw/{self.rundate_path}',
                only_filename=True,
            )

    def export_asin_df(
        self,
        data: list,
    ) -> None:
        if len(data) == 0:
            return

        df = pd.DataFrame(data)
        asins = '_'.join(
            df['asin'].to_list()
        )
        file_name = hashlib.sha256(
            asins.encode()
        ).hexdigest()

        if len(data) > 0:
            if self.local_storage:
                filedir = (
                    f'{self.dev_dir}/conformed/{self.rundate_path}'
                    f'/{file_name}.parquet'
                )
                os.makedirs(
                    os.path.dirname(filedir),
                    exist_ok=True,
                )
                df.to_parquet(
                    filedir,
                    index=False,
                )
            else:
                self.minio_u.load_data(
                    data=df,
                    file_path=f'{self.prod_dir}/conformed/'
                              f'{self.rundate_path}',
                    file_name=file_name,
                )
        del df

    async def fetch(
        self,
        request_params: dict,
        cookies: dict,
    ) -> None:
        asin = request_params.get(
            'url'
        ).split(
            '/dp/'
        )[-1].replace(
            '?th=1',
            '',
        )

        await self.limiter.wait()

        if (
            self.req_made % 64 == 0
        ) & (
            self.req_made > 0
        ):
            self.logging.info(
                f'Total requests made: {self.req_made}'
            )
        if (
            self.resp_received % 64 == 0
        ) & (
            self.resp_received > 0
        ):
            self.logging.info(
                f'Total response received: {self.resp_received}'
            )

        self.req_made += 1
        while True:
            try:
                proxy = generate_proxy_html()
                random_browser = random.choice(self.browser)
                async with AsyncSession(
                    cookies=cookies,
                    proxy=proxy,
                    impersonate=random_browser,
                ) as client:
                    try:
                        resp = await client.get(
                            url=request_params.get('url'),
                            params=request_params.get('payload'),
                            timeout=16,
                        )
                    except RequestsError as e:
                        self.logging.warning(
                            str(e).split('.')[0]
                        )
                        self.to_retries_request.append(
                            request_params
                        )
                        return
                    resp_text = resp.text
                    soup = BeautifulSoup(
                        resp_text,
                        'html.parser',
                    )
                    # Check whether facing captcha
                    if 'captcha' in resp_text:
                        # self.logging.warning(
                        #     f'Asin {asin} face captcha'
                        # )
                        # Solve captcha
                        resp = await async_solve_captcha_cffi(
                            session=client,
                            soup=soup,
                        )
                        resp_text = resp.text
                        soup = BeautifulSoup(
                            resp_text,
                            'html.parser',
                        )
                        # Check whether still facing captcha
                        if 'captcha' in resp_text:
                            self.logging.warning(
                                f'Asin {asin} still face captcha'
                            )
                            # Skip for retrying later
                            self.to_retries_request.append(
                                request_params
                            )
                            return
                        else:
                            self.logging.info(
                                f'Solved captcha for asin {asin}'
                            )
                break
            except Exception as e:
                self.logging.error(
                    f'Asin {[asin]} has problem as: {e} with {random_browser}'
                )
                # self.logging.warning(
                #     f'Proxy {proxy} is not valid',
                # )
                await asyncio.sleep(0.25)

        self.resp_received += 1

        # Validate scraping page

        # Asin not found
        if (
            "Sorry! We couldn't find that page. "
            "Try searching or go to Amazon's home page."
        ) in resp_text:
            self.logging.info(
                f'Asin {asin} not found'
            )
            return
        # Something went wrong
        if (
            "Sorry! Something went wrong!"
        ) in resp_text:
            self.logging.info(
                f'Asin {asin} facing st went wrong'
            )
            self.to_retries_request.append(
                request_params
            )
            return
        # 503 error
        if (
            "503 - Service Unavailable Error"
        ) in resp_text:
            self.logging.info(
                f'Asin {asin} facing 503 error'
            )
            self.to_retries_request.append(
                request_params
            )
            return
        # Sign in
        if (
            "Amazon Sign-In"
        ) in resp_text:
            self.logging.info(
                f'Asin {asin} facing sign in'
            )
            self.to_retries_request.append(
                request_params
            )
            return
        # Navigated page
        if (
            "Amazon Clinic is now Amazon One Medical"
            in resp_text
        ) or (
            '© 1996-2024, Amazon.com' not in resp_text
        ):
            self.logging.info(
                f'Asin {asin} facing navigated page'
            )
            self.to_retries_request.append(
                request_params
            )
            return
        # Zipcode is wrong
        if self.country == 'USA':
            try:
                current_location = soup.find(
                    name='span',
                    attrs={
                        "id": "glow-ingress-line2",
                    },
                ).text.strip()
                if self.zipcode not in current_location:
                    self.logging.error(
                        f"Zipcode location is changed. "
                        f"Current location: {current_location}"
                    )
                    self.to_retries_request.append(
                        request_params
                    )
                    return
            except Exception as e:
                self.logging.warning(
                    f'There is a problem occurs with '
                    f'zipcode or cookies of asin {asin}'
                )
                self.logging.error(e)
                self.to_retries_request.append(
                    request_params
                )
                return
        else:
            # TODO: temporary ignored country != USA
            pass
        # Redirected asin
        current_selected_asin = soup.find(
            name='li',
            attrs={
                "data-csa-c-item-id": asin,
            },
        )
        if current_selected_asin:
            current_selected_asin = current_selected_asin.find(
                name='span',
                attrs={
                    "class": "a-button a-button-selected "
                    "a-button-thumbnail a-button-toggle",
                },
            )
            if not current_selected_asin:
                self.logging.info(
                    f'Asin {asin} is redirected'
                )
                self.to_retries_request.append(
                    request_params
                )
                return

        # Validate needed info
        has_info = soup.find(
            self.info_validate.get('name'),
            self.info_validate.get('attrs'),
        )
        if not has_info:
            # self.logging.warning(
            #     f'Asin {asin} has a problem with {self.info_type}',
            # )
            self.num_error_asin += 1
            cate_path = '/invalid'
        else:
            cate_path = ''

        # Export data
        if self.local_storage:
            filename = (
                f'{self.dev_dir}/raw/{self.rundate_path}'
                f'{cate_path}/{asin}.html'
            )
            os.makedirs(
                os.path.dirname(filename),
                exist_ok=True,
            )
            with open(
                filename,
                'w',
            ) as f:
                f.write(resp_text)
        else:
            self.minio_u.load_data_html(
                data=resp_text,
                file_path=f'{self.prod_dir}/raw/'
                          f'{self.rundate_path}{cate_path}',
                file_name=asin,
            )

        # Export details data
        if self.export_details:
            self.details_data.append(
                {
                    'asin': asin,
                    self.info_type: str(has_info),
                }
            )
            if len(self.details_data) % self.export_details_size == 0:
                self.export_asin_df(
                    self.details_data
                )
                del self.details_data
                self.details_data = []

    async def fetchall(self) -> None:
        cookies = await gen_cookies_by_zipcode(
            zipcode=self.zipcode,
            country=self.country,
        )
        self.logging.info(
            f'Total requests ahead: {len(self.input_li)}'
        )

        await asyncio.gather(
            *(
                self.fetch(
                    params,
                    cookies,
                ) for params in self.input_li
            )
        )

        self.logging.info(
            f'Total requests to retries {len(self.to_retries_request)}'
        )

    async def fetch_retries(self) -> None:
        retries_same_ip_max = 3
        retries_same_ip_count = 0
        len_retries_previous = 0
        while len(self.to_retries_request) > 0:
            if retries_same_ip_count >= retries_same_ip_max:
                self.logging.info(
                    f'Total {len(self.to_retries_request)} '
                    f'must be extracted with browser'
                )
                break
            if len(self.to_retries_request) == len_retries_previous:
                retries_same_ip_count += 1
            len_retries_previous = len(self.to_retries_request)

            del self.input_li
            self.input_li = self.to_retries_request.copy()
            del self.to_retries_request
            self.to_retries_request = []
            self.logging.info(
                f'Total requests ahead: {len(self.input_li)}'
            )
            await self.fetchall()

    async def fetch_main(self) -> None:
        await self.fetchall()
        await self.fetch_retries()
        if self.export_details:
            self.export_asin_df(
                self.details_data
            )
            del self.details_data
        self.logging.info(
            f'Total asins error: {self.num_error_asin}'
        )

    def main(self) -> None:
        return asyncio.run(
            self.fetch_main()
        )
