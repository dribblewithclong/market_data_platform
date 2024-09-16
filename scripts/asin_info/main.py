import re
import os
import sys
import time
import warnings
import datetime
import pytz
from dotenv import load_dotenv

sys.path.append(
    re.search(
        f'.*{re.escape("market_data_platform")}',
        __file__,
    ).group()
)

warnings.filterwarnings('ignore')

from scripts.utils.browser_headers \
    import HEADERS                          # noqa : E402
from scripts.utils.minio_pd \
    import MinioUtils                       # noqa: E402
from scripts.utils.telegram_alert \
    import send_message                     # noqa: E402
from scripts.utils.ggsheet \
    import GGSheetUtils                     # noqa: E402
from scripts.asin_info.scraper \
    import AsinInfoScraper                         # noqa: E402


class AsinInfo:
    def __init__(
        self,
        rundate_path: str,
    ) -> None:
        load_dotenv(
            re.search(
                f'.*{re.escape("market_data_platform")}',
                __file__,
            ).group() + '/.env'
        )

        self.rundate_path = rundate_path

    def get_asins(self) -> list:
        creds = MinioUtils(
            endpoint=os.getenv(
                'MINIO_HOST'
            ),
            access_key=os.getenv(
                'MINIO_ACCESS_KEY'
            ),
            secret=os.getenv(
                'MINIO_SECRET_KEY'
            )
        ).get_data_json(
            file_path='google_sheet',
            file_name='iykyk101',
            bucket_name='credentials',
        )
        ggsheet = GGSheetUtils(creds)

        df = ggsheet.get_data(
            sheet_id=os.getenv('ASIN_SHEET_ID'),
            sheet_name='us',
            range_from='A1',
            range_to='A',
            columns_first_row=True,
        )

        return df['asin'].to_list()

    def retrieve_params(self) -> list:
        asins = self.get_asins()
        asins_already = AsinInfoScraper(
            input_li=[],
            rundate_path=self.rundate_path,
            info_type='asin_info',
        ).get_asins_already()
        asins_to_req = [
            i for i in asins
            if i not in asins_already
        ]

        print(f'Total asin initial: {len(asins)}')
        print(f'Total asin to extract : {len(asins_to_req)}')

        params = []

        for asin in asins_to_req:
            params.append(
                {
                    'url': f'https://www.amazon.com/dp/{asin}?th=1',
                    'headers': HEADERS,
                }
            )

        return params

    def main(self) -> None:
        # Scrape data
        params = self.retrieve_params()

        ainfo = AsinInfoScraper(
            input_li=params,
            rundate_path=self.rundate_path,
            info_type='asin_info',
            info_validate={
                'name': 'div',
                'attrs': {
                    'id': 'productDetails_feature_div',
                },
            },
            limit_rate=8/1,
        )

        ainfo.main()


if __name__ == '__main__':
    start = time.time()

    rundate_path = datetime.datetime.now(
        pytz.timezone('Asia/Ho_Chi_Minh')
    ).strftime(
        '%Y/%m/%d'
    )

    j = AsinInfo(rundate_path)
    j.main()

    send_message(
        text='ASIN INFO IS EXTRACTED SUCCESS',
    )

    end = time.time()
    print(f'Total time {end - start}')
