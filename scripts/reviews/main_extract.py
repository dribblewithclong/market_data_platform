import os
import sys
import re
import warnings
import time
import datetime
import pytz
import yaml

sys.path.append(
    re.search(
        f'.*{re.escape("market_data_platform")}',
        __file__,
    ).group()
)

warnings.filterwarnings('ignore')

from scripts.reviews.crawler import AMZReview       # noqa: E402
from scripts.utils.ggsheet import GGSheetUtils      # noqa: E402


def gen_rundate_path() -> str:
    today = datetime.datetime.now(
        pytz.timezone('Asia/Ho_Chi_Minh')
    )

    return today.strftime('%Y/%m/%d')


class AMZReviewExtract:
    def __init__(
        self,
        country: str,
        rundate_path: str,
    ) -> None:
        self.country = country
        self.current_dir = os.path.dirname(__file__)
        self.rundate_path = rundate_path
        self.config_dir = self.current_dir.replace(
            'reviews',
            'config',
        )
        # load config file
        with open(
            f"{self.config_dir}/config.yaml"
        ) as file:
            self.cfg = yaml.safe_load(file)

        self.marketplace = {
            'US': 'USA',
            'UK': 'GBR',
            'IN': 'IND',
            'MX': 'MEX',
            'IT': 'ITA',
            'FR': 'FRA',
            'ES': 'ESP',
            'JP': 'JPN',
            'CA': 'CAN',
            'DE': 'DEU'
        }

    def get_asins(self) -> list:
        creds = self.cfg.get('ggsheet_creds')

        sheet_id = self.cfg.get('ggsheet').get('asin_sheet_id')

        if self.country == 'USA':
            sheet_name = 'us'
        else:
            sheet_name = 'int'

        ggsheet = GGSheetUtils(creds)

        asins_df = ggsheet.get_data(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
            range_from='A',
            range_to='B',
            columns_first_row=True,
        )

        asins_df['country'] = asins_df['country'].apply(
            lambda x: self.marketplace[x]
        )

        return asins_df[
            asins_df['country'] == self.country
        ]['asin'].to_list()

    def main(self) -> None:
        asins = self.get_asins()
        print(f'Total asins to crawl: {len(asins)}')
        print(f'on {self.rundate_path}')
        num_worker = min(
            len(asins),
            5,
        )

        crawler = AMZReview(
            num_worker,
            self.rundate_path,
            self.country,
        )
        crawler.main(asins)


if __name__ == '__main__':
    start = time.time()

    rundate_path = gen_rundate_path()
    job = AMZReviewExtract(
        'USA',
        rundate_path,
    )
    job.main()

    end = time.time()
    print(f'Total run time: {end-start}')
