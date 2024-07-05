import os
import sys
import warnings
import re
import math
import random
import time
import datetime
import threading
from itertools import cycle
from concurrent.futures \
    import ThreadPoolExecutor, as_completed

from pyvirtualdisplay import Display
import pytz
import yaml
from amazoncaptcha import AmazonCaptcha
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
import numpy as np
from deep_translator import GoogleTranslator
from selenium import webdriver
from selenium.webdriver.chrome.options \
    import Options
from selenium.webdriver.chrome.service \
    import Service as ChromeService
from webdriver_manager.chrome \
    import ChromeDriverManager
from selenium.webdriver.common.action_chains \
    import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

sys.path.append(
    re.search(
        f'.*{re.escape("market_data_platform")}',
        __file__,
    ).group()
)

warnings.filterwarnings('ignore')

from scripts.utils.logger import Logger     # noqa: E402
from scripts.utils.minio_u import MinioUtils        # noqa: E402


class AMZDriver(webdriver.Chrome):
    def __init__(
        self,
        country: str = 'USA',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.country = country
        self.current_dir = os.path.dirname(__file__)
        self.config_dir = self.current_dir.replace(
            'reviews',
            'config',
        )
        # load config file
        with open(
            f"{self.config_dir}/config.yaml"
        ) as file:
            self.cfg = yaml.safe_load(file)
        # go to the amazon captcha page
        self.base_url = self.cfg["amz_base_url"].get(
            self.country
        )
        self.get(
            f"{self.base_url}/errors/validateCaptcha?"
        )
        if not self.check_facing_catpcha():
            print('proxy not ok, skip')
            raise Exception('proxy not ok, skip')

        self.validate_captcha()
        self.get(
            f"{self.base_url}/product-reviews/B0093CMYSM?pageNumber=1"
        )
        time.sleep(2)
        self.get(
            f"{self.base_url}/product-reviews/B0093CMYSM?pageNumber=2"
        )
        time.sleep(2)
        self.get(
            f"{self.base_url}/product-reviews/B0093CMYSM?pageNumber=3"
        )

        if self.title == "Page Not Found":
            raise Exception('proxy face page not found, skip')

    def check_facing_catpcha(self) -> bool:
        try:
            self.find_element(
                By.XPATH, '//*[@id="captchacharacters"]'
            )
            print("FACING CAPTCHA")
            return True
        except Exception:
            print(
                f"NOT FACING CATPCHA, CURRENT TITLE IS {self.title}"
            )
            return False

    def validate_captcha(self) -> None:
        facing_catpcha = self.check_facing_catpcha()

        while facing_catpcha:
            captcha_box = self.find_element(
                By.XPATH,
                '//*[@id="captchacharacters"]'
            )
            captcha_url = self.find_element(
                By.TAG_NAME,
                "img"
            ).get_attribute("src")
            solution = AmazonCaptcha.fromlink(captcha_url).solve()
            print(f"CATPCHA SOLUTION: {solution}")
            captcha_box.send_keys(solution)
            self.find_element(
                By.XPATH,
                '//button[@type="submit"]',
            ).click()
            time.sleep(2)
            self.get(self.base_url)
            time.sleep(2)
            facing_catpcha = self.check_facing_catpcha()
            if self.title == "Sorry! Something went wrong!":
                raise Exception('proxy facing 503, skip')

    def clear_cache(self) -> None:
        self.get("chrome://settings/clearBrowserData")
        time.sleep(1)
        action = ActionChains(self)
        action.send_keys(
            Keys.TAB + Keys.ENTER
        )
        action.perform()
        time.sleep(2)


class AMZReview(object):
    def __init__(
        self,
        num_worker: int,
        rundate_path: str,
        country: str = 'USA',
    ) -> None:
        self.rundate_path = rundate_path
        self.bucket = 'raw'
        self.current_dir = os.path.dirname(__file__)
        self.config_dir = self.current_dir.replace(
            'reviews',
            'config',
        )
        # load config file
        with open(
            f"{self.config_dir}/config.yaml"
        ) as file:
            self.cfg = yaml.safe_load(file)
        self.run_time = datetime.datetime.now(
            pytz.timezone("Asia/Ho_Chi_Minh")
        ).replace(microsecond=0).replace(tzinfo=None)
        self.country = country
        self.base_url = self.cfg["amz_base_url"].get(
            self.country
        )
        self.title_503 = "Sorry! Something went wrong!"
        self.sign_in_title = "Amazon Sign-In"
        self.not_found_title = "Page Not Found"
        self.local_context = threading.local()
        # number worker for multithread
        self.num_worker = num_worker

        self.logging = Logger(
            name=__name__,
            path=f"{self.current_dir}/logs/"
                 f"{self.run_time.strftime('%b_%d_%Y')}.log"
        )
        self.time_sleep = cycle(
            range(10)
        )
        # path for data storage
        self.saving_path = (
            f"amz/review/{self.country}/{self.rundate_path}"
        )
        # minIO utils
        self.minio = MinioUtils(
            endpoint=self.cfg['minio'].get('host'),
            access_key=self.cfg['minio'].get('key'),
            secret=self.cfg['minio'].get('secret'),
        )

    def _check_asin_crawled(
        self,
        asin: str,
    ) -> bool:
        return self.minio.data_exist(
            file_path=self.saving_path,
            file_name=asin,
            bucket_name=self.bucket,
        )

    def _generate_driver(self) -> AMZDriver:
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_extension(
            self.current_dir
            + '/assets/rotated_proxy.zip'
        )
        driver = AMZDriver(
            country=self.country,
            options=chrome_options,
            service=ChromeService(
                ChromeDriverManager().install()
            ),
        )

        return driver

    def init_worker(self) -> None:
        # generate a unique value for the worker thread
        time.sleep(
            next(self.time_sleep)
        )
        self.logging.info(
            "Start generate driver"
        )
        self.local_context.driver = self._generate_driver()
        # store the unique worker key in a thread local variable
        self.logging.info(
            f"Initializing {self.local_context.driver} success"
        )

    def re_init_driver(
        self,
        driver: AMZDriver,
        url: str,
    ) -> None:
        self.logging.info(f"FACING {driver.title}")

        driver.clear_cache()
        driver.get(f"{self.base_url}/errors/validateCaptcha?")
        driver.validate_captcha()
        driver.get(url)
        time.sleep(
            random.choice(
                [1, 2, 3, 4]
            )
        )

        while (
            driver.title == self.title_503
            or driver.title == self.sign_in_title
            or driver.title == self.not_found_title
        ):
            self.logging.info(f"STILL FACING {driver.title}")
            driver.quit()

            self.logging.info(
                "Start generate driver with different proxy"
            )
            driver = self._generate_driver()
            driver.get(url)

        self.local_context.driver = driver
        self.logging.info("CHANGE PROXY SUCCESFULLY")

        time.sleep(
            random.choice(
                [1, 2, 3, 4]
            )
        )

    def quit(
        self,
        local: callable,
    ) -> None:
        time.sleep(1)
        driver = local.driver
        driver.quit()

    @staticmethod
    def get_num_review(
        page_source: str,
    ) -> int:
        num_review = None
        only_rating_count = SoupStrainer(
            attrs={"data-hook": "cr-filter-info-review-rating-count"}
        )
        soup = BeautifulSoup(
            page_source,
            "html.parser",
            parse_only=only_rating_count,
        )
        if soup.text != "":
            review_count_text = soup.text.strip()
            # num_review = int(
            #     review_count_text.split(
            #         ", "
            #     )[1].split()[0].replace(
            #         ",",
            #         "",
            #     )
            # )
            num_review = int(
                re.findall(
                    '[0-9,\\.]+',
                    review_count_text,
                )[-1].replace(
                    ',',
                    '',
                ).replace(
                    '.',
                    '',
                )
            )

        return num_review

    @staticmethod
    def get_num_page(
        page_source: str,
    ) -> int:
        total_page = None
        total_reviews = AMZReview.get_num_review(page_source)
        if total_reviews:
            total_page = math.ceil(total_reviews / 10)

        return total_page

    def get_redirect_asin(
        self,
        driver: AMZDriver,
        asin: str,
    ) -> str:
        url = f"{self.base_url}/product-reviews/{asin}/?pageNumber=1"
        if driver.current_url != url:
            asin_redirect_to = driver.current_url.split("/")[-1]
            self.logging.info(
                f"ASIN {asin} REDIRECT TO {asin_redirect_to}"
            )
            asin = asin_redirect_to

        return asin

    def get_variation(
        self,
        page_source: str,
    ) -> dict:
        variation_data = None

        variation_pattern = r'"dimensionToAsinMap" : ({.+})'
        variation = re.findall(
            variation_pattern,
            page_source,
        )
        parent_asin_pattern = r'"parentAsin" : "(.+)"'
        parent_asin = re.findall(
            parent_asin_pattern,
            page_source,
        )

        if variation and parent_asin:
            variation_data = eval(variation[0])

            variation_data.update(
                {
                    "parentAsin": parent_asin[0]
                }
            )

        return variation_data

    def process_first_page(
        self,
        asin: str,
    ) -> tuple:
        driver = self.local_context.driver
        url = f"{self.base_url}/product-reviews/{asin}/?pageNumber=1"
        driver.get(url)

        facing_captcha = driver.check_facing_catpcha()
        if (
            driver.title == self.title_503
            or driver.title == self.sign_in_title
            or facing_captcha
        ):
            self.re_init_driver(
                driver,
                url,
            )
            driver = self.local_context.driver
            driver.get(url)

        num_page = self.get_num_page(driver.page_source)
        redirect_asin = self.get_redirect_asin(driver, asin)

        driver.get(f"{self.base_url}/dp/{asin}?th=1")
        variations = self.get_variation(driver.page_source)

        return redirect_asin, num_page, variations

    def process_response_data(
        self,
        soup: BeautifulSoup,
    ):
        """Process raw data get from each review page

        Returns:
            Dict: A dictionary contain all data
                need to get from current review page.
        """

        profile_name = []
        profile_url = []
        verified = []
        variation_asin = []
        variation_text = []
        ratings = []
        review_titles = []
        review_bodys = []
        helpful_votes = []
        img_url = []
        review_locations = []
        review_dates = []

        reviews = soup.findAll(
            attrs={"data-hook": "review"}
        )

        translator = GoogleTranslator(
            source='auto',
            target='en',
        )

        # pattern for regex
        DATE_PATTERN = r"\b[A-Z][a-z]+ \d{1,2}, \d{4}\b"
        LOCATION_PATTERN = (
            r"Reviewed in (?:the )?([A-Z][a-z]+(?: [A-Z][a-z]+)*)"
        )
        VARIATION_ASIN_PATTERN = r"/product-reviews/(\w+)"

        for review in reviews:
            profile_name.append(
                review.find(class_="a-profile-name").text
            )
            try:
                profile_url.append(
                    self.base_url + review.find(
                        class_="a-profile"
                    ).get("href")
                )
            except Exception:
                # Some users can't not access profile
                profile_url.append(np.nan)

            try:
                # Verified purchase
                verified.append(
                    review.find(
                        attrs={"data-hook": "avp-badge"}
                    ).text
                )
            except Exception:
                verified.append(np.nan)

            try:
                variation_link = review.find(
                    attrs={"data-hook": "format-strip"}
                ).get(
                    "href"
                )
                variation_asin.append(
                    re.findall(
                        VARIATION_ASIN_PATTERN, variation_link
                    )[0]
                )
            except Exception:
                # Some asin don't have variations
                variation_asin.append(np.nan)

            try:
                variation_text.append(
                    review.find(
                        attrs={"data-hook": "format-strip"}
                    ).text
                )
            except Exception:
                variation_text.append(np.nan)

            try:
                ratings.append(
                    review.find(
                        attrs={"data-hook": "review-star-rating"}
                    ).text
                )
            except Exception:
                ratings.append(
                    review.find(
                        attrs={"data-hook": "cmps-review-star-rating"}
                    ).text
                )

            review_titles.append(
                review.find(
                    attrs={"data-hook": "review-title"}
                )
                .find_all("span")[-1]
                .text.strip()
            )

            if (
                "media could not be loaded"
                not in review.find(
                    attrs={"data-hook": "review-body"}
                ).text
            ):
                review_bodys.append(
                    review.find(
                        attrs={"data-hook": "review-body"}
                    ).text.strip()
                )
            else:
                review_bodys.append(
                    review.find(
                        attrs={"data-hook": "review-body"}
                    )
                    .text.strip()
                    .split("\n")[-1]
                )

            try:
                helpful_votes.append(
                    review.find(
                        attrs={"data-hook": "helpful-vote-statement"}
                    ).text
                )
            except Exception:
                helpful_votes.append(np.nan)

            try:
                all_images_tag = review.find(
                    class_="review-image-tile-section"
                ).findAll("img")
                img_url.append(
                    [
                        image_tag.get("src")
                        for image_tag in all_images_tag
                    ]
                )
            except Exception:
                img_url.append(np.nan)

            review_location_date = review.find(
                attrs={"data-hook": "review-date"}
            ).text.strip()
            if self.country != "USA":
                try:
                    review_location_date = translator.translate(
                        review_location_date
                    )
                except Exception:
                    self.logging.warning(
                        f'CANNOT TRANSLATE: {review_location_date}'
                    )
            try:
                review_locations.append(
                    re.findall(
                        LOCATION_PATTERN,
                        review_location_date,
                    )[0]
                )
            except Exception:
                self.logging.warning(
                    f'CANNOT EXTRACT LOCATION: {review_location_date}'
                )
                review_locations.append(
                    review_location_date
                )
            try:
                review_dates.append(
                    re.findall(
                        DATE_PATTERN,
                        review_location_date,
                    )[0]
                )
            except Exception:
                self.logging.warning(
                    f'CANNOT EXTRACT DATE: {review_location_date}'
                )
                review_dates.append(
                    review_location_date
                )

        result = {
            "PROFILE_NAME": profile_name,
            "PROFILE_URL": profile_url,
            "VERIFIED_PURCHASE": verified,
            "VARIATION_ASIN": variation_asin,
            "VARIATION_TEXT": variation_text,
            "RATING_STARS": ratings,
            "REVIEW_TITLE": review_titles,
            "REVIEW_BODY": review_bodys,
            "HELPFUL_VOTE": helpful_votes,
            "IMAGES_URL": img_url,
            "LOCATION": review_locations,
            "DATETIME": review_dates,
        }

        return result

    def process_filter_by_star(
        self,
        asin: str,
        only_current_asin: bool = False,
    ) -> list:
        STAR_FILTER = {
            1: "one_star",
            2: "two_star",
            3: "three_star",
            4: "four_star",
            5: "five_star",
        }

        driver = self.local_context.driver
        result = []
        for star in range(1, 6):
            num_page = 11
            for current_page in range(1, 11):
                self.logging.info(
                    f"PROCESSING ASIN {asin}, page {current_page}, star {star}"
                )
                if current_page > num_page:
                    self.logging.info(
                        f"current page is {current_page} "
                        f"larger than {num_page}, exit"
                    )
                    break
                if only_current_asin:
                    url = (
                        f"{self.base_url}/product-reviews/{asin}/"
                        f"?pageNumber={current_page}"
                        f"&filterByStar={STAR_FILTER[star]}"
                        f"&formatType=current_format"
                    )
                else:
                    url = (
                        f"{self.base_url}/product-reviews/"
                        f"{asin}/?pageNumber={current_page}"
                        f"&filterByStar={STAR_FILTER[star]}"
                    )
                driver.get(url)
                facing_captcha = driver.check_facing_catpcha()
                if (
                    driver.title == self.title_503
                ) or (
                    driver.title == self.sign_in_title
                ) or facing_captcha or (
                    driver.title == self.not_found_title
                ):
                    self.re_init_driver(driver, url)
                    driver = self.local_context.driver
                    driver.get(url)

                soup = BeautifulSoup(
                    driver.page_source,
                    "html.parser",
                )
                if current_page == 1:
                    num_page = self.get_num_page(driver.page_source)
                    if not num_page:
                        self.logging.warning("CANNOT GET NUM PAGE")
                        break
                    self.logging.info(
                        f"ASIN: {asin}, STAR: {star}, NUM PAGE, {num_page}"
                    )
                result.append(
                    self.process_response_data(soup)
                )

        return result

    def process_filter_by_variations(
        self,
        asin: str,
        variations: dict,
    ) -> list:
        driver = self.local_context.driver
        total_result = []
        for variation in variations.values():
            self.logging.info(
                f"PROCESSING VARIATION {variation}, ORIGINAL ASIN {asin}"
            )
            result = []
            driver.get(
                f"{self.base_url}/product-reviews/{variation}/"
                f"?pageNumber=1&formatType=current_format"
            )
            num_page = self.get_num_page(driver.page_source)
            if not num_page:
                pass
            elif num_page < 10:
                result = self.process_asin_below_limit(
                    variation,
                    num_page,
                    only_current_asin=True,
                )
            else:
                result = self.process_filter_by_star(
                    variation,
                    only_current_asin=True,
                )
            total_result.extend(result)

        return total_result

    def process_asin_below_limit(
        self,
        asin: str,
        num_page: int,
        only_current_asin: bool = False,
    ) -> list:
        driver = self.local_context.driver

        result = []
        for current_page in range(1, 11):
            self.logging.info(
                f"PROCESSING ASIN {asin}, page {current_page},"
            )
            if current_page > num_page:
                self.logging.info(
                    f"current page is {current_page} "
                    f"larger than {num_page}, exit"
                )
                break
            if only_current_asin:
                url = (
                    f"{self.base_url}/product-reviews/{asin}/"
                    f"?pageNumber={current_page}&formatType=current_format"
                )
            else:
                url = (
                    f"{self.base_url}/product-reviews/{asin}/"
                    f"?pageNumber={current_page}"
                )

            driver.get(url)
            self.logging.info(
                f"PROCESSING ASIN {asin}, page "
                f"{current_page}, title is {driver.title}"
            )
            facing_captcha = driver.check_facing_catpcha()

            if (
                driver.title == self.title_503
            ) or (
                driver.title == self.sign_in_title
            ) or facing_captcha or (
                driver.title == self.not_found_title
            ):
                self.re_init_driver(driver, url)
                driver = self.local_context.driver
                driver.get(url)

            soup = BeautifulSoup(
                driver.page_source,
                "html.parser",
            )
            result.append(
                self.process_response_data(soup)
            )

        return result

    def process_asin_above_limit(
        self,
        asin: str,
        variations: dict,
    ) -> list:
        if variations:
            self.logging.info(
                f"ASIN: {asin} HAVE {len(variations)} VARIATIONS"
            )
        if variations is None or len(variations) < 5:
            self.logging.info(
                f"PROCESS ASIN: {asin} BY STAR"
            )
            result = self.process_filter_by_star(asin)
        elif len(variations) >= 5 and len(variations) <= 15:
            self.logging.info(
                f'PROCESS ASIN: {asin} BY VARIATION. VARIATION: {variations}'
            )
            result = self.process_filter_by_variations(
                asin,
                variations,
            )
        else:
            self.logging.info(
                f'PROCESS ASIN: {asin} BY STAR'
            )
            result = self.process_filter_by_star(asin)

        return result

    def task(
        self,
        asin: str,
        path: str,
        idx: int,
    ) -> None:
        # access the unique key for the worker thread
        self.logging.info(
            f"Worker start {asin}, idx {idx}"
        )
        first_page_info = self.process_first_page(asin)
        asin_redirect_to = first_page_info[0]
        num_page = first_page_info[1]
        variations = first_page_info[2]

        if not num_page:
            df = pd.DataFrame(
                {
                    "PROFILE_NAME": [],
                    "PROFILE_URL": [],
                    "VERIFIED_PURCHASE": [],
                    "VARIATION_ASIN": [],
                    "VARIATION_TEXT": [],
                    "RATING_STARS": [],
                    "REVIEW_TITLE": [],
                    "REVIEW_BODY": [],
                    "HELPFUL_VOTE": [],
                    "IMAGES_URL": [],
                    "LOCATION": [],
                    "DATETIME": [],
                }
            )
            self.minio.load_data(
                data=df,
                file_path=path,
                file_name=asin,
                bucket_name=self.bucket,
            )
            return

        if num_page < 10:
            result = self.process_asin_below_limit(
                asin_redirect_to,
                num_page,
            )
        else:
            result = self.process_asin_above_limit(
                asin_redirect_to,
                variations,
            )

        df = pd.DataFrame()
        for page in result:
            df = pd.concat(
                [df, pd.DataFrame(page)],
                ignore_index=True,
            )

        self.minio.load_data(
            data=df,
            file_path=path,
            file_name=asin,
            bucket_name=self.bucket,
        )

    def main(
        self,
        asin_li: list,
    ) -> None:
        start_time = time.time()

        # start a virtual display
        disp = Display()
        disp.start()

        with ThreadPoolExecutor(
            max_workers=self.num_worker,
            initializer=self.init_worker,
        ) as executor:
            mapping = {}
            futures = []

            for idx, asin in enumerate(asin_li):
                if self._check_asin_crawled(asin):
                    self.logging.info(
                        f"ASIN {asin} CRAWLED, SKIP"
                    )
                else:
                    future = executor.submit(
                        self.task,
                        asin,
                        self.saving_path,
                        idx,
                    )
                    mapping[future] = asin
                    futures.append(future)

            for future in as_completed(futures):
                if future.exception():
                    self.logging.exception(
                        f"EXCEPTION: {future.exception()}. "
                        f"ASIN {mapping[future]}"
                    )
                    try:
                        executor.submit(
                            self.task,
                            mapping[future],
                            self.saving_path,
                            1,
                        )
                    except Exception as e:
                        self.logging.exception("CANNOT RESEND TASK")
                        self.logging.exception(e)
                else:
                    pass

            [
                executor.submit(
                    self.quit,
                    self.local_context,
                )
                for _ in range(self.num_worker)
            ]

        disp.stop()
        end_time = time.time()
        self.total_time = round(end_time - start_time, 1)
        self.logging.info(
            f"Total run time: {self.total_time}"
        )


if __name__ == "__main__":
    job = AMZReview(
        num_worker=1,
        rundate_path='2024/07/04',
        country='USA',
    )

    job.main(
        ['B08ZN6FYWN']
    )
