import re
import sys
import warnings
import asyncio
from playwright.async_api import async_playwright
from playwright.async_api import expect as async_expect

sys.path.append(
    re.search(
        f'.*{re.escape("market_data_platform")}',
        __file__,
    ).group()
)

warnings.filterwarnings('ignore')

from scripts.utils.amz_captcha_solver \
    import solve_captcha_playw      # noqa: E402


COUNTRIES_INFO = {
    "USA": {
        "marketPlace": "ATVPDKIKX0DER",
        "countryCode": "US",
        "suffix": ".com",
    },
    "CAN": {
        "marketPlace": "A2EUQ1WTGCTBG2",
        "countryCode": "CA",
        "suffix": ".ca",
    },
    "MEX": {
        "marketPlace": "A1AM78C64UM0Y8",
        "countryCode": "MX",
        "suffix": ".com.mx",
    },
    "JPN": {
        "marketPlace": "A1VC38T7YXB528",
        "countryCode": "JP",
        "suffix": ".co.jp",
    },
    "ARE": {
        "marketPlace": "A2VIGQ35RCS4UG",
        "countryCode": "AE",
        "suffix": ".me",
    },
    "ITA": {
        "marketPlace": "APJ6JRA9NG5V4",
        "countryCode": "IT",
        "suffix": ".it",
    },
    "DEU": {
        "marketPlace": "A1PA6795UKMFR9",
        "countryCode": "DE",
        "suffix": ".de",
    },
    "FRA": {
        "marketPlace": "A13V1IB3VIYZZH",
        "countryCode": "FR",
        "suffix": ".fr",
    },
    "ESP": {
        "marketPlace": "A1RKKUPIHCS9HS",
        "countryCode": "ES",
        "suffix": ".es",
    },
    "NLD": {
        "marketPlace": "A1805IZSGTT6HS",
        "countryCode": "NL",
        "suffix": ".nl",
    },
    "SWE": {
        "marketPlace": "A2NODRKZP88ZB9",
        "countryCode": "SE",
        "suffix": ".se",
    },
    "POL": {
        "marketPlace": "A1C3SOZRARQ6R3",
        "countryCode": "PL",
        "suffix": ".pl",
    },
    "GBR": {
        "marketPlace": "A1F83G8C2ARO7P",
        "countryCode": "UK",
        "suffix": ".co.uk",
    },
    "SGP": {
        "marketPlace": "A19VAU5U5O7RUS",
        "countryCode": "SG",
        "suffix": ".com.sg",
    },
    "AUS": {
        "marketPlace": "A39IBJ37TRP1C6",
        "countryCode": "AU",
        "suffix": ".com.au",
    },
}


async def _executor(
    zipcode: str,
    country: str,
    headless: bool = True,
) -> dict:
    suffix = COUNTRIES_INFO.get(
        country
    ).get(
        'suffix',
    )
    async with async_playwright() as a:
        browser = await a.chromium.launch(
            headless=headless,
        )
        context = await browser.new_context(
            viewport={
                'width': 1920,
                'height': 1080,
            },
        )

        page = await context.new_page()

        base_url = f'https://www.amazon{suffix}'
        await page.goto(
            f'https://www.amazon{suffix}/gp/flex/sign-out.html?'
            f'path=%2Fgp%2Fyourstore%2Fhome&useRedirectOnSuccess='
            '1&signIn=1&action=sign-out&ref_=nav_signout',
            wait_until='domcontentloaded',
        )
        await page.goto(
            base_url,
            wait_until='domcontentloaded',
        )

        await solve_captcha_playw(page)
        await page.wait_for_timeout(1000)

        count = 0
        while count <= 3:
            if '/ref=cs_503_logo' in await page.content():
                print('Facing 503 error, retry after 3 seconds')
                await page.wait_for_timeout(3000)
                await page.goto(
                    f'{base_url}/ref=cs_503_logo',
                    wait_until='domcontentloaded',
                )
                await page.wait_for_timeout(1000)
                count += 1
            else:
                break
        if count == 3:
            raise Exception(
                'Cannot get cookies this time due to 503 error'
            )

        # Click change zipcode
        count = 0
        while count <= 3:
            zipcode_loc = page.locator(
                '#glow-ingress-line2'
            )
            try:
                await async_expect(
                    zipcode_loc
                ).to_be_visible()
                await zipcode_loc.click()
                await page.wait_for_timeout(1000)
                break
            except AssertionError:
                print('Cannot locate zipcode element, retry after 3 seconds')
                await page.wait_for_timeout(3000)
                await page.reload(
                    wait_until='domcontentloaded',
                )
                await solve_captcha_playw(page)
                count += 1
        if count == 3:
            raise Exception(
                'Cannot get cookies this time due to not found zipcode locator'
            )

        # Fill zipcode and apply
        await page.locator(
            'input[data-action="GLUXPostalInputAction"]'
        ).fill(zipcode)
        await page.wait_for_timeout(1000)
        await page.locator(
            '#GLUXZipInputSection div'
        ).filter(has_text="Apply").click()
        await page.wait_for_timeout(1000)
        await page.get_by_role(
            'button',
            name='Continue',
        ).click()
        all_done = page.locator(
            'button[name="glowDoneButton"]'
        )
        if await all_done.count() > 0:
            await page.wait_for_timeout(1000)
            await all_done.click()

        await page.wait_for_timeout(1000)
        await page.reload(
            wait_until='domcontentloaded',
        )
        await solve_captcha_playw(page)

        await async_expect(
            page.locator(
                'a[id="nav-hamburger-menu"]'
            )
        ).to_be_visible()
        print(f'Changed zipcode to {zipcode}')

        cookies = {}
        for i in await context.cookies():
            cookies[i['name']] = i['value']

        await page.close()
        await context.close()
        await browser.close()

    return cookies


def main(
    zipcode: str,
    country: str,
    headless: bool = True,
) -> dict:
    return asyncio.run(
        _executor(
            zipcode,
            country,
            headless,
        )
    )


if __name__ == '__main__':
    cookies = main(
        '10001',
        'USA',
        True,
    )
    print(cookies)
