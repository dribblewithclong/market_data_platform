import requests
from curl_cffi import requests as requests_cffi
from bs4 import BeautifulSoup
from amazoncaptcha import AmazonCaptcha
from playwright.async_api import Page as AsyncPage


def solve_captcha(
    session: requests.Session,
    headers: dict,
    soup: BeautifulSoup,
) -> requests.Response:
    captcha_url = soup.find('img')['src']
    solution = AmazonCaptcha.fromlink(captcha_url).solve()
    amzn = soup.find(
        'input',
        attrs={"name": "amzn"},
    )['value']
    amzn_r = soup.find(
        'input',
        attrs={"name": "amzn-r"},
    )['value']
    params = {
        'amzn': amzn,
        'amzn-r': amzn_r,
        'field-keywords': solution.lower(),
    }

    resp = session.get(
        'https://www.amazon.com/errors/validateCaptcha',
        params=params,
        headers=headers,
    )

    return resp


def solve_captcha_cffi(
    session: requests_cffi.Session,
    headers: dict,
    soup: BeautifulSoup,
) -> requests_cffi.Response:
    captcha_url = soup.find('img')['src']
    solution = AmazonCaptcha.fromlink(captcha_url).solve()
    amzn = soup.find(
        'input',
        attrs={"name": "amzn"},
    )['value']
    amzn_r = soup.find(
        'input',
        attrs={"name": "amzn-r"},
    )['value']
    params = {
        'amzn': amzn,
        'amzn-r': amzn_r,
        'field-keywords': solution.lower(),
    }

    resp = session.get(
        url='https://www.amazon.com/errors/validateCaptcha',
        params=params,
        headers=headers,
    )

    return resp


async def async_solve_captcha_cffi(
    session: requests_cffi.AsyncSession,
    soup: BeautifulSoup,
) -> requests_cffi.Response:
    captcha_url = soup.find('img')['src']
    solution = AmazonCaptcha.fromlink(captcha_url).solve()
    amzn = soup.find(
        'input',
        attrs={"name": "amzn"},
    )['value']
    amzn_r = soup.find(
        'input',
        attrs={"name": "amzn-r"},
    )['value']
    params = {
        'amzn': amzn,
        'amzn-r': amzn_r,
        'field-keywords': solution.lower(),
    }

    resp = await session.get(
        url='https://www.amazon.com/errors/validateCaptcha',
        params=params,
    )

    return resp


async def solve_captcha_playw(
    page: AsyncPage,
) -> None:
    captcha_character = page.locator(
        '#captchacharacters'
    )
    while await captcha_character.count() > 0:
        captcha_url = await page.locator(
            'div[class="a-row a-text-center"]'
        ).locator(
            'img'
        ).get_attribute(
            'src'
        )
        solution = AmazonCaptcha.fromlink(
            captcha_url,
        ).solve()
        await page.locator(
            '#captchacharacters'
        ).fill(solution)
        await page.wait_for_timeout(1000)
        await page.locator(
            'button[type="submit"]'
        ).click()
        await page.wait_for_load_state(
            'domcontentloaded'
        )
        captcha_character = page.locator(
            '#captchacharacters'
        )

    print(f'Captcha {solution} solved')
