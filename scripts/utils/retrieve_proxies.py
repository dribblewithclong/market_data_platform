import random
import os
import pandas as pd


def load_proxies(proxies_path: str) -> list:
    if not proxies_path:
        return None

    proxies = pd.read_pickle(proxies_path)
    proxies_list = [f'{x["proxy_address"]}:{x["port"]}' for x in proxies]
    random.shuffle(proxies_list)
    return proxies_list


def generate_proxy_html(
    return_dict: bool = False,
) -> str:
    current_dir = os.path.dirname(__file__)

    proxies = load_proxies(
        current_dir + '/assets/proxies'
    )

    proxy = random.choice(proxies)
    proxy_html = (
        f"http://team123p-rotate:rxfckevxig2x@"
        f"{proxy}"
    )
    # print(f'proxy used: {proxy}')

    if return_dict:
        return {
            "http": proxy_html,
        }

    return proxy_html


if __name__ == '__main__':
    proxy = generate_proxy_html()
    print(proxy)
