import random
from collections import deque
import pandas as pd


class ProxiesPool(deque):
    def __init__(
        self,
        proxies_path: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.proxies_path = proxies_path
        self._generate_pool()

    @staticmethod
    def load_proxies(
        proxies_path: str,
    ) -> list:
        if not proxies_path:
            return None
        if isinstance(
            proxies_path,
            str
        ):
            proxies = pd.read_pickle(proxies_path)
            proxies_list = [
                f'{x["proxy_address"]}:{x["port"]}'
                for x in proxies
            ]
            random.shuffle(proxies_list)
            return proxies_list
        else:
            raise TypeError(
                f"Proxies path must be str. "
                f"Receive {type(proxies_path)}"
            )

    def _generate_pool(self):
        proxies = self.load_proxies(self.proxies_path)
        if proxies:
            for proxy in proxies:
                self.append(proxy)
