import logging
from rich.logging import RichHandler

DEFAULT_FORMAT = (
    '%(asctime)s - %(threadName)s:%(funcName)s:'
    '%(lineno)s - %(levelname)s: %(message)s'
)


class Logger(logging.Logger):
    def __init__(
        self,
        name: str = __name__,
        path: str = None,
        format: str = DEFAULT_FORMAT,
        level: int = 10,
    ) -> None:
        super().__init__(
            name,
            level,
        )

        formatter = logging.Formatter(format)

        # create logger
        self.setLevel(logging.DEBUG)

        # create handler and add to logger
        if path:
            file_handler = logging.FileHandler(path)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            self.addHandler(file_handler)
        rich_handler = RichHandler(
            rich_tracebacks=True,
            show_time=False,
        )
        rich_handler.setFormatter(formatter)
        self.addHandler(rich_handler)
