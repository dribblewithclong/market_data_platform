import warnings
import functools
import time

warnings.filterwarnings("ignore")


class RetryContext:
    def __init__(self):
        self.in_retry = False


def retry_on_error(
    max_retries: int = 3,
    delay: int = 1,
):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            if retry_context.in_retry:
                return func(*args, **kwargs)

            attempts = 0
            while attempts < max_retries:
                try:
                    retry_context.in_retry = True
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f'Errors: {e}')
                    attempts += 1
                    print(f'Retrying in {delay} seconds...')
                    time.sleep(delay)
                finally:
                    retry_context.in_retry = False
            raise RuntimeError('Maximum retries reached')
        return wrapper_retry
    return decorator_retry


retry_context = RetryContext()
