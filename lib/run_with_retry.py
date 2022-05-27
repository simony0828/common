from typing import Callable, Any
import time


def run_with_retry(fn: Callable[[], Any],
                   retry_number: int,
                   exceptions: Exception = None,
                   delay: int = 30,
                   validate: Callable[[Exception], bool] = None):
    """Simple function to retry method calls when specific exception has occurred.
    :param retry_number: Number of retries
    :param fn: Any callable
    :param exceptions: Exception or exceptions (tuple) to be catched and retried
    :param delay: Delay between calls (we use exponential backoff between calls)
    :param validate: Validation callable that will filter only the relevant exceptions
    """
    exc = None
    for current in range(retry_number + 1):
        try:
            return fn()
        except exceptions as e:
            exc = e
            if validate is not None and validate(e) is False:
                print(f"Cached: {e} but it didn't pass the validation")
                break
            else:
                print(f"Catched: {e}")

        print(f"Retry: Failed run {current}/{retry_number}")

        if current < retry_number:
            print(f"Retry: Sleeping for {delay} seconds...")
            time.sleep(delay)
            delay *= 2
    print(f"Retry: Exhausted number of retries. Quitting. {exc}")
    raise exc
