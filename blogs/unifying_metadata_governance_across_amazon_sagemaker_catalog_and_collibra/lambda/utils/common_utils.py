from time import time, sleep

from utils.env_utils import SMUS_DOMAIN_ID


def get_collibra_synced_glossary_name():
    return f'CollibraSyncedGlossary-{SMUS_DOMAIN_ID}'


def wait_until(sleep_interval: int, max_time_to_wait: int, logger, wait_message: str, method_to_call, *method_args):
    """
    Waits up to `n` seconds, checking every `t` seconds whether `m()` returns True.

    :param sleep_interval: Time interval (in seconds) between checks
    :param max_time_to_wait: Maximum timeout duration (in seconds)
    :param method_to_call: A callable that returns a boolean
    :param method_args: Arguments to be passed to the callable
    :raises TimeoutError: If `m()` doesn't return True within `n` seconds
    """
    start_time = time()

    while True:
        if method_to_call is not None and method_to_call(*method_args):
            return
        if time() - start_time > max_time_to_wait:
            if method_to_call is None:
                return
            raise TimeoutError(f"Condition not met within {max_time_to_wait} seconds")
        if wait_message:
            logger.info(wait_message)
        sleep(sleep_interval)


def extract_collibra_descriptions(asset: dict) -> list[str]:
    descriptions = []
    if 'stringAttributes' not in asset or not asset['stringAttributes']:
        return descriptions

    for attributes in asset['stringAttributes']:
        descriptions.append(attributes['stringValue'])

    return descriptions
