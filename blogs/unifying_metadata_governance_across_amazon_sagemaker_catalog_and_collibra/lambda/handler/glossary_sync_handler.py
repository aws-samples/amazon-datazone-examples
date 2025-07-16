from aws_lambda_powertools import Logger

from business.GlossarySyncBusinessLogic import GlossarySyncBusinessLogic

logger = Logger(service="collibra_smus_sync_handler")


def handle_request(event, context):
    """
    This lambda handler syncs glossary terms from Collibra to SMUS.
    It runs in a paginated fashion, where the next invocation starts
    polling for the glossary terms from where the previous invocation left off.

    This lambda is triggered by the business metadata sync step function workflow

    :event: {"last_seen_glossary_term_id": <id of the last seen glossary term in collibra>}
    :return: {"last_seen_glossary_term_id": <id of the last seen glossary term in collibra>}
    """
    logger.info(f"Initiating glossary sync with event {event}")
    last_seen_id = GlossarySyncBusinessLogic(logger).sync(event.get("last_seen_glossary_term_id", None))
    event["last_seen_glossary_term_id"] = last_seen_id
    return event
