from aws_lambda_powertools import Logger

from business.SubscriptionSyncBusinessLogic import SubscriptionSyncBusinessLogic

logger = Logger(service="start_subscription_request_sync_to_smus")

def handle_request(event, context):
    """
    This lambda handler syncs the approved subscription requests from Collibra to SMUS.

    This lambda is triggered through an Event Bridge schedule
    """
    logger.info(f"Initiating subscription request sync to SMUS")
    SubscriptionSyncBusinessLogic(logger).start_subscription_request_sync_to_smus()
    return event