from aws_lambda_powertools import Logger

from business.SubscriptionSyncBusinessLogic import SubscriptionSyncBusinessLogic

logger = Logger(service="start_subscription_sync_to_collibra")

def handle_request(event, context):
    """
    This lambda handler syncs the pending subscription request from SMUS to Collibra.

    This lambda is triggered through "Subscription Request Created" event received in the
    "default" event bus in the customer's account
    """
    logger.info(f"Initiating subscription sync to Collibra with event: {event}")
    SubscriptionSyncBusinessLogic(logger).sync_subscription_to_collibra(event["detail"]["data"])
    return event