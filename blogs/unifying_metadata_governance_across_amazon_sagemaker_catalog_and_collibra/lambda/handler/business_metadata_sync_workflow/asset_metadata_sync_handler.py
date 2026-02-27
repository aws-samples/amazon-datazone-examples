from aws_lambda_powertools import Logger

from business.business_metadata_sync_workflow.AssetMetadataSyncBusinessLogic import AssetMetadataSyncBusinessLogic

logger = Logger(service="asset_metadata_sync")


def handle_request(event, context):
    """
    This lambda handler syncs the following business metadata from Collibra to SMUS.
    1. Description of tables, and columns
    2. Columns marked as Personal Identifiable Information
    3. Associates table and columns with glossary terms

    It runs in a paginated fashion, where the next invocation starts
    polling for the assets (table) from where the previous invocation left off.

    This lambda is triggered by the business metadata sync step function workflow

    :event: {"last_seen_asset_id": <id of the last seen asset (table) in collibra>}
    :return: {"last_seen_asset_id": <id of the last seen asset (table) in collibra>}
    """
    logger.info(f"Initiating asset metadata sync with event: {event}")
    asset_metadata_sync_business_logic = AssetMetadataSyncBusinessLogic(logger)
    last_seen_id = asset_metadata_sync_business_logic.sync(event.get("last_seen_asset_id", None))
    event["last_seen_asset_id"] = last_seen_id
    return event
