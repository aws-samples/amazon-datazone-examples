from aws_lambda_powertools import Logger

from business.ProjectUserSyncBusinessLogic import ProjectUserSyncBusinessLogic

logger = Logger(service="start_project_user_listing_sync_to_collibra")

def handle_request(event, context):
    """
    This lambda handler syncs the following from SMUS to Collibra:
    1. Projects
    2. Users in the synced projects

    Along with these, additional metadata is also added to the above entities like below:
    1. Projects - SMUS Project ID and published assets (listings)
    2. Users - SMUS Project ID which the User is part of


    This lambda is triggered through an Event Bridge schedule
    """
    logger.info(f"Initiating project & user sync to Collibra")
    ProjectUserSyncBusinessLogic(logger).sync()
    return event