from aws_lambda_powertools import Logger

from business.project_user_listing_workflow.ProjectUserListingSyncBusinessLogic import \
    ProjectUserListingSyncBusinessLogic
from model.ProjectUserListingSyncWorkflowEvent import ProjectUserListingSyncWorkflowEvent

logger = Logger(service="project_sync")


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
    logger.info(f"Initiating project sync to Collibra with event {event}")
    project_user_listing_sync_workflow_event = ProjectUserListingSyncWorkflowEvent(event)
    output = ProjectUserListingSyncBusinessLogic(logger).sync(project_user_listing_sync_workflow_event)
    return output.__dict__()
