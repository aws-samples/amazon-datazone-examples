from aws_lambda_powertools import Logger

from business.GlossaryTermHierarchyEstablisherBusinessLogic import GlossaryTermHierarchyEstablisherBusinessLogic

logger = Logger(service="glossary_term_hierarchy_establisher")

def handle_request(event, context):
    """
    This lambda handler replicates the hierarchy between the glossary terms from Collibra to SMUS

    This lambda is triggered by the business metadata sync step function workflow
    """
    logger.info(f"Initiating glossary term hierarchy establisher with event: {event}")
    GlossaryTermHierarchyEstablisherBusinessLogic(logger).establish()
    return event