from datetime import timedelta, datetime
import time
from typing import Tuple

from adapter.CollibraAdapter import CollibraAdapter
from adapter.SMUSAdapter import SMUSAdapter
from business.CollibraSMUSListingMatcher import CollibraSMUSListingMatcher
from utils.collibra_constants import DISPLAY_NAME_KEY, ID_KEY, TYPE_KEY, NAME_KEY, \
    AWS_CONSUMER_PROJECT_ID_ATTRIBUTE_NAME, STRING_VALUE_KEY, AWS_PRODUCER_PROJECT_ID_ATTRIBUTE_NAME
from utils.env_utils import SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN, COLLIBRA_SUBSCRIPTION_REQUEST_REJECTED_STATUS_ID, \
    COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID
from utils.smus_constants import ASSET_LISTING_KEY, \
    LISTING_ID_KEY


class SubscriptionSyncBusinessLogic:
    __SUBSCRIPTION_REQUEST_AUTO_APPROVAL_WAIT_TIME_IN_MINUTES = 3

    def __init__(self, logger):
        self.__logger = logger
        self.__smus_adapter = SMUSAdapter(self.__logger)
        self.__collibra_adapter = CollibraAdapter(self.__logger)
        self.__projects_ids = {project['id'] for project in self.__smus_adapter.list_all_projects()}

    def sync_subscription_to_collibra(self, event: dict):
        self.__logger.info(f"Running validations on subscription request")
        consumer_project_id = event['subscribedPrincipals'][0]['id']

        requester_id = event['requesterId']

        if self.__is_subscription_request_created_by_smus_collibra_integration_admin_role(requester_id):
            self.__logger.info(f"Subscription request created by SMUS Admin Role, thus ignoring.")
            return

        if event['status'] != 'PENDING':
            self.__logger.warn(
                f"Subscription request status is {event['status']}. Expected PENDING")
            return

        if len(event['subscribedPrincipals']) != 1:
            self.__logger.warn(f"Expected only 1 subscribed principal.")
            return

        if consumer_project_id not in self.__projects_ids:
            self.__logger.warn(
                f"Subscriber must be in a project of which {SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN} is an owner. Either it is null or different.")
            return

        if len(event['subscribedListings']) != 1:
            self.__logger.warn(f"No or multiple subscribed listings found. Expected 1")
            return

        if event['subscribedListings'][0]['ownerProjectId'] not in self.__projects_ids:
            self.__logger.warn(
                f"Owner of the subscribed listing must be in a project of which {SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN} is an owner")
            return

        if 'assetListing' not in event['subscribedListings'][0]['item']:
            self.__logger.warn(f"Subscribed listing is not an asset")
            return

        try:
            consumer_project = self.__smus_adapter.get_project(consumer_project_id)
            asset_id = event['subscribedListings'][0]['item']['assetListing']['entityId']

            self.__logger.info(f"Retrieving asset from SMUS with id {asset_id}")
            asset = self.__smus_adapter.get_asset(asset_id)
            asset_name = asset.get('name')

            self.__logger.info(f"Found asset in SMUS with name {asset_name}")

            self.__logger.info(f"Retrieving asset with name {asset_name} from Collibra")
            collibra_asset = self.__collibra_adapter.get_table_by_name(asset_name)
            collibra_asset_id = collibra_asset.get('id')

            self.__logger.info(f"Found asset with name {asset_name} in Collibra with id {collibra_asset_id}")

            response = self.__collibra_adapter.start_subscription_request_creation_workflow(collibra_asset_id,
                                                                                            consumer_project[NAME_KEY])

            self.__logger.info(
                f"Successfully started subscription request workflow in Collibra with id {collibra_asset_id}. Response: {response}")
        except Exception as e:
            self.__logger.error("Failed to sync subscription request to Collibra", e)

    def start_subscription_request_sync_to_smus(self):
        self.__sync_approved_requests()

    def __sync_approved_requests(self):
        approved_requests = self.__collibra_adapter.get_subscription_requests_by_status("Approved")
        self.__logger.info(f"Found {len(approved_requests)} approved requests")

        if not approved_requests:
            return

        for approved_request in approved_requests:
            try:
                producer_project_id, consumer_project_id = self.__get_smus_project_ids(approved_request)

                if consumer_project_id is None or consumer_project_id not in self.__projects_ids:
                    self.__logger.warn(
                        f"Subscriber must be in a project of which {SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN} is an owner.")
                    continue

                if producer_project_id is None or producer_project_id not in self.__projects_ids:
                    self.__logger.warn(
                        f"Listing must be in a project of which {SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN} is an owner.")
                    continue

                collibra_asset = approved_request["outgoingRelations"][0]["target"]

                listing_id = self.__find_smus_table_listing_id(collibra_asset, producer_project_id)

                if not listing_id:
                    self.__logger.info(
                        f"No listing found in SMUS for collibra asset {collibra_asset[DISPLAY_NAME_KEY]}")
                    continue

                subscription_requests = self.__smus_adapter.search_subscription_requests(listing_id,
                                                                                         producer_project_id,
                                                                                         consumer_project_id)
                should_create_new_subscription_request = True
                if subscription_requests:
                    self.__logger.info(
                        f"Found {len(subscription_requests)} accepted subscription requests for listing {listing_id}")
                    subscription_request_id = subscription_requests[0][ID_KEY]

                    subscriptions = self.__smus_adapter.search_approved_subscription_for_subscription_request_id(
                        subscription_request_id, producer_project_id, consumer_project_id)
                    self.__logger.info(
                        f"Found {len(subscription_requests)} approved subscriptions for subscription request {subscription_request_id}")
                    if subscriptions:
                        should_create_new_subscription_request = False

                if should_create_new_subscription_request:
                    self.__logger.info(f"Creating subscription request for listing {listing_id}")
                    subscription_request_id = \
                    self.__smus_adapter.create_subscription_request(listing_id, consumer_project_id)[ID_KEY]
                    self.__logger.info(
                        f"Successfully created subscription request for listing {listing_id} with id {subscription_request_id}")

                    self.__wait_for_subscription_request_to_get_auto_approved(subscription_request_id,
                                                                              producer_project_id, consumer_project_id)

                    self.__collibra_adapter.update_subscription_request_status(approved_request[ID_KEY],
                                                                               COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID)

                    self.__logger.info(f"Successfully started subscription request approval workflow")
                else:
                    self.__logger.info(
                        f"Subscription request already exists. Granting Collibra subscription request {approved_request}")
                    self.__collibra_adapter.update_subscription_request_status(approved_request[ID_KEY],
                                                                               COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID)

            except Exception as e:
                self.__logger.warn(f"Failed to process request: {approved_request}", e)
                self.__collibra_adapter.update_subscription_request_status(approved_request[ID_KEY],
                                                                           COLLIBRA_SUBSCRIPTION_REQUEST_REJECTED_STATUS_ID)

    def __find_smus_table_listing_id(self, collibra_asset, producer_project_id) -> str | None:
        listings = self.__search_all_listings(collibra_asset[DISPLAY_NAME_KEY], producer_project_id)
        matching_listing_id_in_smus = None
        for listing in listings:
            listing = listing[ASSET_LISTING_KEY]
            if CollibraSMUSListingMatcher.match(listing, collibra_asset):
                matching_listing_id_in_smus = listing[LISTING_ID_KEY]
                break
        return matching_listing_id_in_smus

    def __search_all_listings(self, asset_display_name, project_id):
        return self.__smus_adapter.search_all_listings(project_id, asset_display_name)

    def __get_smus_project_ids(self, approved_collibra_subscription_request) -> Tuple[str | None, str | None]:
        if 'stringAttributes' not in approved_collibra_subscription_request or not \
        approved_collibra_subscription_request['stringAttributes']:
            raise ValueError("Producer and Consumer project info doesn't exist in the subscription request")
        producer_project_id, consumer_project_id = None, None
        for string_attribute in approved_collibra_subscription_request['stringAttributes']:
            if string_attribute[TYPE_KEY][NAME_KEY] == AWS_CONSUMER_PROJECT_ID_ATTRIBUTE_NAME:
                consumer_project_id = string_attribute[STRING_VALUE_KEY]
            if string_attribute[TYPE_KEY][NAME_KEY] == AWS_PRODUCER_PROJECT_ID_ATTRIBUTE_NAME:
                producer_project_id = string_attribute[STRING_VALUE_KEY]

        if not consumer_project_id or not producer_project_id:
            raise ValueError("Producer or Consumer project info doesn't exist in the subscription request")

        return producer_project_id, consumer_project_id

    def __is_subscription_request_created_by_smus_collibra_integration_admin_role(self, requester_id):
        user_profile = self.__smus_adapter.get_user_profile(requester_id)

        if user_profile["type"] == "IAM" and user_profile["details"]["iam"][
            "arn"] == SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN:
            return True

        return False

    def __wait_for_subscription_request_to_get_auto_approved(self, subscription_request_id, producer_project_id,
                                                             consumer_project_id):

        end_time = datetime.now() + timedelta(
            minutes=SubscriptionSyncBusinessLogic.__SUBSCRIPTION_REQUEST_AUTO_APPROVAL_WAIT_TIME_IN_MINUTES)
        subscriptions_found = False
        while datetime.now() < end_time:
            subscriptions = self.__smus_adapter.search_approved_subscription_for_subscription_request_id(
                subscription_request_id, producer_project_id, consumer_project_id)

            if subscriptions:
                subscriptions_found = True
                break

            time.sleep(5)

        if not subscriptions_found:
            message = (f"Auto approval of subscription request {subscription_request_id} did not complete "
                       f"in {SubscriptionSyncBusinessLogic.__SUBSCRIPTION_REQUEST_AUTO_APPROVAL_WAIT_TIME_IN_MINUTES} minutes.")
            raise Exception(message)
