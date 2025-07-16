from typing import List

from adapter.CollibraAdapter import CollibraAdapter
from adapter.SMUSAdapter import SMUSAdapter
from utils.collibra_constants import DISPLAY_NAME_KEY, FULL_NAME_KEY, ID_KEY
from utils.env_utils import SMUS_CONSUMER_PROJECT_ID, SMUS_PRODUCER_PROJECT_ID
from utils.smus_constants import REDSHIFT_TYPE_IDENTIFIER_INFIX, GLUE_TYPE_IDENTIFIER_INFIX, ASSET_LISTING_KEY, \
    ENTITY_TYPE_KEY, ADDITIONAL_ATTRIBUTES_KEY, FORMS_KEY, LISTING_ID_KEY


class SubscriptionSyncBusinessLogic:
    def __init__(self, logger):
        self.__logger = logger
        self.__smus_adapter = SMUSAdapter(self.__logger)
        self.__collibra_adapter = CollibraAdapter(self.__logger)

    def sync_subscription_to_collibra(self, event: dict):
        self.__logger.info(f"Running validations on subscription request")

        if event['status'] != 'PENDING':
            self.__logger.warn(
                f"Subscription request status is {event['status']}. Expected PENDING")
            return

        if len(event['subscribedPrincipals']) != 1:
            self.__logger.warn(f"Expected only 1 subscribed principal.")
            return

        if event['subscribedPrincipals'][0]['id'] != SMUS_CONSUMER_PROJECT_ID:
            self.__logger.warn(f"Subscriber must be {SMUS_CONSUMER_PROJECT_ID}. Either it is null or different.")
            return

        if len(event['subscribedListings']) != 1:
            self.__logger.warn(f"No subscribed listings found. Expected 1")
            return

        if event['subscribedListings'][0]['ownerProjectId'] != SMUS_PRODUCER_PROJECT_ID:
            self.__logger.warn(f"Owner of the subscribed listing must be {SMUS_PRODUCER_PROJECT_ID}")
            return

        if 'assetListing' not in event['subscribedListings'][0]['item']:
            self.__logger.warn(f"Subscribed listing is not an asset")
            return

        try:
            asset_id = event['subscribedListings'][0]['item']['assetListing']['entityId']

            self.__logger.info(f"Retrieving asset from SMUS with id {asset_id}")
            asset = self.__smus_adapter.get_asset(asset_id)
            asset_name = asset.get('name')

            self.__logger.info(f"Found asset in SMUS with name {asset_name}")

            self.__logger.info(f"Retrieving asset with name {asset_name} from Collibra")
            collibra_asset = self.__collibra_adapter.get_table_by_name(asset_name)
            collibra_asset_id = collibra_asset.get('id')

            self.__logger.info(f"Found asset with name {asset_name} in Collibra with id {collibra_asset_id}")

            response = self.__collibra_adapter.start_subscription_request_creation_workflow(collibra_asset_id)

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
                asset_display_name = approved_request["outgoingRelations"][0]["target"][DISPLAY_NAME_KEY]
                asset_full_name = approved_request["outgoingRelations"][0]["target"][FULL_NAME_KEY]
                smus_listing_ids = self.__find_smus_table_listing_ids(asset_display_name, asset_full_name)

                self.__logger.info(f"Found {len(smus_listing_ids)} listings in SMUS")

                for listing_id in smus_listing_ids:
                    subscription_requests = self.__smus_adapter.search_subscription_requests(listing_id)

                    should_create_new_subscription_request = False
                    if subscription_requests:
                        self.__logger.info(
                            f"Found {len(subscription_requests)} approved subscription requests for listing {listing_id}")
                        subscription_request_id = subscription_requests[0][ID_KEY]
                        subscriptions = self.__smus_adapter.search_approved_subscription_for_subscription_request_id(subscription_request_id)
                        if not subscriptions:
                            self.__logger.info("Previously approved subscription request was either revoked or unsubscribed")
                            should_create_new_subscription_request = True


                    if not subscription_requests or should_create_new_subscription_request:
                        self.__logger.info(f"Creating subscription request for listing {listing_id}")
                        subscription_request_id = self.__smus_adapter.create_subscription_request(listing_id)[ID_KEY]
                        self.__logger.info(f"Successfully created subscription request for listing {listing_id}")
                        self.__logger.info(
                            f"Accepting subscription request {subscription_request_id} for listing {listing_id}")
                        self.__smus_adapter.accept_subscription_request(subscription_request_id)
                        self.__logger.info(f"Successfully accepted subscription request for listing {listing_id}")

                    self.__collibra_adapter.start_subscription_request_approval_workflow()
                    self.__logger.info(f"Successfully started subscription request approval workflow")
            except Exception as e:
                self.__logger.error(f"Failed to process request: {approved_request}", e)

    def __extract_redshift_database_schema_table_names(self, table_full_name):
        database, schema, table = table_full_name.rsplit('>', 3)[1:]
        return database, schema, table

    def __get_redshift_external_identifier_suffix(self, type_identifier, database, schema, table):
        if "view" in type_identifier.lower():
            return f"{database}/{schema}/{table}"

        return f"{database}/{schema}/{table}"

    def __extract_glue_database_table_names(self, table_full_name):
        database, table = table_full_name.rsplit('>', 2)[1:]
        return database, table

    def __get_glue_external_identifier_suffix(self, database, table):
        return f"{database}/{table}"

    def __find_smus_table_listing_ids(self, asset_display_name, asset_full_name) -> List[str]:
        listings = self.__smus_adapter.search_all_listings(asset_display_name)
        matching_listing_in_smus = []
        for listing in listings:
            listing = listing[ASSET_LISTING_KEY]
            if REDSHIFT_TYPE_IDENTIFIER_INFIX in listing[ENTITY_TYPE_KEY]:
                database, schema, table_name = self.__extract_redshift_database_schema_table_names(asset_full_name)
                if self.__get_redshift_external_identifier_suffix(listing[ENTITY_TYPE_KEY], database, schema,
                                                                  table_name) in listing[ADDITIONAL_ATTRIBUTES_KEY][
                    FORMS_KEY]:
                    matching_listing_in_smus.append(listing[LISTING_ID_KEY])
            elif GLUE_TYPE_IDENTIFIER_INFIX in listing[ENTITY_TYPE_KEY]:
                database, table_name = self.__extract_glue_database_table_names(asset_full_name)
                if self.__get_glue_external_identifier_suffix(database, table_name) in \
                        listing[ADDITIONAL_ATTRIBUTES_KEY][FORMS_KEY]:
                    matching_listing_in_smus.append(listing[LISTING_ID_KEY])
        return matching_listing_in_smus
