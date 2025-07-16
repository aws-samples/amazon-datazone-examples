from adapter.CollibraAdapter import CollibraAdapter
from adapter.SMUSAdapter import SMUSAdapter
from utils.collibra_constants import ID_KEY
from utils.env_utils import SMUS_PRODUCER_PROJECT_ID, COLLIBRA_AWS_PROJECT_TO_ASSET_RELATION_TYPE_ID, \
    SMUS_CONSUMER_PROJECT_ID
from utils.smus_constants import ASSET_LISTING_KEY


class ProjectUserSyncBusinessLogic:
    def __init__(self, logger):
        self.__logger = logger
        self.__smus_adapter = SMUSAdapter(self.__logger)
        self.__collibra_adapter = CollibraAdapter(self.__logger)

    def sync(self):
        synced_consumer_project, smus_consumer_project_name = self.sync_project(SMUS_CONSUMER_PROJECT_ID)
        self.sync_users_and_associate_with_projects(SMUS_CONSUMER_PROJECT_ID, smus_consumer_project_name)

        synced_publisher_project, smus_publisher_project_name = self.sync_project(SMUS_PRODUCER_PROJECT_ID)
        self.associate_project_with_listings(synced_publisher_project)

        self.sync_users_and_associate_with_projects(SMUS_PRODUCER_PROJECT_ID, smus_publisher_project_name)

    def sync_project(self, smus_project_id):
        smus_project = self.__smus_adapter.get_project(smus_project_id)
        smus_project_name = smus_project['name']

        collibra_project = self.__collibra_adapter.get_or_create_aws_project(smus_project_name, smus_project_id)
        self.__collibra_adapter.add_aws_project_attributes(collibra_project[ID_KEY], smus_project_id)

        return collibra_project, smus_project_name

    def associate_project_with_listings(self, synced_publisher_project):
        listings = [listing_result[ASSET_LISTING_KEY] for listing_result in self.__smus_adapter.search_all_listings()]

        project_id = synced_publisher_project[ID_KEY]
        for listing in listings:
            listing_name = listing['name']
            try:
                collibra_asset = self.__collibra_adapter.get_table_by_name(listing_name)
                collibra_asset_id = collibra_asset[ID_KEY]
            except:
                self.__logger.warn(f"Asset with name {listing_name} doesn't exist in Collibra. Skipping.")
                continue

            try:
                self.__collibra_adapter.create_relation(source_id=project_id, target_id=collibra_asset_id,
                                                        relation_id=COLLIBRA_AWS_PROJECT_TO_ASSET_RELATION_TYPE_ID)

                self.__logger.info(f"Successfully associated project {project_id} with asset {collibra_asset_id}")
            except Exception as e:
                self.__logger.warn(f"Failed to associate project {project_id} with asset {collibra_asset_id}. Exception: {e}")

    def sync_users_and_associate_with_projects(self, project_id, smus_project_name):
        users = self.__smus_adapter.list_all_users_in_project(project_id)

        self.__logger.info(f"Found {len(users)} users in project {smus_project_name}")

        for user in users:
            try:
                user_profile = self.__smus_adapter.get_sso_user_profile(user["memberDetails"]["user"]["userId"])

                if user_profile["type"] == "IAM":
                    continue

                username = user_profile['details']['sso']['username']
                self.__logger.info(f"User {username} associated with project {smus_project_name}")

                user = self.__collibra_adapter.get_or_create_aws_user(username)
                self.__logger.info(f"User {username} associated with collibra user {user[ID_KEY]}")

                should_add_project_attribute = True
                if 'stringAttributes' in user:
                    for attribute in user['stringAttributes']:
                        if attribute['stringValue'] == smus_project_name:
                            should_add_project_attribute = False
                            break

                self.__logger.info(f"Should add project attribute is {should_add_project_attribute} for user {username}")

                if should_add_project_attribute:
                    response = self.__collibra_adapter.add_aws_user_attributes(user[ID_KEY], smus_project_name)
                    if response.status_code == 201:
                        self.__logger.info(f"Successfully added project attribute for user {username}")
                    else:
                        self.__logger.warn(f"Failed to add project attribute for user {username}. Response: {response}")

            except Exception as e:
                self.__logger.warn(f"Failed to add project attribute for user {user}. Exception: {e}")