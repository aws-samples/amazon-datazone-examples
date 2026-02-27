from typing import List

from business.AWSClientFactory import AWSClientFactory
from utils.common_utils import get_collibra_synced_glossary_name, wait_until
from utils.env_utils import SMUS_DOMAIN_ID, SMUS_GLOSSARY_OWNER_PROJECT_ID, \
    SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN
from utils.smus_constants import ACTIVATED_USER_STATUS


class SMUSAdapter:
    GLOSSARY_TERM_SHORT_DESCRIPTION_MAX_LENGTH = 1024
    GLOSSARY_TERM_LONG_DESCRIPTION_MAX_LENGTH = 4096
    MAX_RESULTS = 50
    SLEEP_INTERVAL = 2
    MAX_TIME_TO_WAIT = 5

    def __init__(self, logger):
        self.__logger = logger
        self.__client = AWSClientFactory.create('datazone')
        self.__admin_role_user_id = self.__find_admin_role_user_id()

    def get_project(self, project_id):
        return self.__client.get_project(
            domainIdentifier=SMUS_DOMAIN_ID,
            identifier=project_id
        )

    def create_or_get_glossary(self) -> str:
        """
        Creates glossary in SMUS if it doesn't exist
        :return: Glossary id
        """
        glossary_name = get_collibra_synced_glossary_name()
        self.__logger.info(f"Using glossary with name: {glossary_name}")
        search_response = self.__client.search(domainIdentifier=SMUS_DOMAIN_ID,
                                               searchText=get_collibra_synced_glossary_name(),
                                               searchScope='GLOSSARY')
        if len(search_response['items']) > 0:
            for item in search_response['items']:
                if item['glossaryItem']['name'] == glossary_name:
                    return item['glossaryItem']['id']

        response = self.__client.create_glossary(
            description='Glossary for terms synced from Collibra',
            domainIdentifier=SMUS_DOMAIN_ID,
            name=get_collibra_synced_glossary_name(),
            owningProjectIdentifier=SMUS_GLOSSARY_OWNER_PROJECT_ID,
            status='ENABLED'
        )

        wait_until(SMUSAdapter.SLEEP_INTERVAL, SMUSAdapter.MAX_TIME_TO_WAIT, self.__logger, "Waiting for glossary to create", None)
        return response['id']

    def search_glossary_term_by_name(self, glossary_id: str, glossary_term_name: str):
        terms = self.__client.search(searchScope='GLOSSARY_TERM',
                                     domainIdentifier=SMUS_DOMAIN_ID,
                                     filters={"filter": {"attribute": "BusinessGlossaryTermForm.businessGlossaryId",
                                                       "value": glossary_id}},
                                     searchText=glossary_term_name,
                                     maxResults=SMUSAdapter.MAX_RESULTS
                                     )['items']

        if terms:
            for term in terms:
                if term['glossaryTermItem']["name"] == glossary_term_name:
                    return term['glossaryTermItem']

        return None

    def create_glossary_term(self, glossary_id: str, name: str, descriptions: List[str]):
        self.__client.create_glossary_term(
            domainIdentifier=SMUS_DOMAIN_ID,
            glossaryIdentifier=glossary_id,
            name=name,
            status='ENABLED', **self.__get_description_args_of_glossary_term(descriptions)
        )

    def update_glossary_term_description(self, glossary_term_id: str, descriptions: List[str]):
        self.__client.update_glossary_term(
            domainIdentifier=SMUS_DOMAIN_ID,
            identifier=glossary_term_id,
            status='ENABLED', **self.__get_description_args_of_glossary_term(descriptions)
        )

    def search_all_assets_by_name(self, table_name: str, project_id: str):
        items = []
        next_token = None
        has_more_items = True
        while has_more_items:
            search_response = self.search_asset_by_name(table_name, project_id, next_token)
            items.extend(search_response['items'])
            next_token = search_response.get('nextToken', None)

            if not next_token:
                has_more_items = False
        return items

    def search_asset_by_name(self, table_name: str, project_id: str, next_token: str = None):
        args = {"searchScope": 'ASSET', "owningProjectIdentifier": project_id,
                "domainIdentifier": SMUS_DOMAIN_ID,
                "additionalAttributes": ["FORMS"],
                "searchIn": [{"attribute": "RedshiftTableForm.tableName"}, {"attribute": "GlueTableForm.tableName"}],
                "searchText": table_name,
                "maxResults": SMUSAdapter.MAX_RESULTS,
                }

        if next_token:
            args['nextToken'] = next_token

        return self.__client.search(**args)

    def search_all_listings(self, project_id: str, search_text: str = None):
        items = []
        next_token = None
        has_more_items = True
        while has_more_items:
            search_response = self.search_listings(project_id, search_text, next_token)
            items.extend(search_response['items'])
            next_token = search_response.get('nextToken', None)

            if not next_token:
                has_more_items = False
        return items

    def search_listings(self, project_id: str, search_text: str = None, next_token: str = None):
        args = {"domainIdentifier": SMUS_DOMAIN_ID,
                "additionalAttributes": ["FORMS"],
                "filters": {
                    "and": [{"filter": {"attribute": "owningProjectId", "value": project_id}},
                            {"filter": {"attribute": "amazonmetadata.sourceCategory", "value": "asset"}}]}
                }

        if search_text:
            args['searchText'] = search_text

        if next_token:
            args['nextToken'] = next_token

        return self.__client.search_listings(**args)

    def list_all_terms_in_glossary(self, glossary_id: str):
        items = []
        next_token = None
        has_more_items = True
        while has_more_items:
            search_response = self.list_terms_in_glossary(glossary_id, next_token)
            items.extend(search_response['items'])
            next_token = search_response.get('nextToken', None)

            if not next_token:
                has_more_items = False
        return items

    def list_terms_in_glossary(self, glossary_id: str, next_token: str = None):
        args = {
            "searchScope": 'GLOSSARY_TERM',
            "domainIdentifier": SMUS_DOMAIN_ID,
            "filters": {"filter": {"attribute": "BusinessGlossaryTermForm.businessGlossaryId",
                                   "value": glossary_id}},
            "maxResults": SMUSAdapter.MAX_RESULTS,
        }

        if next_token:
            args['nextToken'] = next_token

        return self.__client.search(**args)

    def list_all_users_in_project(self, project_id: str):
        items = []
        next_token = None
        has_more_items = True
        while has_more_items:
            search_response = self.list_users_in_project(project_id, next_token)
            items.extend(search_response['members'])
            next_token = search_response.get('nextToken', None)

            if not next_token:
                has_more_items = False
        return items

    def list_users_in_project(self, project_id: str, next_token: str = None):
        args = {
            "domainIdentifier":SMUS_DOMAIN_ID,
            "projectIdentifier":project_id,
            "maxResults": SMUSAdapter.MAX_RESULTS
        }

        if next_token:
            args['nextToken'] = next_token

        response = self.__client.list_project_memberships(**args)
        sso_users = []
        for member in response['members']:
            if 'user' in member['memberDetails']:
                sso_users.append(member)

        response['members'] = sso_users
        return response

    def get_user_profile(self, user_id: str):
        return self.__client.get_user_profile(
            domainIdentifier=SMUS_DOMAIN_ID,
            userIdentifier=user_id
        )

    def get_asset(self, asset_id: str):
        return self.__client.get_asset(
            domainIdentifier=SMUS_DOMAIN_ID,
            identifier=asset_id)

    def create_asset_revision(self, asset_name, asset_id, forms_input, **optional_args):
        return self.__client.create_asset_revision(name=asset_name, domainIdentifier=SMUS_DOMAIN_ID,
                                                   formsInput=forms_input,
                                                   identifier=asset_id, **optional_args)

    def update_glossary_term_relations(self, glossary_id: str, id: str, name: str, term_relations: List[str]):
        return self.__client.update_glossary_term(
            domainIdentifier=SMUS_DOMAIN_ID,
            glossaryIdentifier=glossary_id,
            name=name,
            termRelations=term_relations,
            identifier=id,
            status='ENABLED',
        )

    def create_subscription_request(self, listing_id: str, consumer_project_id: str):
        return self.__client.create_subscription_request(
            domainIdentifier=SMUS_DOMAIN_ID,
            requestReason='Automated sync - Subscription request created from Collibra',
            subscribedListings=[
                {
                    'identifier': listing_id
                },
            ],
            subscribedPrincipals=[
                {
                    'project': {
                        'identifier': consumer_project_id
                    }
                },
            ]
        )

    def search_subscription_requests(self, listing_id: str, owning_project_id: str, consumer_project_id: str):
        subscription_requests = self.__client.list_subscription_requests(
            approverProjectId=owning_project_id,
            domainIdentifier=SMUS_DOMAIN_ID,
            owningProjectId=consumer_project_id,
            status='ACCEPTED',
            sortBy='UPDATED_AT',
            sortOrder='DESCENDING',
            subscribedListingId=listing_id
        )['items']

        sorted_subscription_requests = sorted(subscription_requests, key=lambda item: item['updatedAt'], reverse=True)
        return sorted_subscription_requests

    def search_approved_subscription_for_subscription_request_id(self, subscription_request_id: str, owning_project_id: str, consumer_project_id: str):
        return self.__client.list_subscriptions(
            approverProjectId=owning_project_id,
            domainIdentifier=SMUS_DOMAIN_ID,
            owningProjectId=consumer_project_id,
            status='APPROVED',
            subscriptionRequestIdentifier=subscription_request_id
        )['items']

    def accept_subscription_request(self, subscription_request_id: str):
        return self.__client.accept_subscription_request(
            decisionComment='Automated sync - Subscription request approved from Collibra',
            domainIdentifier=SMUS_DOMAIN_ID,
            identifier=subscription_request_id
        )

    def list_all_projects(self):
        items = []
        next_token = None
        has_more_items = True
        while has_more_items:
            search_response = self.list_projects(SMUSAdapter.MAX_RESULTS, next_token)
            for project in search_response['items']:
                if project["projectStatus"] == "ACTIVE":
                    items.append(project)
            next_token = search_response.get('nextToken', None)

            if not next_token:
                has_more_items = False
        return items

    def list_projects(self, max_results: int, next_token: str = None):
        args = {
            'domainIdentifier': SMUS_DOMAIN_ID,
            'maxResults': max_results,
            'userIdentifier': self.__admin_role_user_id
        }

        if next_token:
            args['nextToken'] = next_token

        return self.__client.list_projects(**args)

    def __find_admin_role_user_id(self):
        has_more_items = True
        next_token = None
        admin_role_user_id = None
        while has_more_items:
            args = {
                'domainIdentifier': SMUS_DOMAIN_ID,
                'maxResults': SMUSAdapter.MAX_RESULTS,
                'searchText': SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN,
                'userType': 'DATAZONE_IAM_USER'
            }

            if next_token:
                args['nextToken'] = next_token

            search_user_profiles_response = self.__client.search_user_profiles(**args)

            for user_profile in search_user_profiles_response['items']:
                if user_profile['status'] == ACTIVATED_USER_STATUS and user_profile['details']['iam']['arn'] == SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN:
                    admin_role_user_id = user_profile['id']
                    break

            if admin_role_user_id is None and 'nextToken' in search_user_profiles_response:
                next_token = search_user_profiles_response['nextToken']
            else:
                has_more_items = False

        return admin_role_user_id


    def __get_description_args_of_glossary_term(self, term_descriptions: List[str]):
        description_args = {}
        if not term_descriptions:
            pass
        elif len(term_descriptions) == 1 and len(term_descriptions[0]) <= SMUSAdapter.GLOSSARY_TERM_SHORT_DESCRIPTION_MAX_LENGTH:
            description_args['shortDescription'] = term_descriptions[0]
        else:
            description_args['longDescription'] = '\n\n'.join(term_descriptions)[:SMUSAdapter.GLOSSARY_TERM_LONG_DESCRIPTION_MAX_LENGTH]
        return description_args
