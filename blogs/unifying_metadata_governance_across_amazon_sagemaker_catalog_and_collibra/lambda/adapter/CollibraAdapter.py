import base64
import json

import requests

from business.AWSClientFactory import AWSClientFactory
from model.CollibraAssetType import CollibraAssetType
from model.CollibraConfig import CollibraConfig
from utils.env_utils import COLLIBRA_CONFIG_SECRETS_NAME, COLLIBRA_SUBSCRIPTION_REQUEST_CREATION_WORKFLOW_ID, \
    COLLIBRA_AWS_PROJECT_TYPE_ID, COLLIBRA_AWS_PROJECT_DOMAIN_ID, COLLIBRA_AWS_PROJECT_ATTRIBUTE_TYPE_ID, \
    COLLIBRA_AWS_USER_TYPE_ID, COLLIBRA_AWS_USER_DOMAIN_ID, \
    COLLIBRA_AWS_USER_PROJECT_ATTRIBUTE_TYPE_ID
from utils.queries import GET_BUSINESS_TERMS_QUERY, GET_BUSINESS_TERMS_WITH_CURSOR_QUERY, GET_AWS_TABLE_ASSETS_QUERY, \
    GET_AWS_TABLE_ASSETS_WITH_CURSOR_QUERY, GET_AWS_TABLE_ASSET_QUERY, GET_PII_COLUMNS_QUERY, \
    GET_AWS_TABLE_BUSINESS_TERMS_QUERY, GET_BUSINESS_TERM_HIERARCHY_QUERY, GET_TABLE_BY_NAME_QUERY, \
    GET_SUBSCRIPTION_REQUESTS_BY_STATUS_QUERY, \
    GET_ASSET_AND_STRING_ATTRIBUTES_BY_NAME_AND_TYPE_QUERY, GET_ASSET_BY_NAME_AND_TYPE_QUERY


class CollibraAdapter:
    COLLIBRA_GRAPHQL_URL_FORMAT = "https://{collibra_config_url}/graphql/knowledgeGraph/v1"
    COLLIBRA_REST_URL_FORMAT = "https://{collibra_config_url}/rest/2.0/{resource}"
    DEFAULT_API_TIMEOUT_IN_SECONDS = 180

    def __init__(self, logger):
        self.__logger = logger
        self.__sts_client = AWSClientFactory.create('secretsmanager')
        self.__config = self.__get_collibra_config()
        self.__api_url = CollibraAdapter.COLLIBRA_GRAPHQL_URL_FORMAT.format(collibra_config_url=self.__config.url)
        self.__authorization_token = self.__get_authorization_token(self.__config)

    def get_business_term_metadata(self, last_seen_id: str = None):
        return self.__get_assets(CollibraAssetType.BUSINESS_TERM, last_seen_id)

    def get_tables(self, last_seen_id: str = None):
        return self.__get_assets(CollibraAssetType.TABLE, last_seen_id)

    def get_business_term_hierarchy(self):
        payload = {"query": GET_BUSINESS_TERM_HIERARCHY_QUERY}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']
            self.__logger.info(f'Successfully fetched business term hierarchy from Collibra')
            return data
        else:
            raise Exception(f"Failed to fetch business term hierarchy from Collibra. Error: {response.text}")

    def get_table(self, table_id: str):
        payload = {"query": GET_AWS_TABLE_ASSET_QUERY, "variables": {"assetId": table_id}}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']
            if len(data) != 1:
                raise Exception(f"Failed to fetch table with id {table_id} from Collibra.")

            self.__logger.info(f'Successfully fetched table with id {table_id} from Collibra')
            return data[0]
        else:
            raise Exception(f"Failed to fetch table with id {table_id} from Collibra. Error: {response.text}")

    def get_table_by_name(self, table_name: str):
        payload = {"query": GET_TABLE_BY_NAME_QUERY, "variables": {"tableName": table_name}}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']
            if len(data) < 1:
                raise Exception(f"No table found with name {table_name} in Collibra.")

            self.__logger.info(f'Successfully fetched table with name {table_name} from Collibra')
            return data[0]
        else:
            raise Exception(f"Failed to fetch table with name {table_name} from Collibra. Error: {response.text}")

    def get_table_business_terms(self, table_id: str):
        payload = {"query": GET_AWS_TABLE_BUSINESS_TERMS_QUERY, "variables": {"assetId": table_id}}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']

            self.__logger.info(f'Successfully fetched business terms of table with id {table_id} from Collibra')
            return data[0]
        else:
            raise Exception(
                f"Failed to fetch business terms of table with id {table_id} from Collibra. Error: {response.text}")

    def get_pii_columns(self, table_id: str):
        payload = {"query": GET_PII_COLUMNS_QUERY, "variables": {"assetId": table_id}}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']
            if len(data) != 1:
                raise Exception(f"Failed to fetch PII columns for table with id {table_id} from Collibra.")

            self.__logger.info(f'Successfully fetched PII columns for table with id {table_id} from Collibra')
            return data[0]
        else:
            raise Exception(
                f"Failed to fetch PII columns for table with id {table_id} from Collibra. Error: {response.text}")

    def start_subscription_request_creation_workflow(self, asset_id: str, consumer_project_name: str):
        url = CollibraAdapter.COLLIBRA_REST_URL_FORMAT.format(collibra_config_url=self.__config.url, resource="workflowInstances")
        response = requests.post(
            url,
            json={"workflowDefinitionId": COLLIBRA_SUBSCRIPTION_REQUEST_CREATION_WORKFLOW_ID,
                  "sendNotification": True,
                  "businessItemIds": [asset_id],
                  "businessItemType": "ASSET",
                  "formProperties": {"aws_consumer_project_name":consumer_project_name}
                  },
            auth=(self.__config.username, self.__config.password),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS
        )

        if self.__is_response_status_ok(response.status_code):
            return response.json()
        else:
            raise Exception(
                f"Failed to start subscription workflow in collibra for collibra asset with id {asset_id}. Error: {response.text}.")

    def get_subscription_requests_by_status(self, status):
        payload = {"query": GET_SUBSCRIPTION_REQUESTS_BY_STATUS_QUERY, "variables": {"status": status}}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']

            self.__logger.info(f'Successfully fetched pending subscription requests from Collibra')
            return data
        else:
            raise Exception(
                f"Failed to fetch pending subscription requests from Collibra. Error: {response.text}")

    def __get_assets(self, asset_type: CollibraAssetType, last_seen_id: str):
        if asset_type == CollibraAssetType.BUSINESS_TERM:
            payload = CollibraAdapter.__get_graphql_query_payload(GET_BUSINESS_TERMS_QUERY,
                                                                  GET_BUSINESS_TERMS_WITH_CURSOR_QUERY, last_seen_id)
        elif asset_type == CollibraAssetType.TABLE:
            payload = CollibraAdapter.__get_graphql_query_payload(GET_AWS_TABLE_ASSETS_QUERY,
                                                                  GET_AWS_TABLE_ASSETS_WITH_CURSOR_QUERY, last_seen_id)
        else:
            raise Exception(f'AssetType {asset_type} not supported')

        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            self.__logger.info(f'Successfully fetched {asset_type.value} data from Collibra')
            data = response.json()['data']['assets']
        else:
            raise Exception(f"Failed to fetch {asset_type.value} data from Collibra. Error: {response.text}")
        return data

    def get_or_create_aws_project(self, project_name, smus_project_id):
        project = self.get_aws_project(project_name)

        if project:
            return project[0]

        return self.create_aws_project(project_name, smus_project_id)

    def get_aws_project(self, project_name):
        payload = {"query": GET_ASSET_BY_NAME_AND_TYPE_QUERY, "variables": {"assetName": project_name, "typeId": COLLIBRA_AWS_PROJECT_TYPE_ID}}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']

            self.__logger.info(f'Successfully fetched asset with name {project_name} from Collibra')
            return data
        else:
            raise Exception(
                f"Failed to fetch asset with name {project_name} from Collibra. Error: {response.text}")

    def create_aws_project(self, project_name, smus_project_id):
        url = CollibraAdapter.COLLIBRA_REST_URL_FORMAT.format(collibra_config_url=self.__config.url, resource="assets")
        payload = {"name": project_name,
                   "domainId": COLLIBRA_AWS_PROJECT_DOMAIN_ID,
                   "typeId": COLLIBRA_AWS_PROJECT_TYPE_ID}
        response = requests.post(url, auth=(self.__config.username, self.__config.password), json=payload,
                                 timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS)

        if self.__is_response_status_ok(response.status_code):
            return response.json()

        raise Exception(f"Failed to create project with name {project_name}")

    def add_aws_project_attributes(self, collibra_project_id, smus_project_id):
        url = CollibraAdapter.COLLIBRA_REST_URL_FORMAT.format(collibra_config_url=self.__config.url,
                                                              resource=f"assets/{collibra_project_id}/attributes")
        payload = {"typeId": COLLIBRA_AWS_PROJECT_ATTRIBUTE_TYPE_ID, "values": [smus_project_id]}
        response = requests.put(url, auth=(self.__config.username, self.__config.password), json=payload,
                                timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS)
        if self.__is_response_status_ok(response.status_code):
            self.__logger.info(f'Successfully added project attribute for project {collibra_project_id}')
            return response.json()

        raise Exception(f"Failed to add project attribute for project {collibra_project_id}")

    def create_relation(self, source_id, target_id, relation_id):
        url = CollibraAdapter.COLLIBRA_REST_URL_FORMAT.format(collibra_config_url=self.__config.url,
                                                              resource=f"relations")
        payload = {"sourceId": source_id, "targetId": target_id, "typeId": relation_id}
        response = requests.post(url, auth=(self.__config.username, self.__config.password), json=payload,
                                 timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS)

        if not self.__is_response_status_ok(response.status_code):
            raise Exception(
                f"Failed to create collibra asset relation {relation_id}between {source_id} and {target_id}. Error: {response.text}")

        return response.json()

    def get_or_create_aws_user(self, username):
        try:
            return self.get_aws_user(username)
        except Exception as e:
            self.__logger.info(f"User with username {username} does not exist. Creating new user.")

        return self.create_aws_user(username)

    def get_aws_user(self, username):
        payload = {"query": GET_ASSET_AND_STRING_ATTRIBUTES_BY_NAME_AND_TYPE_QUERY,
                   "variables": {"assetName": username, "type": COLLIBRA_AWS_USER_TYPE_ID,
                                 "stringAttributeType": COLLIBRA_AWS_USER_PROJECT_ATTRIBUTE_TYPE_ID}}
        response = self.__call_collibra_graphql_api(payload)

        if self.__is_response_status_ok(response.status_code):
            data = response.json()['data']['assets']
            if len(data) != 1:
                raise Exception(f"Failed to fetch user with username {username} from Collibra.")

            self.__logger.info(f'Successfully fetched user with username {username} from Collibra')
            return data[0]
        else:
            raise Exception(f"Failed to fetch tuser with username {username} from Collibra. Error: {response.text}")

    def create_aws_user(self, username):
        url = CollibraAdapter.COLLIBRA_REST_URL_FORMAT.format(collibra_config_url=self.__config.url, resource="assets")
        payload = {
            "name": username,
            "domainId": COLLIBRA_AWS_USER_DOMAIN_ID,
            "typeId": COLLIBRA_AWS_USER_TYPE_ID
        }
        response = requests.post(url, auth=(self.__config.username, self.__config.password), json=payload,
                                 timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS)
        if self.__is_response_status_ok(response.status_code):
            self.__logger.info(f'Successfully created user {username} in Collibra')

            return response.json()

        raise Exception(f"Failed to create user {username} from Collibra. Error: {response.text}")

    def add_aws_user_attributes(self, user_id, project_name):
        url = CollibraAdapter.COLLIBRA_REST_URL_FORMAT.format(collibra_config_url=self.__config.url,
                                                              resource=f"attributes")
        payload = {
            "assetId": user_id,
            "typeId": COLLIBRA_AWS_USER_PROJECT_ATTRIBUTE_TYPE_ID,
            "value": project_name
        }
        response = requests.post(url, auth=(self.__config.username, self.__config.password), json=payload,
                                 timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS)

        if self.__is_response_status_ok(response.status_code):
            return response.json()
        else:
            raise Exception(f"Failed to add attributes for user {user_id} to Collibra")

    def update_subscription_request_status(self, subscription_request_id: str, status_id: str):
        url = CollibraAdapter.COLLIBRA_REST_URL_FORMAT.format(collibra_config_url=self.__config.url,
                                                              resource=f"assets/{subscription_request_id}")
        payload = {
            "statusId": status_id
        }

        response = requests.patch(url, auth=(self.__config.username, self.__config.password), json=payload,
                                  headers={
                                      "Content-Type": "application/json",
                                      "Accept": "application/json",
                                  },
                                  timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS)

        if self.__is_response_status_ok(response.status_code):
            return response.json()
        else:
            raise Exception(
                f"Failed to update subscription request status for subscription request id {subscription_request_id}")

    @classmethod
    def __get_authorization_token(cls, config: CollibraConfig):
        authorization_token_string = f"{config.username}:{config.password}"
        authorization_token_bytes = authorization_token_string.encode("utf-8")
        encoded_authorization_token_bytes = base64.b64encode(authorization_token_bytes)
        return encoded_authorization_token_bytes.decode('utf-8')

    def __call_collibra_graphql_api(self, payload: dict):
        return requests.post(
            self.__api_url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {self.__authorization_token}"
            },
            timeout=CollibraAdapter.DEFAULT_API_TIMEOUT_IN_SECONDS
        )

    @staticmethod
    def __get_graphql_query_payload(query: str, query_with_cursor: str, last_seen_id: str = None):
        if last_seen_id:
            payload = {"query": query_with_cursor, "variables": {"lastSeenId": last_seen_id}}
        else:
            payload = {"query": query}

        return payload

    def __get_collibra_config(self) -> CollibraConfig:
        get_secret_value_response = self.__sts_client.get_secret_value(
            SecretId=COLLIBRA_CONFIG_SECRETS_NAME
        )
        secret_string = get_secret_value_response['SecretString']
        return CollibraConfig(json.loads(secret_string))

    def __is_response_status_ok(self, response_status):
        return response_status == 200 or response_status == 201
