import os


class EnvUtils:
    @staticmethod
    def get_env_var(name: str, required: bool = True, default=None) -> str:
        value = os.getenv(name, default)
        if required and value is None:
            raise EnvironmentError(f"Missing required environment variable: {name}")
        return value

SMUS_DOMAIN_ID = EnvUtils.get_env_var("SMUS_DOMAIN_ID", required=True)
SMUS_GLOSSARY_OWNER_PROJECT_ID = EnvUtils.get_env_var("SMUS_GLOSSARY_OWNER_PROJECT_ID", required=True)
SMUS_REGION = EnvUtils.get_env_var("SMUS_REGION", default="us-east-1", required=False)
SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN = EnvUtils.get_env_var("SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN", required=False)
COLLIBRA_CONFIG_SECRETS_NAME = EnvUtils.get_env_var("COLLIBRA_CONFIG_SECRETS_NAME", required=True)
COLLIBRA_SUBSCRIPTION_REQUEST_CREATION_WORKFLOW_ID = EnvUtils.get_env_var("COLLIBRA_SUBSCRIPTION_REQUEST_CREATION_WORKFLOW_ID", required=True)
COLLIBRA_SUBSCRIPTION_REQUEST_APPROVAL_WORKFLOW_ID = EnvUtils.get_env_var("COLLIBRA_SUBSCRIPTION_REQUEST_APPROVAL_WORKFLOW_ID", required=True)

COLLIBRA_AWS_PROJECT_TYPE_ID = EnvUtils.get_env_var("COLLIBRA_AWS_PROJECT_TYPE_ID", required=True)
COLLIBRA_AWS_PROJECT_DOMAIN_ID = EnvUtils.get_env_var("COLLIBRA_AWS_PROJECT_DOMAIN_ID", required=True)
COLLIBRA_AWS_PROJECT_ATTRIBUTE_TYPE_ID = EnvUtils.get_env_var("COLLIBRA_AWS_PROJECT_ATTRIBUTE_TYPE_ID", required=True)
COLLIBRA_AWS_PROJECT_TO_ASSET_RELATION_TYPE_ID = EnvUtils.get_env_var("COLLIBRA_AWS_PROJECT_TO_ASSET_RELATION_TYPE_ID", required=True)
COLLIBRA_AWS_USER_TYPE_ID = EnvUtils.get_env_var("COLLIBRA_AWS_USER_TYPE_ID", required=True)
COLLIBRA_AWS_USER_DOMAIN_ID = EnvUtils.get_env_var("COLLIBRA_AWS_USER_DOMAIN_ID", required=True)
COLLIBRA_AWS_USER_PROJECT_ATTRIBUTE_TYPE_ID = EnvUtils.get_env_var("COLLIBRA_AWS_USER_PROJECT_ATTRIBUTE_TYPE_ID", required=True)
COLLIBRA_SUBSCRIPTION_REQUEST_REJECTED_STATUS_ID = EnvUtils.get_env_var("COLLIBRA_SUBSCRIPTION_REQUEST_REJECTED_STATUS_ID", required=True)
COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID = EnvUtils.get_env_var("COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID", required=True)
