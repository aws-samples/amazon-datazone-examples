import re

from utils.collibra_constants import AWS_REGION_MAP


class AWSGlueMetadataCollibraAttribute:
    AWS_ARN_REGEX = r"^arn:(aws|aws-cn|aws-us-gov):[a-z0-9-]+:[a-z0-9-]*:(\d{12}):.+$"

    def __init__(self, aws_resource_metadata: dict[str, str]):
        """
        :param aws_resource_metadata: Sample AWS Resource Metadata:

        {“glueAccessRoleArn”: “arn:aws:iam::123456789012:role/role-name”, “region”: “NORTHERNVIRGINIA”}
        """
        self._account_id = self.__get_account_id_from_arn(aws_resource_metadata['glueAccessRoleArn'])
        self._region = AWS_REGION_MAP.get(aws_resource_metadata["region"])

    @property
    def account_id(self):
        return self._account_id

    @property
    def region(self):
        return self._region

    def __get_account_id_from_arn(self, arn: str):
        match = re.match(AWSGlueMetadataCollibraAttribute.AWS_ARN_REGEX, arn)
        if match:
            return match.group(2)
        raise ValueError("Invalid ARN or missing account ID provided in AWSGlueMetadata")
