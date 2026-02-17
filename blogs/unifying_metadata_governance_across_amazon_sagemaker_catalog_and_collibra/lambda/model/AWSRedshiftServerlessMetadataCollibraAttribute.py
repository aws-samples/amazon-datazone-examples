import re


class AWSRedshiftServerlessMetadataCollibraAttribute:
    REDSHIFT_WORKGROUP_ENDPOINT_REGEX_PATTERN = re.compile(
        r"^(?P<workgroup_name>[a-z-0-9]{3,63})\.(?P<account_id>\d{12})\.(?P<region>[a-z0-9-]+)\.redshift-serverless\.amazonaws\.com.*"
    )

    def __init__(self, aws_resource_metadata: dict[str, str]):
        """
        :param aws_resource_metadata: Sample AWS Resource Metadata:

        {“redshiftEndpoint”: “workgroup-name.123456789012.us-east-1.redshift-serverless.amazonaws.com:5439/dev”}
        """
        self._workgroup_name, self._account_id, self._region = AWSRedshiftServerlessMetadataCollibraAttribute.__extract_metadata_from_endpoint(
            aws_resource_metadata['redshiftEndpoint'])

    @property
    def workgroup_name(self):
        return self._workgroup_name

    @property
    def account_id(self):
        return self._account_id

    @property
    def region(self):
        return self._region

    @staticmethod
    def __extract_metadata_from_endpoint(endpoint):
        match = AWSRedshiftServerlessMetadataCollibraAttribute.REDSHIFT_WORKGROUP_ENDPOINT_REGEX_PATTERN.search(endpoint)
        if match:
            workgroup_name = match.group('workgroup_name')
            account_id = match.group('account_id')
            region = match.group('region')
            return workgroup_name, account_id, region

        raise ValueError(f"Invalid Redshift Endpoint {endpoint}")
