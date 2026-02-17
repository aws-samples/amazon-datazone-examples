import re


class AWSRedshiftClusterMetadataCollibraAttribute:
    REDSHIFT_CLUSTER_ENDPOINT_REGEX_PATTERN = re.compile(
        r"^(?P<cluster_name>[a-z-]{1,63})\.[a-z0-9-]+\.(?P<region>[a-z0-9-]+)\.redshift\.amazonaws\.com.*"
    )

    def __init__(self, aws_resource_metadata: dict[str, str]):
        """
        :param aws_resource_metadata: Sample AWS Resource Metadata:

        {“redshiftEndpoint”: “cluster-name.cluster-id.us-east-1.redshift.amazonaws.com:5439/dev”}
        """
        self._cluster_name, self._region = AWSRedshiftClusterMetadataCollibraAttribute.__extract_metadata_from_endpoint(
            aws_resource_metadata['redshiftEndpoint'])

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def region(self):
        return self._region

    @staticmethod
    def __extract_metadata_from_endpoint(endpoint):
        match = AWSRedshiftClusterMetadataCollibraAttribute.REDSHIFT_CLUSTER_ENDPOINT_REGEX_PATTERN.search(endpoint)
        if match:
            cluster_name = match.group('cluster_name')
            region = match.group('region')
            return cluster_name, region

        raise ValueError(f"Invalid Redshift Endpoint {endpoint}")
