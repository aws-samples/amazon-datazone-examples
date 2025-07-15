import boto3
from botocore.config import Config

from utils.env_utils import SMUS_REGION


class AWSClientFactory:
    @staticmethod
    def create(service_name: str):
        return boto3.client(service_name,
                            config=Config(region_name=SMUS_REGION))
