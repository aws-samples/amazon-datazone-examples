from typing import Dict


class AWSCredentials:
    def __init__(self, credentials: Dict[str, str]):
        self._access_key_id = credentials['AccessKeyId']
        self._secret_access_key = credentials['SecretAccessKey']
        self._session_token = credentials['SessionToken']

    @property
    def access_key_id(self) -> str:
        return self._access_key_id

    @property
    def secret_access_key(self) -> str:
        return self._secret_access_key

    @property
    def session_token(self) -> str:
        return self._session_token
