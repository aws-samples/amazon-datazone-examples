from typing import Dict


class CollibraConfig:
    def __init__(self, data: Dict[str, str]):
        self._username = data.get("username")
        self._password = data.get("password")
        self._url = data.get("url")

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def url(self):
        return self._url
