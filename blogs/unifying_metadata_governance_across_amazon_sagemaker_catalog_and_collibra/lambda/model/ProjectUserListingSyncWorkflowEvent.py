import json
from typing import Dict


class ProjectUserListingSyncWorkflowEvent:
    def __init__(self, event: Dict[str, str]):
        self.__next_project_token = event.get('next_project_token', None)

    @property
    def next_project_token(self) -> str | None:
        return self.__next_project_token

    @next_project_token.setter
    def next_project_token(self, next_project_token) -> None:
        self.__next_project_token = next_project_token

    def __dict__(self):
        return {
            "next_project_token": self.next_project_token,
        }

    def __str__(self):
        return json.dumps(self.__dict__())
