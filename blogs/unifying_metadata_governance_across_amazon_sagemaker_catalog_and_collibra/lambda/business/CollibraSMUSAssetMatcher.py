import json

from business.CollibraSMUSResourceMatcher import CollibraSMUSResourceMatcher
from utils.smus_constants import FORM_NAME_KEY, CONTENT_KEY, EXTERNAL_IDENTIFIER_KEY


class CollibraSMUSAssetMatcher(CollibraSMUSResourceMatcher):

    @staticmethod
    def _get_deserialized_form_content_by_name(form_names, smus_asset):
        for form in smus_asset['formsOutput']:
            if form[FORM_NAME_KEY] in form_names:
                return json.loads(form[CONTENT_KEY])

        return None

    @staticmethod
    def _get_smus_resource_type():
        return 'asset'

    @staticmethod
    def _is_valid_smus_resource(smus_resource):
        return EXTERNAL_IDENTIFIER_KEY in smus_resource