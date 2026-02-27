import json

from business.CollibraSMUSResourceMatcher import CollibraSMUSResourceMatcher


class CollibraSMUSListingMatcher(CollibraSMUSResourceMatcher):

    @staticmethod
    def _get_deserialized_form_content_by_name(form_names, smus_listing):
        if 'additionalAttributes' in smus_listing and 'forms' in smus_listing['additionalAttributes']:
            forms = json.loads(smus_listing['additionalAttributes']['forms'])

            for form_name in form_names:
                if form_name in forms:
                    return forms[form_name]

        return None


    @staticmethod
    def _get_smus_resource_type():
        return 'listing'

    @staticmethod
    def _is_valid_smus_resource(smus_resource):
        return True