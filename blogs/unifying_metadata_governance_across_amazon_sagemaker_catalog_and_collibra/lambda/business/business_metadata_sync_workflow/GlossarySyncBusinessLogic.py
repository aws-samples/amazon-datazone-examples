from adapter.CollibraAdapter import CollibraAdapter
from adapter.SMUSAdapter import SMUSAdapter
from utils.common_utils import extract_collibra_descriptions


class GlossarySyncBusinessLogic:
    def __init__(self, logger):
        self.__logger = logger
        self.__smus_adapter = SMUSAdapter(logger)
        self.__glossary_id = self.__smus_adapter.create_or_get_glossary()
        self.__collibra_adapter = CollibraAdapter(logger)

    def sync(self, last_seen_glossary_term_id: str):
        glossary_terms = self.__collibra_adapter.get_business_term_metadata(last_seen_glossary_term_id)

        self.__logger.info(f"Found {len(glossary_terms)} terms in Collibra.")

        terms_created = set()
        new_last_seen_id = None
        for glossary_term in glossary_terms:
            new_last_seen_id = glossary_term['id']
            glossary_term_name = glossary_term['displayName']
            glossary_term_descriptions = extract_collibra_descriptions(glossary_term)

            if glossary_term_name in terms_created:
                continue

            glossary_term = self.__smus_adapter.search_glossary_term_by_name(self.__glossary_id, glossary_term['displayName'])

            if glossary_term:
                self.__logger.info(f'Glossary term \'{glossary_term_name}\' already exists. Checking if description has changed.')
                if self.__check_if_glossary_term_description_changed(glossary_term, glossary_term_descriptions):
                    self.__logger.info(f'Updating glossary term \'{glossary_term_name}\' with descriptions \'{glossary_term_descriptions}\'')
                    self.__smus_adapter.update_glossary_term_description(glossary_term['id'], glossary_term_descriptions)
                else:
                    self.__logger.info(f'Update to glossary term \'{glossary_term_name}\' not required.')

            else:
                self.__logger.info(f'Creating glossary term \'{glossary_term_name}\' with descriptions \'{glossary_term_descriptions}\'')
                self.__smus_adapter.create_glossary_term(self.__glossary_id, glossary_term_name, glossary_term_descriptions)
            terms_created.add(glossary_term_name)
        return new_last_seen_id

    def __check_if_glossary_term_description_changed(self, glossary_term, new_description):
        has_description_changed = False
        if new_description and len(new_description) == 1 and 'shortDescription' in glossary_term and glossary_term['shortDescription'] != new_description[0]:
            has_description_changed = True
        elif new_description and len(new_description) > 1 and 'longDescription' in glossary_term and glossary_term['longDescription'] != "\n\n".join(new_description):
            has_description_changed = True
        elif 'shortDescription' not in glossary_term and 'longDescription' not in glossary_term and new_description != [] and new_description is not None:
            has_description_changed = True
        return has_description_changed