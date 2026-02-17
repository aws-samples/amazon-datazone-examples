from adapter.CollibraAdapter import CollibraAdapter
from adapter.SMUSAdapter import SMUSAdapter
from business.SMUSGlossaryCache import SMUSGlossaryCache
from model.BusinessTermHierarchyIndex import BusinessTermHierarchyIndex
from utils.collibra_constants import DISPLAY_NAME_KEY, INCOMING_RELATIONS_KEY, SOURCE_KEY

class GlossaryTermHierarchyEstablisherBusinessLogic:
    def __init__(self, logger):
        self.__logger = logger
        self.__smus_adapter = SMUSAdapter(logger)
        self.__collibra_adapter = CollibraAdapter(logger)
        self.__smus_glossary_cache = SMUSGlossaryCache(logger)
        self.__business_term_hierarchy_index = BusinessTermHierarchyIndex(self.__smus_glossary_cache)
        self.__glossary_id = self.__smus_adapter.create_or_get_glossary()

    def establish(self):
        self.__logger.info(f"Fetching business term hierarchy from Collibra")

        get_business_term_hierarchy_response = self.__collibra_adapter.get_business_term_hierarchy()

        self.__logger.info(
            f"Fetched business term hierarchy from Collibra. Indexing {len(get_business_term_hierarchy_response)} terms")

        self.__populate_hierarchy_index(get_business_term_hierarchy_response)
        terms_names_to_update = self.__business_term_hierarchy_index.get_indexed_term_names()

        self.__logger.info(f"Initiating glossary term relation identification {len(terms_names_to_update)} terms")

        num_of_terms_updated = 0
        for term_name in terms_names_to_update:
            term_relations = self.__business_term_hierarchy_index.get_term_relations(term_name)

            if not term_relations:
                continue

            term_id = self.__smus_glossary_cache.get_smus_term_id(term_name)

            self.__logger.info(f"Updating glossary term relations for {term_name} in glossary {self.__glossary_id}")
            self.__smus_adapter.update_glossary_term_relations(self.__glossary_id, term_id, term_name, term_relations)
            num_of_terms_updated += 1

        self.__logger.info(f"Updated glossary term relations for {num_of_terms_updated} terms")

    def __populate_hierarchy_index(self, get_business_term_hierarchy_response):
        for business_term_hierarchy in get_business_term_hierarchy_response:
            child_term_name = business_term_hierarchy[DISPLAY_NAME_KEY]
            if INCOMING_RELATIONS_KEY in business_term_hierarchy:
                for incoming_relation in business_term_hierarchy[INCOMING_RELATIONS_KEY]:
                    if SOURCE_KEY in incoming_relation:
                        parent_term_name = incoming_relation[SOURCE_KEY][DISPLAY_NAME_KEY]
                        self.__business_term_hierarchy_index.index(child_term_name, parent_term_name)
