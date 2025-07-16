from adapter.SMUSAdapter import SMUSAdapter
from utils.collibra_constants import ID_KEY, NAME_KEY
from utils.smus_constants import GLOSSARY_TERM_ITEM_KEY


class SMUSGlossaryCache:
    def __init__(self, logger):
        self.__logger = logger
        self.__smus_adapter = SMUSAdapter(logger)
        self.__glossary_id = self.__smus_adapter.create_or_get_glossary()
        self.__cache = {}
        self.__load()

    def is_term_present(self, term_name):
        return term_name in self.__cache

    def get_smus_term_id(self, term_name):
        return self.__cache.get(term_name, None)

    def __load(self):
        self.__logger.info("Loading SMUS glossary cache")

        terms = self.__smus_adapter.list_all_terms_in_glossary(self.__glossary_id)

        for term in terms:
            self.__cache[term[GLOSSARY_TERM_ITEM_KEY][NAME_KEY]] = term[GLOSSARY_TERM_ITEM_KEY][ID_KEY]

        self.__logger.info(f"Loaded SMUS glossary cache with {len(terms)} terms")