from business.SMUSGlossaryCache import SMUSGlossaryCache


class BusinessTermHierarchyIndex:
    def __init__(self, smus_glossary_cache: SMUSGlossaryCache):
        self.__smus_glossary_cache = smus_glossary_cache
        self.__index = dict()

    def index(self, child_term_name, parent_term_name):
        if (not self.__smus_glossary_cache.is_term_present(child_term_name)
                or not self.__smus_glossary_cache.is_term_present(parent_term_name)):
            return

        child_term_id = self.__smus_glossary_cache.get_smus_term_id(child_term_name)
        parent_term_id = self.__smus_glossary_cache.get_smus_term_id(parent_term_name)

        if child_term_name not in self.__index:
            self.__index[child_term_name] = self.IndexEntry()

        self.__index[child_term_name].add_to_isA(parent_term_id)

        if parent_term_name not in self.__index:
            self.__index[parent_term_name] = self.IndexEntry()

        self.__index[parent_term_name].add_to_classifies(child_term_id)

    def get_indexed_term_names(self):
        return self.__index.keys()

    def get_term_relations(self, term_name):
        if term_name not in self.__index:
            return {}
        return self.__index[term_name].get_entry()

    class IndexEntry:
        __MAX_RELATIONS = 10
        def __init__(self):
            self.__isA = []
            self.__classifies = []

        def add_to_isA(self, term_id):
            self.__isA.append(term_id)

        def add_to_classifies(self, term_id):
            self.__classifies.append(term_id)

        def get_entry(self):
            entry = dict()
            if self.__isA:
                entry['isA'] = self.__isA[:BusinessTermHierarchyIndex.IndexEntry.__MAX_RELATIONS]
            if self.__classifies:
                entry['classifies'] = self.__classifies[:BusinessTermHierarchyIndex.IndexEntry.__MAX_RELATIONS]
            return entry
