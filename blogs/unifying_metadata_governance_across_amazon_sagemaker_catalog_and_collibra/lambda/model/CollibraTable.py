from typing import List

from business.SMUSGlossaryCache import SMUSGlossaryCache
from utils.collibra_constants import INCOMING_RELATIONS_KEY, SOURCE_KEY, DISPLAY_NAME_KEY, NAME_KEY
from utils.common_utils import extract_collibra_descriptions


class CollibraBusinessTerm:
    def __init__(self, business_term: dict, smus_term_id: str):
        self.name = business_term[DISPLAY_NAME_KEY]
        self.smus_term_id = smus_term_id

class CollibraColumn:
    DESCRIPTION_SIZE_LIMIT = 4096

    def __init__(self, column: dict, smus_glossary_cache: SMUSGlossaryCache):
        self.name = column[DISPLAY_NAME_KEY]
        self.business_terms = self.__create_business_terms(column, smus_glossary_cache)
        self.description = ",".join(extract_collibra_descriptions(column))[:CollibraColumn.DESCRIPTION_SIZE_LIMIT]

    def get_business_term_ids(self):
        return [term.smus_term_id for term in self.business_terms]

    @classmethod
    def __create_business_terms(cls, column: dict, smus_glossary_cache: SMUSGlossaryCache):
        business_terms = list()
        if INCOMING_RELATIONS_KEY in column:
            for business_term in column[INCOMING_RELATIONS_KEY]:
                if SOURCE_KEY in business_term and smus_glossary_cache.is_term_present(business_term[SOURCE_KEY][DISPLAY_NAME_KEY]):
                    business_terms.append(CollibraBusinessTerm(business_term[SOURCE_KEY], smus_glossary_cache.get_smus_term_id(business_term[SOURCE_KEY][DISPLAY_NAME_KEY])))
        return business_terms


class CollibraTable:
    DESCRIPTION_SIZE_LIMIT = 2048

    def __init__(self, table: dict, get_table_business_terms_response: dict, get_pii_columns_response: dict, smus_asset_ids: List[str], smus_glossary_cache: SMUSGlossaryCache):
        self.smus_asset_ids = smus_asset_ids
        self.name = table[DISPLAY_NAME_KEY]
        self.columns = self.__create_columns(table, smus_glossary_cache)
        self.description = ",".join(extract_collibra_descriptions(table))[:CollibraTable.DESCRIPTION_SIZE_LIMIT]
        self.business_terms = self.__create_business_terms(get_table_business_terms_response, smus_glossary_cache)
        self.pii_columns = self.__extract_pii_columns(get_pii_columns_response)

    def get_business_term_ids(self):
        return [term.smus_term_id for term in self.business_terms]

    @classmethod
    def __create_business_terms(cls, get_table_business_term_response: dict, smus_glossary_cache: SMUSGlossaryCache):
        business_terms = list()
        if INCOMING_RELATIONS_KEY in get_table_business_term_response:
            for business_term in get_table_business_term_response[INCOMING_RELATIONS_KEY]:
                if SOURCE_KEY in business_term and smus_glossary_cache.is_term_present(business_term[SOURCE_KEY][DISPLAY_NAME_KEY]):
                    business_terms.append(CollibraBusinessTerm(business_term[SOURCE_KEY], smus_glossary_cache.get_smus_term_id(business_term[SOURCE_KEY][DISPLAY_NAME_KEY])))
        return business_terms

    @classmethod
    def __extract_pii_columns(cls, get_pii_columns_response: dict):
        pii_columns = list()
        if INCOMING_RELATIONS_KEY in get_pii_columns_response:
            for column in get_pii_columns_response[INCOMING_RELATIONS_KEY]:
                if SOURCE_KEY in column:
                    if INCOMING_RELATIONS_KEY in column[SOURCE_KEY]:
                        for business_term in column[SOURCE_KEY][INCOMING_RELATIONS_KEY]:
                            if SOURCE_KEY in business_term:
                                if INCOMING_RELATIONS_KEY in business_term[SOURCE_KEY] and business_term[SOURCE_KEY][
                                    INCOMING_RELATIONS_KEY] != []:
                                    pii_columns.append(column[SOURCE_KEY][DISPLAY_NAME_KEY])
        return pii_columns

    @classmethod
    def __create_columns(cls, table: dict, smus_glossary_cache: SMUSGlossaryCache):
        columns = dict()
        if INCOMING_RELATIONS_KEY in table:
            for column in table[INCOMING_RELATIONS_KEY]:
                if SOURCE_KEY in column:
                    collibra_column = CollibraColumn(column[SOURCE_KEY], smus_glossary_cache)
                    columns[collibra_column.name] = collibra_column
        return columns