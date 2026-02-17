import json
from datetime import timedelta, datetime
from time import time
from typing import List, Dict

from adapter.CollibraAdapter import CollibraAdapter
from adapter.SMUSAdapter import SMUSAdapter
from business.CollibraSMUSAssetMatcher import CollibraSMUSAssetMatcher
from business.SMUSGlossaryCache import SMUSGlossaryCache
from model.CollibraTable import CollibraTable, CollibraColumn
from utils.collibra_constants import DISPLAY_NAME_KEY, FULL_NAME_KEY, ID_KEY
from utils.smus_constants import PII_COLUMNS_README_HEADING, GLOSSARY_TERMS_KEY, ASSET_COMMON_DETAILS_FORM, \
    FORM_NAME_KEY, \
    CONTENT_KEY, README_KEY, ASSET_ITEM_KEY, IDENTIFIER_KEY


class AssetMetadataSyncBusinessLogic:
    def __init__(self, logger):
        self.__logger = logger
        self.__smus_adapter = SMUSAdapter(logger)
        self.__collibra_adapter = CollibraAdapter(logger)
        self.__smus_glossary_cache = SMUSGlossaryCache(logger)
        self.__projects_ids = [project['id'] for project in self.__smus_adapter.list_all_projects()]

    def sync(self, last_seen_asset_id: str):
        start_time = datetime.now()
        self.__logger.info(f"{start_time}, {time()}")

        # Keep processing more tables till there are 5 mins left before lambda times out
        while datetime.now() - start_time <= timedelta(minutes=10):
            previous_last_seen_asset_id = last_seen_asset_id
            self.__logger.info(f"Fetching tables from collibra")
            tables, last_seen_asset_id = self.__get_and_filter_out_system_tables(previous_last_seen_asset_id)

            self.__logger.info(f"Previous last seen asset id: {previous_last_seen_asset_id}, current last seen asset id: {last_seen_asset_id}")
            if previous_last_seen_asset_id == last_seen_asset_id or last_seen_asset_id is None:
                break

            self.__logger.info(f"Found {len(tables)} tables to sync in SMUS")
            for table in tables:
                try:
                    self.__logger.info(
                        f"Finding assets corresponding to Collibra table {table[DISPLAY_NAME_KEY]} in SMUS")

                    smus_asset_ids = self.__find_smus_table_asset_ids(table)

                    self.__logger.info(
                        f"Found {len(smus_asset_ids)} assets for collibra table {table[DISPLAY_NAME_KEY]} in SMUS")

                    # If no matching assets in SMUS, skip the table
                    if not smus_asset_ids:
                        self.__logger.info(f"No matching asset found in SMUS with name {table[DISPLAY_NAME_KEY]}. Skipping.")
                        continue

                    self.__logger.info(f"Fetching table data from Collibra for table {table[DISPLAY_NAME_KEY]}.")
                    get_table_response = self.__collibra_adapter.get_table(table['id'])

                    self.__logger.info(f"Fetching business terms attached to table {table[DISPLAY_NAME_KEY]} from Collibra.")
                    get_table_business_terms_response = self.__collibra_adapter.get_table_business_terms(table['id'])

                    self.__logger.info(f"Fetching PII columns of table {table[DISPLAY_NAME_KEY]} from Collibra.")
                    get_pii_columns_response = self.__collibra_adapter.get_pii_columns(table['id'])

                    self.__logger.info(f"Creating CollibraTable internal data structure using fetched data")
                    collibra_table = CollibraTable(get_table_response, get_table_business_terms_response,
                                                   get_pii_columns_response, smus_asset_ids, self.__smus_glossary_cache)

                    self.__logger.info(f"Updating asset with name {table[DISPLAY_NAME_KEY]} in SMUS")

                    self.update_asset_metadata(collibra_table)
                    self.__logger.info(f"Successfully updated asset with name {table[DISPLAY_NAME_KEY]} in SMUS")
                except Exception as e:
                    self.__logger.error(f"Failed to update asset with name {table[DISPLAY_NAME_KEY]}", e)
                    continue

        return last_seen_asset_id

    def __get_and_filter_out_system_tables(self, last_seen_asset_id: str):
        tables = self.__collibra_adapter.get_tables(last_seen_asset_id)
        filtered_tables = []
        for table in tables:
            if 'information_schema' not in table[FULL_NAME_KEY]:
                filtered_tables.append(table)

        if tables:
            last_seen_id = tables[-1][ID_KEY]
        else:
            last_seen_id = None

        return filtered_tables, last_seen_id

    def __find_smus_table_asset_ids(self, table) -> List[str]:
        assets = self.__get_all_assets_by_name(table[DISPLAY_NAME_KEY])
        matching_assets_in_smus = []
        for asset in assets:
            get_asset_response = self.__smus_adapter.get_asset(asset[ASSET_ITEM_KEY][IDENTIFIER_KEY])
            if CollibraSMUSAssetMatcher.match(get_asset_response, table):
                self.__logger.info(
                    f" SMUS asset {get_asset_response["name"]} matched with Collibra asset {table[DISPLAY_NAME_KEY]}")
                matching_assets_in_smus.append(get_asset_response[ID_KEY])
        return matching_assets_in_smus

    def __get_all_assets_by_name(self, asset_name):
        assets = []
        for project_id in self.__projects_ids:
            assets.extend(self.__smus_adapter.search_all_assets_by_name(asset_name, project_id))
        return assets

    @classmethod
    def __create_table_readme_with_data_category_columns(cls, collibra_table: CollibraTable):
        if not collibra_table.pii_columns:
            return None

        readme = PII_COLUMNS_README_HEADING
        for column in collibra_table.pii_columns:
            readme += f"\n* {column}"

        return readme

    def update_asset_metadata(self, collibra_table: CollibraTable):
        for asset_id in collibra_table.smus_asset_ids:
            smus_asset = self.__smus_adapter.get_asset(asset_id)
            glossary_terms = self.__collate_glossary_terms(smus_asset, collibra_table)
            forms_input = self.__get_forms_input(smus_asset)
            self.__update_asset_common_details_form(collibra_table, forms_input)
            self.__add_or_update_column_business_metadata_form(collibra_table, forms_input)

            optional_args = {}
            if collibra_table.description:
                optional_args["description"] = collibra_table.description

            if glossary_terms:
                optional_args["glossaryTerms"] = glossary_terms
            self.__logger.info(f"Updating asset with name: {collibra_table.name} and id: {asset_id}")
            self.__smus_adapter.create_asset_revision(collibra_table.name, asset_id, forms_input, **optional_args)

    @classmethod
    def __collate_glossary_terms(cls, smus_asset, collibra_table: CollibraTable):
        glossary_terms = smus_asset.get(GLOSSARY_TERMS_KEY, [])
        glossary_terms.extend(collibra_table.get_business_term_ids())
        return list(set(glossary_terms))

    @classmethod
    def __get_forms_input(cls, smus_asset) -> list:
        forms_input = []
        for forms_output in smus_asset['formsOutput']:
            forms_output['typeIdentifier'] = forms_output['typeName']
            del forms_output['typeName']
            del forms_output['typeRevision']
            forms_input.append(forms_output)
        return forms_input

    @classmethod
    def __update_asset_common_details_form(cls, collibra_table: CollibraTable, forms_input: list):
        readme_suffix = cls.__create_table_readme_with_data_category_columns(collibra_table)
        if readme_suffix is None:
            return

        for form in forms_input:
            if form[FORM_NAME_KEY] == ASSET_COMMON_DETAILS_FORM:
                content = json.loads(form[CONTENT_KEY])
                existing_readme = content.get(README_KEY, None)
                content[README_KEY] = cls.replace_data_category_from_readme(existing_readme, readme_suffix)
                content = json.dumps(content)
                form[CONTENT_KEY] = content
                break

    @classmethod
    def replace_data_category_from_readme(cls, existing_readme, new_data_category_readme):
        if existing_readme is None:
            return new_data_category_readme

        existing_readme = existing_readme.split(PII_COLUMNS_README_HEADING)[0].strip()
        return f"{existing_readme}\n\n{new_data_category_readme}"

    @classmethod
    def __add_or_update_column_business_metadata_form(cls, collibra_table: CollibraTable, forms_input: list):
        is_column_business_metadata_form_present = False

        for form in forms_input:
            if form['formName'] == 'ColumnBusinessMetadataForm':
                content = json.loads(form['content'])
                content = cls.__update_column_business_metadata_form_content(content, collibra_table.columns)
                content = json.dumps(content)
                form['content'] = content
                is_column_business_metadata_form_present = True

        if not is_column_business_metadata_form_present:
            column_business_metadata_form = cls.__create_column_business_metadata_form(forms_input,
                                                                                       collibra_table.columns)
            if column_business_metadata_form is not None:
                forms_input.append(column_business_metadata_form)

    @classmethod
    def __update_column_business_metadata_form_content(cls, form_content, collibra_columns: Dict[str, CollibraColumn]):
        new_columns = []
        for column in form_content['columnsBusinessMetadata']:
            column_id = column['columnIdentifier']
            if column_id in collibra_columns:
                business_term_ids = collibra_columns[column_id].get_business_term_ids()
                if business_term_ids:
                    column["glossaryTerms"] = business_term_ids

                description = collibra_columns[column_id].description
                if description:
                    column["description"] = description
            new_columns.append(column)
        new_content = {"columnsBusinessMetadata": new_columns}
        return new_content

    @classmethod
    def __create_column_business_metadata_form(cls, forms_input, collibra_columns: Dict[str, CollibraColumn]):
        columns = None
        for form in forms_input:
            if form['formName'] == 'RedshiftTableForm':
                content = json.loads(form['content'])
                columns = [x["columnName"] for x in content["columns"]]
                break

            if form['formName'] == 'GlueTableForm':
                content = json.loads(form['content'])
                columns = [x["columnName"] for x in content["columns"]]
                break

        if not columns:
            return None

        column_business_metadata_form_content = {}
        column_business_metadata = []
        for column in columns:
            column_business_metadata.append({"columnIdentifier": column})
        column_business_metadata_form_content["columnsBusinessMetadata"] = column_business_metadata
        column_business_metadata = cls.__update_column_business_metadata_form_content(
            column_business_metadata_form_content,
            collibra_columns)

        return {
            "content": json.dumps(column_business_metadata),
            "formName": "ColumnBusinessMetadataForm",
            "typeIdentifier": "amazon.datazone.ColumnBusinessMetadataFormType",
        }
