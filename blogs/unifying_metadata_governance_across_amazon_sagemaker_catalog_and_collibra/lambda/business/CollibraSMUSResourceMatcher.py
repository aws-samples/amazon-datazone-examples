import json
import re
from abc import abstractmethod, ABC

from aws_lambda_powertools import Logger

from model.AWSGlueMetadataCollibraAttribute import AWSGlueMetadataCollibraAttribute
from model.AWSRedshiftClusterMetadataCollibraAttribute import AWSRedshiftClusterMetadataCollibraAttribute
from model.AWSRedshiftServerlessMetadataCollibraAttribute import AWSRedshiftServerlessMetadataCollibraAttribute
from utils.collibra_constants import FULL_NAME_KEY, STRING_ATTRIBUTES_KEY, AWS_RESOURCE_METADATA_KEY, NAME_KEY, \
    TYPE_KEY, STRING_VALUE_KEY, DISPLAY_NAME_KEY
from utils.smus_constants import REDSHIFT_TYPE_INFIX, TYPE_IDENTIFIER_KEY, \
    GLUE_TYPE_INFIX, REDSHIFT_TABLE_FORM, \
    REDSHIFT_VIEW_FORM, GLUE_TABLE_FORM, ENTITY_TYPE_KEY, STORAGE_TYPE_KEY, REDSHIFT_CLUSTER_STORAGE_TYPE, \
    REDSHIFT_SERVERLESS_STORAGE_TYPE

logger = Logger(service="collibra_smus_resource_matcher")


class CollibraSMUSResourceMatcher(ABC):
    @classmethod
    def match(cls, smus_resource, collibra_asset):
        logger.info(
            f"Checking if SMUS {cls._get_smus_resource_type()} {smus_resource["name"]} matches with Collibra asset {collibra_asset[DISPLAY_NAME_KEY]}")
        match_result = False
        if cls._is_valid_smus_resource(smus_resource):
            aws_resource_metadata_string_value = cls.__find_aws_resource_metadata_attribute(
                collibra_asset)
            aws_resource_metadata = cls.__deserialize_aws_resource_metadata(
                aws_resource_metadata_string_value)

            if not aws_resource_metadata:
                logger.warning(
                    f"Missing AWS Resource Metadata in Collibra asset {collibra_asset[DISPLAY_NAME_KEY]}")
            else:
                if ((TYPE_IDENTIFIER_KEY in smus_resource and REDSHIFT_TYPE_INFIX in smus_resource[TYPE_IDENTIFIER_KEY])
                        or (ENTITY_TYPE_KEY in smus_resource and REDSHIFT_TYPE_INFIX in smus_resource[ENTITY_TYPE_KEY])):
                    match_result = cls.__match_redshift_asset(smus_resource, collibra_asset,
                                                                                      aws_resource_metadata)
                elif ((TYPE_IDENTIFIER_KEY in smus_resource and GLUE_TYPE_INFIX in smus_resource[TYPE_IDENTIFIER_KEY])
                     or (ENTITY_TYPE_KEY in smus_resource and GLUE_TYPE_INFIX in smus_resource[ENTITY_TYPE_KEY])):
                    match_result = cls.__match_glue_asset(smus_resource, collibra_asset,
                                                                                  aws_resource_metadata)

        logger.info(
            f"Match {"" if match_result else "not"} found between SMUS {cls._get_smus_resource_type()} {smus_resource["name"]} and Collibra asset {collibra_asset[DISPLAY_NAME_KEY]}")
        return match_result

    @classmethod
    def __match_redshift_asset(cls, smus_resource, collibra_asset, aws_resource_metadata):
        redshift_form = cls._get_deserialized_form_content_by_name(
            [REDSHIFT_TABLE_FORM, REDSHIFT_VIEW_FORM], smus_resource)

        if not redshift_form:
            logger.warning(
                f"Redshift form not found in SMUS {cls._get_smus_resource_type()} {smus_resource["name"]}")
            return False

        database, schema, table = cls.__extract_redshift_database_schema_table_names(
            collibra_asset)

        if REDSHIFT_CLUSTER_STORAGE_TYPE in redshift_form[STORAGE_TYPE_KEY]:
            redshift_resource_metadata = AWSRedshiftClusterMetadataCollibraAttribute(aws_resource_metadata)
            return cls.__match_redshift_cluster_asset(redshift_form, redshift_resource_metadata,
                                                                              schema, database, table)
        elif REDSHIFT_SERVERLESS_STORAGE_TYPE in redshift_form[STORAGE_TYPE_KEY]:
            redshift_resource_metadata = AWSRedshiftServerlessMetadataCollibraAttribute(aws_resource_metadata)
            return cls.__match_redshift_serverless_asset(redshift_form,
                                                                                 redshift_resource_metadata,
                                                                                 schema, database, table)
        return False

    @classmethod
    def __match_redshift_cluster_asset(cls, redshift_form,
                                       redshift_cluster_resource_metadata: AWSRedshiftClusterMetadataCollibraAttribute,
                                       schema, database, table):
        try:
            smus_resource_cluster_name = redshift_form['redshiftStorage']['redshiftClusterSource']['clusterName']
            if (redshift_cluster_resource_metadata.region == redshift_form['region'] and
                    redshift_cluster_resource_metadata.cluster_name == smus_resource_cluster_name and
                    database == redshift_form['databaseName'] and
                    schema == redshift_form['schemaName'] and
                    table == redshift_form['tableName']
            ):
                return True
        except Exception as ex:
            logger.warning(f"Redshift cluster asset matching encountered an exception: {ex}")

        return False

    @classmethod
    def __match_redshift_serverless_asset(cls, redshift_form,
                                          redshift_serverless_resource_metadata: AWSRedshiftServerlessMetadataCollibraAttribute,
                                          schema, database, table):
        try:
            smus_resource_workgroup_name = redshift_form['redshiftStorage']['redshiftServerlessSource']['workgroupName']
            if (redshift_serverless_resource_metadata.region == redshift_form['region'] and
                    redshift_serverless_resource_metadata.workgroup_name == smus_resource_workgroup_name and
                    redshift_serverless_resource_metadata.account_id == redshift_form['accountId'] and
                    database == redshift_form['databaseName'] and
                    schema == redshift_form['schemaName'] and
                    table == redshift_form['tableName']
            ):
                return True
        except Exception as ex:
            logger.warning(f"Redshift serverless asset matching encountered an exception: {ex}")

        return False

    @classmethod
    def __match_glue_asset(cls, smus_resource, collibra_asset, aws_resource_metadata):
        glue_resource_metadata = AWSGlueMetadataCollibraAttribute(aws_resource_metadata)

        glue_form = cls._get_deserialized_form_content_by_name(
            [GLUE_TABLE_FORM], smus_resource)

        if not glue_form:
            return False

        database, table = cls.__extract_glue_database_table_names(collibra_asset)

        try:
            if (glue_resource_metadata.region == glue_form['region'] and
                    glue_resource_metadata.account_id in glue_form['tableArn'] and
                    database == glue_form['databaseName'] and
                    table == glue_form['tableName']
            ):
                return True
        except Exception as ex:
            logger.warning(f"Glue asset matching encountered an exception: {ex}")

        return False

    @classmethod
    def __find_aws_resource_metadata_attribute(cls, collibra_asset):
        if STRING_ATTRIBUTES_KEY not in collibra_asset:
            return None

        for attribute in collibra_asset[STRING_ATTRIBUTES_KEY]:
            if attribute[TYPE_KEY][NAME_KEY] == AWS_RESOURCE_METADATA_KEY:
                return attribute[STRING_VALUE_KEY]

        return None

    @classmethod
    def __deserialize_aws_resource_metadata(cls, aws_resource_metadata_string_value: str) -> dict[str, str]:
        aws_resource_metadata_string_value = re.sub(r"[“”«»„‟❝❞＂]", '"', aws_resource_metadata_string_value)
        return json.loads(aws_resource_metadata_string_value)

    @staticmethod
    @abstractmethod
    def _get_deserialized_form_content_by_name(form_names, smus_resource):
        pass

    @classmethod
    def __extract_redshift_database_schema_table_names(cls, collibra_asset):
        database, schema, table = collibra_asset[FULL_NAME_KEY].rsplit('>', 3)[1:]
        return database, schema, table

    @classmethod
    def __extract_glue_database_table_names(cls, collibra_asset):
        database, table = collibra_asset[FULL_NAME_KEY].rsplit('>', 2)[1:]
        return database, table

    @staticmethod
    @abstractmethod
    def _get_smus_resource_type():
        pass

    @staticmethod
    @abstractmethod
    def _is_valid_smus_resource(smus_resource):
        pass
