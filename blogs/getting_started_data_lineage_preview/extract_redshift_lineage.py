# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import argparse
import json
import sys
from getpass import getpass

import boto3
import redshift_connector
from openlineage_sql import parse as parse_sql

RUN_START_EVENT_TEMPLATE_JSON = """
    {
        "eventType": "START",
        "eventTime": "{start_time}",
        "run": {
            "runId": "{run_id}"
            },
        "job": {
            "namespace": "{namespace}",
            "name": "{job_name}",
            "facets": {
            "sql": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/sql",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SQLJobFacet",
                "query": "{query_text}"
            },
            "jobType": {
                "integration": "Redshift",
                "jobType": "Query",
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json"
            }
            }
        },
        "inputs": "{input_nodes}",
        "outputs": "{output_nodes}",
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
    }
    """

RUN_COMPLETE_EVENT_TEMPLATE_JSON = """
    {
        "eventType": "COMPLETE",
        "eventTime": "{end_time}",
        "run": {
            "runId": "{run_id}"
            },
        "job": {
            "namespace": "{namespace}",
            "name": "{job_name}",
            "facets": {
            "sql": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/sql",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SQLJobFacet",
                "query": "{query_text}"
            },
            "jobType": {
                "integration": "Redshift",
                "jobType": "Query",
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json"
            }
            }
        },
        "inputs": "{input_nodes}",
        "outputs": "{output_nodes}",
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
    }
"""


class ColumnLevelLineageFacet:
    def __init__(self, col_lineage, table_to_schema_db, namespace):
        self.fields = self.build_col_lineage_fields(col_lineage, table_to_schema_db, namespace)

    def build_col_lineage_fields(self, col_lineage, table_to_schema_db, namespace):
        fields = {}

        for col_lineage_record in col_lineage:
            dest_col = col_lineage_record.descendant.name
            source_cols = {"inputFields": []}
            for src_col_meta in col_lineage_record.lineage:
                table = src_col_meta.origin.name
                database = (
                    table_to_schema_db[table]["db"]
                    if src_col_meta.origin.database is None
                    else src_col_meta.origin.database
                )
                schema = (
                    table_to_schema_db[table]["schema"]
                    if src_col_meta.origin.schema is None
                    else src_col_meta.origin.schema
                )
                name = f"{database}.{schema}.{table}"
                source_cols["inputFields"].append(
                    {"namespace": namespace, "name": name, "field": src_col_meta.name}
                )
            fields[dest_col] = source_cols
        return fields

    def get_col_lineage_facet(self):
        if not self.fields:
            return None
        return {
            "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.4.1/integration/",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
            "fields": self.fields,
        }


def unescape_query(query):
    return bytes(query, "utf-8").decode("unicode_escape")


def extract_queries_and_post_lineage(
    session,
    datazone_endpoint_url,
    domain_identifier,
    host_name,
    port,
    database_name,
    start_time,
    user,
    password,
):
    conn = redshift_connector.connect(
        host=host_name, database=database_name, port=port, user=user, password=password
    )

    cursor = conn.cursor()

    query = """
    SELECT DISTINCT sti.database AS database, sti.schema AS schema, sti.table AS table, sti.table_type,
           qh.user_id AS user_id, qh.query_id AS query_id, qh.transaction_id AS transaction_id,
           qh.session_id AS session_id, qh.start_time AS start_time, qh.end_time AS end_time,
           query_txt
    FROM SYS_QUERY_HISTORY qh
    -- concatenate query_text if it's length > 4K characters
    LEFT JOIN (
        SELECT query_id, LISTAGG(RTRIM(text)) WITHIN GROUP (ORDER BY sequence) AS query_txt
        FROM sys_query_text
        WHERE sequence < 16
        GROUP BY query_id
    ) qt ON qh.query_id = qt.query_id
    -- to get table_id
    LEFT JOIN sys_query_detail qd ON qh.query_id = qd.query_id
    -- get table details
    JOIN (
        SELECT database, schema, table_id, "table", table_type
        FROM svv_table_info sti
        INNER JOIN svv_tables st ON sti.table = st.table_name
        AND sti.database = st.table_catalog
        AND sti.schema = st.table_schema
    ) sti ON qd.table_id = sti.table_id
    WHERE query_type IN ('DDL', 'CTAS', 'COPY', 'INSERT', 'UNLOAD')
    """

    if start_time:
        query = query + f" AND qh.start_time >= '{start_time}'"

    cursor.execute(query)
    results = cursor.fetchall()

    if not results:
        print("\n  No Amazon Redshift queries found, no data lineage to extract.\n")
        return

    print(
        f"\n  Found {len(results)} Amazon Redshift {'query' if len(results) == 1 else 'queries'}.\n"
    )

    datazone_client = session.client(service_name="datazone", endpoint_url=datazone_endpoint_url)

    for res in results:
        query_result = {
            "database": res[0].strip(),
            "schema": res[1].strip(),
            "table": res[2].strip(),
            "table_type": res[3].strip(),
            "user_id": res[4],
            "query_id": res[5],
            "transaction_id": res[6],
            "session_id": res[7],
            "start_time": res[8].isoformat()[:-3] + "Z",
            "end_time": res[9].isoformat()[:-3] + "Z",
            "query_txt": unescape_query(res[10].strip()),
        }
        print("\n  Processing data lineage for query: {query_result['query_txt']}")
        build_open_lineage_event(
            datazone_client=datazone_client,
            domain_identifier=domain_identifier,
            host_name=host_name,
            port=port,
            query=query_result,
            cursor=cursor,
        )

    cursor.close()
    conn.close()


def post_lineage_events(datazone_client, domain_identifier, events):
    for event in events:
        print("\n  Posting data lineage for:")
        print(f"    Run ID:     {event['run']['runId']}")
        print(f"    Event type: {event['eventType']}")
        print(f"    Event time: {event['eventTime']}")
        print(f"    Job name:   {event['job']['name']}")
        datazone_client.post_lineage_event(
            domainIdentifier=domain_identifier, event=json.dumps(event)
        )
        print("  Succeeded.")


def build_open_lineage_event(datazone_client, domain_identifier, host_name, port, query, cursor):
    lineage = parse_query(query)

    if not lineage:
        return

    namespace = f"redshift://{host_name}:{port}"
    table_to_schema_db = {}
    input_nodes = build_nodes(lineage.in_tables, query, table_to_schema_db, cursor, namespace)
    output_nodes = build_nodes(lineage.out_tables, query, table_to_schema_db, cursor, namespace)
    col_lineage_facet = ColumnLevelLineageFacet(
        lineage.column_lineage, table_to_schema_db, namespace
    ).get_col_lineage_facet()

    # Assuming only one output node exists in the query
    # OpenLineage limitation: the column_lineage generated by open-lineage parser doesn't provide which destination table the column belongs to
    if col_lineage_facet:
        output_nodes[0]["facets"]["columnLineage"] = col_lineage_facet

    run_id = f"{query['query_id']}#{query['transaction_id']}#{query['session_id']}"
    job_name = (
        "redshift-query-" + run_id
    )  # Using same runId value here as well as there is no "job-name" for the query

    actual_values = {
        "{start_time}": query["start_time"],
        "{end_time}": query["end_time"],
        "{run_id}": run_id,
        "{namespace}": namespace,
        "{job_name}": job_name,
        "{query_text}": query["query_txt"],
    }

    run_start_event = replace_placeholders(json.loads(RUN_START_EVENT_TEMPLATE_JSON), actual_values)
    run_complete_event = replace_placeholders(
        json.loads(RUN_COMPLETE_EVENT_TEMPLATE_JSON), actual_values
    )

    run_start_event["inputs"] = input_nodes
    run_start_event["outputs"] = output_nodes
    run_complete_event["inputs"] = input_nodes
    run_complete_event["outputs"] = output_nodes

    post_lineage_events(
        datazone_client=datazone_client,
        domain_identifier=domain_identifier,
        events=[run_start_event, run_complete_event],
    )


def parse_query(query):
    query_txt = query["query_txt"]
    try:
        lineage = parse_sql(sql=[query_txt], dialect="redshift")
        return lineage
    except Exception as e:
        print(f"Failed to parse SQL: {query_txt}, error: {e}")
        return None


def build_nodes(tables, query, table_to_schema_db, cursor, namespace):
    nodes = []
    for tablemeta in tables:
        database = query["database"] if tablemeta.database is None else tablemeta.database
        schema = query["schema"] if tablemeta.schema is None else tablemeta.schema
        table = tablemeta.name

        node_name = f"{database}.{schema}.{table}.{query['table_type']}"
        schema_facet = build_table_schema_facet(database, schema, table, cursor)
        nodes.append(
            {"namespace": namespace, "name": node_name, "facets": {"schema": schema_facet}}
        )
        table_to_schema_db[table] = {"db": database, "schema": schema}
    return nodes


def build_table_schema_facet(database, schema, table, cursor):
    # Get all columns of the table
    cursor.execute(
        f"""
    SELECT column_name, data_type FROM svv_columns
    WHERE table_catalog='{database}' AND table_schema='{schema}' AND table_name='{table}'
    """
    )

    result = cursor.fetchall()
    return {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/sql",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
        "fields": [{"name": res[0].strip(), "type": res[1].strip()} for res in result],
    }


def replace_placeholders(data, values):
    if isinstance(data, dict):
        return {k: replace_placeholders(v, values) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_placeholders(item, values) for item in data]
    elif isinstance(data, str):
        for placeholder, actual_value in values.items():
            data = data.replace(placeholder, actual_value)
        return data
    return data


def print_identity(session):
    # Print information about the caller's identity if the caller has
    # permission to call: iam.list_account_aliases, sts.get_caller_identity.
    try:
        iam_client = session.client(service_name="iam")
        account_alias = iam_client.list_account_aliases()["AccountAliases"][0]
    except Exception:
        account_alias = "-"
    try:
        sts_client = session.client(service_name="sts")
        caller_identity = sts_client.get_caller_identity()
        account_id = caller_identity["Account"]
        user_id = caller_identity["UserId"]
        user_arn = caller_identity["Arn"]
    except Exception:
        account_id = user_id = user_arn = "-"
    print("  IAM identity:\n")
    print(f"    Account alias: {account_alias}")
    print(f"    Account Id:    {account_id}")
    print(f"    User Id:       {user_id}")
    print(f"    ARN:           {user_arn}")


def verify_identity_and_settings(
    session,
    datazone_endpoint_url,
    domain_identifier,
    host_name,
    port,
    database_name,
    start_time,
    user,
):
    if session.region_name is None:
        print(f"\n{sys.argv[0]}: error: the following arguments are required: -r/--region")
        exit(1)

    print("\nPlease review the settings for this session.\n")
    print(f"  Profile: {session.profile_name}")
    print(f"  Region:  {session.region_name}\n")

    print_identity(session)

    print("\n  Extracting Amazon Redshift data lineage from:\n")
    print(f"    Host:       {host_name}")
    print(f"    Port:       {port}")
    print(f"    Database:   {database_name}")
    if start_time is None:
        print(f"    Start time: {start_time} - All queries will be processed.")
    else:
        print(
            f"    Start time: {start_time} - Only queries started on or after this time will be processed."
        )
    print(f"    User:       {user}")

    print("\n  Posting data lineage to Amazon DataZone:\n")
    print(
        f"    Endpoint:  {session.client(service_name='datazone', endpoint_url=datazone_endpoint_url).meta.endpoint_url}"
    )
    print(f"    Domain Id: {domain_identifier}")

    user_input = input("\nAre the settings above correct? (yes/no): ")
    if not user_input.lower() == "yes":
        print(f'Exiting. You entered "{user_input}", enter "yes" to continue.')
        exit(0)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Extract data lineage from Amazon Redshift and post it to Amazon DataZone."
    )
    parser.add_argument(
        "-p",
        "--profile",
        help="Use a specific profile from your credential file.",
    )
    parser.add_argument(
        "-r",
        "--region",
        help="The region to use. Overrides config/env settings.",
    )
    parser.add_argument(
        "-e",
        "--datazone-endpoint-url",
        help="The Amazon DataZone endpoint URL to use. Overrides the default endpoint URL for the region.",
    )
    parser.add_argument(
        "-i",
        "--domain-identifier",
        help="The identifier for the Amazon DataZone domain where data lineage is stored.",
        required=True,
    )
    parser.add_argument("-n", "--host-name", help="The Amazon Redshift host name.", required=True)
    parser.add_argument(
        "-t",
        "--port",
        help="The Amazon Redshift host port number (default is 5439).",
        required=False,
        default="5439",
    )
    parser.add_argument(
        "-d", "--database-name", help="The Amazon Redshift database name.", required=True
    )
    parser.add_argument(
        "-s",
        "--start-time",
        help='The start time for data lineage extraction. Supported formats: "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" (UTC)',
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)

    user = input("\nEnter your Amazon Redshift connection user name: ")
    password = getpass("Enter your Amazon Redshift connection password: ")

    verify_identity_and_settings(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        host_name=args.host_name,
        port=args.port,
        database_name=args.database_name,
        start_time=args.start_time,
        user=user,
    )
    extract_queries_and_post_lineage(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        host_name=args.host_name,
        port=args.port,
        database_name=args.database_name,
        start_time=args.start_time,
        user=user,
        password=password,
    )
