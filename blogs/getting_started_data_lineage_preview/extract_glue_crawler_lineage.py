# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import argparse
import json
import sys

import boto3


def generate_and_post_lineage(
    session, datazone_endpoint_url, domain_identifier, database_name, table_name
):
    glue_client = session.client(service_name="glue")

    get_table_resp = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    table = get_table_resp["Table"]

    # Currently only supporting basic crawler jobs.
    if "Parameters" not in table or "UPDATED_BY_CRAWLER" not in table["Parameters"]:
        print(f'\nThe table "{database_name}.{table_name}" was not updated by a crawler.')
        print("\nNo data lineage will be created.")
        exit(0)
    print(
        f'\nThe table "{database_name}.{table_name}" was updated by the crawler "{table["Parameters"]["UPDATED_BY_CRAWLER"]}".'
    )
    print("\nCreating data lineage...")

    # Find the crawl that created the table.
    crawls = glue_client.list_crawls(CrawlerName=table["Parameters"]["UPDATED_BY_CRAWLER"])
    matched_crawl = [
        crawl
        for crawl in crawls["Crawls"]
        if (
            crawl["State"] == "COMPLETED"
            and crawl["EndTime"] >= table["CreateTime"]
            and crawl["StartTime"] <= table["CreateTime"]
        )
    ][0]

    # Build the OpenLineage RunEvents
    events = [
        build_s3_to_crawler_lineage_event(table, matched_crawl, session.region_name),
        build_crawler_to_glue_data_catalog_lineage_event(table, matched_crawl, session.region_name),
    ]
    post_lineage_events(
        session=session,
        datazone_endpoint_url=datazone_endpoint_url,
        domain_identifier=domain_identifier,
        events=events,
    )
    print("\nSuccessfully posted the data lineage for the last crawler job run for:\n")
    print(f"  Database: {database_name}")
    print(f"  Table:    {table_name}\n")


def post_lineage_events(session, datazone_endpoint_url, domain_identifier, events):
    datazone_client = session.client(service_name="datazone", endpoint_url=datazone_endpoint_url)
    for event_json in events:
        event = json.loads(event_json)
        print("\n  Posting data lineage for:")
        print(f"    Run ID:     {event['run']['runId']}")
        print(f"    Event type: {event['eventType']}")
        print(f"    Event time: {event['eventTime']}")
        print(f"    Job name:   {event['job']['name']}")
        datazone_client.post_lineage_event(domainIdentifier=domain_identifier, event=event_json)
        print("  Succeeded.")


def build_s3_to_crawler_lineage_event(table, crawl, region):
    inputs = build_dataset_from_table(table)
    job_run = build_job_run_s3_to_crawler(crawl, region)
    return json.dumps(
        {
            "eventTime": crawl["StartTime"].isoformat(),
            "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "eventType": "START",
            "run": job_run,
            "job": {
                "namespace": "glue_crawler",
                "name": table["Parameters"]["UPDATED_BY_CRAWLER"],
                "facets": {
                    "jobType": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
                        "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                        "processingType": "BATCH",
                        "integration": "glue",
                        "jobType": "JOB",
                    }
                },
            },
            "inputs": inputs,
        },
        indent=4,
    )


def build_crawler_to_glue_data_catalog_lineage_event(table, crawl, region):
    outputs = build_dataset_from_table(table, True)
    job_run = build_job_run_crawler_to_glue_data_catalog(crawl, region)
    return json.dumps(
        {
            "eventTime": crawl["EndTime"].isoformat(),
            "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "eventType": "COMPLETE",
            "run": job_run,
            "job": {
                "namespace": "glue_crawler",
                "name": table["Parameters"]["UPDATED_BY_CRAWLER"],
                "facets": {
                    "jobType": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
                        "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                        "processingType": "BATCH",
                        "integration": "glue",
                        "jobType": "JOB",
                    }
                },
            },
            "outputs": outputs,
        },
        indent=4,
    )


def build_job_run_s3_to_crawler(crawl, region):
    run_id = crawl["CrawlId"]
    return {
        "runId": run_id,
        "facets": {
            "environment-properties": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
                "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                "environment-properties": {
                    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/amazon/bin",
                    "AWS_DEFAULT_REGION": f"{region}",
                    "WORKER_TYPE": "Standard",
                    "LANG": "en_US.UTF-8",
                },
            }
        },
    }


def build_job_run_crawler_to_glue_data_catalog(crawl, region):
    run_id = crawl["CrawlId"]
    return {
        "runId": run_id,
        "facets": {
            "environment-properties": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
                "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                "environment-properties": {
                    "GLUE_VERSION": "3.0",
                    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/amazon/bin",
                    "AWS_DEFAULT_REGION": f"{region}",
                    "WORKER_TYPE": "Standard",
                    "GLUE_COMMAND_CRITERIA": "glue_crawler",
                    "LANG": "en_US.UTF-8",
                },
            }
        },
    }


def build_dataset_from_table(table, is_output=False):
    db_name = table["DatabaseName"]
    namespace = "/".join(table["StorageDescriptor"]["Location"].strip("/").split("/")[:-1])
    table_name = table["Name"]

    dataset = {
        "namespace": namespace,
        "name": table["Name"],
        "facets": {
            "schema": convert_colums_to_schema_facets(table["StorageDescriptor"]["Columns"]),
            "symlinks": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                "identifiers": [
                    {"namespace": namespace, "name": f"{db_name}.{table_name}", "type": "TABLE"}
                ],
            },
        },
    }

    if is_output:
        dataset["facets"]["columnLineage"] = construct_column_lineage(
            table["StorageDescriptor"]["Columns"], table["Name"], namespace
        )
    return [dataset]


def convert_colums_to_schema_facets(columns):
    schema = {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
        "fields": [],
    }
    for c in columns:
        schema["fields"].append({"type": c["Type"], "name": c["Name"]})
    return schema


def construct_column_lineage(columns, name, namespace):
    column_lineage = {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.9.1/integration/glue",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
        "fields": {},
    }
    for c in columns:
        column_lineage["fields"][c["Name"]] = {
            "inputFields": [{"namespace": namespace, "name": name, "field": c["Name"]}]
        }
    return column_lineage


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
    session, datazone_endpoint_url, domain_identifier, database_name, table_name
):
    if session.region_name is None:
        print(f"\n{sys.argv[0]}: error: the following arguments are required: -r/--region")
        exit(1)

    print("\nPlease review the settings for this session.\n")
    print(f"  Profile: {session.profile_name}")
    print(f"  Region:  {session.region_name}\n")

    print_identity(session)

    print("\n  Extracting AWS Glue crawler data lineage for:\n")
    print(f"    Database: {database_name}")
    print(f"    Table:    {table_name}")

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
        description="Extract data lineage from AWS Glue crawler job runs and post it to Amazon DataZone."
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
    parser.add_argument("-d", "--database-name", help="The AWS Glue database name.", required=True)
    parser.add_argument("-t", "--table-name", help="The AWS Glue table name.", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    verify_identity_and_settings(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        database_name=args.database_name,
        table_name=args.table_name,
    )
    generate_and_post_lineage(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        database_name=args.database_name,
        table_name=args.table_name,
    )
