# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import argparse
import json
import sys
from datetime import datetime, timezone
from time import sleep, time

import boto3

CONSOLE_TRANSPORT_TEXT = "{{console.py:29}} INFO - "
CONSOLE_TRANSPORT_TEXT_LEN = len(CONSOLE_TRANSPORT_TEXT)
JSON_BEGINS = "{"
JSON_ENDS = "}"
WAIT_TIME_SECONDS = 15


def post_run_event(datazone_client, domain_identifier, run_event, parsed_run_event):
    print("\n  Posting data lineage for:")
    print(f"    Run ID:     {parsed_run_event['run']['runId']}")
    print(f"    Event type: {parsed_run_event['eventType']}")
    print(f"    Event time: {parsed_run_event['eventTime']}")
    print(f"    Job name:   {parsed_run_event['job']['name']}")
    try:
        datazone_client.post_lineage_event(domainIdentifier=domain_identifier, event=run_event)
        print("  Succeeded.")
    except KeyboardInterrupt:
        raise
    except Exception:
        print("\n   Error calling PostLineageEvent with data lineage:\n{run_event}\n")
        raise


def process_partial_run_event(
    logs_client, datazone_client, domain_identifier, log_group_name, log_event, partial_run_event
):
    run_event = partial_run_event
    paginator = logs_client.get_paginator("filter_log_events")
    # The RunEvent log event parts follow the first log event part.
    # Include events up to 100ms after the first part.
    page_iterator = paginator.paginate(
        logGroupName=log_group_name,
        startTime=log_event["timestamp"],
        endTime=log_event["timestamp"] + 100,
    )
    after_first_part_index = None
    for page in page_iterator:
        # Find and skip over the log event for the first part.
        events = page["events"]
        events_count = len(events)
        if after_first_part_index is None:
            for i in range(events_count):
                if log_event["eventId"] == events[i]["eventId"]:
                    after_first_part_index = i + 1
                    break
        else:
            after_first_part_index = 0

        if after_first_part_index is not None:
            for i in range(after_first_part_index, events_count):
                console_msgs = events[i]["message"].split("\n")
                for console_msg in console_msgs:
                    if not console_msg.endswith(JSON_ENDS):
                        continue
                    run_event += console_msg
                    parsed_run_event = json.loads(run_event)
                    post_run_event(
                        datazone_client=datazone_client,
                        domain_identifier=domain_identifier,
                        run_event=run_event,
                        parsed_run_event=parsed_run_event,
                    )
                    return
    if parsed_run_event is None:
        raise RuntimeError(f"Failed to assemble data lineage parts:\n{run_event}\n")


def process_log_event(logs_client, datazone_client, domain_identifier, log_group_name, log_event):
    console_msgs = log_event["message"].split("\n")
    for console_msg in console_msgs:
        run_event_text_pos = console_msg.find(CONSOLE_TRANSPORT_TEXT)
        if run_event_text_pos == -1:
            continue
        run_event_pos = console_msg.find(
            JSON_BEGINS, run_event_text_pos + CONSOLE_TRANSPORT_TEXT_LEN
        )
        if run_event_pos == -1:
            continue
        run_event = console_msg[run_event_pos:]
        parsed_run_event = None
        try:
            parsed_run_event = json.loads(run_event)
            post_run_event(
                datazone_client=datazone_client,
                domain_identifier=domain_identifier,
                run_event=run_event,
                parsed_run_event=parsed_run_event,
            )
        except json.JSONDecodeError:
            process_partial_run_event(
                logs_client=logs_client,
                datazone_client=datazone_client,
                domain_identifier=domain_identifier,
                log_group_name=log_group_name,
                log_event=log_event,
                partial_run_event=run_event,
            )


# Returns the ISO format with milliseconds (3 digits), not microseconds (6 digits)
# precision for the startTime parameter to the Amazon CloudWatch filter_log_events() API.
def start_time_to_iso_format(timestamp):
    return datetime.fromtimestamp(int(timestamp * 1_000) / 1_000, timezone.utc).isoformat(
        timespec="milliseconds"
    )


def extract_and_post_lineage(
    session, datazone_endpoint_url, domain_identifier, log_group_name, start_time
):
    logs_client = session.client(service_name="logs")
    datazone_client = session.client(service_name="datazone", endpoint_url=datazone_endpoint_url)

    start_time_seconds = datetime.fromisoformat(start_time).timestamp()

    try:
        while True:
            print("\nSearching for data lineage...")
            polling_time_seconds = time()

            paginator = logs_client.get_paginator("filter_log_events")
            page_iterator = paginator.paginate(
                logGroupName=log_group_name,
                filterPattern=f'"{CONSOLE_TRANSPORT_TEXT}"',
                startTime=int(start_time_seconds * 1_000),
            )
            events_found = False
            for page in page_iterator:
                log_events = page["events"]
                if log_events:
                    print(f"\nFound {len(log_events)} log events that contain data lineage.")
                    events_found = True
                    # Start the next search after the last log event.
                    start_time_seconds = (log_events[-1]["timestamp"] + 1) / 1_000
                    for log_event in log_events:
                        process_log_event(
                            logs_client=logs_client,
                            datazone_client=datazone_client,
                            domain_identifier=domain_identifier,
                            log_group_name=log_group_name,
                            log_event=log_event,
                        )
            if not events_found:
                # No events found - start the next search from the last polling time.
                print("No data lineage found.")
                start_time_seconds = polling_time_seconds
                print(f"\nPausing for {WAIT_TIME_SECONDS} seconds (CTRL-C to quit)...")
                sleep(WAIT_TIME_SECONDS)
    except KeyboardInterrupt:
        print(f"\nExiting {sys.argv[0]}")
        sys.exit(1)


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
    airflow_environment_name,
    log_group_name,
    datazone_endpoint_url,
    domain_identifier,
    start_time,
):
    if session.region_name is None:
        print(f"\n{sys.argv[0]}: error: the following arguments are required: -r/--region")
        exit(1)

    print("\nPlease review the settings for this session.\n")
    print(f"  Profile: {session.profile_name}")
    print(f"  Region:  {session.region_name}\n")

    print_identity(session)

    print("\n  Extracting Amazon MWAA lineage for:\n")
    print(f"    Airflow environment name: {airflow_environment_name}\n")
    print(f"    Log group:  {log_group_name}")
    start_time_iso = start_time_to_iso_format(datetime.fromisoformat(start_time).timestamp())
    print(f"    Start time: {start_time_iso}")

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
        description="Extract data lineage from Amazon MWAA and post it to Amazon DataZone."
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
        "-a",
        "--airflow-environment-name",
        help="The name of the Airflow environment.",
        required=True,
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
    default_start_time = start_time_to_iso_format(time())
    parser.add_argument(
        "-s",
        "--start-time",
        help="The start time for searching the logs in ISO 8601 format. "
        f"The default start time is from now: {default_start_time}.",
        default=default_start_time,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    log_group_name = f"airflow-{args.airflow_environment_name}-Task"
    verify_identity_and_settings(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        airflow_environment_name=args.airflow_environment_name,
        log_group_name=log_group_name,
        start_time=args.start_time,
    )
    extract_and_post_lineage(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        log_group_name=log_group_name,
        start_time=args.start_time,
    )
