"""
Retrieve lineage events from Amazon DataZone within a specified time range.

This script fetches lineage events from a DataZone domain using timestamp filters,
retrieves detailed event data, and saves the results to a JSON file.

Example:
    python3 retrieve_lineage_events.py --region us-east-1 \
        --domain-identifier dzd-abc123xyz \
        --timestamp-after 1742404458 --timestamp-before 1742440527
"""

import boto3
import json
import argparse
import time
import sys

def main():
    """Main function to retrieve and save DataZone lineage events."""
    parser = argparse.ArgumentParser(description="DataZone Lineage Events CLI")
    parser.add_argument("--region", type=str, required=True, help="Region")
    parser.add_argument("--domain-identifier", type=str, required=True, help="Domain identifier")
    parser.add_argument("--timestamp-after", type=int, required=True, help="Timestamp after (epoch format)")
    parser.add_argument("--timestamp-before", type=int, required=True, help="Timestamp before (epoch format)")
    parser.add_argument("--max-results", type=int, default=50, help="Maximum number of results per page")
    args = parser.parse_args()

    # Validate timestamp range
    if args.timestamp_after >= args.timestamp_before:
        print("Error: --timestamp-after must be less than --timestamp-before")
        sys.exit(1)

    print(f"Fetching lineage events from domain {args.domain_identifier}")
    print(f"Time range: {args.timestamp_after} to {args.timestamp_before}")
    print(f"MaxResults: {args.max_results}")

    # Create a session and a client
    session = boto3.Session(region_name=args.region)
    client = session.client('datazone')

    # Make list-lineage-events API call
    params = {
        "domainIdentifier": args.domain_identifier,
        "timestampAfter": args.timestamp_after,
        "timestampBefore": args.timestamp_before,
        "maxResults": args.max_results
    }
    paginator = client.get_paginator('list_lineage_events')
    lineage_events = []
    for page in paginator.paginate(**params):
        if 'items' in page:
            lineage_events.extend(page['items'])

    # Fetch and store lineage events
    lineage_event_data = []
    for idx, event in enumerate(lineage_events, 1):
        event_id = event["id"]
        try:
            event_response = client.get_lineage_event(domainIdentifier=args.domain_identifier, identifier=event_id)
            content = event_response['event'].read()
            # Convert the content to JSON
            json_data = json.loads(content)
            lineage_event_data.append(json_data)
        except Exception as e:
            print(f"Error fetching event {event_id}: {e}")
            continue

    # Check if any events were found
    if not lineage_event_data:
        print("No lineage events found in the specified time range")
        sys.exit(0)

    print(f"Retrieved {len(lineage_event_data)} lineage events")

    # Save lineage event data to a JSON file
    timestamp = int(time.time())
    filename = f"lineage_events_{args.domain_identifier}_{args.region}_{timestamp}.json"
    with open(filename, "w") as f:
        json.dump(lineage_event_data, f, indent=4)

    print(f"Lineage event data saved to {filename}")


if __name__ == "__main__":
    main()
