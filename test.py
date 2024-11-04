import hashlib
import requests
from requests.auth import HTTPBasicAuth
import json

# JIRA API configuration
JIRA_BASE_URL = "https://your_jira_instance_url/rest/api/2"
USERNAME = "your_username"
API_TOKEN = "your_api_token"  # Use API token instead of password
DATA_FILE = "jira_issues.json"

def calculate_cksum(error_message):
    """Calculate a checksum for the given error message."""
    return hashlib.md5(error_message.encode()).hexdigest()  # Using MD5 for simplicity

def read_data_file():
    """Read transaction ID, issue key, and checksum data from the JSON file."""
    try:
        with open(DATA_FILE, 'r') as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def write_data_file(data):
    """Write transaction ID, issue key, and checksum data to the JSON file."""
    with open(DATA_FILE, 'w') as file:
        json.dump(data, file, indent=4)

def process_transactions(transactions):
    """
    Process multiple transactions, creating or updating JIRA tickets as necessary.
    
    Parameters:
        transactions (list of tuples): Each tuple contains (unique_id, error_message).
    """
    # Step 1: Load existing data into memory
    data = read_data_file()

    # Step 2: Process each transaction and update the in-memory data structure
    for unique_id, error_message in transactions:
        manage_jira_ticket(unique_id, error_message, data)

    # Step 3: Write the updated data to the file once, after processing all transactions
    write_data_file(data)
    print("All transactions processed and data file updated.")

def manage_jira_ticket(unique_id, error_message, data):
    """
    Create, update, or close a JIRA ticket based on the unique ID and error message.

    Parameters:
        unique_id (str): The unique transaction ID.
        error_message (str or None): The error message to be added, or None to close the ticket.
        data (dict): In-memory data dictionary for transaction details.
    """
    # Define headers and authentication
    headers = {
        "Content-Type": "application/json"
    }
    auth = HTTPBasicAuth(USERNAME, API_TOKEN)

    # Calculate checksum of the current error message
    current_cksum = calculate_cksum(error_message) if error_message else None

    # Check if there's an existing record for this transaction ID in the data
    if unique_id in data:
        issue_key = data[unique_id]["issue_key"]
        stored_cksum = data[unique_id]["cksum"]

        if error_message:
            if current_cksum != stored_cksum:
                # If checksums differ, add a comment instead of updating the description
                comment_url = f"{JIRA_BASE_URL}/issue/{issue_key}/comment"
                comment_payload = {
                    "body": f"Updated Error Message:\n{error_message}"
                }
                comment_response = requests.post(comment_url, headers=headers, auth=auth, json=comment_payload)
                comment_response.raise_for_status()
                print(f"Comment added to JIRA ticket {issue_key} with new error message.")

                # Update checksum in the in-memory data dictionary
                data[unique_id]["cksum"] = current_cksum
            else:
                print(f"No changes detected in the error message for transaction {unique_id}.")
        else:
            # If error_message is None, close the JIRA ticket
            transition_url = f"{JIRA_BASE_URL}/issue/{issue_key}/transitions"
            transition_payload = {
                "transition": {
                    "id": "31"  # Replace with the appropriate transition ID for "Done" or "Closed"
                }
            }
            close_response = requests.post(transition_url, headers=headers, auth=auth, json=transition_payload)
            close_response.raise_for_status()
            print(f"JIRA ticket {issue_key} closed.")
            # Remove the entry from the in-memory data dictionary
            del data[unique_id]

    elif error_message:
        # If no existing record and error_message is provided, create a new JIRA ticket
        create_issue_url = f"{JIRA_BASE_URL}/issue"
        create_payload = {
            "fields": {
                "project": {
                    "key": "YOUR_PROJECT_KEY"
                },
                "summary": f"Issue for Transaction ID: {unique_id}",
                "description": error_message,
                "issuetype": {
                    "name": "Bug"
                }
            }
        }
        create_response = requests.post(create_issue_url, headers=headers, auth=auth, json=create_payload)
        create_response.raise_for_status()
        new_issue_key = create_response.json()["key"]
        print(f"New JIRA ticket created: {new_issue_key}")

        # Add the new entry to the in-memory data dictionary
        data[unique_id] = {"issue_key": new_issue_key, "cksum": current_cksum}

    else:
        print("No action taken as there is no existing ticket and error message is None.")

# Example usage with multiple transactions
transactions = [
    ("trans-1234", "Error occurred while processing transaction."),
    ("trans-5678", "A new type of error occurred."),
    ("trans-1234", "Additional error details found."),
    ("trans-5678", None),  # This should close the ticket for trans-5678
]
process_transactions(transactions)
