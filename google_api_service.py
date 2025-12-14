from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

# path to json creds
key_file_location = os.environ["GOOGLE_API_CREDENTIALS_PATH"]

def get_credentials():
    credentials = service_account.Credentials.from_service_account_file(
        key_file_location)
    return credentials

def get_service(api_name, api_version, scopes):

    """Get a service that communicates to a Google API.

    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
    Returns:
        A service that is connected to the specified API.
    """
    
    credentials = get_credentials()
    service = build(api_name, api_version, credentials=credentials, cache_discovery=False)
    
    return service