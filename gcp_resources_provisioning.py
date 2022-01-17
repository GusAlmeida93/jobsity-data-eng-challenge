from google.cloud import storage
from google.oauth2 import service_account
import googleapiclient.discovery
import os

credentials = service_account.Credentials.from_service_account_file(
    filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
    scopes=['https://www.googleapis.com/auth/cloud-platform'])
service = googleapiclient.discovery.build(
    'iam', 'v1', credentials=credentials)


def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the US region with the coldline storage
    class
    """
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

def create_role(name, project, title, description, permissions, stage):
    """Creates a role."""

    # pylint: disable=no-member
    role = service.projects().roles().create(
        parent='projects/' + project,
        body={
            'roleId': name,
            'role': {
                'title': title,
                'description': description,
                'includedPermissions': permissions,
                'stage': stage
            }
        }).execute()

    print('Created role: ' + role['name'])
    return role

def create_service_account(project_id, name, display_name):
    """Creates a service account."""

    credentials = service_account.Credentials.from_service_account_file(
        filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
        scopes=['https://www.googleapis.com/auth/cloud-platform'])

    service = googleapiclient.discovery.build(
        'iam', 'v1', credentials=credentials)

    my_service_account = service.projects().serviceAccounts().create(
        name='projects/' + project_id,
        body={
            'accountId': name,
            'serviceAccount': {
                'displayName': display_name
            }
        }).execute()

    print('Created service account: ' + my_service_account['email'])
    return my_service_account