import boto3


def get_aws_access_and_secret_keys(profile):
    # Create a new boto3 session.
    print('Getting creds from aws')
    session = boto3.Session(profile_name=profile)
    # Get the AWS access key and secret key.
    aws_access_key_id = session.get_credentials().access_key
    aws_secret_access_key = session.get_credentials().secret_key
    return aws_access_key_id, aws_secret_access_key
