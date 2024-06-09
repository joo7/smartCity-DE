from job.aws_helper import get_aws_access_and_secret_keys
from job.config import aws_profile

ACCESS_KEY, SECRET_KEY = get_aws_access_and_secret_keys(aws_profile)

print(ACCESS_KEY, SECRET_KEY)
