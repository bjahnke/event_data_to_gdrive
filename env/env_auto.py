"""
Desc:
env_auto.py is generated from .env by the `invoke buildenvpy` task.
it's purpose is to provide IDE support for environment variables.
"""

import os
from dotenv import load_dotenv
load_dotenv()


PROJECT_NAME = os.environ.get('PROJECT_NAME')
PACKAGE_NAME = os.environ.get('PACKAGE_NAME')
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
GDRIVE_TYPE = os.environ.get('GDRIVE_TYPE')
GDRIVE_PROJECT_ID = os.environ.get('GDRIVE_PROJECT_ID')
GDRIVE_PRIVATE_KEY_ID = os.environ.get('GDRIVE_PRIVATE_KEY_ID')
GDRIVE_PRIVATE_KEY = os.environ.get('GDRIVE_PRIVATE_KEY')
GDRIVE_CLIENT_EMAIL = os.environ.get('GDRIVE_CLIENT_EMAIL')
GDRIVE_CLIENT_ID = os.environ.get('GDRIVE_CLIENT_ID')
GDRIVE_AUTH_URI = os.environ.get('GDRIVE_AUTH_URI')
GDRIVE_TOKEN_URI = os.environ.get('GDRIVE_TOKEN_URI')
GDRIVE_AUTH_PROVIDER_X509_CERT_URL = os.environ.get('GDRIVE_AUTH_PROVIDER_X509_CERT_URL')
GDRIVE_CLIENT_X509_CERT_URL = os.environ.get('GDRIVE_CLIENT_X509_CERT_URL')
GDRIVE_UNIVERSE_DOMAIN = os.environ.get('GDRIVE_UNIVERSE_DOMAIN')
PLANETSCALE_URL = os.environ.get('PLANETSCALE_URL')
DOCKER_TOKEN = os.environ.get('DOCKER_TOKEN')
DOCKER_USERNAME = os.environ.get('DOCKER_USERNAME')
GCR_PROJECT_ID = os.environ.get('GCR_PROJECT_ID')
MONGO_URL = os.environ.get('MONGO_URL')
SEATGEEK_API_SECRET = os.environ.get('SEATGEEK_API_SECRET')
SEATGEEK_CLIENT_ID = os.environ.get('SEATGEEK_CLIENT_ID')
