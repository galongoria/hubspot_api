import os
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInput
from hubspot.crm.contacts.exceptions import ApiException
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("pm_token")
