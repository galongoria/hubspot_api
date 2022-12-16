import smtplib, ssl, os, datetime
import pandas as pd
import numpy as np
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

def send_csv(email, name, filename, df):

	"""
	Input the recipients email and first name. The operations in this function do not require a full name, but you can add an exception if you have a case that does.
	Also, input the filename that you want the recipient to see in their inbox. The last argument is a dataframe that you want to share.
	"""

	ctx = ssl.create_default_context()
	sender = "austin@sputnikatx.com"
	body = f"Hi {name},\nHere's the exported table from HubSpot. Let me know if you have any questions.\n\nBest,\nAustin"
	filename = f'{filename}_{str(datetime.now().month)}_{str(datetime.now().day)}_{str(datetime.now().year)}.csv'
	file = MIMEApplication(df.to_csv())
	disposition = f"attachment; filename={filename}"
	file.add_header("Content-Disposition", disposition)

	message = MIMEMultipart("mixed")
	message["Subject"] = "Export From Hubspot"
	message["From"] = sender
	message["To"] = email
	message.attach(MIMEText(body))
	message.attach(file)

	with smtplib.SMTP_SSL("smtp.gmail.com", port=465, context=ctx) as server:
	    server.login(sender, os.getenv('sputnik_gmail_password'))
	    server.sendmail(sender, email, message.as_string())