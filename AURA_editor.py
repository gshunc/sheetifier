import os.path
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from config import AURA_SPREADSHEET_ID
import pandas as pd
import numpy as np


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly","https://www.googleapis.com/auth/drive"]
PATIENT_RANGE_NAME = "AURA!A3:F"

def fetch_csvs(patient_id, creds):
  try:
    service = build("drive", "v3", credentials=creds)

    query = "mimeType = 'application/vnd.google-apps.folder' and name = " + "'" + patient_id + "'"

    request = service.files().list(q=query)

    folder_id = request.execute().get("files", [])[0].get("id")

    items = service.files().list(q="parents in "+"'"+folder_id+"'").execute().get("files",[])

    csv_ids = reversed([item.get("id") for item in items])

    csv_files = [pd.read_csv(io.BytesIO(service.files().get_media(fileId = csv_id).execute()), encoding="utf-8") for csv_id in csv_ids]

    if not items:
      print("No files found.")
      return
    return csv_files
  except HttpError as error:
    print(f"An error occurred: {error}")

def main():
  creds = None
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)

  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)

    with open("token.json", "w") as token:
      token.write(creds.to_json())

  try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=AURA_SPREADSHEET_ID, range=PATIENT_RANGE_NAME)
        .execute()
    )
    rows = result.get("values", [])

    if not rows:
      print("No data found.")
      return

    patient_ids = [row[1] for row in rows]   

    # Returns a chronologically ordered list of Pandas dataframes from CSV files
    csv_dataframes = [fetch_csvs(patient_id=patient_ids[i], creds=creds) for i in range(0,5)]

    non_nan_values = [[df['Alexa Interaction'].dropna().tolist() for df in frame] for frame in csv_dataframes]
    print(non_nan_values)



  except HttpError as err:
    print(err)


if __name__ == "__main__":
  main()