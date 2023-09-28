import typing
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
import pandas as pd


def generate_and_save_csv(
    data, folder_id, credentials, share_email: typing.List[str] = None
):
    """
    Generate a Google Sheets spreadsheet from a DataFrame and save it to a specific Google Drive folder.

    :param data:
    :param share_email:
    :param credentials:
    :param df: The DataFrame containing the data to be saved in the spreadsheet.
    :type df: pandas.DataFrame

    :param folder_id: The ID of the Google Drive folder where the spreadsheet will be placed.
    :type folder_id: str

    :param credentials_file: The path to the service account JSON key file. Defaults to "your-service-account-key.json".
    :type credentials_file: str, optional

    :return: A message indicating the success of the operation.
    :rtype: str

    :raises Exception: If there is an issue with creating or accessing the Google Sheets spreadsheet.

    :example:

    ::

        data_to_save = pd.DataFrame({
            "Date": ["2023-09-25", "2023-09-26"],
            "Value1": [42, 55],
            "Value2": [67, 78]
        })
        google_drive_folder_id = "your-folder-id"  # Replace with the actual folder ID
        result = generate_and_save_csv_from_dataframe(data_to_save, google_drive_folder_id, credentials_file="your-service-account-key.json")
        print(result)
    """
    # Load the service account credentials
    creds = service_account.Credentials.from_service_account_info(
        credentials, scopes=["https://www.googleapis.com/auth/drive"]
    )

    # Create a Google Drive API service
    drive_service = build("drive", "v3", credentials=creds)

    # Create a new Google Sheets spreadsheet in the specified folder
    # name is current SEATGEEK_EVENT_METRICS_YEAR_MONTH_DAY.csv
    file_name = (
        f"SEATGEEK_EVENT_METRICS_{pd.Timestamp.today().strftime('%Y_%m_%d')}.csv"
    )
    spreadsheet = (
        drive_service.files()
        .create(
            body={
                "name": file_name,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [folder_id],
            }
        )
        .execute()
    )

    # Retrieve the ID of the newly created spreadsheet
    spreadsheet_id = spreadsheet["id"]

    # Share the spreadsheet with specified email(s)
    if share_email:
        for email in share_email:
            drive_service.permissions().create(
                fileId=spreadsheet_id,
                body={"type": "user", "role": "reader", "emailAddress": email},
            ).execute()

    # Use gspread to open the spreadsheet
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(spreadsheet_id)

    # Convert the DataFrame to a list of lists
    # data = df.values.tolist()

    # Write data to the first worksheet of the spreadsheet
    worksheet = sh.get_worksheet(0)
    worksheet.insert_rows(data, 2)

    return f"CSV data has been added to the Google Sheets spreadsheet in the folder."


import env

if __name__ == "__main__":
    creds = {
        "type": env.GDRIVE_TYPE,
        "project_id": env.GDRIVE_PROJECT_ID,
        "private_key_id": env.GDRIVE_PRIVATE_KEY_ID,
        "private_key": env.GDRIVE_PRIVATE_KEY,
        "client_email": env.GDRIVE_CLIENT_EMAIL,
        "client_id": env.GDRIVE_CLIENT_ID,
        "auth_uri": env.GDRIVE_AUTH_URI,
        "token_uri": env.GDRIVE_TOKEN_URI,
        "auth_provider_x509_cert_url": env.GDRIVE_AUTH_PROVIDER_X509_CERT_URL,
        "client_x509_cert_url": env.GDRIVE_CLIENT_X509_CERT_URL,
        "universe_domain": env.GDRIVE_UNIVERSE_DOMAIN,
    }

    # Example usage with a DataFrame:
    data_to_save = pd.DataFrame(
        {"Date": ["2023-09-25", "2023-09-26"], "Value1": [42, 55], "Value2": [67, 78]}
    )
    google_drive_folder_id = env.GDRIVE_FOLDER_ID  # Replace with the actual folder ID
    result = generate_and_save_csv(
        data_to_save.values.tolist(), env.GDRIVE_FOLDER_ID, creds
    )
    print(result)
