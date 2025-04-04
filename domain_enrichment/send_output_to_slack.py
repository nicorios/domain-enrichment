import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from datetime import datetime
import requests

load_dotenv()

def send_file_to_slack(df, channel="#daydream-abstractapi"):
    filepath = "enriched_domains.csv"
    df.to_csv(filepath, index=False)
    slack_token = os.getenv("SLACK_USER_OAUTH_TOKEN")
    if not slack_token:
        print("SLACK_USER_OAUTH_TOKEN not set in environment.")
        return

    client = WebClient(token=slack_token)
    file_name = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)

    try:
        # Step 1: Get upload URL
        upload_url_resp = client.files_getUploadURLExternal(
            filename=file_name,
            length=file_size
        )
        upload_url = upload_url_resp["upload_url"]
        file_id = upload_url_resp["file_id"]

        # Step 2: Upload the file to the URL
        with open(filepath, "rb") as file_data:
            upload_response = requests.post(
                upload_url,
                data=file_data,
                headers={"Content-Type": "application/octet-stream"}
            )
        if upload_response.status_code != 200:
            print("❌ Upload failed:", upload_response.text)
            return

        # Step 3: Complete upload
        client.files_completeUploadExternal(
            files=[{
                "id": file_id,
                "title": f"Enriched Domains - {datetime.now().strftime('%Y-%m-%d')}"
            }]
        )

        # Step 4: Get permalink and share
        file_info = client.files_info(file=file_id)
        permalink = file_info["file"]["permalink"]

        # Step 5: Post message
        client.chat_postMessage(
            channel=channel,
            text=f"📎 Hi team. Here is the list of new disposable domains listed in the past 7 days to create new pages. {datetime.now().strftime('%B %d, %Y')}\n<{permalink}|Download CSV>"
        )

        print("✅ File shared successfully in Slack.")

        if os.path.exists(filepath):
            os.remove(filepath)
            print("✅ Local file deleted.")
        else:
            print("⚠️ File does not exist.")
    except SlackApiError as e:
        print(f"❌ Slack API error: {e.response['error']}")
