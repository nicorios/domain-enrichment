import pandas as pd
import requests
import time
from dotenv import load_dotenv
import os

load_dotenv()  # Loads variables from .env

api_key = os.getenv("ABSTRACT_API_KEY")

# Function to get email validation data
def get_email_validation(email):
    api_url = f"https://emailvalidation.abstractapi.com/v1/?api_key={api_key}&email="

    # Make the API call
    response = requests.get(api_url + email)
    if response.status_code == 200:
        return response.json()  # Parse JSON response
    else:
        return None

def validate_email(df):
    # Iterate over the DataFrame and call the API
    print("🏁 Validate email")
    for index, row in df.iterrows():
        email = 'test@' + row['domain']
        api_data = get_email_validation(email)
        
        if api_data:
            # Extract the fields from the API response and append to the DataFrame
            df.loc[index, 'deliverability'] = api_data.get('deliverability', '')
            df.loc[index, 'is_valid_format'] = api_data.get('is_valid_format', {}).get('text', '')
            df.loc[index, 'is_free_email'] = api_data.get('is_free_email', {}).get('text', '')
            df.loc[index, 'is_disposable_email'] = api_data.get('is_disposable_email', {}).get('text', '')
            df.loc[index, 'is_role_email'] = api_data.get('is_role_email', {}).get('text', '')
            df.loc[index, 'is_catchall_email'] = api_data.get('is_catchall_email', {}).get('text', '')
            df.loc[index, 'is_mx_found'] = api_data.get('is_mx_found', {}).get('text', '')
            df.loc[index, 'is_smtp_valid'] = api_data.get('is_smtp_valid', {}).get('text', '')
        else:
            print(f"API call failed for {email}")

        # Print progress every 200 rows
        if (index + 1) % 200 == 0:
            print(f"Processed {index + 1} rows out of {len(df)} rows.")

        # Rate limit: 2 requests per second
        time.sleep(0.5)

    # Display the updated DataFrame
    print("✅ Validate email")
    return df
