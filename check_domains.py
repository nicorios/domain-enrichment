import pandas as pd
import requests
import time
from urllib.parse import urlparse

# Load CSV file
df = pd.read_csv("df2.csv")  # Replace with your actual file

# Function to fetch WHOIS data

import requests
import time

def fetch_whois_data(domain, retries=3, timeout=10):
    url = f"https://rdap.verisign.com/com/v1/domain/{domain}"

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            
            # Check for HTTP errors
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"Error {response.status_code} for {domain}. Response: {response.text}")

        except requests.exceptions.Timeout:
            print(f"Timeout error for {domain} (Attempt {attempt}/{retries})")
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error for {domain} (Attempt {attempt}/{retries}): {e}")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error for {domain} (Attempt {attempt}/{retries}): {e}")
        except requests.exceptions.RequestException as e:
            print(f"Unexpected error for {domain} (Attempt {attempt}/{retries}): {e}")

        # Wait before retrying (increase delay for each attempt)
        time.sleep(5 * attempt)

    print(f"Skipping {domain} after {retries} failed attempts.")
    return None


def process_domain(domain):
    url = f"https://rdap.verisign.com/com/v1/domain/{domain}"
    response = requests.get(url)
    if response.status_code == 200:
        data = fetch_whois_data(domain)
        
        # Extract key data points
        registration_date = next((e["eventDate"] for e in data.get("events", []) if e["eventAction"] == "registration"), None)
        last_updated = next((e["eventDate"] for e in data.get("events", []) if e["eventAction"] == "last changed"), None)
        expiration_date = next((e["eventDate"] for e in data.get("events", []) if e["eventAction"] == "expiration"), None)
        
        registrar = next((e for e in data.get("entities", []) if "registrar" in e.get("roles", [])), None)
        registrar_name = next((v[3] for v in registrar.get("vcardArray", [[], []])[1] if v[0] == "fn"), None) if registrar else None
        
        registrar_email = next((v[3] for e in registrar.get("entities", []) for v in e.get("vcardArray", [[], []])[1] if v[0] == "email"), None) if registrar else None
        
        if not registrar_email:
            registrar_email = next((v[3] for v in registrar.get("vcardArray", [[], []])[1] if v[0] == "email"), None) if registrar else None
        
        registrar_url = None
        if registrar_email:
            parsed_email = registrar_email.split("@")[-1]
            registrar_url = f"https://{parsed_email}"  # Convert email domain to URL
        
        return {
            "registration_date": registration_date,
            "last_updated": last_updated,
            "expiration_date": expiration_date,
            "registrar_name": registrar_name,
            "registrar_email": registrar_email,
            "registrar_url": registrar_url,
        }
    return {}

# Iterate over domains and fetch WHOIS data
data_list = []
for index, domain in enumerate(df["domain"], start=1):
    whois_data = process_domain(domain)
    whois_data["domain"] = domain
    data_list.append(whois_data)
    
    if index % 200 == 0:  # Print progress every 200 rows
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] Processed {index} domains...")

    time.sleep(1)  # Respect rate limits


# Convert results into DataFrame
whois_df = pd.DataFrame(data_list)

# Merge with original DataFrame
df = df.merge(whois_df, on="domain", how="left")

# Save to CSV
df.to_csv("df2_enriched.csv", index=False)
current_time = time.strftime("%Y-%m-%d %H:%M:%S")
print(f"[{current_time}] Batch completed.csv")