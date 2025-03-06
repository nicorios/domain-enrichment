import pandas as pd
import requests
import time
import random
from urllib.parse import urlparse

# Load CSV file
df = pd.read_csv("df10.csv")  # Replace with your actual file

# Function to fetch WHOIS data

import requests
import time

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"
]

session = requests.Session()

def fetch_whois_data(domain, retries=15, timeout=10):
    url = f"https://rdap.verisign.com/com/v1/domain/{domain}"

    for attempt in range(1, retries + 1):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = session.get(url, timeout=timeout, headers=headers)
            
            # Check for HTTP errors
            if response.status_code == 200:
                data = response.json()
                return data
            elif response.status_code == 429:  # Too Many Requests
                retry_after = int(response.headers.get("Retry-After", 10))
                print(f"Rate limit hit. Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
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

        # Exponential backoff with jitter
        sleep_time = random.uniform(2, 5) * attempt
        print(f"Retrying {domain} in {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    print(f"Skipping {domain} after {retries} failed attempts.")
    return None


def process_domain(domain, retries=15):
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
print("analyzing df10")
for index, domain in enumerate(df["domain"], start=1):
    try:
        whois_data = process_domain(domain)
        whois_data["domain"] = domain
        data_list.append(whois_data)
        
        if index % 500 == 0 or index == 10:  # Print progress every 500 rows
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{current_time}] Processed {index} domains...")

        # Add a random delay (avoid detection)
        sleep_time = random.uniform(3, 6)
        time.sleep(sleep_time)
    
    except Exception as e:
        print(f"Unexpected error processing {domain}: {e}")
        print("Waiting 5 minutes before continuing with the next domain...")
        time.sleep(300)  # Wait 5 minutes


# Convert results into DataFrame
whois_df = pd.DataFrame(data_list)

# Merge with original DataFrame
df = df.merge(whois_df, on="domain", how="left")

# Save to CSV
df.to_csv("df10_enriched.csv", index=False)
current_time = time.strftime("%Y-%m-%d %H:%M:%S")
print(f"[{current_time}] Batch completed.csv")