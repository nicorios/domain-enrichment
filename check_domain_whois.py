import pandas as pd
import whois
import time
import random
import requests
from datetime import datetime, timezone
import socket

# Load CSV file
df = pd.read_csv("second_pass/df_5.csv")  # Replace with your actual file

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"
]

session = requests.Session()
session.headers.update({"User-Agent": random.choice(USER_AGENTS)})

def safe_parse_date(date_value):
    """Ensure the date is a datetime object, converting from string if needed."""
    if isinstance(date_value, list):  # If it's a list, process each item
        date_value = [safe_parse_date(d) for d in date_value]
        return max(date_value)  # Get the latest date if multiple

    if isinstance(date_value, str):  # Convert string to datetime
        try:
            return datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S")  # Adjust format as needed
        except ValueError:
            try:
                return datetime.strptime(date_value, "%Y-%m-%d")  # Some WHOIS providers omit time
            except ValueError:
                return None  # Return None if format is unrecognized

    if isinstance(date_value, datetime):  # If already datetime, ensure it has tzinfo
        return date_value.replace(tzinfo=timezone.utc) if date_value.tzinfo is None else date_value

    return None  # Return None for unexpected values

def fetch_whois_data(domain, retries=5, delay_range=(2, 5)):
    """Fetch WHOIS data using the whois library with retries and random delays."""
    for attempt in range(1, retries + 1):
        try:
            w = whois.whois(domain)
            registration_date = safe_parse_date(w.creation_date)
            last_updated = safe_parse_date(w.updated_date)
            expiration_date = safe_parse_date(w.expiration_date)
            registrar_name = w.registrar
            # Prioritize registrar_email, fallback to emails
            registrar_email = w.registrar_email if hasattr(w, 'registrar_email') and w.registrar_email else None
            if not registrar_email:
                registrar_email = w.emails[0] if isinstance(w.emails, list) else w.emails
            
            registrar_url = f"https://{registrar_email.split('@')[-1]}" if registrar_email else None
            
            return {
                "domain": domain,
                "registration_date": registration_date,
                "last_updated": last_updated,
                "expiration_date": expiration_date,
                "registrar_name": registrar_name,
                "registrar_email": registrar_email,
                "registrar_url": registrar_url
            }
        except whois.parser.PywhoisError:
            return {"domain": domain}
        except socket.timeout:
            print(f"Timeout error fetching WHOIS data for {domain}")
            return {"domain": domain}
        
        except socket.gaierror:
            print(f"DNS resolution failed for {domain} (Name or service not known)")
            return {"domain": domain}
        
        except ConnectionRefusedError:
            print(f"Connection refused for {domain}")
            return {"domain": domain}
        
        except ConnectionResetError:
            print(f"Connection reset by peer for {domain}")
            return {"domain": domain}
    
        except Exception as e:
            print(f"Unexpected error fetching WHOIS data for {domain}: {e}")
            if "429" in str(e) or "Connection reset by peer" in str(e) :
                print("Rate limit hit. Waiting for 5 minutes before retrying...")
                time.sleep(300)  # Wait 5 minutes if rate limited

        if attempt < retries:
            sleep_time = random.uniform(*delay_range)
            time.sleep(sleep_time)
    
    print(f"Skipping {domain} after {retries} failed attempts.")
    return {"domain": domain}

# Process domains
data_list = []
print("Analyzing df5.csv")

for index, domain in enumerate(df["domain"], start=1):
    whois_data = fetch_whois_data(domain)
    data_list.append(whois_data)
    
    if index % 500 == 0 or index == 10:
        print(f"Processed {index} domains...")
    
    time.sleep(random.uniform(1, 3))  # Short delay to avoid being flagged

# Convert results into DataFrame
whois_df = pd.DataFrame(data_list)

# Merge with original DataFrame
df = df.merge(whois_df, on="domain", how="left")

# Save to CSV
df.to_csv("second_pass/df_5_enriched.csv", index=False)
print("WHOIS data collection completed and saved to df5.csv")
