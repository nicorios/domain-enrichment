import pandas as pd
import whois
import time
import random
import requests

# Load CSV file
df = pd.read_csv("whois/df7.csv")  # Replace with your actual file

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"
]

session = requests.Session()
session.headers.update({"User-Agent": random.choice(USER_AGENTS)})

def fetch_whois_data(domain, retries=5, delay_range=(2, 5)):
    """Fetch WHOIS data using the whois library with retries and random delays."""
    for attempt in range(1, retries + 1):
        try:
            w = whois.whois(domain)
            registration_date = w.creation_date[0] if isinstance(w.creation_date, list) else w.creation_date
            last_updated = max(w.updated_date) if isinstance(w.updated_date, list) else w.updated_date
            expiration_date = w.expiration_date[0] if isinstance(w.expiration_date, list) else w.expiration_date
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
            print(f"WHOIS lookup failed for {domain} (Attempt {attempt}/{retries})")
        except Exception as e:
            print(f"Unexpected error fetching WHOIS data for {domain}: {e}")
            if "429" in str(e):
                print("Rate limit hit. Waiting for 5 minutes before retrying...")
                time.sleep(300)  # Wait 5 minutes if rate limited
        
        if attempt < retries:
            sleep_time = random.uniform(*delay_range)
            print(f"Retrying {domain} in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
    
    print(f"Skipping {domain} after {retries} failed attempts.")
    return {"domain": domain}

# Process domains
data_list = []
print("Analyzing df7.csv")

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
df.to_csv("whois/df7_enriched.csv", index=False)
print("WHOIS data collection completed and saved to df5_enriched.csv")
