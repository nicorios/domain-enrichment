import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
import time
from collections import Counter
from datetime import datetime

def clean_and_split_title(title):
    parts = re.split(r"[-—|:^]", title)

    # Remove unwanted parts: "Home", "404", "Not Found", "Login"
    filtered_parts = [
        p.strip() for p in parts 
        if p.strip() 
        and not re.search(r"404|Not Found|Login", p, re.IGNORECASE) 
        and p.strip().lower() != "home"
    ]

    return filtered_parts if filtered_parts else [None]


# Function to extract the main domain (removes subdomains & TLD)
def extract_main_domain(domain):
    match = re.match(r"^(?:www\.)?([^\.]+)", domain)  # Extracts only the main part before the first dot
    return match.group(1).lower() if match else domain.lower()

# Function to clean and normalize a name (remove spaces, lowercase)
def normalize_name(name):
    return name.lower().replace(" ", "") if name else None

def determine_best_name(domain, names):
    cleaned_domain = extract_main_domain(domain)

    # Normalize all names (flatten lists into separate elements)
    normalized_names = {}

    for key, value in names.items():
        if isinstance(value, list):  # If the value is a list (title parts)
            normalized_names[key] = [normalize_name(v) for v in value if v]  # Normalize each part
        else:
            normalized_names[key] = normalize_name(value) if value else None  # Normalize single values

    # Step 1: Check if any name matches the domain
    for key, value in normalized_names.items():
        if isinstance(value, list):  # If it's a list (title parts)
            if cleaned_domain in value:
                original_index = value.index(cleaned_domain)  # Find the matching index
                best_match = names[key][original_index]  # Get the original, unnormalized value
                return best_match if 4 <= len(best_match) <= 30 else None  # Ensure length range
        else:
            if value == cleaned_domain:
                best_match = names[key]  # Get the original, unnormalized name
                return best_match if 4 <= len(best_match) <= 30 else None  # Ensure length range

    # Step 2: Flatten all values into a list for frequency counting
    all_normalized_values = []
    for key, value in normalized_names.items():
        if isinstance(value, list):
            all_normalized_values.extend(value)  # Add all list elements
        elif value:
            all_normalized_values.append(value)  # Add single elements

    # Count occurrences and find the most common value
    name_counts = Counter(filter(None, all_normalized_values))
    if name_counts:
        # Check if all values appear exactly once
        all_unique = all(count == 1 for count in name_counts.values())

        if all_unique:
            # Get all valid names within length constraints
            valid_names = [name for name in all_normalized_values if 4 <= len(name) <= 30]

            if valid_names:
                # Return the shortest valid name
                shortest_name = min(valid_names, key=len)
                for key, value in normalized_names.items():
                    if isinstance(value, list) and shortest_name in value:
                        return names[key][value.index(shortest_name)]
                    elif value == shortest_name:
                        return names[key]

        # Normal case: Return most common name
        most_common_name = name_counts.most_common(1)[0][0]
        for key, value in normalized_names.items():
            if isinstance(value, list) and most_common_name in value:
                return names[key][value.index(most_common_name)]
            elif value == most_common_name:
                return names[key]


    return None  # If no match found, return None


# Function to scrape website information
def scrape_website_info(domain):
    url = f"https://{domain}"  # Assuming https://
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "DNT": "1",  # Do Not Track request header
        "Connection": "keep-alive"
    }

    scraping_status_code = None
    scraping_info = "Success"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise error if request fails
        scraping_status_code = response.status_code
    except requests.exceptions.RequestException as e:
        scraping_status_code = getattr(e.response, "status_code", "Error")
        scraping_info = str(e)
        return {
            "domain": domain, "website_title": None,
            "website_og": None, "website_schema": None, "best_site_name": None,
            "scraping_status_code": scraping_status_code, "scraping_info": scraping_info
        }

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract <title> tag and split into two parts
    title_tag = soup.find("title")
    website_title = title_tag.text.strip() if title_tag else None
    title_parts = clean_and_split_title(website_title) if website_title else []

    # Extract Open Graph <meta property="og:site_name">
    og_tag = soup.find("meta", property="og:site_name")
    website_og = og_tag["content"] if og_tag else None

    # Extract JSON-LD schema name
    website_schema = None
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            schema = json.loads(script.string)
            if isinstance(schema, dict) and "name" in schema:
                website_schema = schema["name"]
                break
        except json.JSONDecodeError:
            continue

    # Determine best site name
    names = {
        "website_title_parts": title_parts,
        "website_og": website_og,
        "website_schema": website_schema
    }
    best_site_name = determine_best_name(domain, names)

    return {
        "domain": domain, "website_title": title_parts,
        "website_og": website_og, "website_schema": website_schema, "best_site_name": best_site_name,
        "scraping_status_code": scraping_status_code, "scraping_info": scraping_info
    }

# Load CSV file with domains
df = pd.read_csv("dftest2.csv")  # Assuming a CSV with a "domain" column

# Scrape each website and store results
results = []
start_time = time.time()

print("Analyzing dftest2.csv")
for i, domain in enumerate(df["domain"], start=1):
    results.append(scrape_website_info(domain))

    # Print progress every 500 rows
    if i % 10 == 0 or i == 1:
        elapsed_time = time.time() - start_time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"📢 {current_time} - Processed {i} rows in {elapsed_time:.2f} seconds...")

# Convert to DataFrame
df_results = pd.DataFrame(results)

# Save results to CSV
df.to_csv("dftest2_ready.csv", index=False)
print("✅ Scraping complete. Results saved to dftest2_ready.csv.")