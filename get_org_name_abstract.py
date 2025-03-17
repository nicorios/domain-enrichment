import requests
import html
import json
import re
import time
import pandas as pd
from collections import Counter
from datetime import datetime
from bs4 import BeautifulSoup
import validators

def clean_and_split_title(title):
    parts = re.split(r"[-—|:^]", title)
    filtered_parts = [
        p.strip() for p in parts 
        if p.strip() 
        and not re.search(r"404|Not Found|Login", p, re.IGNORECASE) 
        and p.strip().lower() != "home"
    ]
    return filtered_parts if filtered_parts else [None]

def extract_main_domain(domain):
    match = re.match(r"^(?:www\.)?([^\.]+)", domain)
    return match.group(1).lower() if match else domain.lower()

def normalize_name(name):
    return name.lower().replace(" ", "") if name else None

def determine_best_name(domain, names):
    cleaned_domain = extract_main_domain(domain)
    normalized_names = {}
    for key, value in names.items():
        if isinstance(value, list):
            normalized_names[key] = [normalize_name(v) for v in value if v]
        else:
            normalized_names[key] = normalize_name(value) if value else None
    
    for key, value in normalized_names.items():
        if isinstance(value, list) and cleaned_domain in value:
            original_index = value.index(cleaned_domain)
            best_match = names[key][original_index]
            return best_match if 4 <= len(best_match) <= 35 else None
        elif value == cleaned_domain:
            best_match = names[key]
            return best_match if 4 <= len(best_match) <= 35 else None
    
    all_normalized_values = []
    for key, value in normalized_names.items():
        if isinstance(value, list):
            all_normalized_values.extend(value)
        elif value:
            all_normalized_values.append(value)
    
    name_counts = Counter(filter(None, all_normalized_values))
    if name_counts:
        all_unique = all(count == 1 for count in name_counts.values())
        if all_unique:
            valid_names = [name for name in all_normalized_values if 4 <= len(name) <= 35]
            if valid_names:
                shortest_name = min(valid_names, key=len)
                for key, value in normalized_names.items():
                    if isinstance(value, list) and shortest_name in value:
                        return names[key][value.index(shortest_name)]
                    elif value == shortest_name:
                        return names[key]
            return None
        most_common_name = name_counts.most_common(1)[0][0]
        for key, value in normalized_names.items():
            if isinstance(value, list) and most_common_name in value:
                return names[key][value.index(most_common_name)]
            elif value == most_common_name:
                return names[key]
    return None

def fetch_website_info(domain):
    api_key = "c58ca34868304d238325a3281ef242d1"
    api_url = f"https://scrape.abstractapi.com/v1/?api_key={api_key}&url=https://{domain}"
    
    if not validators.domain(domain):
        print(f"Skipping invalid domain: {domain}")
        return None, "Invalid Domain"
    
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        response_text = response.text
    except requests.exceptions.RequestException:
        return None, "Error"
    
    # Clean response
    if response_text.startswith('"') and response_text.endswith('"'):
        response_text = response_text[1:-1]  # Remove first and last quote
    cleaned_html = response_text.encode().decode("unicode_escape")
    cleaned_html = html.unescape(cleaned_html).replace("\n", "").replace("\r", "")
    soup = BeautifulSoup(cleaned_html, "html.parser")
    
    # Extract metadata
    website_title = soup.title.string if soup.title else None
    website_og = soup.find("meta", property="og:site_name")
    website_og = website_og["content"] if website_og else None
    
    # Extract schema data
    script_tags = soup.find_all("script", type="application/ld+json")
    website_schema = None
    
    for script in script_tags:
        try:
            json_text = script.string.strip()

            # Fix common JSON issues
            if json_text.startswith("{") and json_text.endswith("}}"):  # Extra closing brace issue
                json_text = json_text[:-1]

            json_data = json.loads(json_text)

            # Handle both cases: list and dictionary
            if isinstance(json_data, list):
                for entry in json_data:
                    if entry.get("@type") in ["WebSite", "Organization"]:
                        website_schema = entry.get("name", "No name found")
                        break
            elif isinstance(json_data, dict):
                if json_data.get("@type") in ["WebSite", "Organization"]:
                    website_schema = json_data.get("name", "No name found")
                    break
        except json.JSONDecodeError as e:
            continue
    
    title_parts = clean_and_split_title(website_title) if website_title else []
    names = {
        "website_title_parts": title_parts,
        "website_og": website_og,
        "website_schema": website_schema
    }
    return determine_best_name(domain, names), response.status_code

df = pd.read_csv("missing_6.csv")
df["best_site_name"] = None
df["scraping_status_code"] = None

print("Analyzing missing_6.csv")
for i, (index, row) in enumerate(df.iterrows(), start=1):
    domain = row["domain"]
    best_name, status_code = fetch_website_info(domain)
    df.at[index, "best_site_name"] = best_name
    df.at[index, "scraping_status_code"] = status_code
    if i % 500 == 0 or i == 10:
        print(f"📢 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Processed {i} rows")

df.to_csv("missing_6-ready.csv", index=False)
print("✅ Scraping complete. Results saved to missing_6-ready.csv.")