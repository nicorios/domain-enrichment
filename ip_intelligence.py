import pandas as pd
import requests
import time

# Load the CSV file containing IP addresses
df = pd.read_csv("ips_to_enrich.csv")

# API URL and key
api_url = "https://ip-intelligence.abstractapi.com/v1/?api_key=ffd624a4b1854bf7896113c1459646e8&ip_address="

# Function to get IP intelligence data
def get_ip_intelligence(ip):
    response = requests.get(api_url + ip)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Iterate over the DataFrame and call the API
print("Analyzing ips_to_enrich.csv")
for index, row in df.iterrows():
    ip = row['ip_address']
    api_data = get_ip_intelligence(ip)
    
    if api_data:
        # Append security fields
        security = api_data.get('security', {})
        df.loc[index, 'is_vpn'] = security.get('is_vpn', '')
        df.loc[index, 'is_proxy'] = security.get('is_proxy', '')
        df.loc[index, 'is_tor'] = security.get('is_tor', '')
        df.loc[index, 'is_hosting'] = security.get('is_hosting', '')
        df.loc[index, 'is_relay'] = security.get('is_relay', '')
        df.loc[index, 'is_mobile'] = security.get('is_mobile', '')
        df.loc[index, 'is_abuse'] = security.get('is_abuse', '')

        # Append ASN and company fields
        asn = api_data.get('asn', {})
        df.loc[index, 'asn'] = asn.get('asn', '')
        df.loc[index, 'asn_name'] = asn.get('name', '')
        df.loc[index, 'asn_domain'] = asn.get('domain', '')
        df.loc[index, 'asn_type'] = asn.get('type', '')

        company = api_data.get('company', {})
        df.loc[index, 'company_name'] = company.get('name', '')
        df.loc[index, 'company_domain'] = company.get('domain', '')
        df.loc[index, 'company_type'] = company.get('type', '')

        # Append location fields
        location = api_data.get('location', {})
        df.loc[index, 'city'] = location.get('city', '')
        df.loc[index, 'region'] = location.get('region', '')
        df.loc[index, 'country'] = location.get('country', '')
        df.loc[index, 'country_code'] = location.get('country_code', '')
        df.loc[index, 'continent'] = location.get('continent', '')
        df.loc[index, 'longitude'] = location.get('longitude', '')
        df.loc[index, 'latitude'] = location.get('latitude', '')

        # Append timezone and currency info
        timezone = api_data.get('timezone', {})
        df.loc[index, 'timezone'] = timezone.get('name', '')
        df.loc[index, 'local_time'] = timezone.get('local_time', '')

        currency = api_data.get('currency', {})
        df.loc[index, 'currency'] = currency.get('code', '')
    else:
        print(f"API call failed for {ip}")

    # Print progress every 200 rows
    if (index + 1) % 200 == 0:
        print(f"Processed {index + 1} rows out of {len(df)} rows.")

    # Rate limit: 2 requests per second
    time.sleep(0.5)

# Display the updated DataFrame
print("✅ Results saved to company_ips_ready-ready.csv")
df.to_csv("ips_to_enrich-ready.csv", index=False)
