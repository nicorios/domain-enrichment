from domain_fetcher import get_new_domains
from whois_checker import enrich_whois_df
from dns_checker import enrich_dns
from email_validator import validate_email
from domain_risk_enricher import enrich_domain_risk
import pandas as pd

# Step 1: Get new domains
df = get_new_domains()
if df.empty:
    print("No new domains added in the last 24 hours.")
    exit()

# Step 2: Enrich with WHOIS
df = enrich_whois_df(df)

# Step 3: Enrich with SPF/DMARC or DNS
df = enrich_dns(df)

# Step 4: Validate example email
df = validate_email(df)

# Step 5: Enrich risk insights
df = enrich_domain_risk(df)

# # Step 6: Save results
df.to_csv("enriched_domains_test.csv", index=False)
# print("✅ Domain enrichment completed.")
