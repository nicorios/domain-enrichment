import pandas as pd
import dns.resolver
import requests
import time
from datetime import datetime

# Create a resolver and set it to Google's public DNS
resolver = dns.resolver.Resolver()
resolver.nameservers = ['8.8.8.8', '8.8.4.4']  # Google Public DNS

def get_mx_records(domain):
    """Fetch MX records for a given domain."""
    try:
        answers = resolver.resolve(domain, 'MX')
        mx_records = [str(r.exchange) for r in answers]
        return ", ".join(mx_records)
    except dns.resolver.NoAnswer as e:
        return "Unknown"
    except dns.resolver.NXDOMAIN as e:
        return "Unknown"
    except dns.exception.DNSException as e:
        return "Unknown"

def get_spf_strict(domain):
    """Check if SPF record is strictly enforced (-all)."""
    try:
        answers = resolver.resolve(domain, 'TXT')
        for rdata in answers:
            txt_record = rdata.to_text()
            if "v=spf1" in txt_record:
                if "-all" in txt_record:
                    return "Strict"
                elif "~all" in txt_record:
                    return "Strict"
                else:
                    return "No strict enforcement"
        return "No strict enforcement"
    except dns.resolver.NoAnswer as e:
        return "No strict enforcement"
    except dns.resolver.NXDOMAIN as e:
        return "No strict enforcement"
    except dns.exception.DNSException as e:
        return "No strict enforcement"

def get_dmarc_policy(domain):
    """Check if DMARC is enforced (p=reject or p=quarantine)."""
    dmarc_domain = f"_dmarc.{domain}"
    try:
        answers = resolver.resolve(dmarc_domain, 'TXT')
        for rdata in answers:
            txt_record = rdata.to_text()
            if "v=DMARC1" in txt_record:
                if "p=reject" in txt_record:
                    return "Enforced"
                elif "p=quarantine" in txt_record:
                    return "Enforced"
                elif "p=none" in txt_record:
                    return "No enforcement"
        return "No enforcement"
    except dns.resolver.NoAnswer as e:
        return "No enforcement"
    except dns.resolver.NXDOMAIN as e:
        return "No enforcement"
    except dns.exception.DNSException as e:
        return "No enforcement"

def is_live_site(domain):
    """Check if a domain has a live website by making an HTTP request."""
    urls = [f"https://{domain}", f"http://{domain}"]  # Try both HTTPS and HTTP
    for url in urls:
        try:
            response = requests.get(
                url, 
                timeout=10, 
                allow_redirects=False, 
                headers={'Accept-Encoding': 'identity'}  # Disable gzip encoding
            )
            if 200 <= response.status_code < 400:  # Site is accessible
                return "Live"
        except requests.TooManyRedirects:
            return "Live"  # Redirect loop, but site exists
        except requests.exceptions.ContentDecodingError:
            # print(f"Decoding error for domain: {domain}")
            return "Not Live"
        except requests.exceptions.RequestException as e:
            # print(f"Request error for {domain}: {e}")  # Print the broken domain
            return "Not Live"
    return "Not Live"


def enrich_dns(df):
    # Process each row and print progress every 500 rows
    print("🏁 Enrich DNS")
    for i, domain in enumerate(df['domain']):
        df.at[i, 'mx_records'] = get_mx_records(domain)
        df.at[i, 'is_spf_strict'] = get_spf_strict(domain)
        df.at[i, 'is_dmarc_enforced'] = get_dmarc_policy(domain)
        df.at[i, 'is_live_site'] = is_live_site(domain)
        
        # Print progress every 500 rows
        if (i + 1) % 100 == 0 or i == 10:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"📢 {current_time} - Processed {i+1} rows")

    print("✅ Enrich DNS")
    return df