import requests
import time
import pandas as pd
from datetime import datetime

def get_new_domains():
    print(f"🏁 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting domain enrichment")

    url = "https://raw.githubusercontent.com/7c/fakefilter/refs/heads/main/json/data_version2.json"
    now = int(time.time())
    seven_days_ago = now - 604800

    response = requests.get(url)
    data = response.json()
    rows = []

    for domain, info in data.get("domains", {}).items():
        for host, details in info.get("hosts", {}).items():
            if details.get("firstseen", 0) >= seven_days_ago:
                rows.append({"domain": domain, "firstseen": details.get("firstseen")})
                break

    return pd.DataFrame(rows)