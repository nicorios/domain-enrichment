import requests
import time
import pandas as pd

def get_new_domains():
    url = "https://raw.githubusercontent.com/7c/fakefilter/refs/heads/main/json/data_version2.json"
    now = int(time.time())
    one_day_ago = now - 86400

    response = requests.get(url)
    data = response.json()
    rows = []

    for domain, info in data.get("domains", {}).items():
        for host, details in info.get("hosts", {}).items():
            if details.get("firstseen", 0) >= one_day_ago:
                rows.append({"domain": domain, "firstseen": details.get("firstseen")})
                break

    return pd.DataFrame(rows)