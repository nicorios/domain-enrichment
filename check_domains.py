import pandas as pd
import subprocess

# Load CSV file
df = pd.read_csv("domains_example.csv")

def is_site_live(domain):
    """Pings the domain and returns True if reachable, False otherwise."""
    try:
        response = subprocess.run(
            ["ping", "-c", "1", domain],  # Send 1 ping
            stdout=subprocess.DEVNULL,   # Hide output
            stderr=subprocess.DEVNULL
        )
        return response.returncode == 0  # 0 means success
    except Exception:
        return False

# Apply the function to each domain
df["is_live"] = df["domain"].apply(is_site_live)

# Save to a new CSV file
df.to_csv("results.csv", index=False)

print("Check completed. Results saved in results.csv.")
