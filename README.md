# WHOIS Data Fetching Script

## Overview

This script fetches WHOIS data for a list of domains from a CSV file using the Verisign RDAP API. It extracts key information such as:

- Registration date
- Last updated date
- Expiration date
- Registrar name
- Registrar email
- Registrar URL (derived from email domain)

The script then merges the fetched data with the original CSV file and saves the updated dataset.

## Prerequisites

### Requirements

- Python 3.x
- Required libraries:
  - `pandas`
  - `requests`
  - `time`

Install missing dependencies using:

```sh
pip install pandas requests
```

## Usage

### 1. Prepare the CSV File

Ensure you have a CSV file with a column named `domain` that contains the list of domains to process.

Example CSV file structure:

```
domain
example.com
anotherdomain.com
sampledomain.net
```

### 2. Load the CSV File

Modify the script to specify the correct path to your CSV file:

```python
# Load CSV file
df = pd.read_csv("/path/to/your/csv_file.csv")
```

### 3. Run the Script

Execute the script using:

```sh
python script.py
```

### 4. Output

- The script will fetch WHOIS data for each domain and print progress updates every 50 domains.
- The enriched data will be merged with the original dataset.
- The updated dataset is saved as `updated_domains.csv` (uncomment the saving line in the script to enable this).

## Error Handling & Retries

- The script includes error handling for:
  - Connection issues
  - Timeout errors
  - HTTP errors
  - Unexpected API responses
- If an error occurs, the script retries up to **3 times** before skipping a domain.
- A delay is added between requests to avoid rate limits.

## Notes

- The Verisign RDAP API is used for querying `.com` domains. If working with other TLDs, a different RDAP endpoint might be required.
- WHOIS data availability depends on domain privacy settings and registry policies.

## Customization

- Modify `retries` and `timeout` values in `fetch_whois_data(domain, retries=3, timeout=10)` as needed.
- Adjust the delay between requests (`time.sleep(1)`) to comply with API rate limits.
- Additional domain fields can be extracted from the API response if needed.

## License

This script is provided under the MIT License.

---

**Author:** Nicolas Rios

