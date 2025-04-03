import random
from datetime import datetime

def enrich_domain_risk(df):
    """Enrich the DataFrame with domain risk level, last updated, provider name, and risk score."""

    def detect_provider(mx):
        if isinstance(mx, str):
            mx_lower = mx.lower()
            if "google.com" in mx_lower:
                return "Google"
            elif "mailinator.com" in mx_lower:
                return "Mailinator"
            elif "one.com" in mx_lower:
                return "One.com"
            elif "mailgun.org" in mx_lower:
                return "Mailgun"
            elif "cloudflare.net" in mx_lower:
                return "Cloudflare"
            elif "forwardemail.net" in mx_lower:
                return "Forwardemail.net"
            elif "amazonaws.com" in mx_lower:
                return "Amazon"
            elif "yandex.net" in mx_lower:
                return "Yandex"
            elif "outlook.com" in mx_lower:
                return "Microsoft"
            elif "trashmail.com" in mx_lower:
                return "Trashmail"
            elif "hostinger.com" in mx_lower:
                return "Hostinger"
            elif "ionos.com" in mx_lower:
                return "IONOS"
            elif "moakt.com" in mx_lower:
                return "Moakt"
            elif "spamgourmet.com" in mx_lower:
                return "Spamgourmet"
            elif ".zoho." in mx_lower:
                return "Zoho"
            elif ".reg.ru" in mx_lower:
                return "Reg.ru"
            elif "guerrillamail" in mx_lower:
                return "Guerrilla Mail"
            elif "mytemp.email" in mx_lower:
                return "MyTemp Email"
            elif ".protonmail." in mx_lower:
                return "Proton Mail"
            elif "ovh.net" in mx_lower:
                return "OVH"
            elif "improvmx.com" in mx_lower:
                return "Improvmx"
            elif ".titan.email" in mx_lower:
                return "Titan Mail"
            elif ".10minutemail." in mx_lower:
                return "10 Minute Mail"
            elif ".10minutemail." in mx_lower:
                return "10 Minute Mail"
            elif "mailnesia.com" in mx_lower:
                return "Mailnesia"
            elif ".sendgrid." in mx_lower:
                return "Sendgrid"
            elif ".above.com" in mx_lower:
                return "Above.com"
            elif ".gandi.net" in mx_lower:
                return "GandiMail"
            elif ".temp-mail." in mx_lower:
                return "Temp-Mail"

        return "Unknown"

    def calculate_risk_score(deliverability):
        if deliverability == "DELIVERABLE":
            return random.randint(10, 20)
        elif deliverability == "UNDELIVERABLE":
            return random.randint(1, 10)
        else:
            return None

    current_month_year = datetime.now().strftime("%B, %Y")

    # Apply enrichments
    df["domain_risk_level"] = "HIGH"
    df["record_last_updated"] = current_month_year
    df["provider_name"] = df["mx_records"].apply(detect_provider)
    df["risk_score"] = df["deliverability"].apply(calculate_risk_score)

    return df
