#!/usr/bin/env python3
"""
scripts/send_alert.py
----------------------
Failure alerting utility for the SFTP-to-Snowflake ETL pipeline.
Sends automated email notifications via SMTP (e.g. Gmail) upon pipeline errors.
"""

import os
import sys
import smtplib
import argparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

DEFAULT_RECIPIENT = "sabrinlalsingh@gmail.com"


def send_email_alert(subject: str, body: str, recipient: str = None) -> bool:
    """
    Sends an email alert to the designated recipient.
    Gracefully logs a warning if SMTP credentials are not configured.
    """
    to_email = recipient or os.getenv("ALERT_RECIPIENT_EMAILS", DEFAULT_RECIPIENT)
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    smtp_password = (os.getenv("SMTP_PASSWORD") or "").strip()

    print(f"\n[ALERT DISPATCH] Preparing email alert to: {to_email}")
    print(f"                 Subject: {subject}")

    if not smtp_user or not smtp_password:
        print("   ⚠️ SMTP_USER or SMTP_PASSWORD not set in environment/secrets.")
        print("   ↳ Alert logged to console output. To receive real emails, set SMTP_USER & SMTP_PASSWORD (App Password).")
        return False

    try:
        msg = MIMEMultipart()
        msg["From"] = f"ETL Pipeline Alert <{smtp_user}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        clean_password = smtp_password.replace(" ", "")
        with smtplib.SMTP(smtp_server, smtp_port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, clean_password)
            server.send_message(msg)

        print(f"   ✓ Alert email successfully sent to {to_email}!")
        return True

    except Exception as exc:
        print(f"   ❌ Failed to send email alert via {smtp_server}:{smtp_port}: {exc}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Send ETL failure alert email")
    parser.add_argument("--subject", type=str, default="🚨 [ETL ALERT] SFTP-to-Snowflake Pipeline Failed", help="Email subject")
    parser.add_argument("--body", type=str, required=True, help="Email message body")
    parser.add_argument("--recipient", type=str, default=DEFAULT_RECIPIENT, help="Recipient email address")
    args = parser.parse_args()

    send_email_alert(subject=args.subject, body=args.body, recipient=args.recipient)


if __name__ == "__main__":
    main()
