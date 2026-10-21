"""
alert_manager.py — Email Alert Manager
─────────────────────────────────────────────────────────────────
Sends email alerts on pipeline failure after all retries exhausted.
Silent on success — no inbox noise.

Supports: Outlook (smtp.office365.com) and Gmail (smtp.gmail.com)
─────────────────────────────────────────────────────────────────
"""

import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class AlertManager:

    def __init__(self, alert_config: dict):
        self.config  = alert_config
        self.enabled = alert_config.get("enabled", False)

    def send_failure_alert(self,
                           pipeline_name: str,
                           layer:         str,
                           error_message: str,
                           retry_count:   int):
        """Send failure email after all retries are exhausted."""
        if not self.enabled:
            print(f"[ALERT SKIPPED] alerting disabled. Error was: {error_message}")
            return

        subject = f"[PIPELINE FAILED] {pipeline_name} | layer={layer}"
        body = f"""
Pipeline Failure Alert
─────────────────────────────────────────────────
Pipeline  : {pipeline_name}
Layer     : {layer}
Time      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Retries   : {retry_count}

Error:
{error_message}

─────────────────────────────────────────────────
Check: pipeline_audit_log Delta table for full details.
        """

        try:
            msg            = MIMEMultipart()
            msg["From"]    = self.config["sender"]
            msg["To"]      = ", ".join(self.config["recipients"])
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(self.config["smtp_host"], self.config["smtp_port"]) as server:
                server.starttls()
                server.login(self.config["sender"], self.config["password"])
                server.send_message(msg)

            print(f"[ALERT SENT] failure alert sent for {pipeline_name}")

        except Exception as e:
            print(f"[ALERT ERROR] could not send email: {e}")

    def send_success_summary(self,
                             pipeline_name: str,
                             layer:         str,
                             rows:          int,
                             duration:      float):
        """Success is only logged — no email sent."""
        print(f"[SUCCESS] {pipeline_name} | rows={rows} | duration={duration:.1f}s")
