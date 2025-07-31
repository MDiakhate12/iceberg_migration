# -*- coding: utf-8 -*-
# %%
import os
import time
import boto3
import requests
import datetime
import smtplib
from getpass import getpass
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.utils import formatdate


class PowerBIClient:
    """A class to connect to Power BI and perform operations like exporting reports to PDF."""

    def __init__(self, env="inte"):

        self.env = env

        # Vault Configuration
        self._vault_addr = f"http://vault.main.{env}"

        credentials = self.get_vault_credentials()

        self._pbi_access_token = self.get_pbi_access_token(
            credentials["client_id"],
            credentials["client_secret"],
            credentials["tenant_id"]
        )

        self._headers = {
            "Authorization": f"Bearer {self._pbi_access_token}",
            "Content-Type": "application/json"
        }

    def get_vault_credentials(self):
        """Retrieve credentials from Vault."""

        if is_running_on_aws_lambda():
            # In AWS Lambda, use environment variables for credentials
            print("🔃 Retrieving credentials from Mounted Vault")

            return {
                "client_id": os.getenv("pbi_client_id"),
                "client_secret": os.getenv("pbi_client_secret"),
                "tenant_id": os.getenv("pbi_tenant_id")
            }

        else:
            # In local environment, prompt for LDAP credentials and retrieve from Vault
            path = "kv/snowlake/data/main/powerbi"
            username = input("Email LDAP pour vault : ")  # mouhammad.diakhate@sogelink.com
            password = getpass("Mot de passe LDAP pour vault : ")

            print(f"🔃 Authenticating with Vault at {self._vault_addr}...")

            url = f"{self._vault_addr}/v1/auth/ldap/login/{username}"
            response = requests.post(
                url,
                json={"password": password},
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 200:
                print(f"✅ Succesfully authenticated to Vault at {self._vault_addr}")
                vault_token = response.json()["auth"]["client_token"]

                print(f"🔃 Retrieving credentials from Vault at {path}...")

                response = requests.get(f"{self._vault_addr}/v1/{path}", headers={"X-Vault-Token": vault_token})
                print(response.status_code, response.json())
                if response.status_code == 200:
                    print(f"✅ Succesfully retrieved credentials from Local Vault at {path}")
                    return response.json()["data"]["data"]
                else:
                    raise Exception(f"❌ Failed to retrieve data: {response.status_code} - {response.text}")
            else:
                raise Exception(f"❌ Login failed: {response.status_code} - {response.text}")

    def get_pbi_access_token(self, client_id, client_secret, tenant_id):
        """Get an access token for Power BI API using client credentials flow."""

        print("🔃 Retrieving PBI access token for Power BI API...")

        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        }
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            print("✅ PBI Access token retrieved successfully.")
            return response.json()["access_token"]
        else:
            raise Exception(f"❌ Failed to get access token: {response.status_code} - {response.text}")

    def export_report_to_pdf(self, group_id, report_id):
        """Export a Power BI report to PDF format and return its name."""

        # Get the report metadata first
        report_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}"
        report_response = requests.get(report_url, headers=self._headers)

        if report_response.status_code != 200:
            raise Exception(f"❌ Failed to retrieve report info: {report_response.status_code} - {report_response.text}")

        report_info = report_response.json()
        report_name = report_info.get("name", "Unknown Report")

        print(f"🔃 Exporting report '{report_name}' ({report_id}) from group {group_id} to PDF...")

        # Prepare export request
        export_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}/ExportTo"
        data = {
            "format": "PDF",
            "powerBIReportConfiguration": {
                "pages": [],
                "settings": {
                    "includeHiddenPages": False
                }
            }
        }

        response = requests.post(export_url, headers=self._headers, json=data)

        if response.status_code == 202:
            export_data = response.json()
            print(f"✅ Export started. Report: '{report_name}' | Export ID: {export_data['id']}")
            return {
                "report": report_info,
                "export": export_data
            }
        else:
            raise Exception(f"❌ Failed to export report: {response.status_code} - {response.text}")

    def get_export_status(self, group_id, report_id, export_id):
        """Check the status of the export operation."""

        url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}/exports/{export_id}"
        response = requests.get(url, headers=self._headers)
        print(f"🔃 Checking export status for Export ID: {export_id}..."
              f" - URL: {url} - Status Code: {response.status_code}")
        if response.status_code == 202:
            print("🔃 ...")
            return response.json()
        if response.status_code == 200:
            print("✅ Export completed successfully.")
            return response.json()
        else:
            raise Exception(f"Failed to get export status: {response.status_code} - {response.text}")

    def download_exported_file(self, download_url, output_file):
        """Download the exported file from the provided URL."""

        print(f"🔃 Downloading file from {download_url} to {output_file}...")

        response = requests.get(download_url, headers=self._headers, stream=True)
        if response.status_code == 200:
            with open(output_file, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            print(f"✅ File downloaded: {output_file}")
        else:
            raise Exception(f"❌ Failed to download file: {response.status_code} - {response.text}")

    def get_workspaces(self):
        url = "https://api.powerbi.com/v1.0/myorg/groups"
        response = requests.get(url, headers=self._headers)
        if response.status_code == 200:
            return response.json()["value"]
        else:
            raise Exception(f"❌ Failed to retrieve workspaces: {response.status_code} - {response.text}")

    def get_reports(self, group_id):
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports"
        response = requests.get(url, headers=self._headers)
        if response.status_code == 200:
            return response.json()["value"]
        else:
            raise Exception(f"❌ Failed to retrieve reports: {response.status_code} - {response.text}")

    def download_report(self, report, output_dir):
        """Export a Power BI report to PDF and download it."""
        # Poll for export status
        print(f"🔃 Checking export status for report '{report['report_name']}' - Report Id {report['report_id']} - Export Id {report['export_id']}...")
        while True:
            status = self.get_export_status(
                group_id=report["workspace_id"],
                report_id=report["report_id"],
                export_id=report["export_id"],
            )
            if status["status"] == "Succeeded":
                output_path = os.path.join(output_dir, f"{report['report_name']}.pdf")
                self.download_exported_file(status["resourceLocation"], output_path)
                print(f"✅ Export completed successfully for {report['report_name']}. File saved at {output_path}")
                return {
                    "report_id": report["report_id"],
                    "report_name": report["report_name"],
                    "workspace_name": report["workspace_name"],
                    "workspace_id": report["workspace_id"],
                    "pdf_path": output_path,
                    "export_id": report["export_id"],
                    "export_time": datetime.datetime.now()
                }
            elif status["status"] == "Failed":
                raise Exception(f"❌ Export failed for {report['report_name']}.")
            else:
                print("🔃 Export in progress...")
                time.sleep(5)

    def build_message_from_reports(
            self,
            sender_email,
            recipient_emails,
            subject,
            exported_reports  # list of dicts with metadata and PDF path
    ):
        # Création de l'objet email
        message = MIMEMultipart("mixed")
        message["From"] = sender_email
        message["To"] = ", ".join(recipient_emails)
        message["Date"] = formatdate(localtime=True)
        message["Subject"] = subject

        # Partie alternative : texte + HTML
        alternative_part = MIMEMultipart("alternative")

        # Corps du message
        text_body = [
            "Bonjour,\n\nVeuillez trouver en pièce jointe l'export PDF des rapports Power BI ci-dessous :\n\n",
        ]
        for report in exported_reports:
            text_body.append(f"📄 Rapport : {report['report_name']} - (ID: {report['report_id']})")
            text_body.append(f"🔹 Workspace : {report['workspace_name']} (ID: {report['workspace_id']})")
            text_body.append(f"🕒 Exporté le : {report['export_time']}")
            text_body.append("")

        text_body.append("Bonne journée,\nL'équipe automatisée")

        text_body = "\n".join(text_body)

        # Version HTML stylisée
        html_body = """
        <html>
        <head>
            <style>
                table {
                    width: 100%;
                    border-collapse: collapse;
                    font-family: Arial, sans-serif;
                }
                th, td {
                    border: 1px solid #dddddd;
                    text-align: left;
                    padding: 8px;
                }
                th {
                    background-color: #f2f2f2;
                }
                tr:nth-child(even) {
                    background-color: #f9f9f9;
                }
                .footer {
                    margin-top: 20px;
                    font-family: Arial, sans-serif;
                }
            </style>
        </head>
        <body>
            <p>Bonjour,</p>
            <p>Veuillez trouver en pièce jointe l'export PDF des rapports Power BI ci-dessous :</p>

            <table>
                <tr>
                    <th>Rapport</th>
                    <th>Workspace</th>
                    <th>Exporté le</th>
                </tr>
        """

        for report in exported_reports:
            html_body += f"""
                <tr>
                    <td>{report['report_name']}<br><small>ID: {report['report_id']}</small></td>
                    <td>{report['workspace_name']}<br><small>ID: {report['workspace_id']}</small></td>
                    <td>{report['export_time']}</td>
                </tr>
            """

        html_body += """
            </table>
            <p class="footer">Bonne journée,<br>L'équipe automatisée</p>
        </body>
        </html>
        """

        # Attacher texte et HTML dans la partie alternative
        alternative_part.attach(MIMEText(text_body, "plain"))
        alternative_part.attach(MIMEText(html_body, "html"))

        # Attacher la partie alternative au message principal
        message.attach(alternative_part)

        # Ajouter les pièces jointes
        for report in exported_reports:
            with open(report["pdf_path"], "rb") as f:
                part = MIMEApplication(f.read(), _subtype="pdf")
                part.add_header("Content-Disposition", "attachment", filename=os.path.basename(report["pdf_path"]))
                message.attach(part)

        print("✅ Email multipart (texte + HTML) construit avec succès.")
        return message

    def send_powerbi_report_email_via_ses(
        self,
        sender_email,
        recipient_emails,
        subject,
        exported_reports  # list of dicts with metadata and PDF path
    ):
        message = self.build_message_from_reports(
            sender_email=sender_email,
            recipient_emails=recipient_emails,
            subject=subject,
            exported_reports=exported_reports
        )  # Build the email message with attachments

        # Envoi via Amazon SES
        ses_client = boto3.client("ses", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
        response = ses_client.send_raw_email(
            Source=sender_email,
            Destinations=recipient_emails,
            RawMessage={"Data": message.as_string()}
        )

        print("📬 E-mail envoyé avec succès via SES.")
        return response

    def send_powerbi_report_email_via_stmplib(
            self,
            smtp_host,
            smtp_port,
            sender_email,
            recipient_emails,
            subject,
            exported_reports):

        message = self.build_message_from_reports(
            sender_email=sender_email,
            recipient_emails=recipient_emails,
            subject=subject,
            exported_reports=exported_reports
        )  # Build the email message with attachments

        # Envoi via SMTP
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(message)

        print("📬 E-mail envoyé avec succès.")

# %%


def is_running_on_aws_lambda():
    return os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None


def main(event, context):
    """
    AWS Lambda handler function to export Power BI reports and send them via email.
    """
    env = os.getenv("env", "inte")
    connector = PowerBIClient(env=env)

    # Input: List of report names to export
    default_reports_to_export = [
            {"workspace_name": "JMAN", "report_name": "GTM"},
            {"workspace_name": "Products", "report_name": "CBYD Daily Dashboard"},
    ]

    # Retrieve reports_to_export from the Lambda event or default to a predifined list
    reports_to_export = event.get("reports_to_export", default_reports_to_export)
    additional_recipient_emails = event.get("emails", [])

    if not reports_to_export:
        print("❌ No reports specified in the event. Exiting.")
        return {"status": "error", "message": "No reports specified in the event."}

    # Validate and enrich reports_to_export with workspace and report IDs
    workspaces = connector.get_workspaces()

    enriched_reports_to_export = []
    for report in reports_to_export:
        workspace_name = report.get("workspace_name")
        report_name = report.get("report_name")

        if not workspace_name or not report_name:
            print(f"❌ Invalid report entry: {report}. Skipping.")
            continue

        # Find the corresponding workspace and report IDs
        matching_workspace = next((ws for ws in workspaces if ws["name"] == workspace_name), None)
        if not matching_workspace:
            print(f"❌ Workspace '{workspace_name}' not found. Skipping report '{report_name}'.")
            continue

        workspace_id = matching_workspace["id"]
        reports = connector.get_reports(workspace_id)
        matching_report = next((r for r in reports if r["name"] == report_name), None)
        if not matching_report:
            print(f"❌ Report '{report_name}' not found in workspace '{workspace_name}'. Skipping.")
            continue

        enriched_reports_to_export.append({
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "report_name": report_name,
            "report_id": matching_report["id"]
        })

    if not enriched_reports_to_export:
        print("❌ No valid reports to export. Exiting.")
        return {"status": "error", "message": "No valid reports to export."}

    # Ensure output directories exist
    for report in enriched_reports_to_export:
        if is_running_on_aws_lambda():
            output_dir = os.path.join("/tmp", report["workspace_name"])
        else:
            output_dir = report["workspace_name"]

        os.makedirs(output_dir, exist_ok=True)

    # Export and download reports sequentially
    exported_reports = []
    for report in enriched_reports_to_export:
        try:
            export_result = connector.export_report_to_pdf(report["workspace_id"], report["report_id"])
            report["export_id"] = export_result["export"]["id"]
            result = connector.download_report(report, output_dir)
            exported_reports.append(result)
        except Exception as e:
            print(f"❌ Failed to export or download report '{report['report_name']}' from workspace '{report['workspace_name']}': {str(e)}")

    # Print summary of exported reports
    print("📄 Exported Reports:")

    # Email configuration
    smtp_host = "cert-mailing.certilience.fr"
    smtp_port = 25
    sender_email = "aws.lambda@sogelink.fr"
    recipient_emails = [
        "dev-snow@sogelink.com",
        *additional_recipient_emails,
    ]
    subject = f"Power BI Reports Exported - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    if exported_reports:
        # Send the email with the exported reports
        connector.send_powerbi_report_email_via_stmplib(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            sender_email=sender_email,
            recipient_emails=recipient_emails,
            subject=subject,
            exported_reports=exported_reports,
        )
    else:
        print("❌ No reports were exported. Email will not be sent.")


if __name__ == "__main__":
    # For local testing purposes only (Important Note ! emails only work when running on-site or in AWS Lambda)
    # To test locally, you can call main with a mock event and context
    mock_event = {
        "reports_to_export": [
            {"workspace_name": "JMAN", "report_name": "GTM Dashboard - 18.12 Refreshed"},
            {"workspace_name": "JMAN", "report_name": "GTM Dashboard - 18.13 Refreshed"},
            {"workspace_name": "Products", "report_name": "CBYD Daily Dashboard"},
        ]
    }
    mock_context = None  # Context is not used in this example
    main(
        event=mock_event,
        context=mock_context
    )
