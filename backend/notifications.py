"""
notifications.py — Email (Brevo) + SMS (Beem Africa) for Kikoba App
"""
import os
import requests
from datetime import date, timedelta

# ── Brevo (Email) ─────────────────────────────────────────────────────
BREVO_API_KEY  = os.environ.get("BREVO_API_KEY", "")
BREVO_SENDER   = {"name": "Kikoba App", "email": os.environ.get("BREVO_SENDER_EMAIL", "noreply@kikoba.app")}
BREVO_ENDPOINT = "https://api.brevo.com/v3/smtp/email"

def send_email(to_email: str, to_name: str, subject: str, html: str) -> bool:
    """Send transactional email via Brevo API."""
    if not BREVO_API_KEY:
        print("⚠️  BREVO_API_KEY not set — skipping email")
        return False
    try:
        res = requests.post(
            BREVO_ENDPOINT,
            headers={
                "api-key": BREVO_API_KEY,
                "Content-Type": "application/json"
            },
            json={
                "sender":     BREVO_SENDER,
                "to":         [{"email": to_email, "name": to_name}],
                "subject":    subject,
                "htmlContent": html
            },
            timeout=10
        )
        return res.status_code in (200, 201)
    except Exception as e:
        print(f"❌ Brevo error: {e}")
        return False


def send_password_reset_email(to_email: str, to_name: str, reset_url: str) -> bool:
    """Send password reset email via Brevo."""
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:480px;margin:auto;
                padding:24px;border:1px solid #e9ecef;border-radius:12px;">
        <div style="text-align:center;margin-bottom:20px;">
            <h2 style="color:#198754;">🌿 Kikoba App</h2>
        </div>
        <p>Hello <strong>{to_name}</strong>,</p>
        <p>You requested a password reset. Click the button below.
           This link expires in <strong>1 hour</strong>.</p>
        <div style="text-align:center;margin:28px 0;">
            <a href="{reset_url}"
               style="background:#198754;color:white;padding:14px 32px;
                      border-radius:8px;text-decoration:none;
                      font-weight:bold;font-size:15px;">
                Reset My Password
            </a>
        </div>
        <p style="color:#6c757d;font-size:13px;">
            If you didn't request this, you can safely ignore this email.
        </p>
        <hr style="border:none;border-top:1px solid #e9ecef;margin:20px 0;">
        <p style="color:#adb5bd;font-size:11px;text-align:center;">
            Kikoba App — Savings Group Management
        </p>
    </div>
    """
    return send_email(to_email, to_name, "Kikoba App — Password Reset", html)


def send_welcome_email(to_email: str, to_name: str, group_name: str, group_id: int) -> bool:
    """Welcome email when admin adds a member."""
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:480px;margin:auto;
                padding:24px;border:1px solid #e9ecef;border-radius:12px;">
        <h2 style="color:#198754;text-align:center;">🌿 Kikoba App</h2>
        <p>Hello <strong>{to_name}</strong>,</p>
        <p>You've been added to <strong>{group_name}</strong> on Kikoba App.</p>
        <p>To log in, you'll need:</p>
        <ul>
            <li><strong>Group ID:</strong> {group_id}</li>
            <li><strong>Your phone number</strong></li>
            <li><strong>Your password</strong> (set by your admin)</li>
        </ul>
        <div style="text-align:center;margin:28px 0;">
            <a href="https://kikoba.app/login"
               style="background:#198754;color:white;padding:14px 32px;
                      border-radius:8px;text-decoration:none;font-weight:bold;">
                Login to Kikoba App
            </a>
        </div>
    </div>
    """
    return send_email(to_email, to_name, f"Welcome to {group_name} — Kikoba App", html)


# ── Beem Africa (SMS) ────────────────────────────────────────────────
BEEM_API_KEY    = os.environ.get("BEEM_API_KEY", "")
BEEM_SECRET_KEY = os.environ.get("BEEM_SECRET_KEY", "")
BEEM_SENDER_ID  = os.environ.get("BEEM_SENDER_ID", "KIKOBA")
BEEM_ENDPOINT   = "https://apigw.beemafrica.com/v1/be/sms"


def send_sms(phone: str, message: str) -> bool:
    """
    Send SMS via Beem Africa.
    Phone should be in international format: 255712345678 (no + prefix)
    """
    if not BEEM_API_KEY or not BEEM_SECRET_KEY:
        print("⚠️  BEEM credentials not set — skipping SMS")
        return False

    # Normalize phone number
    phone = phone.strip().lstrip("+").lstrip("0")
    if not phone.startswith("255"):
        phone = "255" + phone

    try:
        res = requests.post(
            BEEM_ENDPOINT,
            auth=(BEEM_API_KEY, BEEM_SECRET_KEY),
            json={
                "source_addr": BEEM_SENDER_ID,
                "schedule_time": "",
                "encoding": "0",
                "message": message,
                "recipients": [{"recipient_id": 1, "dest_addr": phone}]
            },
            timeout=10
        )
        data = res.json()
        return data.get("code") == 100  # Beem success code
    except Exception as e:
        print(f"❌ Beem SMS error: {e}")
        return False


def send_loan_due_reminder(phone: str, member_name: str,
                            group_name: str, days_until_due: int,
                            monthly_amount: float, due_date: date) -> bool:
    """SMS reminder for upcoming loan repayment."""
    if days_until_due == 3:
        urgency = "in 3 days"
    elif days_until_due == 1:
        urgency = "TOMORROW"
    else:
        urgency = f"on {due_date.strftime('%d %b %Y')}"

    amount_str = f"{int(monthly_amount):,}"
    message = (
        f"Kikoba App [{group_name}]: "
        f"Habari {member_name}, rejesho lako la TZS {amount_str} "
        f"linafikia muda wake {urgency} ({due_date.strftime('%d/%m/%Y')}). "
        f"Tafadhali lipa kwa wakati ili kuepuka adhabu."
    )
    return send_sms(phone, message)


def send_penalty_started_sms(phone: str, member_name: str,
                              group_name: str, daily_rate: int) -> bool:
    """SMS when a penalty starts accruing."""
    message = (
        f"Kikoba App [{group_name}]: "
        f"Habari {member_name}, rejesho lako limechelewa. "
        f"Adhabu ya TZS {daily_rate:,} kwa siku imeanza. "
        f"Lipa haraka ili kupunguza adhabu."
    )
    return send_sms(phone, message)


def send_subscription_reminder_sms(phone: str, admin_name: str,
                                    group_name: str, amount_due: int,
                                    due_date: date) -> bool:
    """SMS to group admin when subscription is due."""
    message = (
        f"Kikoba App: "
        f"Habari {admin_name}, ada ya mwezi ya kikoba '{group_name}' "
        f"ni TZS {amount_due:,} inayohusika tarehe {due_date.strftime('%d/%m/%Y')}. "
        f"Wasiliana na msimamizi kulipa."
    )
    return send_sms(phone, message)