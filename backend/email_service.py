"""
Email Service — SMTP
Buddyliko v2.1 — Fixed: smaller logo, solid button colors (no gradients)
"""
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

SMTP_HOST = os.getenv('SMTP_HOST', '')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', '')
SMTP_PASS = os.getenv('SMTP_PASS', '')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'noreply@buddyliko.com')
FROM_NAME = os.getenv('FROM_NAME', 'Buddyliko')
FRONTEND_URL = os.getenv('FRONTEND_URL', 'https://buddyliko.com')


def _configured() -> bool:
    return bool(SMTP_HOST and SMTP_USER and SMTP_PASS)


def configure(host, port, user, password, from_email, from_name, frontend_url):
    global SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, FROM_EMAIL, FROM_NAME, FRONTEND_URL
    SMTP_HOST = host
    SMTP_PORT = port
    SMTP_USER = user
    SMTP_PASS = password
    FROM_EMAIL = from_email
    FROM_NAME = from_name
    FRONTEND_URL = frontend_url


def send_email(to_email, subject, html_content, text_content=False):
    if not _configured():
        print(f"[EMAIL] SMTP non configurato — avrei inviato a {to_email}: {subject}")
        return False

    msg = MIMEMultipart('alternative')
    msg['From'] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(text_content or subject, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_content, 'html', 'utf-8'))

    try:
        if SMTP_PORT == 465:
            ctx = ssl.create_default_context()
            s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=15)
        else:
            s = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
            s.ehlo()
            ctx = ssl.create_default_context()
            s.starttls(context=ctx)
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
        print(f"[EMAIL] Inviata a {to_email}")
        return True
    except Exception as e:
        print(f"[EMAIL] Errore invio a {to_email}: {e}")
        return False


# ── Email button style (solid colors, no gradients — works in ALL email clients) ──
_BTN_PRIMARY = (
    "display:inline-block;padding:14px 36px;"
    "background-color:#2563eb;color:#ffffff;"
    "text-decoration:none;border-radius:8px;font-weight:600;font-size:15px"
)
_BTN_SUCCESS = (
    "display:inline-block;padding:14px 36px;"
    "background-color:#059669;color:#ffffff;"
    "text-decoration:none;border-radius:8px;font-weight:600;font-size:15px"
)
_BTN_ADMIN = (
    "display:inline-block;padding:10px 24px;"
    "background-color:#2563eb;color:#ffffff;"
    "text-decoration:none;border-radius:6px;font-weight:600;font-size:14px"
)


def _tpl(title, body):
    return (
        '<div style="max-width:560px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,sans-serif;color:#1e293b">'
        # ── Header (solid dark bg — no gradient) ──
        '<div style="background-color:#0d1220;padding:20px 28px;text-align:center;border-radius:12px 12px 0 0">'
        f'<img src="{FRONTEND_URL}/assets/logo-new.png" alt="Buddyliko" style="height:28px">'
        '</div>'
        # ── Body ──
        '<div style="padding:32px 28px;background:#fff;border:1px solid #e2e8f0;border-top:none">'
        f'<h2 style="margin:0 0 16px;font-size:20px">{title}</h2>'
        f'{body}'
        '</div>'
        # ── Footer ──
        '<div style="padding:16px 28px;background:#f8fafc;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px;text-align:center;font-size:12px;color:#94a3b8">'
        '&copy; 2026 Buddyliko</div>'
        '</div>'
    )


def send_verification_email(email, name, token):
    url = f"{FRONTEND_URL}/login.html?verify={token}"
    html = _tpl("Verifica la tua email", f"""
        <p style="color:#475569">Ciao <strong>{name}</strong>,</p>
        <p style="color:#475569">Clicca il pulsante per verificare il tuo indirizzo email:</p>
        <div style="text-align:center;margin:28px 0">
            <a href="{url}" style="{_BTN_PRIMARY}">Verifica Email</a></div>
        <p style="color:#94a3b8;font-size:13px">Il link scade tra 24 ore.</p>""")
    send_email(email, "Verifica la tua email — Buddyliko", html)


def send_mfa_code_email(email, name, code):
    html = _tpl("Codice di verifica", f"""
        <p style="color:#475569">Ciao <strong>{name}</strong>,</p>
        <p style="color:#475569">Il tuo codice di verifica:</p>
        <div style="text-align:center;margin:28px 0">
            <div style="display:inline-block;padding:16px 40px;background:#f1f5f9;border:2px solid #e2e8f0;border-radius:12px;font-size:36px;font-weight:800;letter-spacing:8px;color:#0f172a">{code}</div></div>
        <p style="color:#94a3b8;font-size:13px">Il codice scade tra 10 minuti.</p>""")
    send_email(email, f"Codice: {code} — Buddyliko", html)


def send_approval_email(email, name):
    html = _tpl("Account approvato!", f"""
        <p style="color:#475569">Ciao <strong>{name}</strong>,</p>
        <p style="color:#475569">Il tuo account è stato approvato!</p>
        <div style="text-align:center;margin:28px 0">
            <a href="{FRONTEND_URL}/login.html" style="{_BTN_SUCCESS}">Accedi ora</a></div>""")
    send_email(email, "Account approvato — Buddyliko", html)


def send_password_reset_email(email, token):
    url = f"{FRONTEND_URL}/login.html?reset={token}"
    html = _tpl("Reset password", f"""
        <p style="color:#475569">Hai richiesto il reset della password.</p>
        <div style="text-align:center;margin:28px 0">
            <a href="{url}" style="{_BTN_PRIMARY}">Reset Password</a></div>
        <p style="color:#94a3b8;font-size:13px">Il link scade tra 1 ora.</p>""")
    send_email(email, "Reset password — Buddyliko", html)


def send_new_user_notification(admin_emails, user_email, user_name):
    html = _tpl("Nuova registrazione", f"""
        <p style="color:#475569">Nuovo utente registrato:</p>
        <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:16px;margin:16px 0">
            <p style="margin:4px 0"><strong>Nome:</strong> {user_name}</p>
            <p style="margin:4px 0"><strong>Email:</strong> {user_email}</p></div>
        <div style="text-align:center;margin:20px 0">
            <a href="{FRONTEND_URL}/admin.html" style="{_BTN_ADMIN}">Pannello Admin</a></div>""")
    for ae in admin_emails:
        send_email(ae, f"Nuova registrazione: {user_name}", html)
