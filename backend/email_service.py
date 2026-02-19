"""Email Service - SendGrid"""

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import os

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'noreply@buddyliko.com')

def send_email(to_email: str, subject: str, html_content: str):
    """Send email via SendGrid"""
    
    if not SENDGRID_API_KEY:
        print(f"[EMAIL] Would send to {to_email}: {subject}")
        return
    
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=to_email,
        subject=subject,
        html_content=html_content
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
    except Exception as e:
        print(f"Email error: {e}")

def send_approval_email(email: str, name: str):
    """Send user approval email"""
    
    html = f"""
    <h2>Welcome to Buddyliko, {name}!</h2>
    <p>Your account has been approved. You can now login and start using Buddyliko.</p>
    <p><a href="{os.getenv('FRONTEND_URL')}/login">Login Now</a></p>
    """
    
    send_email(email, "Account Approved - Buddyliko", html)

def send_password_reset_email(email: str, token: str):
    """Send password reset email"""
    
    reset_url = f"{os.getenv('FRONTEND_URL')}/reset-password?token={token}"
    
    html = f"""
    <h2>Password Reset Request</h2>
    <p>Click the link below to reset your password:</p>
    <p><a href="{reset_url}">Reset Password</a></p>
    <p>This link expires in 1 hour.</p>
    <p>If you didn't request this, ignore this email.</p>
    """
    
    send_email(email, "Password Reset - Buddyliko", html)

def send_new_user_notification(admin_emails: list, user_email: str, user_name: str):
    """Notify admins of new pending user"""
    
    html = f"""
    <h2>New User Registration</h2>
    <p><strong>Name:</strong> {user_name}</p>
    <p><strong>Email:</strong> {user_email}</p>
    <p><a href="{os.getenv('FRONTEND_URL')}/admin">Review in Admin Dashboard</a></p>
    """
    
    for admin_email in admin_emails:
        send_email(admin_email, "New User Pending Approval", html)
