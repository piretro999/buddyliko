#!/usr/bin/env python3
"""
Buddyliko - Alerts & Analytics Engine

Moduli:
  AlertEngine        — soglie usage per utente/gruppo, notifiche multi-canale
  AIBudgetMonitor    — credito Anthropic/OpenAI: consumo, burn rate, previsione esaurimento
  ProfitabilityEngine— ARPU, costo per trasformazione, margine netto per utente/gruppo/periodo
  PricingManager     — gestione piani, sconti, coupon, utenti gratuiti con override

Notifiche supportate:
  - Email (SMTP)
  - In-app (tabella alerts, letta dal frontend)
  - Webhook (Slack/Teams/custom HTTP)

Tabelle DB create:
  alert_rules          — regole soglia configurabili
  alert_events         — storico notifiche inviate (idempotente per periodo)
  ai_budget_snapshots  — snapshot giornalieri credito AI
  pricing_rules        — piani, sconti, override per utente
  discount_codes       — coupon con validità e utilizzi
"""

import json
import uuid
import smtplib
import threading
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, List, Any, Tuple
from enum import Enum


# ===========================================================================
# CONSTANTS
# ===========================================================================

class AlertType(str, Enum):
    USER_USAGE_80    = "user_usage_80"     # utente all'80% quota mese
    USER_USAGE_95    = "user_usage_95"     # utente al 95% quota mese
    USER_USAGE_100   = "user_usage_100"    # utente quota esaurita
    GROUP_USAGE_80   = "group_usage_80"
    GROUP_USAGE_95   = "group_usage_95"
    GROUP_USAGE_100  = "group_usage_100"
    AI_BUDGET_50     = "ai_budget_50"      # credito AI al 50%
    AI_BUDGET_25     = "ai_budget_25"      # credito AI al 25%
    AI_BUDGET_10     = "ai_budget_10"      # credito AI al 10% — urgente
    SUBSCRIPTION_EXPIRING_7  = "subscription_expiring_7"   # abbonamento scade in 7 giorni
    SUBSCRIPTION_EXPIRING_3  = "subscription_expiring_3"
    PAYMENT_FAILED   = "payment_failed"


class NotificationChannel(str, Enum):
    EMAIL   = "email"
    IN_APP  = "in_app"
    WEBHOOK = "webhook"


# Costo stimato per operazione (in €-cent) — usato per calcolo margine
COST_PER_OPERATION = {
    "transform_xml_small":    0.001,  # < 100KB
    "transform_xml_large":    0.005,  # > 1MB
    "codegen":                0.020,  # generazione codice (usa LLM)
    "ai_automap":             0.015,  # AI auto-mapping
    "api_call":               0.0005,
    "db_read":                0.0002,
    "db_write":               0.0003,
    "edi_parse":              0.0005,
    "hl7_parse":              0.0005,
    "async_transform":        0.003,
    "storage_per_mb_month":   0.0010,
}

# Revenue mensile per piano
PLAN_REVENUE_EUR = {
    "FREE":       0.0,
    "PRO":        49.0,
    "ENTERPRISE": 299.0,
    "CUSTOM":     0.0,   # override manuale
}


# ===========================================================================
# ALERT ENGINE
# ===========================================================================

class AlertEngine:
    """
    Monitora usage utenti e gruppi, emette alert multi-canale.
    Da chiamare periodicamente (cron / APScheduler / background thread).
    """

    def __init__(self, conn, RealDictCursor, config: Dict = None):
        self.conn = conn
        self.RDC = RealDictCursor
        self.config = config or {}
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alert_rules (
                id VARCHAR(36) PRIMARY KEY,
                rule_type VARCHAR(80) NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                -- canali notifica (JSON list: ["email","in_app","webhook"])
                channels JSONB DEFAULT '["in_app"]',
                -- destinatari email admin (JSON list di email)
                admin_emails JSONB DEFAULT '[]',
                -- webhook URL per Slack/Teams/custom
                webhook_url TEXT,
                -- soglia custom (se NULL usa default)
                threshold_pct INTEGER,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(rule_type)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alert_events (
                id SERIAL PRIMARY KEY,
                alert_type VARCHAR(80) NOT NULL,
                target_type VARCHAR(20) NOT NULL,   -- 'user' | 'group' | 'system'
                target_id VARCHAR(255),
                target_name VARCHAR(255),
                message TEXT,
                severity VARCHAR(20) DEFAULT 'warning', -- 'info' | 'warning' | 'critical'
                channel VARCHAR(20),
                sent_at TIMESTAMPTZ DEFAULT NOW(),
                -- idempotency: non mando lo stesso alert due volte nello stesso periodo
                period_key VARCHAR(40),             -- es. '2026-02_user_123_usage_80'
                UNIQUE(period_key, channel)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS in_app_notifications (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255),               -- NULL = tutti gli admin
                title VARCHAR(255) NOT NULL,
                message TEXT,
                severity VARCHAR(20) DEFAULT 'warning',
                alert_type VARCHAR(80),
                read_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ian_user ON in_app_notifications(user_id, read_at)")
        print("✅ Alert Engine tables initialized")

    # ------------------------------------------------------------------
    # MAIN CHECK — chiamato dal job schedulato
    # ------------------------------------------------------------------

    def run_checks(self) -> Dict:
        """
        Esegue tutti i check e invia notifiche dove necessario.
        Ritorna summary delle azioni intraprese.
        """
        now = datetime.now(timezone.utc)
        month = now.strftime('%Y-%m')
        summary = {"alerts_sent": 0, "alerts_skipped": 0, "errors": []}

        try:
            self._check_user_usage(month, summary)
        except Exception as e:
            summary["errors"].append(f"user_usage: {e}")

        try:
            self._check_group_usage(month, summary)
        except Exception as e:
            summary["errors"].append(f"group_usage: {e}")

        try:
            self._check_expiring_subscriptions(now, summary)
        except Exception as e:
            summary["errors"].append(f"subscriptions: {e}")

        return summary

    def _check_user_usage(self, month: str, summary: Dict):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT u.user_id, u.transforms_count,
                   us.email, us.name, us.plan
            FROM usage_counters u
            JOIN users us ON us.id::text = u.user_id
            WHERE u.month = %s
        """, (month,))
        rows = cur.fetchall()

        plan_limits = {
            "FREE": 100, "PRO": -1, "ENTERPRISE": -1, "CUSTOM": -1
        }

        for row in rows:
            plan = row.get("plan", "FREE")
            limit = plan_limits.get(plan, 100)
            if limit == -1:
                continue  # illimitato

            used = row.get("transforms_count", 0)
            pct = (used / limit * 100) if limit > 0 else 0

            for threshold, atype, severity in [
                (100, AlertType.USER_USAGE_100, "critical"),
                (95,  AlertType.USER_USAGE_95,  "warning"),
                (80,  AlertType.USER_USAGE_80,  "info"),
            ]:
                if pct >= threshold:
                    self._emit_alert(
                        alert_type=atype,
                        target_type="user",
                        target_id=str(row["user_id"]),
                        target_name=row.get("email", row["user_id"]),
                        message=(f"Utente {row.get('email',row['user_id'])} "
                                 f"ha usato {used}/{limit} trasformazioni ({pct:.0f}%) "
                                 f"nel mese {month}"),
                        severity=severity,
                        period_key=f"{month}_user_{row['user_id']}_{threshold}",
                        summary=summary,
                    )
                    break  # invia solo la soglia più alta raggiunta

    def _check_group_usage(self, month: str, summary: Dict):
        """Aggrega usage per gruppo e controlla soglie."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        # Raggruppa per group_id degli utenti
        cur.execute("""
            SELECT us.group_id,
                   SUM(u.transforms_count) as total_transforms,
                   COUNT(DISTINCT u.user_id) as member_count,
                   MAX(g.name) as group_name,
                   MAX(g.plan_override) as group_plan
            FROM usage_counters u
            JOIN users us ON us.id::text = u.user_id
            LEFT JOIN groups g ON g.id::text = us.group_id
            WHERE u.month = %s AND us.group_id IS NOT NULL
            GROUP BY us.group_id
        """, (month,))
        rows = cur.fetchall()

        for row in rows:
            group_plan = row.get("group_plan") or "FREE"
            # Limite gruppo = limite piano * numero membri (semplificazione)
            base_limit = {"FREE": 100, "PRO": -1, "ENTERPRISE": -1, "CUSTOM": -1}.get(group_plan, 100)
            if base_limit == -1:
                continue
            limit = base_limit * max(1, int(row.get("member_count", 1)))
            used = int(row.get("total_transforms", 0))
            pct = (used / limit * 100) if limit > 0 else 0

            for threshold, atype, severity in [
                (100, AlertType.GROUP_USAGE_100, "critical"),
                (95,  AlertType.GROUP_USAGE_95,  "warning"),
                (80,  AlertType.GROUP_USAGE_80,  "info"),
            ]:
                if pct >= threshold:
                    gid = str(row["group_id"])
                    gname = row.get("group_name") or gid
                    self._emit_alert(
                        alert_type=atype,
                        target_type="group",
                        target_id=gid,
                        target_name=gname,
                        message=(f"Gruppo \"{gname}\" ha usato {used}/{limit} "
                                 f"trasformazioni ({pct:.0f}%) nel mese {month}"),
                        severity=severity,
                        period_key=f"{month}_group_{gid}_{threshold}",
                        summary=summary,
                    )
                    break

    def _check_expiring_subscriptions(self, now: datetime, summary: Dict):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT s.user_id, s.plan, s.period_end,
                   u.email, u.name
            FROM subscriptions s
            JOIN users u ON u.id::text = s.user_id
            WHERE s.status = 'active'
              AND s.period_end IS NOT NULL
              AND s.cancel_at_period_end = TRUE
        """)
        for row in cur.fetchall():
            end = row.get("period_end")
            if not end:
                continue
            if isinstance(end, str):
                try:
                    end = datetime.fromisoformat(end.replace("Z", "+00:00"))
                except Exception:
                    continue
            days_left = (end - now).days
            for threshold, atype in [(3, AlertType.SUBSCRIPTION_EXPIRING_3),
                                      (7, AlertType.SUBSCRIPTION_EXPIRING_7)]:
                if days_left <= threshold:
                    uid = str(row["user_id"])
                    self._emit_alert(
                        alert_type=atype,
                        target_type="user",
                        target_id=uid,
                        target_name=row.get("email", uid),
                        message=(f"Abbonamento {row.get('plan','?')} di {row.get('email',uid)} "
                                 f"scade il {end.strftime('%d/%m/%Y')} ({days_left} giorni)"),
                        severity="warning" if days_left > 3 else "critical",
                        period_key=f"{now.strftime('%Y-%m-%d')}_sub_exp_{uid}_{threshold}",
                        summary=summary,
                    )
                    break

    # ------------------------------------------------------------------
    # EMIT
    # ------------------------------------------------------------------

    def _emit_alert(self, alert_type: str, target_type: str, target_id: str,
                    target_name: str, message: str, severity: str,
                    period_key: str, summary: Dict):
        """Emette un alert su tutti i canali configurati, con idempotenza."""
        channels = self._get_channels(alert_type)
        for channel in channels:
            # Check idempotency
            cur = self.conn.cursor()
            cur.execute("""
                SELECT id FROM alert_events
                WHERE period_key = %s AND channel = %s
            """, (period_key, channel))
            if cur.fetchone():
                summary["alerts_skipped"] += 1
                continue
            # Insert record
            cur.execute("""
                INSERT INTO alert_events
                    (alert_type, target_type, target_id, target_name,
                     message, severity, channel, period_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (alert_type, target_type, target_id, target_name,
                  message, severity, channel, period_key))

            # Send via channel
            try:
                if channel == NotificationChannel.IN_APP:
                    self._send_in_app(alert_type, target_name, message, severity)
                elif channel == NotificationChannel.EMAIL:
                    self._send_email(alert_type, target_name, message, severity)
                elif channel == NotificationChannel.WEBHOOK:
                    self._send_webhook(alert_type, target_name, message, severity)
                summary["alerts_sent"] += 1
            except Exception as e:
                summary["errors"].append(f"{channel} send error: {e}")

    def _get_channels(self, alert_type: str) -> List[str]:
        """Recupera canali configurati per questo tipo di alert."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT channels FROM alert_rules
            WHERE rule_type = %s AND enabled = TRUE
        """, (alert_type,))
        row = cur.fetchone()
        if row and row[0]:
            ch = row[0] if isinstance(row[0], list) else json.loads(row[0])
            return ch
        return [NotificationChannel.IN_APP]  # default: solo in-app

    def _send_in_app(self, alert_type: str, target_name: str, message: str, severity: str):
        """Crea notifica in-app visibile agli admin nella dashboard."""
        titles = {
            "user_usage_80":  "⚠️ Utente al 80% quota",
            "user_usage_95":  "🔴 Utente al 95% quota",
            "user_usage_100": "🚫 Utente quota esaurita",
            "group_usage_80": "⚠️ Gruppo al 80% quota",
            "group_usage_95": "🔴 Gruppo al 95% quota",
            "group_usage_100":"🚫 Gruppo quota esaurita",
            "ai_budget_50":   "💰 Credito AI al 50%",
            "ai_budget_25":   "⚠️ Credito AI al 25%",
            "ai_budget_10":   "🚨 Credito AI critico!",
            "subscription_expiring_7": "📅 Abbonamento in scadenza",
            "subscription_expiring_3": "🔴 Abbonamento scade tra 3 giorni",
            "payment_failed": "❌ Pagamento fallito",
        }
        title = titles.get(alert_type, "⚠️ Alert sistema")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO in_app_notifications
                (user_id, title, message, severity, alert_type)
            VALUES (NULL, %s, %s, %s, %s)
        """, (title, message, severity, alert_type))

    def _send_email(self, alert_type: str, target_name: str, message: str, severity: str):
        """Invia email via SMTP. Configurazione da config.yaml: smtp."""
        smtp_cfg = self.config.get("smtp", {})
        if not smtp_cfg.get("host") or not smtp_cfg.get("from_email"):
            return  # SMTP non configurato

        recipients = self._get_admin_emails(alert_type)
        if not recipients:
            return

        subject = f"[Buddyliko] {severity.upper()}: {alert_type.replace('_', ' ').title()}"
        body = f"""
<html><body style="font-family:sans-serif;max-width:600px;margin:0 auto">
<div style="background:#1a1e35;padding:24px;border-radius:8px">
  <h2 style="color:#e2e8f0;margin:0 0 16px">⚠️ {subject}</h2>
  <p style="color:#94a3b8;font-size:14px">{message}</p>
  <div style="margin-top:20px;padding:12px;background:#0f1225;border-radius:6px">
    <p style="color:#64748b;font-size:12px;margin:0">
      Generato il {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M')} UTC<br>
      Buddyliko Platform — <a href="https://buddyliko.com/finance" style="color:#2563eb">Apri Dashboard</a>
    </p>
  </div>
</div>
</body></html>
"""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_cfg["from_email"]
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText(body, "html"))

        port = int(smtp_cfg.get("port", 587))
        use_tls = smtp_cfg.get("tls", True)
        try:
            if use_tls:
                server = smtplib.SMTP(smtp_cfg["host"], port, timeout=10)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(smtp_cfg["host"], port, timeout=10)
            if smtp_cfg.get("username"):
                server.login(smtp_cfg["username"], smtp_cfg.get("password", ""))
            server.sendmail(smtp_cfg["from_email"], recipients, msg.as_string())
            server.quit()
        except Exception as e:
            raise RuntimeError(f"SMTP error: {e}")

    def _send_webhook(self, alert_type: str, target_name: str, message: str, severity: str):
        """Invia webhook Slack/Teams/HTTP. Configurazione da alert_rules.webhook_url."""
        cur = self.conn.cursor()
        cur.execute("SELECT webhook_url FROM alert_rules WHERE rule_type = %s", (alert_type,))
        row = cur.fetchone()
        if not row or not row[0]:
            return

        webhook_url = row[0]
        # Formato Slack-compatible (funziona anche con Teams via Incoming Webhook)
        color = {"critical": "#ef4444", "warning": "#f59e0b", "info": "#3b82f6"}.get(severity, "#94a3b8")
        payload = {
            "attachments": [{
                "color": color,
                "title": f"[Buddyliko] {alert_type.replace('_', ' ').upper()}",
                "text": message,
                "footer": "Buddyliko Platform",
                "ts": int(datetime.now(timezone.utc).timestamp()),
                "fields": [
                    {"title": "Severity", "value": severity.upper(), "short": True},
                    {"title": "Target", "value": target_name, "short": True},
                ]
            }]
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url, data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status not in (200, 204):
                    raise RuntimeError(f"Webhook returned {resp.status}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"Webhook error: {e}")

    def _get_admin_emails(self, alert_type: str) -> List[str]:
        cur = self.conn.cursor()
        # Prima: email dalla regola
        cur.execute("SELECT admin_emails FROM alert_rules WHERE rule_type = %s", (alert_type,))
        row = cur.fetchone()
        if row and row[0]:
            emails = row[0] if isinstance(row[0], list) else json.loads(row[0])
            if emails:
                return emails
        # Fallback: tutti gli admin nel DB
        cur.execute("SELECT email FROM users WHERE role IN ('MASTER','ADMIN') AND email IS NOT NULL")
        return [r[0] for r in cur.fetchall()]

    # ------------------------------------------------------------------
    # CRUD REGOLE
    # ------------------------------------------------------------------

    def upsert_rule(self, rule_type: str, enabled: bool = True,
                    channels: List[str] = None, admin_emails: List[str] = None,
                    webhook_url: str = None, threshold_pct: int = None) -> str:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO alert_rules (id, rule_type, enabled, channels, admin_emails, webhook_url, threshold_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (rule_type) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                channels = EXCLUDED.channels,
                admin_emails = EXCLUDED.admin_emails,
                webhook_url = EXCLUDED.webhook_url,
                threshold_pct = EXCLUDED.threshold_pct,
                updated_at = NOW()
        """, (str(uuid.uuid4()), rule_type, enabled,
              json.dumps(channels or ["in_app"]),
              json.dumps(admin_emails or []),
              webhook_url, threshold_pct))
        return rule_type

    def get_rules(self) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM alert_rules ORDER BY rule_type")
        return [dict(r) for r in cur.fetchall()]

    def get_notifications(self, limit: int = 50, unread_only: bool = False) -> List[Dict]:
        """Notifiche in-app per la dashboard admin."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        where = "WHERE (user_id IS NULL OR user_id = 'admin')" + (" AND read_at IS NULL" if unread_only else "")
        cur.execute(f"""
            SELECT * FROM in_app_notifications
            {where}
            ORDER BY created_at DESC LIMIT %s
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]

    def mark_read(self, notification_ids: List[int]):
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE in_app_notifications SET read_at = NOW()
            WHERE id = ANY(%s)
        """, (notification_ids,))

    def get_alert_history(self, days: int = 30) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT * FROM alert_events
            WHERE sent_at > NOW() - INTERVAL '%s days'
            ORDER BY sent_at DESC LIMIT 500
        """ % int(days))
        return [dict(r) for r in cur.fetchall()]


# ===========================================================================
# AI BUDGET MONITOR
# ===========================================================================

class AIBudgetMonitor:
    """
    Monitora il credito residuo sui provider AI (Anthropic, OpenAI).
    - Salva snapshot giornaliero
    - Calcola burn rate (media ultimi 7/30 giorni)
    - Stima data di esaurimento
    - Emette alert quando sotto le soglie (50%, 25%, 10%)
    """

    def __init__(self, conn, RealDictCursor, config: Dict, alert_engine: AlertEngine = None):
        self.conn = conn
        self.RDC = RealDictCursor
        self.config = config
        self.alert_engine = alert_engine
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ai_budget_snapshots (
                id SERIAL PRIMARY KEY,
                provider VARCHAR(50) NOT NULL,  -- 'anthropic' | 'openai'
                snapshot_date DATE NOT NULL,
                -- credito configurato (da config.yaml)
                total_budget_usd DECIMAL(12,4),
                -- credito residuo (da API)
                remaining_usd DECIMAL(12,4),
                -- consumo del giorno
                daily_spend_usd DECIMAL(12,4),
                -- burn rate medio 7 giorni
                burn_rate_7d_usd DECIMAL(12,4),
                -- stima giorni rimanenti
                estimated_days_left INTEGER,
                -- raw response da API (per debug)
                api_response JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(provider, snapshot_date)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aibs_prov ON ai_budget_snapshots(provider, snapshot_date DESC)")
        print("✅ AI Budget Monitor tables initialized")

    def take_snapshot(self) -> Dict:
        """
        Prende snapshot del credito attuale da Anthropic e OpenAI.
        Ritorna dict con stato per ogni provider.
        """
        results = {}
        for provider in ["anthropic", "openai"]:
            try:
                data = self._fetch_balance(provider)
                if data:
                    self._save_snapshot(provider, data)
                    results[provider] = data
                else:
                    results[provider] = {"error": "API non disponibile o non configurata"}
            except Exception as e:
                results[provider] = {"error": str(e)}
        return results

    def _fetch_balance(self, provider: str) -> Optional[Dict]:
        """Interroga l'API di billing del provider."""
        cfg = self.config.get("ai_providers", {}).get(provider, {})
        api_key = cfg.get("api_key", "")
        total_budget = float(cfg.get("monthly_budget_usd", 0))

        if not api_key:
            return None

        if provider == "anthropic":
            return self._fetch_anthropic(api_key, total_budget)
        elif provider == "openai":
            return self._fetch_openai(api_key, total_budget)
        return None

    def _fetch_anthropic(self, api_key: str, total_budget: float) -> Dict:
        """
        Anthropic non ha una billing API pubblica per il saldo residuo.
        Calcoliamo il consumo dal numero di tokens (se tracciato) o
        approssimiamo dallo storico dei job in buddyliko.
        Per ora: legge consumption stimate dall'usage_counters.
        """
        # Calcola consumo stimato dal nostro DB
        consumed = self._estimate_ai_spend_from_db("anthropic")
        remaining = max(0.0, total_budget - consumed)
        pct_remaining = (remaining / total_budget * 100) if total_budget > 0 else 100.0

        return {
            "provider": "anthropic",
            "total_budget_usd": total_budget,
            "consumed_usd": consumed,
            "remaining_usd": remaining,
            "pct_remaining": pct_remaining,
            "source": "estimated_from_usage",
        }

    def _fetch_openai(self, api_key: str, total_budget: float) -> Dict:
        """
        OpenAI Usage API: GET https://api.openai.com/v1/dashboard/billing/subscription
        Disponibile solo per account con billing abilitato.
        """
        try:
            req = urllib.request.Request(
                "https://api.openai.com/v1/dashboard/billing/subscription",
                headers={"Authorization": f"Bearer {api_key}"},
                method="GET"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            hard_limit = float(data.get("hard_limit_usd", total_budget or 0))
            # Recupera usage del mese corrente
            now = datetime.now(timezone.utc)
            start = now.replace(day=1).strftime("%Y-%m-%d")
            end = now.strftime("%Y-%m-%d")
            req2 = urllib.request.Request(
                f"https://api.openai.com/v1/dashboard/billing/usage?start_date={start}&end_date={end}",
                headers={"Authorization": f"Bearer {api_key}"},
                method="GET"
            )
            with urllib.request.urlopen(req2, timeout=10) as resp2:
                usage_data = json.loads(resp2.read())
            consumed = float(usage_data.get("total_usage", 0)) / 100.0  # in cents → USD
            remaining = max(0.0, hard_limit - consumed)
            pct = (remaining / hard_limit * 100) if hard_limit > 0 else 100.0
            return {
                "provider": "openai",
                "total_budget_usd": hard_limit,
                "consumed_usd": consumed,
                "remaining_usd": remaining,
                "pct_remaining": pct,
                "source": "openai_api",
                "raw": data,
            }
        except Exception as e:
            # Fallback a stima locale
            consumed = self._estimate_ai_spend_from_db("openai")
            remaining = max(0.0, total_budget - consumed)
            pct = (remaining / total_budget * 100) if total_budget > 0 else 100.0
            return {
                "provider": "openai",
                "total_budget_usd": total_budget,
                "consumed_usd": consumed,
                "remaining_usd": remaining,
                "pct_remaining": pct,
                "source": "estimated_from_usage",
                "fetch_error": str(e),
            }

    def _estimate_ai_spend_from_db(self, provider: str) -> float:
        """
        Stima il consumo AI.
        PRIORITÀ 1: Token reali dalla tabella ai_token_usage (preciso)
        PRIORITÀ 2: Stima approssimativa da usage_counters (fallback)
        """
        now = datetime.now(timezone.utc)
        month = now.strftime('%Y-%m')
        cur = self.conn.cursor()

        # Prova prima con i token reali
        try:
            cur.execute("""
                SELECT COALESCE(SUM(cost_usd), 0)
                FROM ai_token_usage
                WHERE month = %s AND provider = %s
            """, (month, provider))
            row = cur.fetchone()
            real_cost = float(row[0]) if row else 0.0
            if real_cost > 0:
                return real_cost
        except Exception:
            pass  # Tabella non esiste ancora, usa fallback

        # Fallback: stima da usage_counters
        cur.execute("""
            SELECT SUM(codegen_count), SUM(transforms_count)
            FROM usage_counters WHERE month = %s
        """, (month,))
        row = cur.fetchone()
        if not row:
            return 0.0
        codegen = int(row[0] or 0)
        transforms = int(row[1] or 0)

        if provider == "anthropic":
            return codegen * 0.05 + transforms * 0.0001
        else:
            return codegen * 0.03

    def _save_snapshot(self, provider: str, data: Dict):
        today = datetime.now(timezone.utc).date()

        # Calcola burn rate ultimi 7 giorni
        burn_rate_7d = self._calc_burn_rate(provider, days=7)
        days_left = None
        if burn_rate_7d and burn_rate_7d > 0:
            days_left = int(data.get("remaining_usd", 0) / burn_rate_7d)

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO ai_budget_snapshots
                (provider, snapshot_date, total_budget_usd, remaining_usd,
                 daily_spend_usd, burn_rate_7d_usd, estimated_days_left, api_response)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (provider, snapshot_date) DO UPDATE SET
                remaining_usd = EXCLUDED.remaining_usd,
                daily_spend_usd = EXCLUDED.daily_spend_usd,
                burn_rate_7d_usd = EXCLUDED.burn_rate_7d_usd,
                estimated_days_left = EXCLUDED.estimated_days_left,
                api_response = EXCLUDED.api_response,
                created_at = NOW()
        """, (
            provider, today,
            data.get("total_budget_usd", 0),
            data.get("remaining_usd", 0),
            self._calc_daily_spend(provider),
            burn_rate_7d,
            days_left,
            json.dumps(data),
        ))

        # Emetti alert se sotto soglia
        if self.alert_engine:
            pct = data.get("pct_remaining", 100.0)
            today_str = str(today)
            for threshold, atype, severity in [
                (10.0, AlertType.AI_BUDGET_10, "critical"),
                (25.0, AlertType.AI_BUDGET_25, "warning"),
                (50.0, AlertType.AI_BUDGET_50, "info"),
            ]:
                if pct <= threshold:
                    remaining = data.get("remaining_usd", 0)
                    msg = (f"Credito {provider.upper()} al {pct:.1f}% — "
                           f"${remaining:.2f} rimanenti")
                    if days_left is not None:
                        msg += f" (stima esaurimento: {days_left} giorni)"
                    self.alert_engine._emit_alert(
                        alert_type=atype,
                        target_type="system",
                        target_id=provider,
                        target_name=provider.upper(),
                        message=msg,
                        severity=severity,
                        period_key=f"{today_str}_ai_{provider}_{threshold}",
                        summary={"alerts_sent": 0, "alerts_skipped": 0, "errors": []},
                    )
                    break

    def _calc_burn_rate(self, provider: str, days: int = 7) -> Optional[float]:
        """Media spesa giornaliera degli ultimi N giorni."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT AVG(daily_spend_usd) FROM ai_budget_snapshots
            WHERE provider = %s
              AND snapshot_date >= CURRENT_DATE - INTERVAL '%s days'
              AND daily_spend_usd > 0
        """ % ('%s', days), (provider,))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] else None

    def _calc_daily_spend(self, provider: str) -> float:
        """Spesa di oggi = differenza con snapshot di ieri."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT remaining_usd FROM ai_budget_snapshots
            WHERE provider = %s AND snapshot_date = CURRENT_DATE - 1
        """, (provider,))
        row = cur.fetchone()
        if not row:
            return 0.0
        # stima: se ieri avevo X e oggi ho Y, ho speso X-Y
        # (non disponibile qui perché stiamo costruendo lo snapshot ora)
        return 0.0

    def get_status(self) -> List[Dict]:
        """Ritorna ultimo snapshot per ogni provider."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT DISTINCT ON (provider)
                provider, snapshot_date, total_budget_usd, remaining_usd,
                daily_spend_usd, burn_rate_7d_usd, estimated_days_left,
                (remaining_usd / NULLIF(total_budget_usd, 0) * 100) as pct_remaining
            FROM ai_budget_snapshots
            ORDER BY provider, snapshot_date DESC
        """)
        return [dict(r) for r in cur.fetchall()]

    def get_history(self, provider: str = None, days: int = 30) -> List[Dict]:
        """Storico snapshot per grafico burn rate."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        where = f"WHERE snapshot_date >= CURRENT_DATE - {int(days)}"
        if provider:
            where += f" AND provider = '{provider}'"
        cur.execute(f"""
            SELECT provider, snapshot_date, remaining_usd,
                   daily_spend_usd, burn_rate_7d_usd, estimated_days_left,
                   (remaining_usd / NULLIF(total_budget_usd, 0) * 100) as pct_remaining
            FROM ai_budget_snapshots
            {where}
            ORDER BY provider, snapshot_date
        """)
        return [dict(r) for r in cur.fetchall()]


# ===========================================================================
# PROFITABILITY ENGINE
# ===========================================================================

class ProfitabilityEngine:
    """
    Calcola ARPU, costo di servizio stimato, margine netto
    per singolo utente, gruppo e totale, per periodo definito.
    """

    def __init__(self, conn, RealDictCursor, config: Dict = None):
        self.conn = conn
        self.RDC = RealDictCursor
        self.config = config or {}

    def get_user_profitability(self, months: int = 3) -> List[Dict]:
        """
        Ritorna lista utenti con: revenue, costo_stimato, margine, ARPU.
        """
        cur = self.conn.cursor(cursor_factory=self.RDC)
        now = datetime.now(timezone.utc)
        month_list = [
            (now.replace(day=1) - timedelta(days=i * 28)).strftime('%Y-%m')
            for i in range(months)
        ]

        cur.execute("""
            SELECT
                u.id::text as user_id,
                u.email, u.name, u.plan,
                COALESCE(SUM(uc.transforms_count), 0) as total_transforms,
                COALESCE(SUM(uc.codegen_count), 0) as total_codegen,
                COALESCE(SUM(uc.bytes_processed), 0) as total_bytes,
                COALESCE(SUM(uc.api_calls_count), 0) as total_api_calls,
                COUNT(DISTINCT uc.month) as active_months,
                MIN(uc.month) as first_month,
                MAX(uc.month) as last_month
            FROM users u
            LEFT JOIN usage_counters uc ON uc.user_id = u.id::text
                AND uc.month = ANY(%s)
            GROUP BY u.id, u.email, u.name, u.plan
            ORDER BY total_transforms DESC
        """, (month_list,))
        rows = cur.fetchall()

        result = []
        for row in rows:
            revenue = self._calc_revenue(row, months)
            cost = self._calc_cost(row)
            margin = revenue - cost
            margin_pct = (margin / revenue * 100) if revenue > 0 else (0.0 if cost == 0 else -100.0)
            result.append({
                "user_id": row["user_id"],
                "email": row["email"] or "—",
                "name": row["name"] or "—",
                "plan": row["plan"] or "FREE",
                "total_transforms": int(row["total_transforms"]),
                "total_codegen": int(row["total_codegen"]),
                "total_bytes_mb": round(int(row["total_bytes"]) / 1048576, 2),
                "total_api_calls": int(row["total_api_calls"]),
                "active_months": int(row["active_months"]),
                "revenue_eur": round(revenue, 2),
                "cost_eur": round(cost, 2),
                "margin_eur": round(margin, 2),
                "margin_pct": round(margin_pct, 1),
                "arpu_eur": round(revenue / max(1, int(row["active_months"])), 2),
                "is_profitable": margin >= 0,
            })
        return result

    def get_group_profitability(self, months: int = 3) -> List[Dict]:
        """Redditività aggregata per gruppo."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        now = datetime.now(timezone.utc)
        month_list = [
            (now.replace(day=1) - timedelta(days=i * 28)).strftime('%Y-%m')
            for i in range(months)
        ]

        cur.execute("""
            SELECT
                g.id::text as group_id,
                g.name as group_name,
                g.plan_override as group_plan,
                COUNT(DISTINCT u.id) as member_count,
                COALESCE(SUM(uc.transforms_count), 0) as total_transforms,
                COALESCE(SUM(uc.codegen_count), 0) as total_codegen,
                COALESCE(SUM(uc.bytes_processed), 0) as total_bytes,
                COALESCE(SUM(uc.api_calls_count), 0) as total_api_calls
            FROM groups g
            LEFT JOIN users u ON u.group_id::text = g.id::text
            LEFT JOIN usage_counters uc ON uc.user_id = u.id::text
                AND uc.month = ANY(%s)
            GROUP BY g.id, g.name, g.plan_override
            ORDER BY total_transforms DESC
        """, (month_list,))
        rows = cur.fetchall()

        result = []
        for row in rows:
            members = max(1, int(row["member_count"]))
            plan = row.get("group_plan") or "FREE"
            revenue = PLAN_REVENUE_EUR.get(plan, 0.0) * months * members
            cost = self._calc_cost_raw(
                int(row["total_transforms"]),
                int(row["total_codegen"]),
                int(row["total_bytes"]),
                int(row["total_api_calls"]),
            )
            margin = revenue - cost
            margin_pct = (margin / revenue * 100) if revenue > 0 else (0.0 if cost == 0 else -100.0)
            result.append({
                "group_id": row["group_id"],
                "group_name": row["group_name"] or "—",
                "group_plan": plan,
                "member_count": members,
                "total_transforms": int(row["total_transforms"]),
                "total_codegen": int(row["total_codegen"]),
                "total_bytes_mb": round(int(row["total_bytes"]) / 1048576, 2),
                "revenue_eur": round(revenue, 2),
                "cost_eur": round(cost, 2),
                "margin_eur": round(margin, 2),
                "margin_pct": round(margin_pct, 1),
                "revenue_per_member": round(revenue / members, 2),
                "is_profitable": margin >= 0,
            })
        return result

    def get_summary(self, months: int = 3) -> Dict:
        """KPI summary: MRR, ARR, ARPU, margine totale, churn estimate."""
        users = self.get_user_profitability(months)
        total_revenue = sum(u["revenue_eur"] for u in users)
        total_cost = sum(u["cost_eur"] for u in users)
        paid_users = [u for u in users if u["plan"] != "FREE"]
        free_users = [u for u in users if u["plan"] == "FREE"]

        # MRR = revenue / mesi
        mrr = total_revenue / max(1, months)
        arr = mrr * 12

        # ARPU su utenti paganti
        arpu = (sum(u["revenue_eur"] for u in paid_users) /
                max(1, len(paid_users) * months)) if paid_users else 0.0

        # Margine lordo
        gross_margin = total_revenue - total_cost
        gross_margin_pct = (gross_margin / total_revenue * 100) if total_revenue > 0 else 0.0

        # Utenti non profittevoli
        unprofitable = [u for u in users if not u["is_profitable"] and u["plan"] != "FREE"]

        # Distribuzione per piano
        plan_dist = {}
        for u in users:
            p = u["plan"]
            if p not in plan_dist:
                plan_dist[p] = {"count": 0, "revenue": 0.0}
            plan_dist[p]["count"] += 1
            plan_dist[p]["revenue"] += u["revenue_eur"]

        # Trend MRR mese per mese
        mrr_trend = self._calc_mrr_trend(months + 3)

        return {
            "mrr_eur": round(mrr, 2),
            "arr_eur": round(arr, 2),
            "arpu_eur": round(arpu, 2),
            "total_revenue_eur": round(total_revenue, 2),
            "total_cost_eur": round(total_cost, 2),
            "gross_margin_eur": round(gross_margin, 2),
            "gross_margin_pct": round(gross_margin_pct, 1),
            "total_users": len(users),
            "paid_users": len(paid_users),
            "free_users": len(free_users),
            "unprofitable_paid_users": len(unprofitable),
            "plan_distribution": plan_dist,
            "mrr_trend": mrr_trend,
            "period_months": months,
        }

    def _calc_revenue(self, row: Dict, months: int) -> float:
        plan = row.get("plan", "FREE")
        active = max(1, int(row.get("active_months", 1)))
        monthly_rev = PLAN_REVENUE_EUR.get(plan, 0.0)
        # Custom pricing override?
        custom = self._get_custom_price(str(row.get("user_id", "")))
        if custom is not None:
            monthly_rev = custom
        return monthly_rev * active

    def _get_custom_price(self, user_id: str) -> Optional[float]:
        """Controlla se c'è un prezzo custom in pricing_rules."""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT custom_price_eur FROM pricing_rules
                WHERE user_id = %s AND active = TRUE
                ORDER BY created_at DESC LIMIT 1
            """, (user_id,))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None
        except Exception:
            return None

    def _calc_cost(self, row: Dict) -> float:
        return self._calc_cost_raw(
            int(row.get("total_transforms", 0)),
            int(row.get("total_codegen", 0)),
            int(row.get("total_bytes", 0)),
            int(row.get("total_api_calls", 0)),
        )

    def _calc_cost_raw(self, transforms: int, codegen: int,
                       bytes_proc: int, api_calls: int) -> float:
        """Costo stimato in EUR per set di operazioni."""
        cost = 0.0
        mb = bytes_proc / 1048576
        cost += transforms * COST_PER_OPERATION["transform_xml_small"]
        cost += codegen * COST_PER_OPERATION["codegen"]
        cost += api_calls * COST_PER_OPERATION["api_call"]
        cost += mb * COST_PER_OPERATION["storage_per_mb_month"]
        # Infrastruttura base per utente attivo: ~€0.50/mese
        cost += 0.50
        return cost

    def _calc_mrr_trend(self, months: int) -> List[Dict]:
        """Trend MRR mese per mese dai subscriptions e usage."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT month,
                   COUNT(DISTINCT user_id) as active_users,
                   SUM(transforms_count) as transforms,
                   SUM(codegen_count) as codegen
            FROM usage_counters
            WHERE month >= to_char(CURRENT_DATE - INTERVAL '%s months', 'YYYY-MM')
            GROUP BY month ORDER BY month
        """ % int(months))
        trend = []
        for row in cur.fetchall():
            # Stima MRR da subscriptions attive quel mese
            cur2 = self.conn.cursor()
            cur2.execute("""
                SELECT SUM(
                    CASE plan
                        WHEN 'PRO' THEN 49.0
                        WHEN 'ENTERPRISE' THEN 299.0
                        ELSE 0.0
                    END
                ) as mrr
                FROM subscriptions
                WHERE status = 'active'
                  AND to_char(period_start, 'YYYY-MM') <= %s
                  AND (period_end IS NULL OR to_char(period_end, 'YYYY-MM') >= %s)
            """, (row["month"], row["month"]))
            mrr_row = cur2.fetchone()
            trend.append({
                "month": row["month"],
                "mrr_eur": float(mrr_row[0] or 0) if mrr_row else 0.0,
                "active_users": int(row["active_users"]),
                "transforms": int(row["transforms"]),
                "codegen": int(row["codegen"]),
            })
        return trend


# ===========================================================================
# PRICING MANAGER
# ===========================================================================

class PricingManager:
    """
    Gestisce:
    - Prezzi custom per utente o gruppo
    - Codici sconto (% o importo fisso, con scadenza e limite utilizzi)
    - Utenti gratuiti (override permanente FREE o CUSTOM a €0)
    - Override pricing per accordi commerciali
    """

    SUPPORTED_PLANS = {"FREE", "PRO", "ENTERPRISE", "CUSTOM"}

    def __init__(self, conn, RealDictCursor):
        self.conn = conn
        self.RDC = RealDictCursor
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pricing_rules (
                id VARCHAR(36) PRIMARY KEY,
                -- target: utente o gruppo
                user_id VARCHAR(255),
                group_id VARCHAR(255),
                -- tipo regola
                rule_type VARCHAR(50) NOT NULL,
                -- 'free_override'  → accesso gratuito permanente
                -- 'custom_price'   → prezzo mensile custom
                -- 'plan_override'  → forza piano specifico
                -- 'trial'          → trial gratuito N giorni
                -- piano assegnato
                plan VARCHAR(50) DEFAULT 'CUSTOM',
                -- prezzo mensile in EUR (se custom_price)
                custom_price_eur DECIMAL(10,2),
                -- note interne
                note TEXT,
                -- chi ha creato la regola
                created_by VARCHAR(255),
                active BOOLEAN DEFAULT TRUE,
                -- scadenza (NULL = permanente)
                expires_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pr_user ON pricing_rules(user_id, active)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pr_group ON pricing_rules(group_id, active)")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS discount_codes (
                id VARCHAR(36) PRIMARY KEY,
                code VARCHAR(50) NOT NULL UNIQUE,
                description TEXT,
                -- tipo sconto
                discount_type VARCHAR(20) NOT NULL,  -- 'percent' | 'fixed_eur' | 'free_months'
                discount_value DECIMAL(10,2) NOT NULL, -- % o EUR o numero mesi
                -- piani a cui si applica (JSON list, [] = tutti)
                applicable_plans JSONB DEFAULT '[]',
                -- limite utilizzi (NULL = illimitato)
                max_uses INTEGER,
                current_uses INTEGER DEFAULT 0,
                -- validità
                valid_from TIMESTAMPTZ DEFAULT NOW(),
                valid_until TIMESTAMPTZ,
                -- stripe coupon id (se sincronizzato)
                stripe_coupon_id VARCHAR(255),
                active BOOLEAN DEFAULT TRUE,
                created_by VARCHAR(255),
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dc_code ON discount_codes(code, active)")
        print("✅ Pricing Manager tables initialized")

    # ------------------------------------------------------------------
    # PRICING RULES
    # ------------------------------------------------------------------

    def create_rule(self, rule_type: str, user_id: str = None, group_id: str = None,
                    plan: str = "CUSTOM", custom_price_eur: float = None,
                    note: str = "", created_by: str = "admin",
                    expires_at: datetime = None) -> str:
        """Crea una regola di pricing custom."""
        rule_id = str(uuid.uuid4())
        cur = self.conn.cursor()

        # Disattiva regole precedenti per lo stesso utente/gruppo
        if user_id:
            cur.execute("""
                UPDATE pricing_rules SET active = FALSE, updated_at = NOW()
                WHERE user_id = %s AND active = TRUE AND rule_type = %s
            """, (str(user_id), rule_type))
        elif group_id:
            cur.execute("""
                UPDATE pricing_rules SET active = FALSE, updated_at = NOW()
                WHERE group_id = %s AND active = TRUE AND rule_type = %s
            """, (str(group_id), rule_type))

        cur.execute("""
            INSERT INTO pricing_rules
                (id, user_id, group_id, rule_type, plan, custom_price_eur,
                 note, created_by, active, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
        """, (rule_id, user_id and str(user_id), group_id and str(group_id),
              rule_type, plan, custom_price_eur, note, created_by, expires_at))
        return rule_id

    def get_rules(self, active_only: bool = True) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RDC)
        where = "WHERE active = TRUE" if active_only else ""
        cur.execute(f"""
            SELECT pr.*,
                   u.email as user_email, u.name as user_name,
                   g.name as group_name
            FROM pricing_rules pr
            LEFT JOIN users u ON u.id::text = pr.user_id
            LEFT JOIN groups g ON g.id::text = pr.group_id
            {where}
            ORDER BY created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]

    def deactivate_rule(self, rule_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE pricing_rules SET active = FALSE, updated_at = NOW()
            WHERE id = %s
        """, (rule_id,))
        return cur.rowcount > 0

    def get_user_effective_plan(self, user_id: str) -> Dict:
        """
        Ritorna il piano effettivo dell'utente considerando tutte le regole attive.
        Precedenza: pricing_rule > billing subscription > users.plan
        """
        cur = self.conn.cursor(cursor_factory=self.RDC)
        # Check pricing rule
        cur.execute("""
            SELECT rule_type, plan, custom_price_eur, expires_at
            FROM pricing_rules
            WHERE user_id = %s AND active = TRUE
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY created_at DESC LIMIT 1
        """, (str(user_id),))
        rule = cur.fetchone()
        if rule:
            return {
                "source": "pricing_rule",
                "plan": rule["plan"],
                "rule_type": rule["rule_type"],
                "custom_price_eur": float(rule["custom_price_eur"] or 0),
                "expires_at": rule["expires_at"],
            }
        # Fallback su users.plan
        cur.execute("SELECT plan FROM users WHERE id::text = %s", (str(user_id),))
        user = cur.fetchone()
        return {
            "source": "subscription",
            "plan": user["plan"] if user else "FREE",
            "rule_type": None,
            "custom_price_eur": None,
            "expires_at": None,
        }

    # ------------------------------------------------------------------
    # DISCOUNT CODES
    # ------------------------------------------------------------------

    def create_discount_code(self, code: str, description: str,
                              discount_type: str, discount_value: float,
                              applicable_plans: List[str] = None,
                              max_uses: int = None,
                              valid_until: datetime = None,
                              created_by: str = "admin") -> str:
        """Crea un codice sconto."""
        if discount_type not in ("percent", "fixed_eur", "free_months"):
            raise ValueError(f"discount_type non valido: {discount_type}")
        code_id = str(uuid.uuid4())
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO discount_codes
                (id, code, description, discount_type, discount_value,
                 applicable_plans, max_uses, valid_until, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (code_id, code.upper(), description, discount_type,
              discount_value, json.dumps(applicable_plans or []),
              max_uses, valid_until, created_by))
        return code_id

    def validate_code(self, code: str, plan: str = None) -> Tuple[bool, str, Optional[Dict]]:
        """
        Valida un codice sconto.
        Ritorna (valid: bool, message: str, discount_data: dict|None).
        """
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT * FROM discount_codes
            WHERE code = %s AND active = TRUE
        """, (code.upper(),))
        dc = cur.fetchone()
        if not dc:
            return False, "Codice non valido", None

        dc = dict(dc)
        now = datetime.now(timezone.utc)

        if dc.get("valid_until") and dc["valid_until"] < now:
            return False, "Codice scaduto", None

        if dc.get("max_uses") is not None:
            if int(dc.get("current_uses", 0)) >= int(dc["max_uses"]):
                return False, "Codice esaurito (limite utilizzi raggiunto)", None

        plans = dc.get("applicable_plans", [])
        if isinstance(plans, str):
            plans = json.loads(plans)
        if plans and plan and plan.upper() not in plans:
            return False, f"Codice non valido per il piano {plan}", None

        return True, "Codice valido", dc

    def apply_code(self, code: str) -> bool:
        """Incrementa il contatore utilizzi."""
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE discount_codes
            SET current_uses = current_uses + 1
            WHERE code = %s AND active = TRUE
        """, (code.upper(),))
        return cur.rowcount > 0

    def get_discount_codes(self, active_only: bool = False) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RDC)
        where = "WHERE active = TRUE" if active_only else ""
        cur.execute(f"""
            SELECT *, (max_uses IS NULL OR current_uses < max_uses) as usable
            FROM discount_codes {where}
            ORDER BY created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]

    def deactivate_code(self, code_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("UPDATE discount_codes SET active = FALSE WHERE id = %s", (code_id,))
        return cur.rowcount > 0

    def calc_discounted_price(self, base_price_eur: float, code: str,
                              plan: str = None) -> Tuple[float, float, str]:
        """
        Calcola prezzo scontato.
        Ritorna (final_price, savings, description).
        """
        valid, msg, dc = self.validate_code(code, plan)
        if not valid or dc is None:
            return base_price_eur, 0.0, msg

        dtype = dc["discount_type"]
        value = float(dc["discount_value"])

        if dtype == "percent":
            savings = base_price_eur * value / 100
            final = base_price_eur - savings
            return max(0.0, final), savings, f"-{value:.0f}%"
        elif dtype == "fixed_eur":
            savings = min(value, base_price_eur)
            return max(0.0, base_price_eur - savings), savings, f"-€{value:.2f}"
        elif dtype == "free_months":
            # Non riduce il prezzo unitario, ma dà mesi gratis (gestito a livello Stripe)
            return base_price_eur, 0.0, f"{int(value)} mesi gratis"
        return base_price_eur, 0.0, ""
