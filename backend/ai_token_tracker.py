"""
ai_token_tracker.py — Traccia i token reali da ogni chiamata Anthropic/OpenAI.
Metti questo file in /opt/buddyliko/backend/

Pricing aggiornato Feb 2026 (USD per 1M tokens):
  Claude Haiku 4.5:   input $1.00  / output $5.00
  Claude Sonnet 4:    input $3.00  / output $15.00
  Claude Opus 4:      input $15.00 / output $75.00
  GPT-4o:             input $2.50  / output $10.00
  GPT-4-turbo:        input $10.00 / output $30.00
"""

from datetime import datetime, timezone
from typing import Optional, Dict, List

# Prezzi per 1M token (USD)
PRICING = {
    # Anthropic
    "claude-haiku-4-5-20251001":    {"input": 1.00,  "output": 5.00},
    "claude-3-5-haiku-20241022":    {"input": 1.00,  "output": 5.00},
    "claude-sonnet-4-5-20250514":   {"input": 3.00,  "output": 15.00},
    "claude-3-5-sonnet-20241022":   {"input": 3.00,  "output": 15.00},
    "claude-opus-4-5-20250514":     {"input": 15.00, "output": 75.00},
    # OpenAI
    "gpt-4o":                       {"input": 2.50,  "output": 10.00},
    "gpt-4-turbo-preview":          {"input": 10.00, "output": 30.00},
    "gpt-4-turbo":                  {"input": 10.00, "output": 30.00},
    "gpt-3.5-turbo":                {"input": 0.50,  "output": 1.50},
}

# Fallback per modelli non in lista
DEFAULT_PRICING = {"input": 3.00, "output": 15.00}


def calc_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calcola il costo in USD."""
    prices = PRICING.get(model, DEFAULT_PRICING)
    cost = (input_tokens * prices["input"] + output_tokens * prices["output"]) / 1_000_000
    return round(cost, 6)


def extract_anthropic_usage(response_json: dict) -> Dict:
    """Estrai token usage dalla risposta Anthropic."""
    usage = response_json.get("usage", {})
    return {
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
    }


def extract_openai_usage(response_json: dict) -> Dict:
    """Estrai token usage dalla risposta OpenAI."""
    usage = response_json.get("usage", {})
    return {
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
    }


class AITokenTracker:
    """Salva ogni chiamata AI nel DB con token reali e costo."""

    def __init__(self, conn, cursor_factory):
        self.conn = conn
        self.cursor_factory = cursor_factory

    def track(self, *,
              provider: str,
              model: str,
              operation: str,
              input_tokens: int,
              output_tokens: int,
              http_status: int = 200,
              duration_ms: int = 0,
              user_id: str = None):
        """Salva una chiamata API nel DB."""
        total = input_tokens + output_tokens
        cost = calc_cost(model, input_tokens, output_tokens)
        month = datetime.now(timezone.utc).strftime('%Y-%m')

        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO ai_token_usage
                    (provider, model, operation, user_id,
                     input_tokens, output_tokens, total_tokens,
                     cost_usd, http_status, duration_ms, month)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (provider, model, operation, user_id,
                  input_tokens, output_tokens, total,
                  cost, http_status, duration_ms, month))
            self.conn.commit()
        except Exception as e:
            print(f"⚠️ Token tracking failed: {e}")
            try:
                self.conn.rollback()
            except:
                pass

    def get_month_spend(self, provider: str = None, month: str = None) -> float:
        """Ritorna spesa totale del mese in USD."""
        if not month:
            month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor()
        if provider:
            cur.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) FROM ai_token_usage WHERE month = %s AND provider = %s",
                (month, provider))
        else:
            cur.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) FROM ai_token_usage WHERE month = %s",
                (month,))
        return float(cur.fetchone()[0])

    def get_month_stats(self, provider: str = None, month: str = None) -> Dict:
        """Statistiche dettagliate del mese."""
        if not month:
            month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.cursor_factory)
        where = "WHERE month = %s"
        params = [month]
        if provider:
            where += " AND provider = %s"
            params.append(provider)
        cur.execute(f"""
            SELECT
                provider,
                COUNT(*) as calls,
                SUM(input_tokens) as input_tokens,
                SUM(output_tokens) as output_tokens,
                SUM(total_tokens) as total_tokens,
                SUM(cost_usd) as total_cost_usd,
                AVG(duration_ms) as avg_duration_ms
            FROM ai_token_usage
            {where}
            GROUP BY provider
        """, params)
        rows = [dict(r) for r in cur.fetchall()]
        return {
            "month": month,
            "providers": rows,
            "total_cost_usd": sum(float(r["total_cost_usd"] or 0) for r in rows),
            "total_calls": sum(int(r["calls"] or 0) for r in rows),
        }

    def get_today_spend(self, provider: str = None) -> float:
        """Spesa di oggi."""
        cur = self.conn.cursor()
        if provider:
            cur.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) FROM ai_token_usage WHERE created_at::date = CURRENT_DATE AND provider = %s",
                (provider,))
        else:
            cur.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) FROM ai_token_usage WHERE created_at::date = CURRENT_DATE")
        return float(cur.fetchone()[0])

    def save_balance(self, provider: str, balance_usd: float,
                     auto_recharge: bool = False,
                     recharge_amount: float = None,
                     recharge_threshold: float = None,
                     source: str = "scraper"):
        """Salva il saldo reale (da scraping o manuale)."""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO ai_provider_balance
                    (provider, balance_usd, auto_recharge,
                     recharge_amount, recharge_threshold, source)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (provider, balance_usd, auto_recharge,
                  recharge_amount, recharge_threshold, source))
            self.conn.commit()
        except Exception as e:
            print(f"⚠️ Balance save failed: {e}")
            try:
                self.conn.rollback()
            except:
                pass

    def get_latest_balance(self, provider: str) -> Optional[Dict]:
        """Ultimo saldo noto del provider."""
        cur = self.conn.cursor(cursor_factory=self.cursor_factory)
        cur.execute("""
            SELECT * FROM ai_provider_balance
            WHERE provider = %s
            ORDER BY checked_at DESC LIMIT 1
        """, (provider,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_estimated_balance(self, provider: str = "anthropic") -> Dict:
        """
        Calcola il saldo stimato:
          saldo = ultimo_saldo_noto - spesa_da_quel_momento
          se auto_recharge e saldo < threshold → saldo += recharge_amount
        """
        last = self.get_latest_balance(provider)
        if not last:
            return {
                "provider": provider,
                "estimated_balance_usd": None,
                "last_calibration": None,
                "spend_since_calibration": 0,
                "recharges_estimated": 0,
                "status": "no_calibration",
                "message": "Nessun saldo inserito. Calibra manualmente."
            }

        last_balance = float(last["balance_usd"]) if last["balance_usd"] else 0
        last_date = last["checked_at"]
        auto_recharge = last.get("auto_recharge", False)
        recharge_amount = float(last.get("recharge_amount") or 15.0)
        recharge_threshold = float(last.get("recharge_threshold") or 5.0)

        # Calcola spesa dalla data di calibrazione
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COALESCE(SUM(cost_usd), 0)
            FROM ai_token_usage
            WHERE provider = %s AND created_at >= %s
        """, (provider, last_date))
        spend_since = float(cur.fetchone()[0])

        # Simula saldo con auto-recharge
        balance = last_balance - spend_since
        recharges = 0
        if auto_recharge:
            while balance < recharge_threshold:
                balance += recharge_amount
                recharges += 1
                if recharges > 100:  # safety
                    break

        # Status
        if balance > recharge_threshold * 2:
            status = "healthy"
        elif balance > recharge_threshold:
            status = "warning"
        else:
            status = "critical"

        return {
            "provider": provider,
            "estimated_balance_usd": round(balance, 2),
            "last_calibration": last_date.isoformat() if last_date else None,
            "last_calibration_balance": last_balance,
            "spend_since_calibration": round(spend_since, 6),
            "recharges_estimated": recharges,
            "auto_recharge": auto_recharge,
            "recharge_amount": recharge_amount,
            "recharge_threshold": recharge_threshold,
            "status": status,
        }

    def get_spend_by_day(self, provider: str = None, days: int = 30) -> List:
        """Spesa giornaliera per grafico."""
        cur = self.conn.cursor(cursor_factory=self.cursor_factory)
        where = "WHERE created_at >= CURRENT_DATE - %s"
        params = [days]
        if provider:
            where += " AND provider = %s"
            params.append(provider)
        cur.execute(f"""
            SELECT
                created_at::date as day,
                provider,
                COUNT(*) as calls,
                SUM(input_tokens) as input_tokens,
                SUM(output_tokens) as output_tokens,
                SUM(cost_usd) as cost_usd
            FROM ai_token_usage
            {where}
            GROUP BY created_at::date, provider
            ORDER BY day
        """, params)
        return [dict(r) for r in cur.fetchall()]
