#!/usr/bin/env python3
"""
patch_tracker_balance.py — Aggiunge il calcolatore automatico del saldo
Esegui da /opt/buddyliko/backend/:
    python3 patch_tracker_balance.py
"""

FILE = "/opt/buddyliko/backend/ai_token_tracker.py"

NEW_METHOD = '''
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

    def get_spend_by_day(self, provider: str = None, days: int = 30) -> list:
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
'''

def patch():
    with open(FILE, 'r') as f:
        code = f.read()
    
    if "get_estimated_balance" in code:
        print("⏭️  get_estimated_balance già presente")
        return
    
    # Aggiungi prima della fine del file (dopo get_latest_balance)
    old = "        return dict(row) if row else None"
    # Find the LAST occurrence (end of get_latest_balance)
    idx = code.rfind(old)
    if idx == -1:
        print("❌ Non trovo la fine di get_latest_balance")
        return
    
    insert_point = idx + len(old)
    code = code[:insert_point] + NEW_METHOD + code[insert_point:]
    
    # Assicurati che Dict sia importato
    if "from typing import Optional, Dict" not in code:
        code = code.replace(
            "from typing import Optional",
            "from typing import Optional, Dict"
        )
    
    with open(FILE, 'w') as f:
        f.write(code)
    
    print("✅ get_estimated_balance e get_spend_by_day aggiunti a ai_token_tracker.py")

if __name__ == "__main__":
    patch()
