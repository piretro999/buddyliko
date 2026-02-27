#!/usr/bin/env python3
"""
ai_balance_scraper.py — Scraper per leggere il saldo da console.anthropic.com
Metti in /opt/buddyliko/backend/

Uso:
    python3 ai_balance_scraper.py                  # scrape + salva in DB
    python3 ai_balance_scraper.py --check-only      # solo stampa saldo
    python3 ai_balance_scraper.py --manual 14.99    # salva saldo manuale

Setup:
    pip install playwright
    playwright install chromium

Nota: la prima volta devi fare login manuale per salvare i cookie.
    python3 ai_balance_scraper.py --login
"""

import argparse
import json
import os
import sys
import re
from pathlib import Path
from datetime import datetime, timezone

# Percorsi
SCRIPT_DIR = Path(__file__).parent
COOKIE_FILE = SCRIPT_DIR / ".anthropic_cookies.json"
CONFIG_FILE = SCRIPT_DIR.parent / "config.yml"
STATE_FILE = SCRIPT_DIR / ".anthropic_state"

def load_config():
    """Carica config.yml per avere la connection string DB."""
    try:
        import yaml
        with open(CONFIG_FILE) as f:
            return yaml.safe_load(f)
    except:
        return {}

def get_db_connection(config):
    """Connessione PostgreSQL."""
    import psycopg2
    import psycopg2.extras
    db = config.get("database", {}).get("postgresql", {})
    
    # Risolvi variabili d'ambiente
    password = db.get("password", "")
    if password.startswith("${") and password.endswith("}"):
        env_var = password[2:-1]
        password = os.environ.get(env_var, "")
    
    conn = psycopg2.connect(
        host=db.get("host", "localhost"),
        port=db.get("port", 5432),
        dbname=db.get("database", "buddyliko"),
        user=db.get("user", "buddyliko_user"),
        password=password
    )
    return conn, psycopg2.extras.RealDictCursor


def scrape_anthropic_balance(headless=True):
    """
    Accede a console.anthropic.com/settings/billing
    e legge il saldo corrente.
    Ritorna dict con balance_usd, auto_recharge, etc.
    """
    from playwright.sync_api import sync_playwright
    
    result = {
        "provider": "anthropic",
        "balance_usd": None,
        "auto_recharge": False,
        "recharge_amount": None,
        "recharge_threshold": None,
        "error": None,
    }
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        
        # Carica cookies salvati
        context_opts = {"viewport": {"width": 1280, "height": 800}}
        if COOKIE_FILE.exists():
            context_opts["storage_state"] = str(COOKIE_FILE)
        
        context = browser.new_context(**context_opts)
        page = context.new_page()
        
        try:
            # Vai alla pagina billing
            page.goto("https://console.anthropic.com/settings/billing", 
                      wait_until="networkidle", timeout=30000)
            
            # Controlla se siamo loggati
            if "login" in page.url.lower() or "sign" in page.url.lower():
                result["error"] = "Non loggato. Esegui: python3 ai_balance_scraper.py --login"
                return result
            
            # Aspetta che la pagina carichi
            page.wait_for_timeout(3000)
            
            # Cerca il saldo — prova diversi selettori
            # Anthropic console mostra il credito in vari formati
            body_text = page.text_content("body")
            
            # Pattern: "$14.99" o "14.99 USD" o "Credit balance $14.99"
            balance_patterns = [
                r'(?:balance|credit|remaining|saldo)[:\s]*\$?([\d,]+\.?\d*)',
                r'\$([\d,]+\.?\d*)\s*(?:remaining|left|credit|balance)',
                r'(?:credit|balance)\s*\$?([\d,]+\.?\d*)',
                r'\$([\d,]+\.?\d*)',  # fallback: qualsiasi importo
            ]
            
            balance = None
            for pattern in balance_patterns:
                matches = re.findall(pattern, body_text, re.IGNORECASE)
                if matches:
                    # Prendi il primo match ragionevole (< $10000)
                    for m in matches:
                        val = float(m.replace(",", ""))
                        if 0 < val < 10000:
                            balance = val
                            break
                    if balance:
                        break
            
            if balance is not None:
                result["balance_usd"] = balance
            else:
                # Prova via JS — cerca nei React props o data attributes
                js_balance = page.evaluate("""
                    () => {
                        // Cerca in tutti gli elementi di testo
                        const walker = document.createTreeWalker(
                            document.body, NodeFilter.SHOW_TEXT);
                        const amounts = [];
                        while (walker.nextNode()) {
                            const text = walker.currentNode.textContent.trim();
                            const match = text.match(/\\$([\\d,]+\\.?\\d*)/);
                            if (match) amounts.push(parseFloat(match[1].replace(',', '')));
                        }
                        return amounts.filter(a => a > 0 && a < 10000);
                    }
                """)
                if js_balance:
                    result["balance_usd"] = js_balance[0]
                else:
                    result["error"] = "Saldo non trovato nella pagina"
                    # Salva screenshot per debug
                    page.screenshot(path=str(SCRIPT_DIR / "billing_debug.png"))
            
            # Cerca info auto-recharge
            if "auto" in body_text.lower() and "recharge" in body_text.lower():
                result["auto_recharge"] = True
                # Prova a estrarre amount e threshold
                recharge_match = re.search(
                    r'(?:recharge|ricarica)[:\s]*\$?([\d.]+)', body_text, re.IGNORECASE)
                threshold_match = re.search(
                    r'(?:threshold|below|under|quando)[:\s]*\$?([\d.]+)', body_text, re.IGNORECASE)
                if recharge_match:
                    result["recharge_amount"] = float(recharge_match.group(1))
                if threshold_match:
                    result["recharge_threshold"] = float(threshold_match.group(1))
            
            # Salva cookies aggiornati
            context.storage_state(path=str(COOKIE_FILE))
            
        except Exception as e:
            result["error"] = str(e)
            try:
                page.screenshot(path=str(SCRIPT_DIR / "billing_error.png"))
            except:
                pass
        finally:
            browser.close()
    
    return result


def interactive_login():
    """Apre browser visibile per login manuale e salva cookies."""
    from playwright.sync_api import sync_playwright
    
    print("🌐 Apro il browser per il login su console.anthropic.com...")
    print("   Fai login normalmente, poi chiudi il browser.")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1280, "height": 800})
        page = context.new_page()
        
        page.goto("https://console.anthropic.com/settings/billing")
        
        print("⏳ In attesa che fai login... (chiudi il browser quando hai finito)")
        
        try:
            # Aspetta fino a che l'utente non chiude il browser
            page.wait_for_event("close", timeout=300000)  # 5 minuti
        except:
            pass
        
        # Salva cookies
        context.storage_state(path=str(COOKIE_FILE))
        print(f"✅ Cookies salvati in {COOKIE_FILE}")
        
        try:
            browser.close()
        except:
            pass


def save_to_db(result, config):
    """Salva il risultato nel DB."""
    if result.get("balance_usd") is None:
        print(f"❌ Nessun saldo da salvare: {result.get('error', 'unknown')}")
        return False
    
    try:
        conn, RDC = get_db_connection(config)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_provider_balance
                (provider, balance_usd, auto_recharge,
                 recharge_amount, recharge_threshold, source)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            result["provider"],
            result["balance_usd"],
            result.get("auto_recharge", False),
            result.get("recharge_amount"),
            result.get("recharge_threshold"),
            "scraper"
        ))
        conn.commit()
        conn.close()
        print(f"✅ Saldo salvato: ${result['balance_usd']:.2f}")
        return True
    except Exception as e:
        print(f"❌ Errore DB: {e}")
        return False


def save_manual(amount, config):
    """Salva un saldo inserito manualmente."""
    result = {
        "provider": "anthropic",
        "balance_usd": amount,
        "auto_recharge": True,
        "recharge_amount": 15.0,
        "recharge_threshold": 5.0,
    }
    try:
        conn, RDC = get_db_connection(config)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_provider_balance
                (provider, balance_usd, auto_recharge,
                 recharge_amount, recharge_threshold, source)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ("anthropic", amount, True, 15.0, 5.0, "manual"))
        conn.commit()
        conn.close()
        print(f"✅ Saldo manuale salvato: ${amount:.2f}")
    except Exception as e:
        print(f"❌ Errore DB: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anthropic Balance Scraper")
    parser.add_argument("--login", action="store_true", help="Login interattivo per salvare cookies")
    parser.add_argument("--check-only", action="store_true", help="Solo stampa saldo, non salva")
    parser.add_argument("--manual", type=float, help="Salva saldo manuale (es: --manual 14.99)")
    parser.add_argument("--headless", action="store_true", default=True, help="Browser headless (default)")
    parser.add_argument("--visible", action="store_true", help="Browser visibile")
    args = parser.parse_args()
    
    # Carica .env
    env_file = SCRIPT_DIR.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    
    config = load_config()
    
    if args.login:
        interactive_login()
    elif args.manual is not None:
        save_manual(args.manual, config)
    else:
        headless = not args.visible
        print(f"🔍 Scraping saldo Anthropic... ({'headless' if headless else 'visible'})")
        result = scrape_anthropic_balance(headless=headless)
        
        if result["balance_usd"] is not None:
            print(f"💰 Saldo: ${result['balance_usd']:.2f}")
            if result["auto_recharge"]:
                print(f"🔄 Auto-recharge: sì (${result.get('recharge_amount', '?')} sotto ${result.get('recharge_threshold', '?')})")
        else:
            print(f"❌ Errore: {result.get('error', 'sconosciuto')}")
        
        if not args.check_only:
            save_to_db(result, config)
