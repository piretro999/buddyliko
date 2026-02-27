#!/usr/bin/env python3
"""
Version: 20260216_115000
Version: 20260216_111828
Last Modified: 2026-02-16T11:18:36.971041

FIXES:
- Rimossa definizione duplicata di SCHEMAS_DIR (usava Path sbagliato)
- Usa Path("schemas") consistentemente in tutto il file
"""

#!/usr/bin/env python3
"""
Mapping System API
FastAPI backend for visual mapper

Endpoints:
- /api/schemas - Import/manage schemas
- /api/mappings - CRUD mappings
- /api/execute - Execute mapping
- /api/idoc - IDOC operations
- /api/ai - AI-powered auto-mapping
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, Response
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import json
import os
import jwt
import re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import httpx
import time as _time_mod
from ai_token_tracker import AITokenTracker, extract_anthropic_usage, extract_openai_usage

# Load environment variables - search in current dir and parent dir, both .env and _env
from pathlib import Path as _Path
_found = False
for _base in [_Path('.'), _Path('..'), _Path(__file__).parent, _Path(__file__).parent.parent]:
    for _name in ['.env', '_env', 'env']:
        _envfile = _base / _name
        if _envfile.exists():
            load_dotenv(dotenv_path=str(_envfile), override=True)
            print(f"\u2705 Loaded env from: {_envfile.resolve()}")
            _found = True
            break
    if _found:
        break
if not _found:
    load_dotenv()
    print("\u26a0\ufe0f  No .env file found, using system environment variables")

# Import our modules
from schema_parser import SchemaParser
from mapper_engine import MappingDefinition, MappingRule, MappingEngine
from idoc_parser import IDOCParser, IDOCDefinition
from csv_parser import CSVSchemaParser, MappingCSVExporter

# === INTEGRATED COMPONENTS (AUTO-ADDED) ===
from storage_layer import StorageFactory
from transformation_engine import TransformationEngine, XSDValidator
from formulas import list_formulas as _list_formulas
from auth_system import AuthManager
import yaml
from pathlib import Path
from fastapi import Depends, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse

app = FastAPI(
    title="Buddyliko API",
    version="2.0.0",
    description="API per visual data mapping, trasformazione, code generation e gestione workspace.",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# AI API Keys from environment
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === STORAGE & TRANSFORMATION (AUTO-INTEGRATED) ===
# Config resolution: environment\ → root → fallback {}
_config_candidates = [
    Path(__file__).parent.parent / 'environment' / 'config.yml',
    Path(__file__).parent.parent / 'config.yml',
    Path(__file__).parent / 'environment' / 'config.yml',
    Path(__file__).parent / 'config.yml',
]
APP_CONFIG = {}
CONFIG_PATH = None
for _cp in _config_candidates:
    if _cp.exists():
        CONFIG_PATH = _cp
        with open(_cp) as f:
            APP_CONFIG = yaml.safe_load(f) or {}
        print(f"📁 Config loaded: {_cp.resolve()}")
        break
if not CONFIG_PATH:
    print("⚠️  config.yml non trovato — usando defaults. Crea environment/config.yml")

# Override/merge con .env (variabili d'ambiente hanno priorità assoluta)
import os as _os
_env_candidates = [
    Path(__file__).parent.parent / 'environment' / '.env',
    Path(__file__).parent.parent / '.env',
]
for _ep in _env_candidates:
    if _ep.exists():
        with open(_ep, encoding='utf-8', errors='ignore') as _ef:
            for _line in _ef:
                _line = _line.strip()
                if _line and not _line.startswith('#') and '=' in _line:
                    _k, _v = _line.split('=', 1)
                    if _v.strip():
                        # Sovrascrive sempre: l'ultimo .env trovato vince
                        # (environment/.env ha priorita' su .env root)
                        _os.environ[_k.strip()] = _v.strip()
        print(f"📁 .env loaded: {_ep.resolve()}")
        break

# Inject env vars into APP_CONFIG where config uses ${ENV_VAR} syntax
def _resolve_env(val):
    if isinstance(val, str) and val.startswith('${') and val.endswith('}'):
        return _os.environ.get(val[2:-1], val)
    return val

# Merge top-level env overrides into APP_CONFIG
_env_map = {
    'ANTHROPIC_API_KEY':      ['ai_providers', 'anthropic', 'api_key'],
    'OPENAI_API_KEY':         ['ai_providers', 'openai', 'api_key'],
    'STRIPE_SECRET_KEY':      ['billing', 'stripe_secret_key'],
    'STRIPE_WEBHOOK_SECRET':  ['billing', 'stripe_webhook_secret'],
    'STRIPE_PUBLISHABLE_KEY': ['billing', 'stripe_publishable_key'],
    'DB_PASSWORD':            ['database', 'postgresql', 'password'],
    'SECRET_KEY':             ['auth', 'secret_key'],
    'GOOGLE_CLIENT_SECRET':   ['auth', 'oauth', 'google', 'client_secret'],
    'FACEBOOK_APP_SECRET':    ['auth', 'oauth', 'facebook', 'app_secret'],
    'GITHUB_CLIENT_SECRET':   ['auth', 'oauth', 'github', 'client_secret'],
    'MICROSOFT_CLIENT_SECRET':['auth', 'oauth', 'microsoft', 'client_secret'],
    'SMTP_USERNAME':          ['smtp', 'username'],
    'SMTP_PASSWORD':          ['smtp', 'password'],
    'DB_CONNECTOR_SECRET':    ['db_connector', 'secret_key'],
}
for _env_key, _path in _env_map.items():
    _val = _os.environ.get(_env_key)
    if _val:
        _d = APP_CONFIG
        for _p in _path[:-1]:
            _d = _d.setdefault(_p, {})
        _d[_path[-1]] = _val

# Storage
try:
    storage = StorageFactory.get_storage(APP_CONFIG.get('database', {}))
    print(f"✅ Storage: {type(storage).__name__}")
except:
    storage = None
    print("⚠️ Using in-memory storage")

# Authentication
# AUTH_ENABLED: priorità → variabile d'ambiente BUDDYLIKO_AUTH_ENABLED → config.yml → False
_auth_env = _os.environ.get('BUDDYLIKO_AUTH_ENABLED', '').lower()
if _auth_env == 'true':
    AUTH_ENABLED = True
elif _auth_env == 'false':
    AUTH_ENABLED = False
else:
    AUTH_ENABLED = APP_CONFIG.get('auth', {}).get('enabled', False)
print(f"🔐 Auth: {'ENABLED' if AUTH_ENABLED else 'DISABLED'} (source: {'env override' if _auth_env else 'config.yml'})")

# Federated Identity Manager (provider linking)
federated_identity = None
try:
    from auth_system import FederatedIdentityManager
    if hasattr(storage, 'conn') and hasattr(storage, 'RealDictCursor'):
        federated_identity = FederatedIdentityManager(storage.conn, storage.RealDictCursor)
        print("✅ Federated Identity Manager initialized")
except Exception as _fie:
    print(f"⚠️  Federated identity init failed: {_fie}")
auth_manager = None
security = HTTPBearer(auto_error=False)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to get current user from JWT token"""
    if not AUTH_ENABLED:
        return {"id": "anonymous", "email": "anonymous@local", "name": "Anonymous", "role": "MASTER"}

    if not credentials:
        raise HTTPException(401, "Authentication required")

    token = credentials.credentials
    try:
        valid, payload = auth_manager.verify_token(token)
        if not valid:
            raise HTTPException(401, "Invalid or expired token")
        # Aggiorna dati utente freschi dal DB (role/status potrebbero essere cambiati)
        if storage and payload.get('user_id'):
            try:
                db_user = storage.get_user(str(payload['user_id']))
                if db_user:
                    payload['id'] = str(db_user.get('id', payload.get('user_id', '')))
                    payload['role'] = db_user.get('role', payload.get('role', 'USER'))
                    payload['status'] = db_user.get('status', payload.get('status', 'APPROVED'))
                    payload['name'] = db_user.get('name', payload.get('name', ''))
                    payload['plan'] = db_user.get('plan', payload.get('plan', 'FREE'))
            except Exception:
                pass
        if not payload.get('id'):
            payload['id'] = str(payload.get('user_id', 'unknown'))
        return payload
    except Exception as e:
        raise HTTPException(401, f"Authentication error: {str(e)}")


if AUTH_ENABLED and storage:
    secret_key = APP_CONFIG.get('auth', {}).get('secret_key', 'default-secret-key')
    token_expiry = APP_CONFIG.get('auth', {}).get('token_expiry_hours', 24)
    auth_manager = AuthManager(storage, secret_key, token_expiry)
    print(f"🔐 Authentication: ENABLED")
else:
    print(f"🔓 Authentication: DISABLED")

# Group & File Storage
group_storage = None
perm_checker = None
try:
    from groups_models import PostgreSQLGroupStorage, PermissionChecker
    from groups_api import register_groups_api
    if hasattr(storage, 'conn') and hasattr(storage, 'RealDictCursor'):
        group_storage = PostgreSQLGroupStorage(storage.conn, storage.RealDictCursor)
        perm_checker = PermissionChecker(group_storage)
        print("✅ Group & File storage initialized")
    else:
        print("⚠️  Group storage requires PostgreSQL - skipping")
except Exception as _ge:
    print(f"⚠️  Group storage init failed: {_ge}")

# Audit Log
audit_log = None
try:
    from audit_log import AuditLogManager, AuditAction, AuditOutcome, AuditLevel
    if hasattr(storage, 'conn') and hasattr(storage, 'RealDictCursor'):
        audit_log = AuditLogManager(storage.conn, storage.RealDictCursor)
        print("✅ Audit log initialized")
    else:
        print("⚠️  Audit log requires PostgreSQL - skipping")
except Exception as _ale:
    print(f"⚠️  Audit log init failed: {_ale}")

# Job Engine
job_engine = None
try:
    from job_engine import JobEngine, JobType, JobStatus
    if hasattr(storage, 'conn') and hasattr(storage, 'RealDictCursor'):
        job_engine = JobEngine(storage.conn, storage.RealDictCursor)
        print("✅ Job engine initialized")
    else:
        print("⚠️  Job engine requires PostgreSQL - skipping")
except Exception as _jee:
    print(f"⚠️  Job engine init failed: {_jee}")

# Code Generator
try:
    from code_generator import generate_zip, save_engine_module
    GENERATED_ENGINES_DIR = "/opt/buddyliko/backend/generated_engines"
    os.makedirs(GENERATED_ENGINES_DIR, exist_ok=True)
    print("✅ Code generator initialized")
except Exception as _cge:
    print(f"⚠️  Code generator init failed: {_cge}")
    GENERATED_ENGINES_DIR = "/opt/buddyliko/backend/generated_engines"
    def generate_zip(*a, **k): raise RuntimeError("Code generator not available")
    def save_engine_module(*a, **k): raise RuntimeError("Code generator not available")

# Fallback stubs if audit_log or job_engine failed to import
try:
    AuditAction
except NameError:
    class _AuditStub:
        def __getattr__(self, name): return name
    AuditAction = _AuditStub()
    AuditOutcome = _AuditStub()
    AuditLevel = _AuditStub()

try:
    JobType
except NameError:
    class _JobStub:
        def __getattr__(self, name): return name
    JobType = _JobStub()
    JobStatus = _JobStub()

# ── ALERTS & ANALYTICS ──────────────────────────────────────────────────────
alert_engine = None
ai_budget_monitor = None
ai_token_tracker = None
profitability_engine = None
pricing_manager = None
try:
    from alerts_analytics import (
        AlertEngine, AIBudgetMonitor, ProfitabilityEngine,
        PricingManager, AlertType, NotificationChannel
    )
    if hasattr(storage, 'conn'):
        _analytics_config = APP_CONFIG
        alert_engine = AlertEngine(storage.conn, storage.RealDictCursor, _analytics_config)
        ai_budget_monitor = AIBudgetMonitor(
            storage.conn, storage.RealDictCursor, _analytics_config, alert_engine
        )
        profitability_engine = ProfitabilityEngine(
            storage.conn, storage.RealDictCursor, _analytics_config
        )
        pricing_manager = PricingManager(storage.conn, storage.RealDictCursor)
    print("✅ Alerts & Analytics initialized")
    if hasattr(storage, "conn"):
        ai_token_tracker = AITokenTracker(storage.conn, storage.RealDictCursor)
        print("✅ AI Token Tracker initialized")
except Exception as _ae:
    print(f"⚠️  Alerts & Analytics init failed: {_ae}")
    class AlertType: pass
    class NotificationChannel: pass

# ── STRIPE BILLING ──────────────────────────────────────────────────────────
billing_manager = None
try:
    from stripe_billing import BillingManager, Plan, PLAN_LIMITS, PLAN_PRICES_EUR
    _billing_config = APP_CONFIG.get('billing', {})
    if _billing_config.get('stripe_secret_key') and hasattr(storage, 'conn'):
        billing_manager = BillingManager(storage.conn, storage.RealDictCursor, _billing_config)
    else:
        # Init tabelle anche senza Stripe (per admin override e usage tracking)
        if hasattr(storage, 'conn'):
            billing_manager = BillingManager(storage.conn, storage.RealDictCursor, _billing_config)
            print("⚠️  Stripe billing: no secret key — billing UI disabled, usage tracking active")
except Exception as _be:
    print(f"⚠️  Billing init failed: {_be}")
    class Plan:
        FREE = 'FREE'; PRO = 'PRO'; ENTERPRISE = 'ENTERPRISE'; CUSTOM = 'CUSTOM'
    PLAN_LIMITS = {'FREE': {}, 'PRO': {}, 'ENTERPRISE': {}, 'CUSTOM': {}}
    PLAN_PRICES_EUR = {}

# ── EMAIL SERVICE ────────────────────────────────────────────────────────────
try:
    import email_service
    _email_cfg = APP_CONFIG.get('email', {})
    _smtp_cfg = APP_CONFIG.get('smtp', {})
    email_service.configure(
        host=_resolve_env(_email_cfg.get('smtp_host') or _smtp_cfg.get('host', '')),
        port=int(_resolve_env(_email_cfg.get('smtp_port') or _smtp_cfg.get('port', 587))),
        user=_resolve_env(_email_cfg.get('smtp_user') or _smtp_cfg.get('username', '')),
        password=_resolve_env(_email_cfg.get('smtp_password') or _smtp_cfg.get('password', '')),
        from_email=_resolve_env(_email_cfg.get('from_address', '')),
        from_name=_resolve_env(_email_cfg.get('from_name', '')),
        frontend_url=APP_CONFIG.get('app', {}).get('frontend_url', 'https://buddyliko.com')
    )
    print(f"📧 Email service: {'configurato' if email_service._configured() else 'SMTP non configurato'}")
except Exception as _ee:
    print(f"⚠️  Email service init failed: {_ee}")

# ── DB CONNECTOR ─────────────────────────────────────────────────────────────
db_connector_manager = None
try:
    from db_connector import DBConnectorManager
    if hasattr(storage, 'conn'):
        _dbconn_secret = APP_CONFIG.get('db_connector', {}).get('secret_key', 'buddyliko-dbconn-secret')
        db_connector_manager = DBConnectorManager(storage.conn, storage.RealDictCursor, _dbconn_secret)
    print("✅ DB Connector initialized")
except Exception as _dce:
    print(f"⚠️  DB Connector init failed: {_dce}")

# ── EDI PARSER ───────────────────────────────────────────────────────────────
edi_available = False
try:
    from edi_parser import parse_edi, to_buddyliko_schema as edi_to_schema, edi_to_flat, build_edi, detect_edi_format
    edi_available = True
    print("✅ EDI Parser (X12/EDIFACT) initialized")
except Exception as _edi_e:
    print(f"⚠️  EDI Parser init failed: {_edi_e}")
    def parse_edi(c): raise RuntimeError("EDI parser not available")
    def edi_to_schema(p, n=None): raise RuntimeError("EDI parser not available")
    def edi_to_flat(p): raise RuntimeError("EDI parser not available")
    def build_edi(d, fmt='X12', transaction_type='850'): raise RuntimeError("EDI parser not available")
    def detect_edi_format(c): return 'UNKNOWN'

# ── HL7 PARSER ───────────────────────────────────────────────────────────────
hl7_available = False
try:
    from hl7_parser import parse_hl7, hl7v2_to_flat, fhir_to_flat, to_buddyliko_schema_hl7, build_hl7v2, detect_hl7_format
    hl7_available = True
    print("✅ HL7 Parser (v2/FHIR) initialized")
except Exception as _hl7_e:
    print(f"⚠️  HL7 Parser init failed: {_hl7_e}")
    def parse_hl7(c): raise RuntimeError("HL7 parser not available")
    def hl7v2_to_flat(p): raise RuntimeError("HL7 parser not available")
    def fhir_to_flat(p): raise RuntimeError("HL7 parser not available")
    def to_buddyliko_schema_hl7(p, n=None): raise RuntimeError("HL7 parser not available")
    def build_hl7v2(d, t='ADT^A01'): raise RuntimeError("HL7 parser not available")
    def detect_hl7_format(c): return 'UNKNOWN'

# ── ALERTS & ANALYTICS ──────────────────────────────────────────────────────
alerts_engine       = None
profitability       = None
pricing_config_mgr  = None
try:
    from alerts_analytics import AlertEngine, ProfitabilityEngine, PricingManager
    if hasattr(storage, 'conn'):
        _analytics_cfg = APP_CONFIG.get('analytics', {})
        _analytics_cfg.setdefault('anthropic_api_key', APP_CONFIG.get('anthropic_api_key', ''))
        _analytics_cfg.setdefault('openai_api_key',    APP_CONFIG.get('openai_api_key', ''))
        _analytics_cfg.setdefault('smtp', APP_CONFIG.get('smtp', {}))
        _analytics_cfg.setdefault('alert_webhook_url', APP_CONFIG.get('alert_webhook_url', ''))
        alerts_engine      = AlertEngine(storage.conn, storage.RealDictCursor, _analytics_cfg)
        profitability      = ProfitabilityEngine(storage.conn, storage.RealDictCursor)
        pricing_config_mgr = PricingManager(storage.conn, storage.RealDictCursor)
        print("✅ Alerts & Analytics initialized")
    if hasattr(storage, "conn"):
        ai_token_tracker = AITokenTracker(storage.conn, storage.RealDictCursor)
        print("✅ AI Token Tracker initialized")
except Exception as _ae:
    print(f"⚠️  Analytics init failed: {_ae}")

# Schemas - usa path relativo alla directory di lavoro corrente
# (dove viene eseguito il backend, di solito la directory principale del progetto)
SCHEMAS_DIR = Path("schemas")
SCHEMAS_DIR.mkdir(exist_ok=True)

# Transformation
transformation_engine = TransformationEngine()

def detect_ubl_document_type(xml_content: str = None, mapping_rules: dict = None) -> str:
    """
    Auto-detect UBL document type from XML content or mapping rules
    
    Returns: 'Invoice', 'CreditNote', 'DebitNote', 'Order', 'DespatchAdvice', etc.
    """
    # DEBUG: Log what we receive
    if mapping_rules:
        print(f"🔍 DEBUG mapping_rules keys: {list(mapping_rules.keys())}")
        if 'outputSchema' in mapping_rules:
            print(f"🔍 DEBUG outputSchema keys: {list(mapping_rules['outputSchema'].keys()) if isinstance(mapping_rules['outputSchema'], dict) else 'NOT A DICT'}")
            if isinstance(mapping_rules['outputSchema'], dict):
                print(f"🔍 DEBUG rootElement in outputSchema: {'rootElement' in mapping_rules['outputSchema']}")
                if 'rootElement' in mapping_rules['outputSchema']:
                    print(f"🔍 DEBUG rootElement value: {mapping_rules['outputSchema']['rootElement']}")
    
    # Strategy 1: Check mapping rules for output schema root element
    if mapping_rules:
        if isinstance(mapping_rules, dict) and 'outputSchema' in mapping_rules:
            root_elem = mapping_rules['outputSchema'].get('rootElement', '')
            if root_elem:
                print(f"🔍 Document type from mapping rules: {root_elem}")
                return root_elem  # ← PRENDE DA QUI!
    
    # Strategy 2: Try to detect from XML content (output or input)
    if xml_content:
        try:
            import xml.etree.ElementTree as ET
            # Parse just enough to get root tag
            root = ET.fromstring(xml_content.encode('utf-8') if isinstance(xml_content, str) else xml_content)
            
            # Extract tag name (remove namespace)
            tag = root.tag
            if '}' in tag:
                tag = tag.split('}')[1]
            
            # Check if it's a known UBL document type
            known_types = [
                'Invoice', 'CreditNote', 'DebitNote', 'Reminder',
                'Order', 'OrderResponse', 'OrderChange', 'OrderCancellation',
                'DespatchAdvice', 'ReceiptAdvice', 'ApplicationResponse',
                'Catalogue', 'CatalogueRequest', 'CatalogueItemSpecificationUpdate',
                'Statement', 'SelfBilledInvoice', 'SelfBilledCreditNote'
            ]
            
            for known_type in known_types:
                if known_type.lower() in tag.lower():
                    print(f"🔍 Document type detected from XML: {known_type}")
                    return known_type
            
            print(f"🔍 Unknown document type from XML: {tag}")
            return tag
        except Exception as e:
            print(f"⚠️  Could not parse XML to detect type: {e}")
    
    # Default fallback
    print(f"⚠️  Could not detect document type, defaulting to Invoice")
    return 'Invoice'


def find_xsd(format_name, io='input', document_type=None):
    """
    Find XSD file for a format - COMPLETELY DYNAMIC
    
    Args:
        format_name: Schema format (e.g., 'ubl', 'fatturapa')
        io: 'input' or 'output'
        document_type: Specific document type (e.g., 'Invoice', 'CreditNote', 'Order')
                      If provided, will look for UBL-{document_type}-*.xsd
    
    Takes ANY .xsd file in the directory, WHATEVER its name is.
    Returns ABSOLUTE PATH so imports can be resolved correctly.
    
    NO hardcoding, NO assumptions about filenames!
    """
    # Build path: schemas/{io}/{format_name}/
    base_dir = SCHEMAS_DIR / io / format_name
    
    # Try exact match first
    if not (base_dir.exists() and base_dir.is_dir()):
        # Try case-insensitive match and variations (e.g., UBL-21 for ubl)
        print(f"📂 Exact path not found: {base_dir}, trying fuzzy match...")
        io_dir = SCHEMAS_DIR / io
        if io_dir.exists():
            for subdir in io_dir.iterdir():
                if subdir.is_dir():
                    # Match case-insensitive
                    if format_name.lower() in subdir.name.lower():
                        print(f"✅ Found schema directory (case-insensitive): {subdir.name}")
                        base_dir = subdir
                        break
                    # Match with version suffix removed (e.g., UBL-21 → UBL for ubl)
                    clean_subdir = subdir.name.lower().replace('-', '').replace('.', '')
                    clean_format = format_name.lower().replace('-', '').replace('.', '')
                    if clean_format in clean_subdir or clean_subdir in clean_format:
                        print(f"✅ Found schema directory (fuzzy match): {subdir.name}")
                        base_dir = subdir
                        break
    
    if base_dir.exists() and base_dir.is_dir():
        print(f"📂 Searching XSD in: {base_dir}")
        
        # Search recursively for ANY .xsd file
        xsd_files = list(base_dir.rglob("*.xsd"))
        
        if not xsd_files:
            print(f"⚠️  No XSD files found in {base_dir}")
            return None
        
        print(f"📋 Found {len(xsd_files)} XSD files")
        
        # If document_type is specified, look for that specific type
        if document_type:
            print(f"🎯 Looking for {document_type} XSD specifically...")
            
            # Pattern 1: EXACT match UBL-{DocumentType}-*.xsd in maindoc
            # CRITICAL: Must match EXACTLY to avoid FreightInvoice matching Invoice
            exact_candidates = [
                f for f in xsd_files
                if f.name.startswith(f"UBL-{document_type}-")  # MUST START WITH!
                and ("maindoc" in str(f).lower() or f.parent.name.lower() == "maindoc")
                and not any(skip in f.name.lower() for skip in ['common', 'component', 'type', 'extension'])
            ]
            
            if exact_candidates:
                xsd_path = str(exact_candidates[0].resolve())  # ABSOLUTE PATH!
                print(f"✅ Found {document_type} XSD (exact match): {xsd_path}")
                return xsd_path
            
            # Pattern 2: Loose match {DocumentType} in maindoc (for other schemas)
            type_candidates = [
                f for f in xsd_files
                if document_type.lower() in f.name.lower()
                and ("maindoc" in str(f).lower() or f.parent.name.lower() == "maindoc")
                and not any(skip in f.name.lower() for skip in ['common', 'component', 'type', 'extension'])
            ]
            
            if type_candidates:
                xsd_path = str(type_candidates[0].resolve())  # ABSOLUTE PATH!
                print(f"✅ Found {document_type} XSD: {xsd_path}")
                return xsd_path
            
            # Pattern 3: Any file matching the document type
            type_files = [f for f in xsd_files if document_type.lower() in f.name.lower()]
            if type_files:
                xsd_path = str(type_files[0].resolve())
                print(f"✅ Found {document_type} XSD (fallback): {xsd_path}")
                return xsd_path
        
        # STRATEGY: Prefer main document XSD (has document type name and "maindoc" or is in root)
        # This works for UBL structure: xsd/maindoc/UBL-{Type}-2.1.xsd
        
        # 1. Look for main XSD in maindoc folder (any document type)
        main_candidates = [
            f for f in xsd_files 
            if ("maindoc" in str(f).lower() or f.parent.name.lower() == "maindoc")
            and not any(skip in f.name.lower() for skip in ['common', 'component', 'type', 'extension'])
        ]
        
        if main_candidates:
            # Prefer Invoice if multiple found and no type specified
            invoice_files = [f for f in main_candidates if "invoice" in f.name.lower()]
            if invoice_files:
                xsd_path = str(invoice_files[0].resolve())
                print(f"✅ Found Invoice XSD: {xsd_path}")
                return xsd_path
            
            # Otherwise take first main doc
            xsd_path = str(main_candidates[0].resolve())
            print(f"✅ Found main XSD: {xsd_path}")
            return xsd_path
        
        # 2. Fallback: Take the FIRST XSD in the root directory
        root_xsds = [f for f in xsd_files if f.parent == base_dir]
        if root_xsds:
            xsd_path = str(root_xsds[0].resolve())  # ABSOLUTE PATH!
            print(f"✅ Using root XSD: {xsd_path}")
            return xsd_path
        
        # 3. Last resort: ANY XSD file
        xsd_path = str(xsd_files[0].resolve())  # ABSOLUTE PATH!
        print(f"⚠️  Using first XSD found: {xsd_path}")
        return xsd_path
    
    # If directory doesn't exist, try searching in parent directories
    io_dir = SCHEMAS_DIR / io
    if io_dir.exists():
        print(f"📂 Format directory not found, searching in: {io_dir}")
        # Search ALL subdirectories
        for subdir in io_dir.iterdir():
            if subdir.is_dir():
                # Check if directory name matches format (case-insensitive, partial match)
                if format_name.lower() in subdir.name.lower() or subdir.name.lower() in format_name.lower():
                    print(f"📂 Checking subdirectory: {subdir.name}")
                    # Recursively search in this directory
                    xsd_files = list(subdir.rglob("*.xsd"))
                    if xsd_files:
                        # If document type specified, look for it
                        if document_type:
                            type_candidates = [
                                f for f in xsd_files
                                if document_type.lower() in f.name.lower()
                                and "maindoc" in str(f).lower()
                            ]
                            if type_candidates:
                                xsd_path = str(type_candidates[0].resolve())
                                print(f"✅ Found {document_type} XSD in {subdir.name}: {xsd_path}")
                                return xsd_path
                        
                        # Same strategy as above
                        main_candidates = [
                            f for f in xsd_files 
                            if "maindoc" in str(f).lower()
                            and not any(skip in f.name.lower() for skip in ['common', 'component'])
                        ]
                        if main_candidates:
                            xsd_path = str(main_candidates[0].resolve())
                            print(f"✅ Found XSD in {subdir.name}: {xsd_path}")
                            return xsd_path
                        
                        xsd_path = str(xsd_files[0].resolve())
                        print(f"✅ Found XSD in {subdir.name}: {xsd_path}")
                        return xsd_path
    
    print(f"❌ No XSD found for format '{format_name}' in {io}")
    return None

def find_schematron(format_name, io='input'):
    """Find Schematron file for a format (searches recursively for rules.sch)"""
    # First try exact match
    d = SCHEMAS_DIR / io / format_name
    if d.exists():
        # Look for rules.sch specifically (standard name)
        rules_sch = d / "rules.sch"
        if rules_sch.exists():
            return str(rules_sch)
        # Fallback: any SCH file
        sch = list(d.glob("*.sch"))
        if sch:
            return str(sch[0])
    
    # If not found, search in ALL subdirectories
    io_dir = SCHEMAS_DIR / io
    if io_dir.exists():
        for schema_dir in io_dir.iterdir():
            if schema_dir.is_dir():
                rules_sch = schema_dir / "rules.sch"
                if rules_sch.exists():
                    # Check if directory name matches format
                    if format_name.lower() in schema_dir.name.lower() or schema_dir.name.lower() in format_name.lower():
                        print(f"📂 Found schematron in subdirectory: {schema_dir.name}")
                        return str(rules_sch)
    
    return None

def get_validation_files(input_format: str, output_format: str, input_content: str = None, 
                         mapping_rules: dict = None, input_schema: str = None, output_schema: str = None):
    """
    Get XSD and Schematron files for input and output.
    
    Args:
        input_format: file type (xml, json, csv) — for auto-detection only
        output_format: file type (xml, json, csv) — for auto-detection only
        input_content: raw content for auto-detecting schema type
        mapping_rules: for detecting UBL document type (Invoice, CreditNote, etc.)
        input_schema: EXACT schema directory name (e.g. 'FatturaPA', 'UBL-21') — if provided, skips auto-detection
        output_schema: EXACT schema directory name (e.g. 'UBL-21', 'PEPPOL') — if provided, skips auto-detection
    
    Returns:
        (input_xsd, input_sch, output_xsd, output_sch)
    """
    input_schema_type = input_schema
    output_schema_type = output_schema
    document_type = None
    
    # Auto-detect INPUT schema type if not explicitly provided
    if not input_schema_type:
        if input_format.lower() == 'xml' and input_content:
            if '<FatturaElettronica' in input_content or 'FatturaPA' in input_content:
                input_schema_type = 'FatturaPA'
                print(f"  🔍 Auto-detected input: FatturaPA")
            elif '<Invoice' in input_content or 'UBL' in input_content:
                input_schema_type = 'UBL-21'
                print(f"  🔍 Auto-detected input: UBL-21")
            else:
                input_schema_type = 'FatturaPA'
                print(f"  ⚠️  Could not detect XML type, defaulting to FatturaPA")
        elif input_format.lower() not in ('json', 'csv'):
            input_schema_type = None
    
    # Auto-detect OUTPUT schema type if not explicitly provided
    if not output_schema_type:
        if output_format.lower() == 'xml':
            output_schema_type = 'UBL-21'  # Default for XML output
            print(f"  ⚠️  No output_schema provided, defaulting to UBL-21")
        elif output_format.lower() not in ('json', 'csv'):
            output_schema_type = None
    
    print(f"  📋 Input schema: {input_schema_type}, Output schema: {output_schema_type}")
    
    # Auto-detect UBL document type (Invoice, CreditNote, etc.) for correct XSD
    if output_schema_type and 'ubl' in output_schema_type.lower():
        document_type = detect_ubl_document_type(
            xml_content=input_content,
            mapping_rules=mapping_rules
        )
        print(f"  📄 Document type: {document_type}")
    
    # Find XSD files — search in both output AND input directories
    input_xsd = find_xsd(input_schema_type, 'input') if input_schema_type else None
    input_sch = find_schematron(input_schema_type, 'input') if input_schema_type else None
    
    output_xsd = None
    output_sch = None
    if output_schema_type:
        # Try output dir first, then input dir (schemas are often only in input)
        output_xsd = find_xsd(output_schema_type, 'output', document_type=document_type)
        if not output_xsd:
            print(f"  📂 XSD not found in output dir, trying input dir...")
            output_xsd = find_xsd(output_schema_type, 'input', document_type=document_type)
        output_sch = find_schematron(output_schema_type, 'output')
        if not output_sch:
            output_sch = find_schematron(output_schema_type, 'input')
    
    if input_xsd or input_sch or output_xsd or output_sch:
        print(f"\n📋 Validation files:")
        if input_xsd: print(f"  Input XSD: {input_xsd}")
        if input_sch: print(f"  Input Schematron: {input_sch}")
        if output_xsd: print(f"  Output XSD: {output_xsd}")
        if output_sch: print(f"  Output Schematron: {output_sch}")
    else:
        print(f"  ⚠️  No validation files found!")
    
    return input_xsd, input_sch, output_xsd, output_sch


# Storage
# SCHEMAS_DIR già definito all'inizio del file (riga 88)
MAPPINGS_DIR = Path("mappings")
IDOC_DEFS_DIR = Path("idoc_definitions")

for d in [SCHEMAS_DIR, MAPPINGS_DIR, IDOC_DEFS_DIR, SCHEMAS_DIR/"input", SCHEMAS_DIR/"output"]:
    d.mkdir(exist_ok=True, parents=True)

# In-memory cache
schemas_cache = {}
mappings_cache = {}


# Models
class SchemaUploadRequest(BaseModel):
    name: str
    type: str  # xsd, json_schema, sample_xml, idoc
    direction: str  # input, output


class MappingRuleModel(BaseModel):
    id: str
    source: Any
    target: Any
    transformation: Optional[Dict] = {}
    condition: Optional[Dict] = None
    cardinality_handling: str = "direct"
    enabled: bool = True


class MappingCreateRequest(BaseModel):
    name: str
    input_schema: str
    output_schema: str
    rules: List[MappingRuleModel] = []


class ExecuteMappingRequest(BaseModel):
    mapping_id: str
    input_data: Dict


class AIAutoMapRequest(BaseModel):
    input_fields: List[Dict]
    output_fields: List[Dict]
    input_sample: str = ""
    output_sample: str = ""


class AISuggestion(BaseModel):
    source_field: str
    target_field: str
    confidence: float
    reasoning: str
    suggested_formula: Optional[str] = None


# Endpoints
@app.get("/", include_in_schema=False)
async def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/docs")


# ============================================================================
# FORMULAS ENDPOINT
# ============================================================================

@app.get("/api/formulas")
async def get_formulas():
    """Return the list of available transformation formulas from formulas.py"""
    return {"formulas": _list_formulas()}


# ============================================================================
# SCHEMA ENDPOINTS
# ============================================================================



@app.get("/api/schemas")
async def list_schemas():
    """List all schemas"""
    schemas = []
    
    for direction in ['input', 'output']:
        dir_path = SCHEMAS_DIR / direction
        for file in dir_path.glob('*'):
            schemas.append({
                'id': f"{direction}_{file.name}",
                'name': file.name,
                'direction': direction,
                'size': file.stat().st_size,
                'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
            })
    
    return schemas


@app.get("/api/schemas/list")
async def list_available_schemas():
    """List all available schemas in schemas/input and schemas/output"""
    try:
        # Use global SCHEMAS_DIR instead of reconstructing path
        schemas_dir = SCHEMAS_DIR
        
        input_schemas = []
        output_schemas = []
        
        # List input schemas
        input_dir = schemas_dir / 'input'
        if input_dir.exists():
            for schema_name in os.listdir(input_dir):
                schema_path = input_dir / schema_name
                if schema_path.is_dir():
                    # Look for ANY .xsd file, not just schema.xsd
                    xsd_files = list(schema_path.rglob("*.xsd"))
                    sch_files = list(schema_path.rglob("*.sch"))
                    
                    input_schemas.append({
                        'name': schema_name,
                        'hasXsd': len(xsd_files) > 0,
                        'hasSchematron': len(sch_files) > 0,
                        'xsdCount': len(xsd_files),
                        'schCount': len(sch_files)
                    })
        
        # List output schemas
        output_dir = schemas_dir / 'output'
        if output_dir.exists():
            for schema_name in os.listdir(output_dir):
                schema_path = output_dir / schema_name
                if schema_path.is_dir():
                    # Look for ANY .xsd file, not just schema.xsd
                    from pathlib import Path
                    xsd_files = list(Path(schema_path).rglob("*.xsd"))
                    sch_files = list(Path(schema_path).rglob("*.sch"))
                    
                    output_schemas.append({
                        'name': schema_name,
                        'hasXsd': len(xsd_files) > 0,
                        'hasSchematron': len(sch_files) > 0,
                        'xsdCount': len(xsd_files),
                        'schCount': len(sch_files)
                    })
        
        return {
            'input': sorted(input_schemas, key=lambda x: x['name']),
            'output': sorted(output_schemas, key=lambda x: x['name'])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/schemas/{schema_id}")
async def get_schema(schema_id: str):
    """Get schema details"""
    if schema_id in schemas_cache:
        return schemas_cache[schema_id]
    
    raise HTTPException(404, "Schema not found")


@app.get("/api/schemas/{schema_id}/tree")
async def get_schema_tree(schema_id: str):
    """Get schema as tree structure"""
    if schema_id not in schemas_cache:
        raise HTTPException(404, "Schema not found")
    
    # Reconstruct parser
    parser = SchemaParser()
    schema = schemas_cache[schema_id]
    
    # Rebuild from cached schema
    for field_id, field_data in schema['fields'].items():
        from schema_parser import SchemaField
        parser.fields[field_id] = SchemaField(**field_data)
    
    parser.root_fields = schema['root_fields']
    
    return parser.to_tree_structure()


# ============================================================================
# MAPPING ENDPOINTS
# ============================================================================

@app.post("/api/mappings")
async def create_mapping(request: MappingCreateRequest):
    """Create new mapping"""
    mapping = MappingDefinition(request.name)
    mapping.input_schema = request.input_schema
    mapping.output_schema = request.output_schema
    
    for rule_data in request.rules:
        mapping.add_rule(MappingRule(rule_data.dict()))
    
    # Save
    mapping_id = f"{request.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    mapping_path = MAPPINGS_DIR / f"{mapping_id}.json"
    mapping.save(str(mapping_path))
    
    # Cache
    mappings_cache[mapping_id] = mapping
    
    if audit_log:
        try:
            audit_log.log(AuditAction.PROJECT_SAVE, user=user if 'user' in dir() else None,
                          resource_type='mapping')
        except Exception: pass
    return {
        "success": True,
        "mapping_id": mapping_id,
        "mapping": mapping.to_dict()
    }


@app.get("/api/mappings")
async def list_mappings():
    """List all mappings"""
    mappings = []
    
    for file in MAPPINGS_DIR.glob('*.json'):
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            mappings.append({
                'id': file.stem,
                'name': data['name'],
                'input_schema': data.get('input_schema'),
                'output_schema': data.get('output_schema'),
                'rules_count': len(data.get('rules', [])),
                'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
            })
        except:
            pass
    
    return mappings


@app.get("/api/mappings/{mapping_id}")
async def get_mapping(mapping_id: str):
    """Get mapping details"""
    # Check cache
    if mapping_id in mappings_cache:
        return mappings_cache[mapping_id].to_dict()
    
    # Load from file
    mapping_path = MAPPINGS_DIR / f"{mapping_id}.json"
    if not mapping_path.exists():
        raise HTTPException(404, "Mapping not found")
    
    mapping = MappingDefinition.load(str(mapping_path))
    mappings_cache[mapping_id] = mapping
    
    return mapping.to_dict()


@app.put("/api/mappings/{mapping_id}")
async def update_mapping(mapping_id: str, request: MappingCreateRequest):
    """Update mapping"""
    mapping = MappingDefinition(request.name)
    mapping.input_schema = request.input_schema
    mapping.output_schema = request.output_schema
    
    for rule_data in request.rules:
        mapping.add_rule(MappingRule(rule_data.dict()))
    
    # Save
    mapping_path = MAPPINGS_DIR / f"{mapping_id}.json"
    mapping.save(str(mapping_path))
    
    # Update cache
    mappings_cache[mapping_id] = mapping
    
    return {
        "success": True,
        "mapping": mapping.to_dict()
    }


@app.delete("/api/mappings/{mapping_id}")
async def delete_mapping(mapping_id: str):
    """Delete mapping"""
    mapping_path = MAPPINGS_DIR / f"{mapping_id}.json"
    
    if mapping_path.exists():
        mapping_path.unlink()
        if mapping_id in mappings_cache:
            del mappings_cache[mapping_id]
        if audit_log:
            try:
                audit_log.log(AuditAction.PROJECT_DELETE, user=user if 'user' in dir() else None,
                              resource_type='mapping')
            except Exception: pass
        return {"success": True}
    
    raise HTTPException(404, "Mapping not found")


# ============================================================================
# EXECUTION ENDPOINTS
# ============================================================================

@app.post("/api/execute")
async def execute_mapping(request: ExecuteMappingRequest):
    """Execute mapping on input data"""
    try:
        # Load mapping
        if request.mapping_id not in mappings_cache:
            mapping_path = MAPPINGS_DIR / f"{request.mapping_id}.json"
            if not mapping_path.exists():
                raise HTTPException(404, "Mapping not found")
            mapping = MappingDefinition.load(str(mapping_path))
            mappings_cache[request.mapping_id] = mapping
        else:
            mapping = mappings_cache[request.mapping_id]
        
        # Execute
        engine = MappingEngine(mapping)
        output_data = engine.execute(request.input_data)
        
        if audit_log:
            try:
                audit_log.log(AuditAction.TRANSFORM, user=user if 'user' in dir() else None,
                              resource_type='transform')
            except Exception: pass
        return {
            "success": True,
            "output": output_data,
            "errors": engine.errors,
            "warnings": engine.warnings
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/execute/file")
async def execute_mapping_file(
    mapping_id: str,
    file: UploadFile = File(...)
):
    """Execute mapping on uploaded file"""
    try:
        # Read file
        content = await file.read()
        
        # Parse based on file type
        if file.filename.endswith('.json'):
            input_data = json.loads(content)
        elif file.filename.endswith('.xml'):
            # Parse XML to dict (simplified)
            import xml.etree.ElementTree as ET
            root = ET.fromstring(content)
            input_data = _xml_to_dict(root)
        else:
            raise HTTPException(400, "Unsupported file format")
        
        # Execute
        request = ExecuteMappingRequest(
            mapping_id=mapping_id,
            input_data=input_data
        )
        return await execute_mapping(request)
    
    except Exception as e:
        raise HTTPException(500, str(e))


def _xml_to_dict(element):
    """Convert XML element to dict"""
    result = {}
    
    # Add attributes
    if element.attrib:
        result['@attributes'] = element.attrib
    
    # Add text
    if element.text and element.text.strip():
        if len(element) == 0:
            return element.text.strip()
        result['#text'] = element.text.strip()
    
    # Add children
    for child in element:
        child_data = _xml_to_dict(child)
        tag = child.tag.split('}')[1] if '}' in child.tag else child.tag
        
        if tag in result:
            if not isinstance(result[tag], list):
                result[tag] = [result[tag]]
            result[tag].append(child_data)
        else:
            result[tag] = child_data
    
    return result


# ============================================================================
# IDOC ENDPOINTS
# ============================================================================

@app.post("/api/idoc/parse")
async def parse_idoc_file(
    file: UploadFile = File(...),
    definition_file: Optional[str] = None
):
    """Parse IDOC file"""
    try:
        content = await file.read()
        lines = content.decode('utf-8').splitlines()
        
        # Load definition if provided
        parser = IDOCParser()
        if definition_file:
            def_path = IDOC_DEFS_DIR / definition_file
            if def_path.exists():
                parser.load_definition(str(def_path))
        
        # Parse
        result = parser.parse_lines(lines)
        
        return {
            "success": True,
            "data": result,
            "schema": parser.generate_mapping_schema()
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/idoc/create-definition")
async def create_idoc_definition(
    file: UploadFile = File(...),
    idoc_type: str = "CUSTOM"
):
    """Create IDOC definition from sample file"""
    try:
        content = await file.read()
        
        # Save sample
        sample_path = IDOC_DEFS_DIR / f"sample_{file.filename}"
        with open(sample_path, 'wb') as f:
            f.write(content)
        
        # Create definition
        from idoc_parser import IDOCDefinitionBuilder
        builder = IDOCDefinitionBuilder()
        definition = builder.create_from_sample(str(sample_path), idoc_type)
        
        # Save definition
        def_path = IDOC_DEFS_DIR / f"{idoc_type}.json"
        definition.to_json(str(def_path))
        
        return {
            "success": True,
            "definition_id": idoc_type,
            "segments": len(definition.segments)
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


# ============================================================================
# MAIN
# ============================================================================

# =============================================================================
# DB CONNECTOR ENDPOINTS
# =============================================================================

@app.post("/api/dbconn/save")
async def save_db_connection(request: Request, user = Depends(get_current_user)):
    """Salva nuova connessione DB. Body: {name, db_type, connection_params}"""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    if billing_manager:
        ok, reason = billing_manager.check_limit(str(user["id"]), "db_connector")
        if not ok:
            raise HTTPException(402, reason)
    body = await request.json()
    name = body.get("name")
    db_type = body.get("db_type")
    params = body.get("connection_params", {})
    if not name or not db_type:
        raise HTTPException(400, "name e db_type richiesti")
    try:
        conn_id = db_connector_manager.save_connection(str(user["id"]), name, db_type, params)
        if audit_log:
            try:
                audit_log.log(AuditAction.PROJECT_SAVE, user=user,
                              resource_type="db_connection", resource_id=conn_id,
                              metadata={"name": name, "db_type": db_type})
            except Exception: pass
        return {"success": True, "connection_id": conn_id}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/dbconn/list")
async def list_db_connections(user = Depends(get_current_user)):
    if not db_connector_manager:
        return {"connections": []}
    try:
        conns = db_connector_manager.list_connections(str(user["id"]))
        return {"connections": conns}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/api/dbconn/{conn_id}")
async def delete_db_connection(conn_id: str, user = Depends(get_current_user)):
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    ok = db_connector_manager.delete_connection(conn_id, str(user["id"]))
    if not ok:
        raise HTTPException(404, "Connessione non trovata")
    return {"success": True}


@app.post("/api/dbconn/{conn_id}/test")
async def test_db_connection(conn_id: str, user = Depends(get_current_user)):
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    try:
        result = db_connector_manager.test_connection(conn_id, str(user["id"]))
        return result
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.get("/api/dbconn/{conn_id}/tables")
async def list_db_tables(conn_id: str, user = Depends(get_current_user)):
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    try:
        tables = db_connector_manager.list_tables(conn_id, str(user["id"]))
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/dbconn/{conn_id}/schema/{table_name}")
async def get_db_table_schema(conn_id: str, table_name: str, user = Depends(get_current_user)):
    """Ritorna schema tabella in formato Buddyliko (pronto per il mapper)."""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    try:
        schema = db_connector_manager.get_table_schema(conn_id, str(user["id"]), table_name)
        return schema
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/dbconn/{conn_id}/preview")
async def preview_db_table(conn_id: str, request: Request, user = Depends(get_current_user)):
    """Anteprima dati tabella (prime 50 righe)."""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    body = await request.json()
    table = body.get("table_name")
    limit = min(int(body.get("limit", 50)), 200)
    where = body.get("where_clause")
    if not table:
        raise HTTPException(400, "table_name richiesto")
    try:
        return db_connector_manager.preview_data(conn_id, str(user["id"]), table, limit, where)
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/dbconn/{conn_id}/execute-write")
async def execute_db_write(conn_id: str, request: Request, user = Depends(get_current_user)):
    """Scrivi righe trasformate nel DB di destinazione."""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    body = await request.json()
    table = body.get("table_name")
    rows = body.get("rows", [])
    mode = body.get("mode", "insert")
    pk_cols = body.get("pk_columns", [])
    if not table or not rows:
        raise HTTPException(400, "table_name e rows richiesti")
    try:
        result = db_connector_manager.execute_write(
            conn_id, str(user["id"]), table, rows, mode, pk_cols
        )
        if audit_log:
            try:
                audit_log.log(AuditAction.TRANSFORM, user=user,
                              resource_type="db_write",
                              metadata={"table": table, "rows": len(rows),
                                        "mode": mode, "inserted": result.get("inserted", 0)})
            except Exception: pass
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


# =============================================================================
# EDI ENDPOINTS (X12 / EDIFACT)
# =============================================================================

@app.post("/api/edi/parse")
async def parse_edi_file(
    file: UploadFile = File(...),
    user = Depends(get_current_user)
):
    """Parsa file EDI X12 o EDIFACT. Ritorna struttura navigabile + schema."""
    if not edi_available:
        raise HTTPException(400, "EDI Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_edi(content)
        schema = edi_to_schema(parsed, file.filename.split(".")[0])
        flat = edi_to_flat(parsed)
        if audit_log:
            try:
                audit_log.log(AuditAction.FILE_UPLOAD, user=user,
                              resource_type="edi",
                              file_name=file.filename,
                              metadata={"format": parsed.get("format"),
                                        "summary": parsed.get("_summary")})
            except Exception: pass
        return {
            "success": True,
            "format": parsed.get("format"),
            "summary": parsed.get("_summary"),
            "parsed": parsed,
            "schema": schema,
            "flat_records": flat[:10],   # Preview prime 10 righe
            "total_records": len(flat),
        }
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/edi/schema-from-file")
async def edi_schema_from_file(
    file: UploadFile = File(...),
    schema_name: str = Form(None),
    user = Depends(get_current_user)
):
    """Genera schema Buddyliko da file EDI — pronto per usare nel mapper."""
    if not edi_available:
        raise HTTPException(400, "EDI Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_edi(content)
        name = schema_name or file.filename.rsplit(".", 1)[0]
        schema = edi_to_schema(parsed, name)
        return schema
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/edi/build")
async def build_edi_endpoint(request: Request, user = Depends(get_current_user)):
    """Genera EDI X12 o EDIFACT da dict strutturato.
    Body: {format: 'X12'|'EDIFACT', transaction_type: '850', data: {...}}"""
    if not edi_available:
        raise HTTPException(400, "EDI Parser non disponibile")
    body = await request.json()
    fmt = body.get("format", "X12").upper()
    tx_type = body.get("transaction_type", "850")
    data = body.get("data", {})
    try:
        edi_content = build_edi(data, fmt=fmt, transaction_type=tx_type)
        return Response(
            content=edi_content,
            media_type="text/plain",
            headers={"Content-Disposition": f'attachment; filename="output.{fmt.lower()}.edi"'}
        )
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/edi/detect")
async def detect_edi_endpoint(file: UploadFile = File(...), user = Depends(get_current_user)):
    """Rileva formato EDI del file caricato."""
    content = (await file.read())[:512].decode("utf-8", errors="replace")
    fmt = detect_edi_format(content)
    return {"format": fmt, "filename": file.filename, "edi_available": edi_available}


# =============================================================================
# HL7 ENDPOINTS (v2 + FHIR)
# =============================================================================

@app.post("/api/hl7/parse")
async def parse_hl7_file(
    file: UploadFile = File(...),
    user = Depends(get_current_user)
):
    """Parsa file HL7 v2 o FHIR JSON/XML. Ritorna struttura + schema."""
    if not hl7_available:
        raise HTTPException(400, "HL7 Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_hl7(content)
        schema = to_buddyliko_schema_hl7(parsed, file.filename.split(".")[0])
        fmt = parsed.get("format", "")
        flat = hl7v2_to_flat(parsed) if fmt == "HL7v2" else fhir_to_flat(parsed)
        if audit_log:
            try:
                audit_log.log(AuditAction.FILE_UPLOAD, user=user,
                              resource_type="hl7",
                              file_name=file.filename,
                              metadata={"format": fmt})
            except Exception: pass
        return {
            "success": True,
            "format": fmt,
            "resource_type": parsed.get("resource_type", parsed.get("message_type", "")),
            "parsed": parsed,
            "schema": schema,
            "flat": flat,
        }
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/hl7/schema-from-file")
async def hl7_schema_from_file(
    file: UploadFile = File(...),
    schema_name: str = Form(None),
    user = Depends(get_current_user)
):
    """Genera schema Buddyliko da file HL7 — pronto per il mapper."""
    if not hl7_available:
        raise HTTPException(400, "HL7 Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_hl7(content)
        name = schema_name or file.filename.rsplit(".", 1)[0]
        schema = to_buddyliko_schema_hl7(parsed, name)
        return schema
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/hl7/build-v2")
async def build_hl7v2_endpoint(request: Request, user = Depends(get_current_user)):
    """Genera messaggio HL7 v2 da dict.
    Body: {message_type: 'ADT^A01', data: {...}}"""
    if not hl7_available:
        raise HTTPException(400, "HL7 Parser non disponibile")
    body = await request.json()
    msg_type = body.get("message_type", "ADT^A01")
    data = body.get("data", {})
    try:
        hl7_content = build_hl7v2(data, message_type=msg_type)
        return Response(
            content=hl7_content,
            media_type="text/plain",
            headers={"Content-Disposition": f'attachment; filename="output.hl7"'}
        )
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/hl7/detect")
async def detect_hl7_endpoint(file: UploadFile = File(...), user = Depends(get_current_user)):
    """Rileva formato HL7 del file caricato."""
    content = (await file.read())[:512].decode("utf-8", errors="replace")
    fmt = detect_hl7_format(content)
    return {"format": fmt, "filename": file.filename, "hl7_available": hl7_available}


# =============================================================================
# BILLING USAGE HISTORY & EXPORT
# =============================================================================

@app.get("/api/billing/usage/history")
async def get_usage_history(
    months: int = 6,
    user = Depends(get_current_user)
):
    """Storico usage mensile degli ultimi N mesi."""
    if not billing_manager:
        return {"history": []}
    from datetime import datetime, timezone, timedelta
    cur = billing_manager.conn.cursor(cursor_factory=billing_manager.RealDictCursor)
    # Genera lista mesi da oggi indietro
    now = datetime.now(timezone.utc)
    month_list = []
    for i in range(months):
        d = now.replace(day=1) - timedelta(days=i*28)
        month_list.append(d.strftime('%Y-%m'))

    cur.execute("""
        SELECT month, transforms_count, api_calls_count,
               bytes_processed, codegen_count
        FROM usage_counters
        WHERE user_id = %s AND month = ANY(%s)
        ORDER BY month DESC
    """, (str(user["id"]), month_list))
    rows = [dict(r) for r in cur.fetchall()]

    # Riempie i mesi senza dati con zero
    existing = {r['month'] for r in rows}
    for m in month_list:
        if m not in existing:
            rows.append({"month": m, "transforms_count": 0,
                         "api_calls_count": 0, "bytes_processed": 0, "codegen_count": 0})
    rows.sort(key=lambda r: r["month"], reverse=True)
    return {"history": rows, "user_id": str(user["id"])}


@app.get("/api/billing/usage/export")
async def export_usage_csv(
    months: int = 12,
    user = Depends(get_current_user)
):
    """Esporta usage CSV per l'utente corrente."""
    if not billing_manager:
        return Response("month,transforms,api_calls,bytes,codegen\n",
                        media_type="text/csv")
    from datetime import datetime, timezone, timedelta
    import io
    cur = billing_manager.conn.cursor(cursor_factory=billing_manager.RealDictCursor)
    now = datetime.now(timezone.utc)
    month_list = []
    for i in range(months):
        d = now.replace(day=1) - timedelta(days=i*28)
        month_list.append(d.strftime('%Y-%m'))

    cur.execute("""
        SELECT month, transforms_count, api_calls_count,
               bytes_processed, codegen_count
        FROM usage_counters
        WHERE user_id = %s AND month = ANY(%s)
        ORDER BY month DESC
    """, (str(user["id"]), month_list))
    rows = cur.fetchall()

    buf = io.StringIO()
    buf.write("month,transforms_count,api_calls_count,bytes_processed,codegen_count\n")
    existing = {r['month'] for r in rows}
    for row in rows:
        buf.write(f"{row['month']},{row['transforms_count']},"
                  f"{row['api_calls_count']},{row['bytes_processed']},"
                  f"{row['codegen_count']}\n")
    for m in sorted(month_list, reverse=True):
        if m not in existing:
            buf.write(f"{m},0,0,0,0\n")

    email = user.get("email", "user").split("@")[0]
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="usage_{email}_{now.strftime("%Y%m")}.csv"'}
    )


@app.get("/api/billing/admin/usage-all")
async def admin_usage_all(
    month: str = None,
    user = Depends(get_current_user)
):
    """Admin: usage di tutti gli utenti per un mese."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not billing_manager:
        return {"usage": []}
    from datetime import datetime, timezone
    target_month = month or datetime.now(timezone.utc).strftime('%Y-%m')
    cur = billing_manager.conn.cursor(cursor_factory=billing_manager.RealDictCursor)
    cur.execute("""
        SELECT u.user_id, us.email, us.name, us.plan,
               u.transforms_count, u.api_calls_count,
               u.bytes_processed, u.codegen_count
        FROM usage_counters u
        LEFT JOIN users us ON us.id::text = u.user_id
        WHERE u.month = %s
        ORDER BY u.transforms_count DESC
    """, (target_month,))
    rows = [dict(r) for r in cur.fetchall()]
    return {"month": target_month, "usage": rows, "total_users": len(rows)}



@app.post("/api/auth/change-password")
async def change_password(request: Request, user = Depends(get_current_user)):
    """
    Cambia o imposta la password dell'account.
    Se l'utente non ha password (solo OAuth), old_password può essere vuoto.
    """
    body = await request.json()
    old_password = body.get("old_password", "")
    new_password = body.get("new_password", "")

    if not new_password or len(new_password) < 8:
        raise HTTPException(400, "La nuova password deve essere di almeno 8 caratteri")

    raw_user = storage.get_user(str(user["id"])) if storage else None
    if not raw_user:
        raise HTTPException(404, "Utente non trovato")

    has_password = bool(raw_user.get("password_hash"))

    if has_password:
        # Verifica la vecchia password
        if not old_password:
            raise HTTPException(400, "Inserisci la password attuale")
        import hashlib
        old_hash = hashlib.sha256(old_password.encode()).hexdigest()
        # Try bcrypt first, then sha256
        pw_match = False
        stored_hash = raw_user.get("password_hash", "")
        if stored_hash.startswith("$2"):
            try:
                import bcrypt
                pw_match = bcrypt.checkpw(old_password.encode(), stored_hash.encode())
            except Exception:
                pw_match = (old_hash == stored_hash)
        else:
            pw_match = (old_hash == stored_hash)
        if not pw_match:
            raise HTTPException(400, "Password attuale non corretta")

    # Hash della nuova password
    try:
        import bcrypt as _bcrypt
        new_hash = _bcrypt.hashpw(new_password.encode(), _bcrypt.gensalt()).decode()
    except ImportError:
        import hashlib
        new_hash = hashlib.sha256(new_password.encode()).hexdigest()

    if storage:
        storage.update_user(str(user["id"]), {"password_hash": new_hash})

    if audit_log:
        try:
            audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                          resource_type="password",
                          metadata={"action": "change_password", "had_password": has_password})
        except Exception: pass

    return {"success": True, "message": "Password aggiornata con successo"}


# =============================================================================
# FINANCE & ANALYTICS ENDPOINTS
# =============================================================================

# ── ALERTS ───────────────────────────────────────────────────────────────────

@app.get("/api/finance/alerts/unread")
async def get_unread_alerts(user = Depends(get_current_user)):
    """Alert non letti per l'admin — per il badge nel header."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alerts_engine:
        return {"alerts": [], "count": 0}
    try:
        alerts = alerts_engine.get_unread_alerts(str(user["id"]))
        return {"alerts": alerts, "count": len(alerts),
                "has_critical": any(a["severity"] == "critical" for a in alerts)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/finance/alerts/history")
async def get_alert_history(
    days: int = 30,
    severity: str = None,
    user = Depends(get_current_user)
):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alerts_engine:
        return {"alerts": []}
    try:
        alerts = alerts_engine.get_alert_history(days=min(days, 365))
        if severity:
            alerts = [a for a in alerts if a.get('severity') == severity]
        return {"alerts": alerts, "total": len(alerts)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/finance/alerts/{alert_id}/read")
async def mark_alert_read(alert_id: str, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alerts_engine:
        raise HTTPException(503, "Alerts engine non disponibile")
    alerts_engine.mark_alert_read(alert_id, str(user["id"]))
    return {"success": True}


@app.post("/api/finance/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alerts_engine:
        raise HTTPException(503, "Alerts engine non disponibile")
    alerts_engine.resolve_alert(alert_id)
    return {"success": True}


@app.post("/api/finance/alerts/run-check")
async def run_alerts_check(user = Depends(get_current_user)):
    """Esegui manualmente il check degli alert (normalmente schedulato ogni ora)."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alerts_engine:
        raise HTTPException(503, "Alerts engine non disponibile")
    try:
        result = alerts_engine.run_alerts_check()
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


# ── AI CREDIT MONITOR ────────────────────────────────────────────────────────

@app.get("/api/finance/ai-credits")
async def get_ai_credits(user = Depends(get_current_user)):
    """Stato credito AI (Anthropic + OpenAI) con burn rate e previsione."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alerts_engine:
        return {"credits": []}
    try:
        credits = alerts_engine.get_ai_credit_status()
        return {"credits": credits}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/finance/ai-credits/refresh")
async def refresh_ai_credits(user = Depends(get_current_user)):
    """Forza polling immediato del credito AI."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alerts_engine:
        raise HTTPException(503, "Alerts engine non disponibile")
    try:
        alerts = alerts_engine.check_ai_credits()
        return {"success": True, "alerts_generated": len(alerts), "alerts": alerts}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── PROFITABILITY ─────────────────────────────────────────────────────────────

@app.get("/api/finance/profitability")
async def get_profitability(
    period_start: str = None,
    period_end: str = None,
    group_by: str = "user",
    user = Depends(get_current_user)
):
    """
    Report di redditività per periodo.
    group_by: 'user' | 'group' | 'plan'
    """
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not profitability:
        raise HTTPException(503, "Analytics non disponibile")
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    if not period_start:
        period_start = now.replace(day=1).strftime('%Y-%m-%d')
    if not period_end:
        period_end = now.strftime('%Y-%m-%d')
    # Validazione formato
    try:
        datetime.strptime(period_start, '%Y-%m-%d')
        datetime.strptime(period_end, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(400, "Formato date: YYYY-MM-DD")
    if group_by not in ('user', 'group', 'plan'):
        raise HTTPException(400, "group_by: 'user' | 'group' | 'plan'")
    try:
        report = profitability.get_profitability_report(period_start, period_end, group_by)
        return report
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/finance/mrr-trend")
async def get_mrr_trend(
    months: int = 12,
    user = Depends(get_current_user)
):
    """MRR mensile storico per grafico trend."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not profitability:
        raise HTTPException(503, "Analytics non disponibile")
    try:
        trend = profitability.get_mrr_trend(months=min(months, 36))
        return {"trend": trend}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/finance/churn")
async def get_churn(user = Depends(get_current_user)):
    """Analisi churn ultimi 90 giorni."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not profitability:
        raise HTTPException(503, "Analytics non disponibile")
    try:
        return profitability.get_churn_analysis()
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/finance/profitability/export")
async def export_profitability(
    period_start: str = None,
    period_end: str = None,
    group_by: str = "user",
    user = Depends(get_current_user)
):
    """Esporta report redditività come CSV."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not profitability:
        raise HTTPException(503, "Analytics non disponibile")
    import io
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    if not period_start:
        period_start = now.replace(day=1).strftime('%Y-%m-%d')
    if not period_end:
        period_end = now.strftime('%Y-%m-%d')
    try:
        report = profitability.get_profitability_report(period_start, period_end, group_by)
        buf = io.StringIO()
        rows = report.get('rows', [])
        if rows:
            headers = list(rows[0].keys())
            buf.write(','.join(headers) + '\n')
            for row in rows:
                vals = [str(row.get(h, '')) for h in headers]
                buf.write(','.join(vals) + '\n')
        # Totals
        buf.write('\n')
        totals = report.get('totals', {})
        for k, v in totals.items():
            buf.write(f"TOTAL_{k.upper()},{v}\n")
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="profitability_{period_start}_{period_end}.csv"'}
        )
    except Exception as e:
        raise HTTPException(500, str(e))


# ── PRICING CONFIG ────────────────────────────────────────────────────────────

@app.get("/api/finance/pricing")
async def get_pricing(user = Depends(get_current_user)):
    """Prezzi piani correnti + sconti attivi + utenti gratuiti."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_config_mgr:
        from stripe_billing import PLAN_PRICES_EUR, Plan
        return {"prices": {p.value: PLAN_PRICES_EUR.get(p, {}) for p in Plan},
                "discounts": [], "free_users": []}
    try:
        prices = pricing_config_mgr.get_current_prices()
        discounts = pricing_config_mgr.list_discounts(active_only=False)
        free_users = pricing_config_mgr.get_free_users()
        history = pricing_config_mgr.get_price_history()
        return {
            "prices": prices,
            "discounts": discounts,
            "free_users": free_users,
            "price_history": history,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.put("/api/finance/pricing/{plan}/{period}")
async def update_plan_price(
    plan: str, period: str,
    request: Request,
    user = Depends(get_current_user)
):
    """Aggiorna prezzo di un piano."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_config_mgr:
        raise HTTPException(503, "Pricing config non disponibile")
    body = await request.json()
    price = body.get("price_eur")
    note = body.get("note", "")
    if price is None or float(price) < 0:
        raise HTTPException(400, "price_eur richiesto e >= 0")
    if period not in ("monthly", "yearly"):
        raise HTTPException(400, "period: 'monthly' | 'yearly'")
    try:
        pid = pricing_config_mgr.update_price(
            plan.upper(), period, float(price),
            changed_by=user.get("email", "admin"), note=note
        )
        if audit_log:
            try:
                audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                              resource_type="pricing",
                              metadata={"plan": plan, "period": period, "price": price})
            except Exception: pass
        return {"success": True, "config_id": pid}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/finance/discounts")
async def create_discount(request: Request, user = Depends(get_current_user)):
    """Crea nuovo sconto o coupon."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_config_mgr:
        raise HTTPException(503, "Pricing config non disponibile")
    body = await request.json()
    required = ["name", "discount_type", "discount_value"]
    for f in required:
        if f not in body:
            raise HTTPException(400, f"Campo richiesto: {f}")
    try:
        did = pricing_config_mgr.create_discount(
            name=body["name"],
            discount_type=body["discount_type"],
            value=float(body["discount_value"]),
            applies_to=body.get("applies_to", "all"),
            target_plan=body.get("target_plan"),
            target_user_id=body.get("target_user_id"),
            target_group_id=body.get("target_group_id"),
            coupon_code=body.get("coupon_code", "").upper() or None,
            max_uses=body.get("max_uses"),
            valid_to=body.get("valid_to"),
            created_by=user.get("email", "admin"),
        )
        return {"success": True, "discount_id": did}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.delete("/api/finance/discounts/{discount_id}")
async def delete_discount(discount_id: str, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_config_mgr:
        raise HTTPException(503, "Pricing config non disponibile")
    ok = pricing_config_mgr.deactivate_discount(discount_id)
    if not ok:
        raise HTTPException(404, "Sconto non trovato")
    return {"success": True}


@app.post("/api/finance/discounts/validate")
async def validate_coupon(request: Request, user = Depends(get_current_user)):
    """Valida un coupon (chiamato dal checkout frontend)."""
    if not pricing_config_mgr:
        raise HTTPException(503, "Pricing config non disponibile")
    body = await request.json()
    code = body.get("coupon_code", "")
    plan = body.get("plan", "PRO")
    if not code:
        raise HTTPException(400, "coupon_code richiesto")
    valid, message, discount = pricing_config_mgr.validate_coupon(
        code, str(user.get("id", "")), plan
    )
    return {"valid": valid, "message": message, "discount_eur": discount}


# =============================================================================
# FINANCE & ANALYTICS ENDPOINTS
# =============================================================================

# ── ALERTS ───────────────────────────────────────────────────────────────────

@app.get("/api/finance/alerts/notifications")
async def get_notifications(unread_only: bool = False, limit: int = 50,
                             user = Depends(get_current_user)):
    """Notifiche in-app per admin dashboard."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alert_engine:
        return {"notifications": [], "unread_count": 0}
    notifs = alert_engine.get_notifications(limit=limit, unread_only=unread_only)
    unread = sum(1 for n in notifs if not n.get("read_at"))
    return {"notifications": notifs, "unread_count": unread}


@app.post("/api/finance/alerts/mark-read")
async def mark_notifications_read(request: Request, user = Depends(get_current_user)):
    """Marca notifiche come lette."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    body = await request.json()
    ids = body.get("ids", [])
    if alert_engine and ids:
        alert_engine.mark_read([int(i) for i in ids])
    return {"success": True}


@app.get("/api/finance/alerts/rules")
async def get_alert_rules(user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alert_engine:
        return {"rules": []}
    return {"rules": alert_engine.get_rules()}


@app.post("/api/finance/alerts/rules")
async def upsert_alert_rule(request: Request, user = Depends(get_current_user)):
    """Crea/aggiorna regola di alert."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alert_engine:
        raise HTTPException(400, "Alert engine non disponibile")
    body = await request.json()
    rule_type = body.get("rule_type")
    if not rule_type:
        raise HTTPException(400, "rule_type richiesto")
    alert_engine.upsert_rule(
        rule_type=rule_type,
        enabled=body.get("enabled", True),
        channels=body.get("channels", ["in_app"]),
        admin_emails=body.get("admin_emails", []),
        webhook_url=body.get("webhook_url"),
        threshold_pct=body.get("threshold_pct"),
    )
    return {"success": True, "rule_type": rule_type}


@app.post("/api/finance/alerts/run-checks")
async def run_alert_checks(user = Depends(get_current_user)):
    """Esegue manualmente tutti i check alert (normalmente schedulato)."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alert_engine:
        raise HTTPException(400, "Alert engine non disponibile")
    summary = alert_engine.run_checks()
    return {"success": True, "summary": summary}


@app.get("/api/finance/alerts/history")
async def get_alert_history(days: int = 30, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not alert_engine:
        return {"history": []}
    return {"history": alert_engine.get_alert_history(days=days)}


# ── AI BUDGET ─────────────────────────────────────────────────────────────────

@app.get("/api/finance/ai-budget/status")
async def get_ai_budget_status(user = Depends(get_current_user)):
    """Stato credito AI per ogni provider."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not ai_budget_monitor:
        return {"providers": [], "available": False}
    status = ai_budget_monitor.get_status()
    return {"providers": status, "available": True}


@app.get("/api/finance/ai-budget/history")
async def get_ai_budget_history(provider: str = None, days: int = 30,
                                 user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not ai_budget_monitor:
        return {"history": []}
    return {"history": ai_budget_monitor.get_history(provider=provider, days=days)}


@app.post("/api/finance/ai-budget/snapshot")
async def take_ai_budget_snapshot(user = Depends(get_current_user)):
    """Prende snapshot manuale del credito AI."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not ai_budget_monitor:
        raise HTTPException(400, "AI Budget Monitor non disponibile")
    result = ai_budget_monitor.take_snapshot()
    return {"success": True, "result": result}


# ── PROFITABILITY ─────────────────────────────────────────────────────────────

@app.get("/api/finance/profitability/summary")
async def get_profitability_summary(months: int = 3, user = Depends(get_current_user)):
    """KPI finanziari: MRR, ARR, ARPU, margine lordo, distribuzione piani."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not profitability_engine:
        return {"mrr_eur": 0, "available": False}
    try:
        data = profitability_engine.get_summary(months=months)
        data["available"] = True
        return data
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/finance/profitability/users")
async def get_user_profitability(months: int = 3, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not profitability_engine:
        return {"users": []}
    try:
        data = profitability_engine.get_user_profitability(months=months)
        return {"users": data, "months": months}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/finance/profitability/groups")
async def get_group_profitability(months: int = 3, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not profitability_engine:
        return {"groups": []}
    try:
        data = profitability_engine.get_group_profitability(months=months)
        return {"groups": data, "months": months}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── PRICING ───────────────────────────────────────────────────────────────────

@app.get("/api/finance/pricing/rules")
async def get_pricing_rules(user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_manager:
        return {"rules": []}
    return {"rules": pricing_manager.get_rules()}


@app.post("/api/finance/pricing/rules")
async def create_pricing_rule(request: Request, user = Depends(get_current_user)):
    """Crea regola di pricing (free override, custom price, trial)."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_manager:
        raise HTTPException(400, "Pricing manager non disponibile")
    body = await request.json()
    rule_type = body.get("rule_type")
    if not rule_type:
        raise HTTPException(400, "rule_type richiesto")

    expires_at = None
    if body.get("expires_at"):
        try:
            from datetime import datetime, timezone
            expires_at = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00"))
        except Exception:
            pass

    rule_id = pricing_manager.create_rule(
        rule_type=rule_type,
        user_id=body.get("user_id"),
        group_id=body.get("group_id"),
        plan=body.get("plan", "CUSTOM"),
        custom_price_eur=body.get("custom_price_eur"),
        note=body.get("note", ""),
        created_by=user.get("email", "admin"),
        expires_at=expires_at,
    )
    if audit_log:
        try:
            audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                          resource_type="pricing_rule", resource_id=rule_id,
                          metadata={"rule_type": rule_type, "target_user": body.get("user_id")})
        except Exception: pass
    return {"success": True, "rule_id": rule_id}


@app.delete("/api/finance/pricing/rules/{rule_id}")
async def delete_pricing_rule(rule_id: str, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_manager:
        raise HTTPException(400, "Pricing manager non disponibile")
    ok = pricing_manager.deactivate_rule(rule_id)
    if not ok:
        raise HTTPException(404, "Regola non trovata")
    return {"success": True}


@app.get("/api/finance/pricing/discounts")
async def get_discount_codes(user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_manager:
        return {"codes": []}
    return {"codes": pricing_manager.get_discount_codes()}


@app.post("/api/finance/pricing/discounts")
async def create_discount_code(request: Request, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_manager:
        raise HTTPException(400, "Pricing manager non disponibile")
    body = await request.json()
    code = body.get("code", "").strip().upper()
    if not code:
        raise HTTPException(400, "code richiesto")

    valid_until = None
    if body.get("valid_until"):
        try:
            from datetime import datetime, timezone
            valid_until = datetime.fromisoformat(body["valid_until"].replace("Z", "+00:00"))
        except Exception:
            pass

    code_id = pricing_manager.create_discount_code(
        code=code,
        description=body.get("description", ""),
        discount_type=body.get("discount_type", "percent"),
        discount_value=float(body.get("discount_value", 0)),
        applicable_plans=body.get("applicable_plans", []),
        max_uses=body.get("max_uses"),
        valid_until=valid_until,
        created_by=user.get("email", "admin"),
    )
    return {"success": True, "code_id": code_id, "code": code}


@app.post("/api/finance/pricing/discounts/validate")
async def validate_discount_code(request: Request):
    """Valida codice sconto (endpoint pubblico, usato nel checkout)."""
    if not pricing_manager:
        return {"valid": False, "message": "Codici sconto non disponibili"}
    body = await request.json()
    code = body.get("code", "").strip()
    plan = body.get("plan", "")
    valid, message, dc = pricing_manager.validate_code(code, plan)
    if valid and dc:
        base = {"PRO": 49.0, "ENTERPRISE": 299.0}.get(plan.upper(), 0.0)
        final, savings, desc = pricing_manager.calc_discounted_price(base, code, plan)
        return {
            "valid": True, "message": message,
            "discount_type": dc["discount_type"],
            "discount_value": float(dc["discount_value"]),
            "description": desc,
            "original_price": base,
            "final_price": final,
            "savings": savings,
        }
    return {"valid": False, "message": message}


@app.delete("/api/finance/pricing/discounts/{code_id}")
async def delete_discount_code(code_id: str, user = Depends(get_current_user)):
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not pricing_manager:
        raise HTTPException(400, "Pricing manager non disponibile")
    ok = pricing_manager.deactivate_code(code_id)
    if not ok:
        raise HTTPException(404, "Codice non trovato")
    return {"success": True}



# ── AI REAL USAGE STATS ──────────────────────────────────────────────────────

@app.get("/api/finance/ai-usage/stats")
async def get_ai_usage_stats(month: str = None, user = Depends(get_current_user)):
    """Statistiche reali di consumo AI basate sui token."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not ai_token_tracker:
        return {"available": False}
    stats = ai_token_tracker.get_month_stats(month=month)
    stats["today_spend"] = ai_token_tracker.get_today_spend()
    balance = ai_token_tracker.get_latest_balance("anthropic")
    if balance:
        stats["anthropic_balance"] = {
            "balance_usd": float(balance["balance_usd"]) if balance["balance_usd"] else None,
            "checked_at": balance["checked_at"].isoformat() if balance["checked_at"] else None,
            "auto_recharge": balance.get("auto_recharge", False),
            "source": balance.get("source", "unknown"),
        }
    return {"available": True, **stats}

@app.post("/api/finance/ai-balance/manual")
async def set_ai_balance_manual(body: dict, user = Depends(get_current_user)):
    """Inserisci saldo manuale Anthropic/OpenAI."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not ai_token_tracker:
        raise HTTPException(400, "Token tracker non disponibile")
    balance = body.get("balance_usd")
    if balance is None:
        raise HTTPException(400, "balance_usd richiesto")
    ai_token_tracker.save_balance(
        provider=body.get("provider", "anthropic"),
        balance_usd=float(balance),
        auto_recharge=body.get("auto_recharge", True),
        recharge_amount=body.get("recharge_amount", 15.0),
        recharge_threshold=body.get("recharge_threshold", 5.0),
        source="manual"
    )
    return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8080)

# ============================================================================
# SESSION ENDPOINTS
# ============================================================================

SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True, parents=True)

@app.get("/api/session/load")
async def load_session():
    """Load last session"""
    try:
        session_file = SESSIONS_DIR / "last_session.json"
        
        if not session_file.exists():
            return {
                "success": True,
                "session": None,
                "message": "No previous session found"
            }
        
        with open(session_file, 'r', encoding='utf-8') as f:
            session_data = json.load(f)
        
        return {
            "success": True,
            "session": session_data
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/session/save")
async def save_session(session_data: Dict[str, Any]):
    """Save current session"""
    try:
        session_file = SESSIONS_DIR / "last_session.json"
        
        # Add timestamp
        session_data['saved_at'] = datetime.now().isoformat()
        
        with open(session_file, 'w', encoding='utf-8') as f:
            json.dump(session_data, f, indent=2, ensure_ascii=False)
        
        if audit_log:
            try:
                audit_log.log(AuditAction.PROJECT_SAVE, user=user if 'user' in dir() else None,
                              resource_type='session')
            except Exception: pass
        return {
            "success": True,
            "message": "Session saved"
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/api/session")
async def clear_session():
    """Clear current session"""
    try:
        session_file = SESSIONS_DIR / "last_session.json"
        
        if session_file.exists():
            session_file.unlink()
        
        return {
            "success": True,
            "message": "Session cleared"
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


# ============================================================================
# CSV EXPORT ENDPOINT
# ============================================================================

@app.get("/api/mappings/{mapping_id}/export/csv")
async def export_mapping_csv(mapping_id: str):
    """Export mapping to CSV file"""
    try:
        # Load mapping
        if mapping_id not in mappings_cache:
            mapping_path = MAPPINGS_DIR / f"{mapping_id}.json"
            if not mapping_path.exists():
                raise HTTPException(404, "Mapping not found")
            mapping = MappingDefinition.load(str(mapping_path))
            mappings_cache[mapping_id] = mapping
        else:
            mapping = mappings_cache[mapping_id]
        
        # Get input/output schemas
        input_schema_id = mapping.input_schema
        output_schema_id = mapping.output_schema
        
        if input_schema_id not in schemas_cache or output_schema_id not in schemas_cache:
            raise HTTPException(400, "Input or output schema not found in cache")
        
        input_schema = schemas_cache[input_schema_id]
        output_schema = schemas_cache[output_schema_id]
        
        # Export to CSV
        import tempfile
        import os
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
        temp_path = temp_file.name
        temp_file.close()
        
        exporter = MappingCSVExporter(mapping.to_dict(), input_schema, output_schema)
        exporter.export_to_csv(temp_path)
        
        # Return file
        return FileResponse(
            temp_path,
            media_type='text/csv',
            filename=f"{mapping.name}_mapping.csv",
            background=None
        )
    
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/mappings/import/csv")
async def import_mapping_csv(
    file: UploadFile = File(...),
    name: str = "Imported Mapping"
):
    """Import mapping from CSV file"""
    try:
        content = await file.read()
        
        # Save to temp
        import tempfile
        temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
        temp_file.write(content)
        temp_path = temp_file.name
        temp_file.close()
        
        # Parse CSV
        import csv as csv_module
        rules = []
        
        with open(temp_path, 'r', encoding='utf-8-sig') as f:
            reader = csv_module.DictReader(f)
            
            for row in reader:
                if row.get('campo_input') and row.get('campo_output'):
                    rule = {
                        'id': f"rule_{len(rules)+1}",
                        'source': row['campo_input'],
                        'target': row['campo_output'],
                        'transformation': {
                            'type': 'direct'
                        },
                        'enabled': True
                    }
                    
                    # Parse transformation rule
                    trans_rule = row.get('regola_trasformazione', '').strip()
                    if trans_rule and trans_rule != 'direct':
                        # Parse transformation
                        if trans_rule.startswith('format_date'):
                            rule['transformation'] = {
                                'type': 'function',
                                'function': 'format_date',
                                'params': {}
                            }
                        elif trans_rule.startswith('concat'):
                            rule['transformation'] = {
                                'type': 'function',
                                'function': 'concat',
                                'params': {}
                            }
                        elif trans_rule.startswith('lookup'):
                            rule['transformation'] = {
                                'type': 'function',
                                'function': 'lookup',
                                'params': {}
                            }
                        else:
                            # Store as note
                            rule['note'] = trans_rule
                    
                    rules.append(rule)
        
        # Clean up
        os.unlink(temp_path)
        
        # Create mapping
        mapping = MappingDefinition(name)
        for rule_data in rules:
            mapping.add_rule(MappingRule(rule_data))
        
        # Save
        mapping_id = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        mapping_path = MAPPINGS_DIR / f"{mapping_id}.json"
        mapping.save(str(mapping_path))
        
        mappings_cache[mapping_id] = mapping
        
        return {
            "success": True,
            "mapping_id": mapping_id,
            "rules_count": len(rules)
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/csv/sample/{type}")
async def get_csv_sample(type: str):
    """Get sample CSV file (input or output)"""
    try:
        import tempfile
        from csv_parser import create_sample_input_csv, create_sample_output_csv
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
        temp_path = temp_file.name
        temp_file.close()
        
        if type == 'input':
            create_sample_input_csv(temp_path)
            filename = 'sample_input_schema.csv'
        elif type == 'output':
            create_sample_output_csv(temp_path)
            filename = 'sample_output_schema.csv'
        else:
            raise HTTPException(400, "Type must be 'input' or 'output'")
        
        return FileResponse(
            temp_path,
            media_type='text/csv',
            filename=filename
        )
    
    except Exception as e:
        raise HTTPException(500, str(e))


# ============================================================================
# AI AUTO-MAPPING ENDPOINT
# ============================================================================

def _ai_path(f: Dict) -> str:
    """Canonical path identifier for a field."""
    return (f.get("path") or f.get("id") or f.get("name") or "").strip()

def _ai_compact(f: Dict) -> Dict:
    """Minimal field representation sent to AI — saves tokens."""
    return {
        "path": _ai_path(f),
        "name": f.get("name", ""),
        "bt":   (f.get("business_term") or f.get("bt") or "").strip(),
        "desc": (f.get("description") or f.get("spiegazione") or "")[:100],
        "type": f.get("type", ""),
    }

def _ai_chunk_size(fields: List[Dict], budget: int = 1400) -> int:
    """How many fields fit in ~budget tokens."""
    avg = max(1, sum(len(json.dumps(_ai_compact(f))) for f in fields[:20]) // max(1, min(20, len(fields)))) // 4
    return max(5, budget // avg)

def _ai_chunks(fields: List[Dict], budget: int = 1400) -> List[List[Dict]]:
    size = _ai_chunk_size(fields, budget)
    return [fields[i:i+size] for i in range(0, len(fields), size)]

def _fuzzy_resolve(value: str, lookup: Dict[str, str]) -> Optional[str]:
    """
    Resolve a value Claude returned to an actual field path.
    lookup: {path -> path, name -> path, ...}
    Returns the canonical path or None.
    """
    if not value:
        return None
    v = value.strip()
    # 1. Exact
    if v in lookup:
        return lookup[v]
    # 2. Case-insensitive
    vl = v.lower()
    for k, canon in lookup.items():
        if k.lower() == vl:
            return canon
    # 3. Suffix match (Claude may return last part of path)
    for k, canon in lookup.items():
        if k.endswith(v) or v.endswith(k):
            return canon
    # 4. Contains
    for k, canon in lookup.items():
        if vl in k.lower() or k.lower() in vl:
            return canon
    return None

async def _ai_call_chunk(inp_chunk: List[Dict], out_chunk: List[Dict], sample: str) -> List[Dict]:
    """Call AI for one chunk pair. Returns raw suggestion dicts."""
    inp_c = [_ai_compact(f) for f in inp_chunk]
    out_c = [_ai_compact(f) for f in out_chunk]
    sample_snip = sample[:300] if sample else ""
    _data_sample_1 = ("DATA SAMPLE:\n" + sample_snip) if sample_snip else ""

    prompt = f"""You are a data mapping expert. Match input fields to output fields.

INPUT FIELDS:
{json.dumps(inp_c, indent=2)}

OUTPUT FIELDS:
{json.dumps(out_c, indent=2)}

{_data_sample_1}

Return ONLY a JSON array. For source_field use the "path" or "name" value from INPUT FIELDS. For target_field use the "path" or "name" value from OUTPUT FIELDS. Only include matches with confidence >= 0.5. Skip fields with no good match.

[
  {{
    "source_field": "path or name from input",
    "target_field": "path or name from output",
    "confidence": 0.9,
    "reasoning": "brief reason",
    "suggested_formula": null
  }}
]

Return ONLY valid JSON array, no markdown, no explanation."""

    import re as _re

    def _extract(text: str) -> List[Dict]:
        m = _re.search(r'\[[\s\S]*\]', text)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
        return []

    print(f"🤖 _ai_call_chunk: {len(inp_chunk)} inp x {len(out_chunk)} out | ANTHROPIC_KEY={'YES' if ANTHROPIC_API_KEY else 'NO'} OPENAI_KEY={'YES' if OPENAI_API_KEY else 'NO'}")
    print(f"📝 Prompt length: {len(prompt)} chars (~{len(prompt)//4} tokens)")

    if ANTHROPIC_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    'https://api.anthropic.com/v1/messages',
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': ANTHROPIC_API_KEY,
                        'anthropic-version': '2023-06-01'
                    },
                    json={
                        'model': 'claude-haiku-4-5-20251001',
                        'max_tokens': 2000,
                        'messages': [{'role': 'user', 'content': prompt}]
                    },
                    timeout=90.0
                )
                print(f"✅ Anthropic response: HTTP {r.status_code}")
                if r.status_code == 200:
                    _resp_json = r.json()
                    text = _resp_json['content'][0]['text']
                    # ── TOKEN TRACKING ──
                    if ai_token_tracker:
                        _usage = extract_anthropic_usage(_resp_json)
                        ai_token_tracker.track(
                            provider="anthropic",
                            model="claude-haiku-4-5-20251001",
                            operation="ai_automap",
                            input_tokens=_usage["input_tokens"],
                            output_tokens=_usage["output_tokens"],
                            http_status=200,
                        )
                    # ── FINE TRACKING ──
                    print(f"📄 Raw response (first 500): {text[:500]}")
                    result = _extract(text)
                    print(f"🔢 Extracted suggestions: {len(result)}")
                    if result:
                        return result
                else:
                    print(f"❌ Anthropic error body: {r.text[:300]}")
        except Exception as e:
            print(f"❌ Anthropic exception: {e}")

    if OPENAI_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    'https://api.openai.com/v1/chat/completions',
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {OPENAI_API_KEY}'
                    },
                    json={
                        'model': 'gpt-4-turbo-preview',
                        'messages': [
                            {'role': 'system', 'content': 'You are a data mapping expert. Respond with valid JSON only.'},
                            {'role': 'user', 'content': prompt}
                        ],
                        'temperature': 0.2,
                        'max_tokens': 2000
                    },
                    timeout=90.0
                )
                print(f"✅ OpenAI response: HTTP {r.status_code}")
                if r.status_code == 200:
                    text = r.json()['choices'][0]['message']['content']
                    print(f"📄 Raw response (first 500): {text[:500]}")
                    result = _extract(text)
                    print(f"🔢 Extracted suggestions: {len(result)}")
                    if result:
                        return result
                else:
                    print(f"❌ OpenAI error body: {r.text[:300]}")
        except Exception as e:
            print(f"❌ OpenAI exception: {e}")

    print("❌ _ai_call_chunk returning EMPTY")
    return []


@app.post("/api/ai/auto-map", response_model=List[AISuggestion])
async def ai_auto_map(request: AIAutoMapRequest):
    """
    AI-powered mapping. Chunked strategy with fuzzy path resolution.
    Phase 1: cross-product of input/output chunks.
    Phase 2: second pass on unmatched fields.
    """
    if not ANTHROPIC_API_KEY and not OPENAI_API_KEY:
        raise HTTPException(
            status_code=400,
            detail="No AI API keys configured. Add ANTHROPIC_API_KEY or OPENAI_API_KEY to .env file"
        )

    try:
        inp_fields = request.input_fields
        out_fields = request.output_fields
        sample     = (request.input_sample or "")[:400]

        # Build fuzzy lookup tables: any identifier -> canonical path
        inp_lookup: Dict[str, str] = {}
        for f in inp_fields:
            canon = _ai_path(f)
            for val in [canon, f.get("name",""), f.get("id",""), f.get("offset","")]:
                if val and val.strip():
                    inp_lookup[val.strip()] = canon

        out_lookup: Dict[str, str] = {}
        for f in out_fields:
            canon = _ai_path(f)
            for val in [canon, f.get("name",""), f.get("id",""), f.get("offset","")]:
                if val and val.strip():
                    out_lookup[val.strip()] = canon

        # best_match: canonical_target_path -> suggestion dict
        best_match: Dict[str, Dict] = {}

        print(f"🚀 AI automap start: {len(inp_fields)} input fields, {len(out_fields)} output fields")
        print(f"🔑 inp_lookup size: {len(inp_lookup)}, out_lookup size: {len(out_lookup)}")
        print(f"📋 Sample inp_lookup keys: {list(inp_lookup.keys())[:5]}")
        print(f"📋 Sample out_lookup keys: {list(out_lookup.keys())[:5]}")

        async def _process_chunks(i_list: List[Dict], o_list: List[Dict]):
            inp_chunks = _ai_chunks(i_list, 1200)
            out_chunks = _ai_chunks(o_list, 1200)
            print(f"📦 Chunks: {len(inp_chunks)} inp x {len(out_chunks)} out = {len(inp_chunks)*len(out_chunks)} calls")
            for i_chunk in inp_chunks:
                for o_chunk in out_chunks:
                    raw = await _ai_call_chunk(i_chunk, o_chunk, sample)
                    print(f"  → raw suggestions from chunk: {len(raw)}")
                    for s in raw:
                        conf = float(s.get("confidence", 0))
                        src_raw = s.get("source_field","")
                        tgt_raw = s.get("target_field","")
                        src_canon = _fuzzy_resolve(src_raw, inp_lookup)
                        tgt_canon = _fuzzy_resolve(tgt_raw, out_lookup)
                        print(f"    '{src_raw}' → '{tgt_raw}' | conf={conf} | resolved: {src_canon} → {tgt_canon}")
                        if conf < 0.5:
                            print(f"    ⚠️ SKIPPED: conf < 0.5")
                            continue
                        if not src_canon or not tgt_canon:
                            print(f"    ⚠️ SKIPPED: fuzzy resolve failed (src={src_canon}, tgt={tgt_canon})")
                            continue
                        if tgt_canon not in best_match or conf > best_match[tgt_canon]["confidence"]:
                            best_match[tgt_canon] = {
                                "source_field":      src_canon,
                                "target_field":      tgt_canon,
                                "confidence":        conf,
                                "reasoning":         s.get("reasoning", ""),
                                "suggested_formula": s.get("suggested_formula"),
                            }

        # Phase 1: all fields
        await _process_chunks(inp_fields, out_fields)
        print(f"✅ Phase 1 done: {len(best_match)} matches")

        # Phase 2: second pass on unmatched
        matched_src = {v["source_field"] for v in best_match.values()}
        matched_tgt = set(best_match.keys())
        unmatched_inp = [f for f in inp_fields if _ai_path(f) not in matched_src]
        unmatched_out = [f for f in out_fields if _ai_path(f) not in matched_tgt]
        print(f"🔄 Phase 2: {len(unmatched_inp)} unmatched inp, {len(unmatched_out)} unmatched out")
        if unmatched_inp and unmatched_out:
            await _process_chunks(unmatched_inp, unmatched_out)

        results = sorted(best_match.values(), key=lambda x: x["confidence"], reverse=True)
        print(f"🎯 FINAL RESULTS: {len(results)} suggestions")
        return results

    except Exception as e:
        raise HTTPException(500, f"AI error: {str(e)}")


# ===========================================================================
# AI DEBUG ENDPOINT
# ===========================================================================

@app.post("/api/ai/debug")
async def ai_debug(request: AIAutoMapRequest):
    """
    Debug endpoint: runs one single AI call with first 5 input + 5 output fields,
    returns full prompt, raw response, extracted suggestions, and fuzzy resolution.
    Call from browser: POST /api/ai/debug with same body as /api/ai/auto-map
    """
    inp5 = request.input_fields[:5]
    out5 = request.output_fields[:5]
    sample = (request.input_sample or "")[:300]

    inp_c = [_ai_compact(f) for f in inp5]
    out_c = [_ai_compact(f) for f in out5]

    prompt = f"""You are a data mapping expert. Match input fields to output fields.

INPUT FIELDS:
{json.dumps(inp_c, indent=2)}

OUTPUT FIELDS:
{json.dumps(out_c, indent=2)}

Return ONLY a JSON array. Use "path" or "name" values from the lists above.
[
  {{
    "source_field": "path or name from input",
    "target_field": "path or name from output",
    "confidence": 0.9,
    "reasoning": "brief reason",
    "suggested_formula": null
  }}
]
Return ONLY valid JSON array, no markdown."""

    result = {
        "config": {
            "ANTHROPIC_KEY_SET": bool(ANTHROPIC_API_KEY),
            "OPENAI_KEY_SET": bool(OPENAI_API_KEY),
            "inp_fields_received": len(request.input_fields),
            "out_fields_received": len(request.output_fields),
        },
        "compact_input": inp_c,
        "compact_output": out_c,
        "prompt": prompt,
        "prompt_chars": len(prompt),
        "prompt_tokens_est": len(prompt) // 4,
        "http_status": None,
        "raw_response": None,
        "http_error": None,
        "extracted": [],
        "fuzzy_resolved": [],
    }

    import re as _re

    if ANTHROPIC_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    'https://api.anthropic.com/v1/messages',
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': ANTHROPIC_API_KEY,
                        'anthropic-version': '2023-06-01'
                    },
                    json={
                        'model': 'claude-haiku-4-5-20251001',
                        'max_tokens': 1000,
                        'messages': [{'role': 'user', 'content': prompt}]
                    },
                    timeout=30.0
                )
                result["http_status"] = r.status_code
                if r.status_code == 200:
                    _resp_json2 = r.json()
                    text = _resp_json2['content'][0]['text']
                    result["raw_response"] = text
                    # ── TOKEN TRACKING ──
                    if ai_token_tracker:
                        _usage2 = extract_anthropic_usage(_resp_json2)
                        ai_token_tracker.track(
                            provider="anthropic",
                            model="claude-haiku-4-5-20251001",
                            operation="ai_debug",
                            input_tokens=_usage2["input_tokens"],
                            output_tokens=_usage2["output_tokens"],
                            http_status=200,
                        )
                    # ── FINE TRACKING ──
                    m = _re.search(r'\[[\s\S]*\]', text)
                    if m:
                        try:
                            result["extracted"] = json.loads(m.group(0))
                        except Exception as e:
                            result["parse_error"] = str(e)
                            result["matched_text"] = m.group(0)
                else:
                    result["http_error"] = r.text
        except Exception as e:
            result["exception"] = str(e)
    elif OPENAI_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    'https://api.openai.com/v1/chat/completions',
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {OPENAI_API_KEY}'
                    },
                    json={
                        'model': 'gpt-4-turbo-preview',
                        'messages': [
                            {'role': 'system', 'content': 'Respond with valid JSON only.'},
                            {'role': 'user', 'content': prompt}
                        ],
                        'temperature': 0.2,
                        'max_tokens': 1000
                    },
                    timeout=30.0
                )
                result["http_status"] = r.status_code
                if r.status_code == 200:
                    text = r.json()['choices'][0]['message']['content']
                    result["raw_response"] = text
                    m = _re.search(r'\[[\s\S]*\]', text)
                    if m:
                        try:
                            result["extracted"] = json.loads(m.group(0))
                        except Exception as e:
                            result["parse_error"] = str(e)
                else:
                    result["http_error"] = r.text
        except Exception as e:
            result["exception"] = str(e)

    # Show fuzzy resolution for extracted suggestions
    inp_lookup = {}
    for f in inp5:
        canon = _ai_path(f)
        for val in [canon, f.get("name",""), f.get("id","")]:
            if val and val.strip():
                inp_lookup[val.strip()] = canon

    out_lookup = {}
    for f in out5:
        canon = _ai_path(f)
        for val in [canon, f.get("name",""), f.get("id","")]:
            if val and val.strip():
                out_lookup[val.strip()] = canon

    result["inp_lookup"] = inp_lookup
    result["out_lookup"] = out_lookup

    for s in result["extracted"]:
        src_r = s.get("source_field","")
        tgt_r = s.get("target_field","")
        src_c = _fuzzy_resolve(src_r, inp_lookup)
        tgt_c = _fuzzy_resolve(tgt_r, out_lookup)
        result["fuzzy_resolved"].append({
            "source_raw": src_r, "source_resolved": src_c,
            "target_raw": tgt_r, "target_resolved": tgt_c,
            "confidence": s.get("confidence"),
            "would_be_kept": bool(src_c and tgt_c and s.get("confidence",0) >= 0.5)
        })

    return result


# ===========================================================================
# AI AUTO-MAP STREAMING (Server-Sent Events) — "AI at Work" live log
# ===========================================================================

from fastapi.responses import StreamingResponse as _StreamingResponse
import asyncio as _asyncio

@app.post("/api/ai/auto-map-stream")
async def ai_auto_map_stream(request: AIAutoMapRequest):
    """
    Same logic as /api/ai/auto-map but streams progress as Server-Sent Events.
    Each event is a JSON line: { "type": "log"|"chunk_done"|"done"|"error", ... }
    """
    if not ANTHROPIC_API_KEY and not OPENAI_API_KEY:
        async def _err():
            yield 'data: ' + json.dumps({"type":"error","msg":"No API keys configured"}) + '\n\n'
        if audit_log:
            try:
                audit_log.log(AuditAction.CODE_GENERATE, user=user if 'user' in dir() else None,
                              resource_type='ai_automap')
            except Exception: pass
        return _StreamingResponse(_err(), media_type="text/event-stream")

    inp_fields = request.input_fields
    out_fields = request.output_fields
    sample     = (request.input_sample or "")[:400]

    async def _generate():
        try:
            def _ev(obj): return 'data: ' + json.dumps(obj) + '\n\n'

            yield _ev({"type":"log","level":"info","msg":f"🚀 Starting AI automap: {len(inp_fields)} input × {len(out_fields)} output fields"})
            yield _ev({"type":"log","level":"info","msg":f"🔑 ANTHROPIC={'✅' if ANTHROPIC_API_KEY else '❌'}  OPENAI={'✅' if OPENAI_API_KEY else '❌'}"})

            # Build lookup tables
            inp_lookup: Dict[str, str] = {}
            for f in inp_fields:
                canon = _ai_path(f)
                for val in [canon, f.get("name",""), f.get("id",""), f.get("offset","")]:
                    if val and val.strip():
                        inp_lookup[val.strip()] = canon
            out_lookup: Dict[str, str] = {}
            for f in out_fields:
                canon = _ai_path(f)
                for val in [canon, f.get("name",""), f.get("id",""), f.get("offset","")]:
                    if val and val.strip():
                        out_lookup[val.strip()] = canon

            inp_chunks = _ai_chunks(inp_fields, 1200)
            out_chunks = _ai_chunks(out_fields, 1200)
            total_calls = len(inp_chunks) * len(out_chunks)
            yield _ev({"type":"log","level":"info","msg":f"📦 Phase 1: {len(inp_chunks)} inp chunks × {len(out_chunks)} out chunks = {total_calls} calls"})

            best_match: Dict[str, Dict] = {}
            call_n = 0

            async def _run_chunk(i_chunk, o_chunk, phase):
                nonlocal call_n
                call_n += 1
                inp_c = [_ai_compact(f) for f in i_chunk]
                out_c = [_ai_compact(f) for f in o_chunk]
                _sample_section = ("DATA SAMPLE:\n" + sample[:300]) if sample else ""
                prompt_text = (
                    "You are a data mapping expert. Match input fields to output fields.\n\n"
                    "INPUT FIELDS:\n" + json.dumps(inp_c, indent=2) + "\n\n"
                    "OUTPUT FIELDS:\n" + json.dumps(out_c, indent=2) + "\n\n" +
                    _sample_section + "\n\n"
                    "Return ONLY a JSON array. Use EXACT path or name values from the lists above. "
                    "Only include matches confidence>=0.5.\n"
                    '[{"source_field":"...","target_field":"...","confidence":0.9,"reasoning":"...","suggested_formula":null}]\n'
                    "Return ONLY valid JSON array, no markdown."
                )
                prompt_tokens = len(prompt_text) // 4
                yield _ev({"type":"log","level":"call",
                    "msg":f"🤖 [{phase}] Call {call_n}/{total_calls}: {len(i_chunk)} inp × {len(o_chunk)} out | ~{prompt_tokens} tokens",
                    "call": call_n, "inp_count": len(i_chunk), "out_count": len(o_chunk),
                    "inp_sample": [_ai_compact(f)["path"] for f in i_chunk[:3]],
                    "out_sample": [_ai_compact(f)["path"] for f in o_chunk[:3]],
                })

                raw = await _ai_call_chunk(i_chunk, o_chunk, sample)

                valid_inp = {_ai_compact(f)["path"] for f in i_chunk}
                valid_out = {_ai_compact(f)["path"] for f in o_chunk}

                accepted, skipped = [], []
                for s in raw:
                    conf = float(s.get("confidence", 0))
                    src_canon = _fuzzy_resolve(s.get("source_field",""), inp_lookup)
                    tgt_canon = _fuzzy_resolve(s.get("target_field",""), out_lookup)
                    if not src_canon or not tgt_canon or conf < 0.5:
                        skipped.append({"src": s.get("source_field"), "tgt": s.get("target_field"), "reason": "no_resolve" if not src_canon or not tgt_canon else "low_conf"})
                        continue
                    if tgt_canon not in best_match or conf > best_match[tgt_canon]["confidence"]:
                        best_match[tgt_canon] = {"source_field": src_canon, "target_field": tgt_canon,
                            "confidence": conf, "reasoning": s.get("reasoning",""), "suggested_formula": s.get("suggested_formula")}
                    accepted.append({"src": src_canon, "tgt": tgt_canon, "conf": conf, "reason": s.get("reasoning","")[:60]})

                yield _ev({"type":"chunk_done","call": call_n,
                    "msg": f"  ✅ {len(accepted)} accepted, {len(skipped)} skipped → total matches so far: {len(best_match)}",
                    "accepted": accepted, "skipped": skipped})

            for i_chunk in inp_chunks:
                for o_chunk in out_chunks:
                    async for ev in _run_chunk(i_chunk, o_chunk, "P1"):
                        yield ev

            yield _ev({"type":"log","level":"info","msg":f"✅ Phase 1 done: {len(best_match)} matches"})

            # Phase 2
            matched_src = {v["source_field"] for v in best_match.values()}
            matched_tgt = set(best_match.keys())
            unmatched_inp = [f for f in inp_fields if _ai_path(f) not in matched_src]
            unmatched_out = [f for f in out_fields if _ai_path(f) not in matched_tgt]

            if unmatched_inp and unmatched_out:
                u_inp_chunks = _ai_chunks(unmatched_inp, 1200)
                u_out_chunks = _ai_chunks(unmatched_out, 1200)
                p2_calls = len(u_inp_chunks) * len(u_out_chunks)
                total_calls += p2_calls
                yield _ev({"type":"log","level":"info","msg":f"🔄 Phase 2: {len(unmatched_inp)} unmatched inp, {len(unmatched_out)} unmatched out → {p2_calls} calls"})
                for i_chunk in u_inp_chunks:
                    for o_chunk in u_out_chunks:
                        async for ev in _run_chunk(i_chunk, o_chunk, "P2"):
                            yield ev
                yield _ev({"type":"log","level":"info","msg":f"✅ Phase 2 done: {len(best_match)} total matches"})
            else:
                yield _ev({"type":"log","level":"info","msg":f"⏭️ Phase 2 skipped (all fields matched or no output left)"})

            results = sorted(best_match.values(), key=lambda x: x["confidence"], reverse=True)
            green  = sum(1 for r in results if r["confidence"] >= 0.75)
            yellow = sum(1 for r in results if 0.5 <= r["confidence"] < 0.75)
            yield _ev({"type":"done","suggestions": results,
                "msg": f"🎯 Done: {len(results)} suggestions ({green} 🟢 confident, {yellow} 🟡 uncertain)",
                "green": green, "yellow": yellow})

        except Exception as e:
            yield 'data: ' + json.dumps({"type":"error","msg": str(e)}) + '\n\n'

    return _StreamingResponse(_generate(), media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


# ===========================================================================
# MAPPING DIAGRAM EXPORT (SVG)
# ===========================================================================

@app.post("/api/mapping/diagram")
async def export_mapping_diagram(request: Dict[str, Any]):
    """Generate an SVG diagram of the current mapping connections."""
    try:
        connections = request.get("connections", [])
        project_name = request.get("projectName", "Mapping")
        inp_name = request.get("inputSchemaName", "Input")
        out_name = request.get("outputSchemaName", "Output")

        if not connections:
            raise HTTPException(400, "No connections to diagram")

        # Build simple SVG
        row_h = 28
        pad = 20
        col_w = 220
        gap = 140
        n = len(connections)
        height = pad * 2 + row_h * (n + 1) + 40
        width = col_w * 2 + gap + pad * 2

        lines = []
        lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" font-family="monospace" font-size="12">')
        lines.append(f'<rect width="{width}" height="{height}" fill="#f8f9fa"/>')
        # Title
        lines.append(f'<text x="{width//2}" y="20" text-anchor="middle" font-size="14" font-weight="bold" fill="#333">{project_name}</text>')
        # Column headers
        lines.append(f'<text x="{pad+col_w//2}" y="42" text-anchor="middle" font-weight="bold" fill="#00796B">{inp_name}</text>')
        lines.append(f'<text x="{pad+col_w+gap+col_w//2}" y="42" text-anchor="middle" font-weight="bold" fill="#1565C0">{out_name}</text>')

        colors = {"0.9": "#2e7d32", "0.8": "#388e3c", "0.7": "#f57c00", "0.6": "#e65100", "0.5": "#c62828"}

        for i, conn in enumerate(connections):
            y = 55 + i * row_h
            src = conn.get("sourceName") or conn.get("source", "")
            tgt = conn.get("targetName") or conn.get("target", "")
            conf = conn.get("confidence", 1.0)
            color = "#1565C0" if conf >= 0.75 else "#e65100" if conf >= 0.5 else "#888"

            # Source box
            lines.append(f'<rect x="{pad}" y="{y-14}" width="{col_w}" height="20" rx="3" fill="#E0F2F1" stroke="#00796B" stroke-width="1"/>')
            lines.append(f'<text x="{pad+6}" y="{y}" fill="#00796B">{src[:28]}</text>')
            # Arrow
            x1 = pad + col_w
            x2 = pad + col_w + gap
            lines.append(f'<line x1="{x1}" y1="{y-4}" x2="{x2}" y2="{y-4}" stroke="{color}" stroke-width="1.5" marker-end="url(#arr)"/>')
            # Target box
            lines.append(f'<rect x="{x2}" y="{y-14}" width="{col_w}" height="20" rx="3" fill="#E3F2FD" stroke="#1565C0" stroke-width="1"/>')
            lines.append(f'<text x="{x2+6}" y="{y}" fill="#1565C0">{tgt[:28]}</text>')

        # Arrowhead marker
        lines.append('<defs><marker id="arr" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L0,6 L8,3 z" fill="#888"/></marker></defs>')
        lines.append('</svg>')

        svg = '\n'.join(lines)
        return Response(content=svg, media_type="image/svg+xml",
            headers={"Content-Disposition": "attachment; filename=mapping_diagram.svg"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ===========================================================================
# REVERSE MAPPING
# ===========================================================================

from reverse_mapper import MappingReverser

class ReverseRequest(BaseModel):
    project: Dict[str, Any]

class ReverseResponse(BaseModel):
    reversed_project: Dict[str, Any]
    report: Dict[str, Any]

@app.post("/api/mapping/reverse", response_model=ReverseResponse)
async def reverse_mapping(request: ReverseRequest):
    """
    Reverse mapping: input↔output swap with transformation inversion
    
    Handles:
    - Schema swap (input becomes output, vice versa)
    - Connection reversal (source→target becomes target→source)
    - Transformation inversion (CONCAT→SPLIT, *→/, +→-, etc.)
    - Warns about non-invertible transformations
    """
    try:
        reverser = MappingReverser()
        reversed_project = reverser.reverse_mapping(request.project)
        report = reverser.get_report()
        
        return ReverseResponse(
            reversed_project=reversed_project,
            report=report
        )
    
    except Exception as e:
        raise HTTPException(500, f"Reverse error: {str(e)}")


# ===========================================================================
# PREVIEW EXTRACTION - XML/JSON Support
# ===========================================================================

from preview_extractor import extract_preview_value

class PreviewRequest(BaseModel):
    example_content: str
    field_path: str
    field_name: str
    format_type: str  # 'xml', 'json', or 'flat'

class PreviewResponse(BaseModel):
    value: Optional[str]
    context_lines: List[str]
    highlight_line: int
    element_xml: Optional[str] = None
    error: Optional[str] = None

@app.post("/api/preview/extract", response_model=PreviewResponse)
async def extract_preview(request: PreviewRequest):
    """
    Extract preview value from XML/JSON example file
    
    Supports:
    - XML with XPath
    - JSON with JSONPath
    - Flat files with offset/length (existing IDOC support)
    """
    try:
        if request.format_type in ['xml', 'json']:
            result = extract_preview_value(
                request.example_content,
                request.field_path,
                request.field_name,
                request.format_type
            )
            
            return PreviewResponse(
                value=result.get('value'),
                context_lines=result.get('context_lines', []),
                highlight_line=result.get('highlight_line', -1),
                element_xml=result.get('element_xml'),
                error=result.get('error')
            )
        else:
            # For flat files, return empty (handled by frontend IDOC logic)
            return PreviewResponse(
                value=None,
                context_lines=[],
                highlight_line=-1,
                error="Flat file format - use frontend IDOC logic"
            )
    
    except Exception as e:
        raise HTTPException(500, f"Preview extraction error: {str(e)}")


# ===========================================================================
# SCHEMA EDITOR - Visual Structure Builder
# ===========================================================================

from schema_editor import SchemaEditor, SchemaField

# In-memory schema editors (in production, use database)
schema_editors = {}

class CreateSchemaRequest(BaseModel):
    name: str
    format: str  # csv, xml, json, excel, flat

class AddFieldRequest(BaseModel):
    name: str
    field_type: str  # string, number, date, boolean, array, object
    parent_path: Optional[str] = None
    description: str = ""
    required: bool = False
    cardinality: str = "0..1"
    default_value: str = ""
    
class UpdateFieldRequest(BaseModel):
    field_id: str
    updates: Dict[str, Any]

class MoveFieldRequest(BaseModel):
    field_id: str
    new_parent_id: Optional[str]

class ReorderFieldsRequest(BaseModel):
    parent_id: Optional[str]
    new_order: List[str]

class ImportSchemaRequest(BaseModel):
    format: str  # json_schema, csv_header
    content: str

@app.post("/api/schema/create")
async def create_schema(request: CreateSchemaRequest):
    """Create new schema"""
    try:
        editor = SchemaEditor()
        schema = editor.create_schema(request.name, request.format)
        
        # Store editor
        schema_id = f"schema_{len(schema_editors)}"
        schema_editors[schema_id] = editor
        
        if audit_log:
            try:
                audit_log.log(AuditAction.SCHEMA_CREATE, user=user if 'user' in dir() else None,
                              resource_type='schema')
            except Exception: pass
        return {
            "schema_id": schema_id,
            "schema": schema
        }
    except Exception as e:
        raise HTTPException(500, f"Create schema error: {str(e)}")

@app.post("/api/schema/{schema_id}/field/add")
async def add_field(schema_id: str, request: AddFieldRequest):
    """Add field to schema"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        field = editor.add_field(
            name=request.name,
            field_type=request.field_type,
            parent_path=request.parent_path,
            description=request.description,
            required=request.required,
            cardinality=request.cardinality,
            default_value=request.default_value
        )
        
        return {
            "field": field,
            "schema": editor.schema
        }
    except Exception as e:
        raise HTTPException(500, f"Add field error: {str(e)}")

@app.delete("/api/schema/{schema_id}/field/{field_id}")
async def remove_field(schema_id: str, field_id: str):
    """Remove field from schema"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        success = editor.remove_field(field_id)
        
        if not success:
            raise HTTPException(404, "Field not found")
        
        return {
            "success": True,
            "schema": editor.schema
        }
    except Exception as e:
        raise HTTPException(500, f"Remove field error: {str(e)}")

@app.put("/api/schema/{schema_id}/field/update")
async def update_field(schema_id: str, request: UpdateFieldRequest):
    """Update field properties"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        success = editor.update_field(request.field_id, **request.updates)
        
        if not success:
            raise HTTPException(404, "Field not found")
        
        return {
            "success": True,
            "schema": editor.schema
        }
    except Exception as e:
        raise HTTPException(500, f"Update field error: {str(e)}")

@app.post("/api/schema/{schema_id}/field/move")
async def move_field(schema_id: str, request: MoveFieldRequest):
    """Move field to new parent"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        success = editor.move_field(request.field_id, request.new_parent_id)
        
        if not success:
            raise HTTPException(404, "Field not found")
        
        return {
            "success": True,
            "schema": editor.schema
        }
    except Exception as e:
        raise HTTPException(500, f"Move field error: {str(e)}")

@app.post("/api/schema/{schema_id}/field/reorder")
async def reorder_fields(schema_id: str, request: ReorderFieldsRequest):
    """Reorder fields"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        success = editor.reorder_fields(request.parent_id, request.new_order)
        
        if not success:
            raise HTTPException(404, "Parent not found")
        
        return {
            "success": True,
            "schema": editor.schema
        }
    except Exception as e:
        raise HTTPException(500, f"Reorder fields error: {str(e)}")

@app.get("/api/schema/{schema_id}")
async def get_schema(schema_id: str):
    """Get schema details"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        
        return {
            "schema": editor.schema,
            "tree": editor.get_tree_structure()
        }
    except Exception as e:
        raise HTTPException(500, f"Get schema error: {str(e)}")

@app.get("/api/schema/{schema_id}/export/csv")
async def export_schema_csv(schema_id: str):
    """Export schema as CSV definition"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        csv_content = editor.export_to_csv_schema()
        
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={editor.schema['name']}.csv"}
        )
    except Exception as e:
        raise HTTPException(500, f"Export CSV error: {str(e)}")

@app.get("/api/schema/{schema_id}/export/sample/{format}")
async def export_sample_file(schema_id: str, format: str):
    """Export sample file (XML/JSON/CSV)"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        
        if format == 'xml':
            content = editor.export_sample_xml()
            media_type = "application/xml"
        elif format == 'json':
            content = editor.export_sample_json()
            media_type = "application/json"
        elif format == 'csv':
            content = editor.export_sample_csv()
            media_type = "text/csv"
        else:
            raise HTTPException(400, f"Unsupported format: {format}")
        
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename=sample.{format}"}
        )
    except Exception as e:
        raise HTTPException(500, f"Export sample error: {str(e)}")

@app.post("/api/schema/{schema_id}/import")
async def import_schema(schema_id: str, request: ImportSchemaRequest):
    """Import schema from JSON Schema or CSV header"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        
        if request.format == 'json_schema':
            import json
            json_schema = json.loads(request.content)
            editor.import_from_json_schema(json_schema)
        elif request.format == 'csv_header':
            editor.import_from_sample_csv(request.content)
        else:
            raise HTTPException(400, f"Unsupported import format: {request.format}")
        
        if audit_log:
            try:
                audit_log.log(AuditAction.SCHEMA_IMPORT, user=user if 'user' in dir() else None,
                              resource_type='schema')
            except Exception: pass
        return {
            "success": True,
            "schema": editor.schema
        }
    except Exception as e:
        raise HTTPException(500, f"Import schema error: {str(e)}")

@app.get("/api/schema/{schema_id}/validate")
async def validate_schema(schema_id: str):
    """Validate schema completeness"""
    try:
        if schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[schema_id]
        errors = editor.validate_schema()
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    except Exception as e:
        raise HTTPException(500, f"Validate schema error: {str(e)}")


# ===========================================================================
# SCHEMA STORAGE - Save/List/Load schemas
# ===========================================================================

# In-memory schema storage (in production, use database)
stored_schemas = {}

class SaveSchemaRequest(BaseModel):
    schema_id: str
    name: str
    description: str = ""

@app.post("/api/schema/save")
async def save_schema(request: SaveSchemaRequest):
    """Save schema for later use"""
    try:
        if request.schema_id not in schema_editors:
            raise HTTPException(404, "Schema not found")
        
        editor = schema_editors[request.schema_id]
        
        # Store schema with metadata
        stored_id = f"stored_{len(stored_schemas)}"
        stored_schemas[stored_id] = {
            "id": stored_id,
            "name": request.name,
            "description": request.description,
            "schema": editor.schema,
            "created": datetime.now().isoformat()
        }
        
        if audit_log:
            try:
                audit_log.log(AuditAction.SCHEMA_CREATE, user=user if 'user' in dir() else None,
                              resource_type='schema')
            except Exception: pass
        return {
            "stored_id": stored_id,
            "success": True
        }
    except Exception as e:
        raise HTTPException(500, f"Save schema error: {str(e)}")

@app.get("/api/schema/list")
async def list_schemas():
    """List all saved schemas"""
    try:
        schemas = [
            {
                "id": s["id"],
                "name": s["name"],
                "description": s["description"],
                "format": s["schema"]["format"],
                "field_count": s["schema"]["field_count"],
                "created": s["created"]
            }
            for s in stored_schemas.values()
        ]
        return {"schemas": schemas}
    except Exception as e:
        raise HTTPException(500, f"List schemas error: {str(e)}")

@app.get("/api/schema/load/{stored_id}")
async def load_stored_schema(stored_id: str):
    """Load a saved schema"""
    try:
        if stored_id not in stored_schemas:
            raise HTTPException(404, "Stored schema not found")
        
        stored = stored_schemas[stored_id]
        
        # Create new editor with stored schema
        editor = SchemaEditor()
        editor.schema = stored["schema"]
        
        schema_id = f"schema_{len(schema_editors)}"
        schema_editors[schema_id] = editor
        
        return {
            "schema_id": schema_id,
            "schema": editor.schema,
            "tree": editor.get_tree_structure()
        }
    except Exception as e:
        raise HTTPException(500, f"Load schema error: {str(e)}")

@app.delete("/api/schema/stored/{stored_id}")
async def delete_stored_schema(stored_id: str):
    """Delete a saved schema"""
    try:
        if stored_id not in stored_schemas:
            raise HTTPException(404, "Stored schema not found")
        
        del stored_schemas[stored_id]
        
        if audit_log:
            try:
                audit_log.log(AuditAction.SCHEMA_DELETE, user=user if 'user' in dir() else None,
                              resource_type='schema')
            except Exception: pass
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, f"Delete schema error: {str(e)}")


# Run server


# === TRANSFORMATION ENDPOINTS (AUTO-ADDED) ===

@app.post("/api/transform/execute")
async def execute_transform(
    file: UploadFile = File(...),
    output_format: str = Form('xml'),
    validate: bool = Form(False),
    mapping_rules: str = Form(None),  # JSON string of mapping rules
    input_schema: str = Form(None),   # Schema directory name (e.g. 'FatturaPA', 'UBL-21')
    output_schema: str = Form(None)   # Schema directory name (e.g. 'UBL-21', 'PEPPOL')
):
    """Execute transformation with mapping rules"""
    try:
        import time as _time
        _t0 = _time.time()
        content = (await file.read()).decode('utf-8')
        
        print(f"\n{'='*60}")
        print(f"🔄 TRANSFORM EXECUTE")
        print(f"{'='*60}")
        print(f"📁 File: {file.filename}")
        print(f"📏 Content length: {len(content)} chars")
        print(f"📝 First 200 chars: {content[:200]}")
        
        # Detect input format — IDoc flat files look like CSV but are actually fixed-width
        stripped = content.strip()
        if stripped.startswith('<'):
            input_fmt = 'xml'
        elif stripped.startswith('{') or stripped.startswith('['):
            input_fmt = 'json'
        else:
            # Check if it looks like an IDoc flat file (lines starting with segment names like E1EDK01)
            first_lines = stripped.split('\n')[:5]
            is_idoc = any(
                len(l) > 10 and (l.startswith('EDI_DC') or (len(l.split()) >= 2 and l[:8].replace('0','').isalpha()))
                for l in first_lines if l.strip()
            )
            input_fmt = 'idoc' if is_idoc else 'csv'
        
        print(f"📊 Input format: {input_fmt}")
        print(f"📤 Output format: {output_format}")
        
        # Parse mapping rules from JSON string
        import json
        rules = {'connections': []}
        
        print(f"📦 mapping_rules received: {mapping_rules is not None}")
        
        if mapping_rules:
            try:
                rules = json.loads(mapping_rules)
                print(f"✅ Parsed mapping_rules successfully")
                print(f"🔗 Connections: {len(rules.get('connections', []))}")
                print(f"📋 First connection: {rules.get('connections', [None])[0] if rules.get('connections') else 'None'}")
            except Exception as e:
                print(f"❌ Failed to parse mapping_rules: {e}")
                print(f"   Raw value: {mapping_rules[:500]}")
        else:
            print(f"⚠️  No mapping_rules provided!")
        
        # Get validation files (XSD and Schematron) - use explicit schema names if provided
        input_xsd, input_sch, output_xsd, output_sch = get_validation_files(
            input_fmt, 
            output_format,
            input_content=content,
            mapping_rules=rules,
            input_schema=input_schema,
            output_schema=output_schema
        )
        
        # Create TransformationEngine with validation files
        engine = TransformationEngine(
            input_xsd=input_xsd,
            output_xsd=output_xsd,
            input_schematron=input_sch,
            output_schematron=output_sch
        )
        
        print(f"\n🚀 Calling transformation_engine.transform...")
        
        # Normalize output_format for engine (expects 'xml', 'json', 'csv')
        _engine_fmt = output_format.lower() if output_format.lower() in ('xml', 'json', 'csv') else 'xml'
        
        # Transform with actual mapping
        result = engine.transform(
            input_content=content,
            input_format=input_fmt,
            output_format=_engine_fmt,
            mapping_rules=rules,
            validate_input=validate,
            validate_output=validate
        )
        
        print(f"✅ Transform complete!")
        
        # Check if output_content is valid before accessing it
        if result.output_content:
            print(f"📤 Output length: {len(result.output_content)} chars")
            print(f"📝 Output preview: {result.output_content[:200]}")
        else:
            print(f"⚠️ WARNING: output_content is None or empty!")
            print(f"🔍 Result success: {result.success}")
            print(f"🔍 Result errors: {result.validation_errors if hasattr(result, 'validation_errors') else 'N/A'}")
        
        print(f"{'='*60}\n")
        
        import time as _time
        _duration = int((_time.time() - _t0) * 1000) if '_t0' in dir() else 0
        if result.success:
            if audit_log:
                try:
                    audit_log.log(
                        AuditAction.TRANSFORM,
                        outcome=AuditOutcome.SUCCESS,
                        resource_type="transform",
                        file_name=file.filename,
                        file_size_bytes=len(content),
                        input_format=input_fmt,
                        output_format=output_format,
                        duration_ms=_duration,
                        output_preview=result.output_content[:500] if result.output_content else None
                    )
                except Exception: pass
            # Incrementa usage counter
            if billing_manager:
                try:
                    uid = user.get("id") if user else None
                    if uid:
                        billing_manager.increment_usage(str(uid), "transforms_count")
                        billing_manager.increment_usage(str(uid), "bytes_processed", len(content))
                except Exception: pass
            return Response(
                content=result.output_content,
                media_type='application/xml' if _engine_fmt == 'xml' else f'application/{_engine_fmt}',
                headers={'Content-Disposition': f'attachment; filename=output.{_engine_fmt}'}
            )
        else:
            if audit_log:
                try:
                    audit_log.log(
                        AuditAction.TRANSFORM,
                        outcome=AuditOutcome.FAILURE,
                        resource_type="transform",
                        file_name=file.filename,
                        file_size_bytes=len(content),
                        input_format=input_fmt,
                        output_format=output_format,
                        error_message=str(result.validation_errors)[:300] if hasattr(result, 'validation_errors') else None
                    )
                except Exception: pass
            return {"success": False, "errors": result.validation_errors}
    except Exception as e:
        if audit_log:
            try:
                audit_log.log(AuditAction.TRANSFORM, outcome=AuditOutcome.FAILURE,
                              resource_type="transform", file_name=file.filename,
                              error_message=str(e)[:300])
            except Exception: pass
        print(f"Transform error: {e}")
        raise HTTPException(500, str(e))

@app.post("/api/xsd/upload")
async def upload_xsd(file: UploadFile = File(...), format_name: str = None, io_type: str = 'input'):
    """Upload XSD file"""
    try:
        content = await file.read()
        if not format_name:
            format_name = file.filename.split('_')[0]
        
        target = SCHEMAS_DIR / io_type / format_name
        target.mkdir(parents=True, exist_ok=True)
        
        (target / file.filename).write_bytes(content)
        
        return {"success": True, "path": str(target / file.filename)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/xsd/list")
async def list_xsd():
    """List XSD files"""
    files = []
    for xsd in SCHEMAS_DIR.rglob("*.xsd"):
        files.append({"name": xsd.name, "path": str(xsd.relative_to(SCHEMAS_DIR))})
    return {"files": files}


# ============================================================================
# AUTHENTICATION ENDPOINTS
# ============================================================================

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: Optional[str] = ""

class LoginRequest(BaseModel):
    email: str
    password: str

class OAuthCallbackRequest(BaseModel):
    code: str
    state: Optional[str] = None

# Register Groups & Files API
if group_storage and perm_checker:
    _UPLOAD_DIR = APP_CONFIG.get('storage', {}).get('upload_dir', 'data/uploads')
    _fe_host = APP_CONFIG.get('app', {}).get('host', '127.0.0.1')
    _fe_port = APP_CONFIG.get('app', {}).get('frontend_port', 8000)
    _BASE_URL = f"http://{_fe_host}:{_fe_port}"
    register_groups_api(
        app=app,
        group_storage=group_storage,
        permission_checker=perm_checker,
        get_current_user=get_current_user,
        get_optional_user=None,
        UPLOAD_DIR=_UPLOAD_DIR,
        BASE_URL=_BASE_URL
    )

@app.get("/api/auth/status")
async def auth_status():
    """Check if authentication is enabled"""
    return {
        "enabled": AUTH_ENABLED,
        "providers": {
            "local": True,
            "google": APP_CONFIG.get('auth', {}).get('oauth', {}).get('google', {}).get('enabled', False),
            "facebook": APP_CONFIG.get('auth', {}).get('oauth', {}).get('facebook', {}).get('enabled', False),
            "github": APP_CONFIG.get('auth', {}).get('oauth', {}).get('github', {}).get('enabled', False),
        }
    }

@app.post("/api/auth/register")
async def register(request: RegisterRequest):
    """Register new user with email/password — sends verification email"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    success, message, user_data = auth_manager.register_user(
        request.email, 
        request.password, 
        request.name
    )
    
    if not success:
        raise HTTPException(400, message)
    
    # Send verification email
    try:
        token = auth_manager.generate_email_verification_token(user_data['id'], request.email)
        import email_service
        email_service.send_verification_email(request.email, user_data.get('name', ''), token)
    except Exception as e:
        print(f"[AUTH] Errore invio email verifica: {e}")
    
    # Notify admins
    try:
        import email_service
        # Find admin emails
        if hasattr(storage, 'conn') and storage.conn:
            cursor = storage.conn.cursor()
            cursor.execute("SELECT email FROM users WHERE role IN ('MASTER','ADMIN') AND status='APPROVED'")
            admin_emails = [r[0] for r in cursor.fetchall()]
            if admin_emails:
                email_service.send_new_user_notification(admin_emails, request.email, user_data.get('name', ''))
    except Exception as e:
        print(f"[AUTH] Errore notifica admin: {e}")

    if audit_log:
        try:
            audit_log.log(AuditAction.REGISTER, user=user_data if 'user_data' in dir() else None,
                          resource_type='auth', outcome=AuditOutcome.SUCCESS,
                          metadata={"email": request.email})
        except Exception: pass
    return {"success": True, "message": "Registrazione completata! Controlla la tua email per verificare l'account.", "user": user_data}

@app.post("/api/auth/login")
async def login(request: LoginRequest):
    """Login with email/password — may require MFA step 2"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    success, message, user_data = auth_manager.login_step1(request.email, request.password)
    
    if not success:
        if audit_log:
            try:
                audit_log.log(AuditAction.LOGIN_FAILED, user=None,
                              resource_type='auth', outcome=AuditOutcome.FAILURE,
                              metadata={"email": request.email}, error_message=message)
            except Exception: pass
        raise HTTPException(401, message)
    
    # Check if MFA is required
    if user_data and user_data.get('mfa_required'):
        # If user has email MFA, auto-send code
        if 'email' in user_data.get('mfa_methods', []):
            try:
                code, email_token = auth_manager.generate_mfa_email_code(
                    jwt.decode(user_data['mfa_token'], auth_manager.secret_key, algorithms=['HS256'])['sub']
                )
                import email_service
                email_service.send_mfa_code_email(user_data['user_email'], user_data['user_name'], code)
                user_data['mfa_email_token'] = email_token
            except Exception as e:
                print(f"[MFA] Errore invio codice email: {e}")
        return user_data  # Contains mfa_required, mfa_methods, mfa_token
    
    if audit_log:
        try:
            audit_log.log(AuditAction.LOGIN, user=user_data,
                          resource_type='auth', outcome=AuditOutcome.SUCCESS,
                          metadata={"email": request.email})
        except Exception: pass
    return {"success": True, "message": message, "user": user_data, "token": user_data['token']}

@app.post("/api/auth/mfa/verify")
async def mfa_verify(request: Request):
    """Step 2: verify MFA code after login"""
    body = await request.json()
    partial_token = body.get('mfa_token', '')
    code = body.get('code', '')
    method = body.get('method', 'totp')
    email_mfa_token = body.get('mfa_email_token')
    
    success, message, user_data = auth_manager.login_step2_mfa(
        partial_token, code, method, email_mfa_token
    )
    if not success:
        raise HTTPException(401, message)
    return {"success": True, "message": message, "user": user_data, "token": user_data['token']}


@app.get("/api/auth/verify-email")
async def verify_email(token: str):
    """Verify email address via link from registration email."""
    success, message, user_id = auth_manager.verify_email_token(token)
    if not success:
        raise HTTPException(400, message)
    return {"success": True, "message": message}


@app.post("/api/auth/resend-verification")
async def resend_verification(request: Request):
    """Resend email verification link."""
    body = await request.json()
    email = body.get('email', '')
    if not email:
        raise HTTPException(400, "Email richiesta")
    user = storage.get_user_by_email(email)
    if not user:
        return {"success": True, "message": "Se l'email è registrata, riceverai il link."}
    if user.get('email_verified'):
        return {"success": True, "message": "Email già verificata."}
    try:
        token = auth_manager.generate_email_verification_token(user['id'], email)
        import email_service
        email_service.send_verification_email(email, user.get('name', ''), token)
    except Exception as e:
        print(f"[AUTH] Errore reinvio verifica: {e}")
    return {"success": True, "message": "Se l'email è registrata, riceverai il link."}


@app.get("/api/auth/mfa/status")
async def mfa_status(user=Depends(get_current_user)):
    """Get MFA status for current user."""
    return auth_manager.get_mfa_status(str(user['id']))


@app.post("/api/auth/mfa/setup-totp")
async def mfa_setup_totp(user=Depends(get_current_user)):
    """Start TOTP setup — returns secret + QR URI."""
    return auth_manager.setup_mfa_totp(str(user['id']))


@app.post("/api/auth/mfa/confirm-totp")
async def mfa_confirm_totp(request: Request, user=Depends(get_current_user)):
    """Confirm TOTP by verifying first code."""
    body = await request.json()
    code = body.get('code', '')
    success, message = auth_manager.confirm_mfa_totp(str(user['id']), code)
    if not success:
        raise HTTPException(400, message)
    return {"success": True, "message": message}


@app.post("/api/auth/mfa/enable-email")
async def mfa_enable_email(user=Depends(get_current_user)):
    """Enable email-based MFA."""
    success, message = auth_manager.enable_mfa_email(str(user['id']))
    if not success:
        raise HTTPException(400, message)
    return {"success": True, "message": message}


@app.post("/api/auth/mfa/disable")
async def mfa_disable(user=Depends(get_current_user)):
    """Disable all MFA."""
    success, message = auth_manager.disable_mfa(str(user['id']))
    return {"success": True, "message": message}


@app.post("/api/auth/mfa/send-email-code")
async def mfa_send_email_code(request: Request):
    """Send MFA email code (during login flow, before full auth)."""
    body = await request.json()
    partial_token = body.get('mfa_token', '')
    try:
        payload = jwt.decode(partial_token, auth_manager.secret_key, algorithms=['HS256'])
        user_id = payload['sub']
        user = storage.get_user(user_id) if hasattr(storage, 'get_user') else None
        if not user:
            raise HTTPException(400, "Utente non trovato")
        code, email_token = auth_manager.generate_mfa_email_code(user_id)
        import email_service
        email_service.send_mfa_code_email(user['email'], user.get('name', ''), code)
        return {"success": True, "mfa_email_token": email_token}
    except jwt.InvalidTokenError:
        raise HTTPException(400, "Token non valido")


@app.get("/api/auth/google/login")
async def google_login(link_token: Optional[str] = None):
    """Redirect to Google OAuth. Pass ?link_token=JWT to link account instead of login."""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    google_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('google', {})
    if not google_config.get('enabled'):
        raise HTTPException(400, "Google OAuth is not enabled")
    
    client_id = google_config.get('client_id')
    redirect_uri = google_config.get('redirect_uri')
    
    # If link_token provided, encode it in state so callback knows it's a link operation
    state_param = ""
    if link_token:
        import urllib.parse
        state_param = f"&state=link:{urllib.parse.quote(link_token)}"
    
    auth_url = (
        f"https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={client_id}&"
        f"redirect_uri={redirect_uri}&"
        f"response_type=code&"
        f"scope=openid email profile&"
        f"access_type=offline"
        f"{state_param}"
    )
    
    print("🔗 Google redirect_uri:", redirect_uri)
    print("🔗 Google auth_url:", auth_url)
    if link_token:
        print("🔗 Google OAuth: LINK MODE (federating account)")
    return {"auth_url": auth_url}

@app.get("/api/auth/google/callback")
async def google_callback(code: str, state: Optional[str] = None):
    """Handle Google OAuth callback"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    google_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('google', {})
    
    # Exchange code for token
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": google_config.get('client_id'),
                "client_secret": google_config.get('client_secret'),
                "redirect_uri": google_config.get('redirect_uri'),
                "grant_type": "authorization_code"
            }
        )
        
        if token_response.status_code != 200:
            err_detail = token_response.json() if token_response.content else {}
            err_msg = err_detail.get('error_description') or err_detail.get('error') or token_response.text[:200]
            print(f"❌ Google token exchange failed: {token_response.status_code} - {err_msg}")
            print(f"   redirect_uri used: {google_config.get('redirect_uri')}")
            print(f"   client_id: {google_config.get('client_id', 'MISSING')[:20]}...")
            print(f"   client_secret present: {bool(google_config.get('client_secret'))}")
            raise HTTPException(400, f"Google OAuth error: {err_msg}")
        
        tokens = token_response.json()
        access_token = tokens.get('access_token')
        
        # Get user info
        user_response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if user_response.status_code != 200:
            raise HTTPException(400, "Failed to get user info")
        
        user_info = user_response.json()
    
    # Login or register user — OR link if state=link:JWT
    oauth_data = {
        'provider_id': user_info.get('id'),
        'email': user_info.get('email'),
        'name': user_info.get('name'),
        'picture': user_info.get('picture')
    }
    
    from fastapi.responses import RedirectResponse
    _frontend_url = APP_CONFIG.get('app', {}).get('frontend_url', 'http://localhost:8000')
    
    # Check if this is a LINK operation (federation)
    if state and state.startswith('link:'):
        import urllib.parse
        link_jwt = urllib.parse.unquote(state[5:])
        try:
            # Validate the JWT to get the current user
            valid, payload = auth_manager.verify_token(link_jwt)
            if not valid:
                raise ValueError("Invalid token")
            current_user_id = str(payload.get("user_id") or payload.get("sub"))
            
            if federated_identity:
                success, message = federated_identity.link_provider(
                    current_user_id, 'google', str(oauth_data['provider_id']),
                    oauth_data.get('email', ''), oauth_data.get('name', '')
                )
                if success:
                    print(f"✅ Google account linked to user {current_user_id}")
                    return RedirectResponse(url=f"{_frontend_url}/app.html?link_success=google", status_code=302)
                else:
                    print(f"❌ Google link failed: {message}")
                    return RedirectResponse(url=f"{_frontend_url}/app.html?link_error={urllib.parse.quote(message)}", status_code=302)
            else:
                return RedirectResponse(url=f"{_frontend_url}/app.html?link_error=federation_not_available", status_code=302)
        except Exception as e:
            print(f"❌ Link JWT validation failed: {e}")
            return RedirectResponse(url=f"{_frontend_url}/app.html?link_error=invalid_token", status_code=302)
    
    # Normal login/register flow
    success, message, user_data = auth_manager.oauth_login('google', oauth_data)
    
    if not success:
        raise HTTPException(400, message)
    
    # Redirect to frontend with token
    token = user_data.get('token')
    return RedirectResponse(url=f"{_frontend_url}/app.html?token={token}", status_code=302)

# ALSO ADD WITHOUT /api/ PREFIX for Google OAuth compatibility
@app.get("/auth/google/callback")
async def google_callback_no_prefix(code: str, state: Optional[str] = None):
    """Handle Google OAuth callback (alternative path without /api/)"""
    return await google_callback(code, state)

@app.get("/api/auth/facebook/login")
async def facebook_login(link_token: Optional[str] = None):
    """Redirect to Facebook OAuth. Pass ?link_token=JWT to link account."""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    fb_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('facebook', {})
    if not fb_config.get('enabled'):
        raise HTTPException(400, "Facebook OAuth is not enabled")
    
    app_id = fb_config.get('app_id')
    redirect_uri = fb_config.get('redirect_uri')
    
    state_param = ""
    if link_token:
        import urllib.parse
        state_param = f"&state=link:{urllib.parse.quote(link_token)}"
    
    auth_url = (
        f"https://www.facebook.com/v12.0/dialog/oauth?"
        f"client_id={app_id}&"
        f"redirect_uri={redirect_uri}&"
        f"scope=email,public_profile"
        f"{state_param}"
    )
    
    return {"auth_url": auth_url}

@app.get("/api/auth/facebook/callback")
async def facebook_callback(code: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None):
    """Handle Facebook OAuth callback"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    if error:
        raise HTTPException(400, f"Facebook OAuth error: {error}")
    if not code:
        raise HTTPException(400, "Missing authorization code")
    fb_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('facebook', {})
    
    async with httpx.AsyncClient() as client:
        token_response = await client.get(
            "https://graph.facebook.com/v12.0/oauth/access_token",
            params={
                "client_id": fb_config.get('app_id'),
                "client_secret": fb_config.get('app_secret'),
                "redirect_uri": fb_config.get('redirect_uri'),
                "code": code
            }
        )
        
        if token_response.status_code != 200:
            err_detail = token_response.json() if token_response.content else {}
            err_msg = err_detail.get('error_description') or err_detail.get('error') or token_response.text[:200]
            print(f"\u274c Facebook token exchange failed: {token_response.status_code} - {err_msg}")
            raise HTTPException(400, f"Facebook OAuth error: {err_msg}")
        
        tokens = token_response.json()
        access_token = tokens.get('access_token')
        
        user_response = await client.get(
            "https://graph.facebook.com/me",
            params={"fields": "id,name,email,picture", "access_token": access_token}
        )
        
        if user_response.status_code != 200:
            raise HTTPException(400, "Failed to get user info")
        
        user_info = user_response.json()
    
    oauth_data = {
        'provider_id': user_info.get('id'),
        'email': user_info.get('email'),
        'name': user_info.get('name'),
        'picture': user_info.get('picture', {}).get('data', {}).get('url')
    }
    
    from fastapi.responses import RedirectResponse
    _frontend_url = APP_CONFIG.get('app', {}).get('frontend_url', 'http://localhost:8000')
    
    if state and state.startswith('link:'):
        import urllib.parse
        link_jwt = urllib.parse.unquote(state[5:])
        try:
            valid, payload = auth_manager.verify_token(link_jwt)
            if not valid:
                raise ValueError("Invalid token")
            current_user_id = str(payload.get("user_id") or payload.get("sub"))
            if federated_identity:
                success, message = federated_identity.link_provider(
                    current_user_id, 'facebook', str(oauth_data['provider_id']),
                    oauth_data.get('email', ''), oauth_data.get('name', '')
                )
                if success:
                    return RedirectResponse(url=f"{_frontend_url}/app.html?link_success=facebook", status_code=302)
                else:
                    return RedirectResponse(url=f"{_frontend_url}/app.html?link_error={urllib.parse.quote(message)}", status_code=302)
            else:
                return RedirectResponse(url=f"{_frontend_url}/app.html?link_error=federation_not_available", status_code=302)
        except Exception as e:
            return RedirectResponse(url=f"{_frontend_url}/app.html?link_error=invalid_token", status_code=302)
    
    success, message, user_data = auth_manager.oauth_login('facebook', oauth_data)
    if not success:
        raise HTTPException(400, message)
    
    token = user_data.get('token')
    return RedirectResponse(url=f"{_frontend_url}/app.html?token={token}", status_code=302)

# ALSO ADD WITHOUT /api/ PREFIX for Facebook OAuth compatibility
@app.get("/auth/facebook/callback")
async def facebook_callback_no_prefix(code: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None):
    """Handle Facebook OAuth callback (alternative path without /api/)"""
    return await facebook_callback(code, error, state)

@app.get("/api/auth/github/login")
async def github_login(link_token: Optional[str] = None):
    """Redirect to GitHub OAuth. Pass ?link_token=JWT to link account."""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    gh_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('github', {})
    if not gh_config.get('enabled'):
        raise HTTPException(400, "GitHub OAuth is not enabled")
    
    client_id = gh_config.get('client_id')
    redirect_uri = gh_config.get('redirect_uri')
    
    state_param = ""
    if link_token:
        import urllib.parse
        state_param = f"&state=link:{urllib.parse.quote(link_token)}"
    
    auth_url = (
        f"https://github.com/login/oauth/authorize?"
        f"client_id={client_id}&"
        f"redirect_uri={redirect_uri}&"
        f"scope=user:email"
        f"{state_param}"
    )
    
    return {"auth_url": auth_url}

@app.get("/api/auth/github/callback")
async def github_callback(code: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None):
    """Handle GitHub OAuth callback"""
    if error:
        raise HTTPException(400, f"OAuth error: {error}")
    if not code:
        raise HTTPException(400, "Missing authorization code")
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    gh_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('github', {})
    
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            "https://github.com/login/oauth/access_token",
            data={
                "client_id": gh_config.get('client_id'),
                "client_secret": gh_config.get('client_secret'),
                "code": code,
                "redirect_uri": gh_config.get('redirect_uri')
            },
            headers={"Accept": "application/json"}
        )
        
        if token_response.status_code != 200:
            err_detail = token_response.json() if token_response.content else {}
            err_msg = err_detail.get('error_description') or err_detail.get('error') or token_response.text[:200]
            print(f"\u274c GitHub token exchange failed: {token_response.status_code} - {err_msg}")
            raise HTTPException(400, f"GitHub OAuth error: {err_msg}")
        
        tokens = token_response.json()
        access_token = tokens.get('access_token')
        
        user_response = await client.get(
            "https://api.github.com/user",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if user_response.status_code != 200:
            raise HTTPException(400, "Failed to get user info")
        
        user_info = user_response.json()
        
        email = user_info.get('email')
        if not email:
            email_response = await client.get(
                "https://api.github.com/user/emails",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            emails = email_response.json()
            primary_email = next((e for e in emails if e.get('primary')), None)
            email = primary_email.get('email') if primary_email else None
    
    oauth_data = {
        'provider_id': str(user_info.get('id')),
        'email': email,
        'name': user_info.get('name') or user_info.get('login'),
        'picture': user_info.get('avatar_url')
    }
    
    from fastapi.responses import RedirectResponse
    _frontend_url = APP_CONFIG.get('app', {}).get('frontend_url', 'http://localhost:8000')
    
    if state and state.startswith('link:'):
        import urllib.parse
        link_jwt = urllib.parse.unquote(state[5:])
        try:
            valid, payload = auth_manager.verify_token(link_jwt)
            if not valid:
                raise ValueError("Invalid token")
            current_user_id = str(payload.get("user_id") or payload.get("sub"))
            if federated_identity:
                success, message = federated_identity.link_provider(
                    current_user_id, 'github', str(oauth_data['provider_id']),
                    oauth_data.get('email', ''), oauth_data.get('name', '')
                )
                if success:
                    return RedirectResponse(url=f"{_frontend_url}/app.html?link_success=github", status_code=302)
                else:
                    return RedirectResponse(url=f"{_frontend_url}/app.html?link_error={urllib.parse.quote(message)}", status_code=302)
            else:
                return RedirectResponse(url=f"{_frontend_url}/app.html?link_error=federation_not_available", status_code=302)
        except Exception as e:
            return RedirectResponse(url=f"{_frontend_url}/app.html?link_error=invalid_token", status_code=302)
    
    success, message, user_data = auth_manager.oauth_login('github', oauth_data)
    if not success:
        raise HTTPException(400, message)
    
    token = user_data.get('token')
    return RedirectResponse(url=f"{_frontend_url}/app.html?token={token}", status_code=302)

# ALSO ADD WITHOUT /api/ PREFIX for GitHub OAuth compatibility
@app.get("/auth/github/callback")
async def github_callback_no_prefix(code: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None):
    """Handle GitHub OAuth callback (alternative path without /api/)"""
    return await github_callback(code, error, state)

@app.get("/api/auth/me")
async def get_me(user = Depends(get_current_user)):
    """Get current user info"""
    return {"user": user}

@app.post("/api/auth/logout")
async def logout():
    """Logout (client should delete token)"""
    if audit_log:
        try:
            audit_log.log(AuditAction.LOGOUT, user=None,
                          resource_type='auth', outcome=AuditOutcome.SUCCESS)
        except Exception: pass
    return {"success": True, "message": "Logged out successfully"}


# =============================================================================
# FEDERATED IDENTITY — link/unlink OAuth providers per account esistente
# =============================================================================

@app.get("/api/auth/providers")
async def get_linked_providers(user = Depends(get_current_user)):
    """
    Lista i provider OAuth collegati all'account corrente.
    Ritorna anche se l'utente ha password locale.
    """
    providers = []
    if federated_identity:
        try:
            providers = federated_identity.get_linked_providers(str(user["id"]))
        except Exception: pass

    # Controlla se l'utente ha password locale
    raw_user = storage.get_user(str(user["id"])) if storage else None
    has_password = bool(raw_user and raw_user.get("password_hash"))

    # Migrazione automatica vecchio schema (auth_provider su users)
    if federated_identity:
        legacy_provider = user.get("auth_provider")
        legacy_pid = user.get("auth_provider_id")
        if legacy_provider and legacy_provider != "local" and legacy_pid:
            linked_names = [p["provider"] for p in providers]
            if legacy_provider not in linked_names:
                try:
                    federated_identity.migrate_legacy_provider(
                        str(user["id"]), legacy_provider,
                        str(legacy_pid), user.get("email")
                    )
                    providers = federated_identity.get_linked_providers(str(user["id"]))
                except Exception: pass

    all_supported = ["google", "facebook", "github", "microsoft"]
    linked_names = [p["provider"] for p in providers]

    return {
        "providers": providers,
        "has_password": has_password,
        "supported": all_supported,
        "unlinked": [p for p in all_supported if p not in linked_names],
    }


@app.post("/api/auth/link/{provider}")
async def link_provider(provider: str, request: Request, user = Depends(get_current_user)):
    """
    Collega un provider OAuth all'account corrente.
    Body: { provider_id, email, name }
    Questo endpoint è chiamato dopo che il frontend ha completato
    il flow OAuth e ha ottenuto i dati del provider.
    """
    if not federated_identity:
        raise HTTPException(400, "Federated identity non disponibile")

    body = await request.json()
    provider_id = body.get("provider_id")
    provider_email = body.get("email", "")
    provider_name = body.get("name", "")

    if not provider_id:
        raise HTTPException(400, "provider_id richiesto")

    success, message = federated_identity.link_provider(
        str(user["id"]), provider, str(provider_id),
        provider_email, provider_name
    )
    if not success:
        raise HTTPException(409, message)

    if audit_log:
        try:
            audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                          resource_type="auth_provider",
                          metadata={"action": "link", "provider": provider})
        except Exception: pass

    return {"success": True, "message": message}


@app.delete("/api/auth/link/{provider}")
async def unlink_provider(provider: str, user = Depends(get_current_user)):
    """
    Scollega un provider OAuth dall'account.
    Blocca se è l'unico metodo di login rimasto.
    """
    if not federated_identity:
        raise HTTPException(400, "Federated identity non disponibile")

    # Controlla se ha password locale
    raw_user = storage.get_user(str(user["id"])) if storage else None
    has_password = bool(raw_user and raw_user.get("password_hash"))

    success, message = federated_identity.unlink_provider(
        str(user["id"]), provider, has_password
    )
    if not success:
        raise HTTPException(400, message)

    if audit_log:
        try:
            audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                          resource_type="auth_provider",
                          metadata={"action": "unlink", "provider": provider})
        except Exception: pass

    return {"success": True, "message": message}


@app.post("/api/auth/oauth-callback/{provider}")
async def oauth_callback_link(provider: str, request: Request):
    """
    Callback OAuth unificato.
    - Se c'è un token Bearer nella request → link al account esistente
    - Altrimenti → login/registrazione normale con quel provider
    """
    body = await request.json()
    provider_id = body.get("provider_id")
    email = body.get("email", "")
    name = body.get("name", "")

    if not provider_id or not email:
        raise HTTPException(400, "provider_id e email richiesti")

    # Controlla se c'è già un account con questo provider (via federated_identity)
    existing_user_id = None
    if federated_identity:
        existing_user_id = federated_identity.find_user_by_provider(provider, str(provider_id))

    if existing_user_id:
        # Login con provider esistente
        user_data = storage.get_user(existing_user_id) if storage else None
        if not user_data:
            raise HTTPException(404, "Utente non trovato")
        if federated_identity:
            federated_identity.touch_last_used(existing_user_id, provider)
    else:
        # Fallback al vecchio oauth_login (crea utente se non esiste)
        success, message, user_data = auth_manager.oauth_login(provider, {
            "provider_id": provider_id,
            "email": email,
            "name": name,
        })
        if not success:
            raise HTTPException(401, message)
        # Migra al nuovo schema
        if federated_identity and user_data:
            try:
                federated_identity.link_provider(
                    str(user_data["id"]), provider, str(provider_id), email, name
                )
            except Exception: pass

    if not user_data:
        raise HTTPException(500, "Errore durante il login")

    # Genera token
    from auth_system import AuthManager as _AM
    token = auth_manager._generate_token(user_data)
    result = {k: v for k, v in user_data.items() if k != "password_hash"}
    result["token"] = token

    if audit_log:
        try:
            audit_log.log(AuditAction.LOGIN, user=result,
                          resource_type="oauth",
                          outcome=AuditOutcome.SUCCESS,
                          metadata={"provider": provider})
        except Exception: pass

    return {"success": True, "user": result, "token": token}


@app.get("/api/schemas/{schema_id}/document-types")
async def get_document_types(schema_id: str):
    """
    Get available document types for a schema
    
    Returns list of document types with their XSD files.
    Example: For UBL-21, returns [Invoice, CreditNote, DebitNote, ...]
    """
    try:
        # Map schema IDs to their directories
        schema_dirs = {
            'UBL-21': 'UBL-21',
            'ubl': 'ubl',
            'peppol': 'UBL-21',
            'fatturapa': 'FatturaPA',
            'FatturaPA': 'FatturaPA'
        }
        
        schema_dir = schema_dirs.get(schema_id, schema_id)
        schema_path = SCHEMAS_DIR / 'output' / schema_dir
        
        if not schema_path.exists():
            return {
                "success": False,
                "error": f"Schema directory not found: {schema_id}"
            }
        
        # Find all XSD files in maindoc folder
        maindoc_path = schema_path / 'xsd' / 'maindoc'
        if not maindoc_path.exists():
            # Try root directory
            maindoc_path = schema_path
        
        document_types = []
        
        if maindoc_path.exists():
            for xsd_file in maindoc_path.glob("*.xsd"):
                filename = xsd_file.name
                
                # Skip common/component files
                if any(skip in filename.lower() for skip in ['common', 'component', 'extension', 'aggregate', 'basic']):
                    continue
                
                # Extract document type from filename
                # Pattern: UBL-{DocumentType}-2.1.xsd
                if filename.startswith('UBL-'):
                    doc_type = filename.replace('UBL-', '').replace('-2.1.xsd', '').replace('-2.0.xsd', '').replace('.xsd', '')
                else:
                    doc_type = filename.replace('.xsd', '')
                
                # Add to list with metadata
                document_types.append({
                    'type': doc_type,
                    'label': doc_type,
                    'filename': filename,
                    'path': str(xsd_file)
                })
        
        # Sort by type name
        document_types.sort(key=lambda x: x['type'])
        
        print(f"📋 Found {len(document_types)} document types for {schema_id}")
        
        return {
            "success": True,
            "schema_id": schema_id,
            "document_types": document_types,
            "count": len(document_types)
        }
    
    except Exception as e:
        print(f"❌ Error getting document types: {e}")
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================
# SCHEMA MANAGEMENT APIs
# ============================================================

@app.post("/api/schemas/upload")
async def upload_schema(file: UploadFile = File(...)):
    """
    Upload schema file (ZIP with XSD or CSV)
    
    Supports:
    1. ZIP file containing XSD schemas
    2. CSV file with schema definition
    
    Auto-detects file type and processes accordingly.
    """
    try:
        import zipfile
        import tempfile
        import shutil
        
        # Check file extension
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        # ============================================================
        # CSV FILE UPLOAD
        # ============================================================
        if file_ext == '.csv':
            print(f"📊 CSV schema upload: {file.filename}")
            
            # Import CSV parser
            from csv_parser import CSVSchemaParser
            
            # Save CSV temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
                content = await file.read()
                temp_csv.write(content)
                temp_csv_path = temp_csv.name
            
            try:
                # Parse CSV
                parser = CSVSchemaParser()
                schema_data = parser.parse_csv(temp_csv_path)
                
                # Extract metadata (now included by parser)
                schema_name = schema_data.get('name', 'schema')
                root_element = schema_data.get('rootElement', 'root')
                format_type = schema_data.get('format', 'xml')
                namespace = schema_data.get('namespace', '')
                
                print(f"✅ Parsed CSV: {schema_data['field_count']} fields")
                print(f"📋 Schema: {schema_name}")
                print(f"📦 Format: {format_type}")
                print(f"🎯 Root element: {root_element}")
                print(f"🔗 Namespace: {namespace}")
                
                # Return schema data (already has all metadata)
                return {
                    "success": True,
                    "message": f"CSV schema '{schema_name}' parsed successfully",
                    "schema": schema_data,
                    "type": "csv",
                    "fields_count": schema_data['field_count']
                }
            
            finally:
                # Clean up temp file
                os.unlink(temp_csv_path)
        
        # ============================================================
        # ZIP FILE UPLOAD (existing code)
        # ============================================================
        import zipfile
        import tempfile
        import shutil
        
        # File extensions to remove (documentation, images, etc.)
        UNWANTED_EXTENSIONS = {
            '.html', '.htm', '.pdf', '.png', '.jpg', '.jpeg', '.gif', 
            '.svg', '.css', '.js', '.txt', '.md', '.xml',  # Remove .txt and .md docs
            '.doc', '.docx', '.odt', '.rtf',  # Office docs
            '.zip', '.tar', '.gz'  # Archives
        }
        
        # Keep only these file types
        WANTED_EXTENSIONS = {'.xsd', '.sch'}
        
        def should_keep_file(filename):
            """Decide if file should be kept"""
            name_lower = filename.lower()
            ext = os.path.splitext(name_lower)[1]
            
            # Keep XSD and Schematron
            if ext in WANTED_EXTENSIONS:
                return True
            
            # Remove unwanted
            if ext in UNWANTED_EXTENSIONS:
                return False
            
            # Keep files without extension or unknown extensions
            return True
        
        def clean_directory(directory):
            """Remove unwanted files from directory recursively"""
            removed_count = 0
            for root, dirs, files in os.walk(directory, topdown=False):
                for filename in files:
                    if not should_keep_file(filename):
                        file_path = os.path.join(root, filename)
                        os.remove(file_path)
                        removed_count += 1
                
                # Remove empty directories
                for dirname in dirs:
                    dir_path = os.path.join(root, dirname)
                    if os.path.exists(dir_path) and not os.listdir(dir_path):
                        os.rmdir(dir_path)
            
            return removed_count
        
        def find_schema_xsd(directory):
            """Find schema.xsd or main XSD file recursively"""
            for root, dirs, files in os.walk(directory):
                for filename in files:
                    if filename.lower() in ['schema.xsd', 'invoice.xsd', 'ubl-invoice-2.1.xsd']:
                        return os.path.join(root, filename)
            
            # Fallback: any .xsd file
            for root, dirs, files in os.walk(directory):
                for filename in files:
                    if filename.endswith('.xsd'):
                        return os.path.join(root, filename)
            
            return None
        
        def get_schema_name_from_zip(extract_dir):
            """Intelligently determine schema name"""
            items = os.listdir(extract_dir)
            
            # Case 1: Single root directory (simple structure)
            if len(items) == 1 and os.path.isdir(os.path.join(extract_dir, items[0])):
                return items[0], os.path.join(extract_dir, items[0])
            
            # Case 2: Multiple items or files at root (complex structure like UBL)
            # Use extract_dir itself as schema root
            return 'extracted_schema', extract_dir
        
        # Create temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save uploaded ZIP
            zip_path = os.path.join(temp_dir, 'upload.zip')
            with open(zip_path, 'wb') as f:
                content = await file.read()
                f.write(content)
            
            # Extract ZIP
            extract_dir = os.path.join(temp_dir, 'extracted')
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            print(f"📦 Extracted ZIP to: {extract_dir}")
            print(f"📂 Contents: {os.listdir(extract_dir)}")
            
            # Determine schema name and root
            schema_name, schema_root = get_schema_name_from_zip(extract_dir)
            print(f"📋 Schema name: {schema_name}")
            print(f"📁 Schema root: {schema_root}")
            
            # Clean unwanted files
            removed = clean_directory(schema_root)
            print(f"🧹 Removed {removed} unwanted files")
            
            # Find main XSD file
            main_xsd = find_schema_xsd(schema_root)
            if not main_xsd:
                raise HTTPException(
                    status_code=400,
                    detail="No .xsd file found in ZIP"
                )
            
            print(f"✅ Found main XSD: {main_xsd}")
            
            # Check if we need to wrap (complex structure)
            need_wrapper = False
            
            # If main XSD is NOT directly in schema_root, we need wrapper
            main_xsd_dir = os.path.dirname(main_xsd)
            if main_xsd_dir != schema_root:
                need_wrapper = True
                print(f"🔄 Complex structure detected, will create wrapper")
            
            # Prepare final schema directory
            if need_wrapper:
                # Create wrapper directory
                wrapper_dir = os.path.join(temp_dir, 'wrapped')
                os.makedirs(wrapper_dir, exist_ok=True)
                
                # Copy main XSD to root as schema.xsd
                shutil.copy2(main_xsd, os.path.join(wrapper_dir, 'schema.xsd'))
                
                # Copy ALL subdirectories maintaining structure
                # This preserves xsd/common/, xsd/maindoc/, etc.
                for item in os.listdir(schema_root):
                    src_path = os.path.join(schema_root, item)
                    dest_path = os.path.join(wrapper_dir, item)
                    
                    # Skip the main XSD file itself (already copied as schema.xsd)
                    if src_path == main_xsd:
                        continue
                    
                    # Copy directories recursively
                    if os.path.isdir(src_path):
                        shutil.copytree(src_path, dest_path)
                        print(f"📁 Copied directory: {item}")
                    # Copy other files (like .sch, other .xsd)
                    elif src_path.endswith(('.xsd', '.sch')):
                        shutil.copy2(src_path, dest_path)
                        print(f"📄 Copied file: {item}")
                
                final_source = wrapper_dir
            else:
                # Simple structure, use as-is
                final_source = schema_root
            
            print(f"📦 Final source: {final_source}")
            print(f"📂 Final contents: {os.listdir(final_source)}")
            
            # Determine final schema name (use filename from original ZIP if possible)
            if file.filename:
                # Remove .zip extension
                suggested_name = file.filename.replace('.zip', '').replace('.ZIP', '')
                # Clean name (remove special chars)
                suggested_name = ''.join(c for c in suggested_name if c.isalnum() or c in ['-', '_'])
                if suggested_name and suggested_name != 'upload':
                    schema_name = suggested_name
            
            # Copy to schemas/input and schemas/output
            schemas_base = os.path.join(os.path.dirname(__file__), 'schemas')
            
            for dest_type in ['input', 'output']:
                dest_dir = os.path.join(schemas_base, dest_type, schema_name)
                
                # Remove existing if present
                if os.path.exists(dest_dir):
                    shutil.rmtree(dest_dir)
                
                # Copy schema directory
                shutil.copytree(final_source, dest_dir)
                print(f"✅ Copied to {dest_type}/{schema_name}")
            
            # Count files
            file_count = sum(1 for _ in os.walk(final_source) for _ in _[2])
            
            return {
                'success': True,
                'schemaName': schema_name,
                'fileCount': file_count,
                'removedFiles': removed,
                'hadWrapper': need_wrapper
            }
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.delete("/api/schemas/{schema_name}")
async def delete_schema(schema_name: str):
    """Delete a schema from both input and output directories"""
    try:
        import shutil
        
        schemas_base = os.path.join(os.path.dirname(__file__), 'schemas')
        deleted = []
        
        for dest_type in ['input', 'output']:
            schema_dir = os.path.join(schemas_base, dest_type, schema_name)
            if os.path.exists(schema_dir):
                shutil.rmtree(schema_dir)
                deleted.append(dest_type)
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Schema not found")
        
        return {
            'success': True,
            'schemaName': schema_name,
            'deletedFrom': deleted
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# STARTUP / LIFESPAN
# ===========================================================================

@app.on_event("startup")
async def on_startup():
    """Avvia task in background al bootstrap del server."""
    import asyncio
    if job_engine:
        asyncio.create_task(job_engine.start_cleanup_loop(interval_hours=24))
        print("✅ Job cleanup loop started")


# ===========================================================================
# ADMIN — USER MANAGEMENT ENDPOINTS
# ===========================================================================

@app.get("/api/admin/stats")
async def admin_stats(user=Depends(get_current_user)):
    """Dashboard stats for admin"""
    if user.get('role') not in ('ADMIN', 'MASTER'):
        raise HTTPException(403, "Admin required")
    cursor = storage.conn.cursor(cursor_factory=storage.RealDictCursor)
    cursor.execute("SELECT COUNT(*) as total FROM users")
    total = cursor.fetchone()['total']
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE status = 'PENDING'")
    pending = cursor.fetchone()['c']
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE status = 'APPROVED'")
    active = cursor.fetchone()['c']
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE status = 'SUSPENDED' OR status = 'BLOCKED'")
    suspended = cursor.fetchone()['c']
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE role = 'ADMIN' OR role = 'MASTER'")
    admins = cursor.fetchone()['c']
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE created_at > NOW() - INTERVAL '7 days'")
    new_7d = cursor.fetchone()['c']
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE plan = 'FREE' OR plan IS NULL")
    free_users = cursor.fetchone()['c']
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE plan = 'PRO'")
    pro_users = cursor.fetchone()['c']
    # Groups & files stats
    groups_total = 0
    files_total = 0
    files_size = 0
    try:
        cursor.execute("SELECT COUNT(*) as c FROM groups")
        groups_total = cursor.fetchone()['c']
    except Exception:
        storage.conn.rollback()
    try:
        cursor.execute("SELECT COUNT(*) as c, COALESCE(SUM(file_size), 0) as s FROM workspace_files")
        row = cursor.fetchone()
        files_total = row['c']
        files_size = row['s']
    except Exception:
        storage.conn.rollback()
    return {
        "users": {
            "total_users": total, "pending_users": pending, "active_users": active,
            "suspended_users": suspended, "admin_count": admins, "new_last_7d": new_7d,
            "free_users": free_users, "pro_users": pro_users
        },
        "groups": {"total_groups": groups_total},
        "files": {"total_files": files_total, "total_size": files_size}
    }

@app.get("/api/admin/users")
async def admin_list_users(
    status: Optional[str] = None,
    role: Optional[str] = None,
    search: Optional[str] = None,
    user=Depends(get_current_user)
):
    """List all users (admin only)"""
    if user.get('role') not in ('ADMIN', 'MASTER'):
        raise HTTPException(403, "Admin required")
    cursor = storage.conn.cursor(cursor_factory=storage.RealDictCursor)
    query = "SELECT id, email, name, role, status, plan, auth_provider, created_at FROM users WHERE 1=1"
    params = []
    if status:
        query += " AND status = %s"
        params.append(status)
    if role:
        query += " AND role = %s"
        params.append(role)
    if search:
        query += " AND (email ILIKE %s OR name ILIKE %s)"
        params.extend([f"%{search}%", f"%{search}%"])
    query += " ORDER BY created_at DESC LIMIT 500"
    cursor.execute(query, params)
    return {"users": cursor.fetchall()}

@app.put("/api/admin/users/{user_id}/role")
async def admin_update_role(user_id: str, role: str, user=Depends(get_current_user)):
    """Change a user's role (MASTER only for promoting to ADMIN)"""
    if user.get('role') not in ('ADMIN', 'MASTER'):
        raise HTTPException(403, "Admin required")
    valid_roles = ['USER', 'ADMIN', 'MASTER']
    if role not in valid_roles:
        raise HTTPException(400, f"Role must be one of: {valid_roles}")
    if role in ('ADMIN', 'MASTER') and user.get('role') != 'MASTER':
        raise HTTPException(403, "Only MASTER can promote to ADMIN/MASTER")
    success = storage.update_user(user_id, {'role': role})
    if not success:
        raise HTTPException(404, "User not found")
    if audit_log:
        try:
            audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                          resource_type="user", resource_id=user_id,
                          metadata={"action": "change_role", "new_role": role})
        except Exception: pass
    return {"success": True, "message": f"Role updated to {role}"}

@app.put("/api/admin/users/{user_id}/status")
async def admin_update_status(user_id: str, status: str, user=Depends(get_current_user)):
    """Change a user's status (APPROVED, SUSPENDED, BLOCKED, PENDING)"""
    if user.get('role') not in ('ADMIN', 'MASTER'):
        raise HTTPException(403, "Admin required")
    valid_statuses = ['APPROVED', 'PENDING', 'SUSPENDED', 'BLOCKED']
    if status not in valid_statuses:
        raise HTTPException(400, f"Status must be one of: {valid_statuses}")
    success = storage.update_user(user_id, {'status': status})
    if not success:
        raise HTTPException(404, "User not found")
    if audit_log:
        try:
            audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                          resource_type="user", resource_id=user_id,
                          metadata={"action": "change_status", "new_status": status})
        except Exception: pass
    return {"success": True, "message": f"Status updated to {status}"}

@app.get("/api/admin/users/{user_id}")
async def admin_get_user(user_id: str, user=Depends(get_current_user)):
    """Get user detail"""
    if user.get('role') not in ('ADMIN', 'MASTER'):
        raise HTTPException(403, "Admin required")
    target = storage.get_user(user_id)
    if not target:
        raise HTTPException(404, "User not found")
    target.pop('password_hash', None)
    return target

@app.delete("/api/admin/users/{user_id}")
async def admin_delete_user(user_id: str, user=Depends(get_current_user)):
    """Delete a user (MASTER only)"""
    if user.get('role') != 'MASTER':
        raise HTTPException(403, "Only MASTER can delete users")
    if str(user.get('id')) == user_id:
        raise HTTPException(400, "Cannot delete yourself")
    cursor = storage.conn.cursor()
    cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
    if cursor.rowcount == 0:
        raise HTTPException(404, "User not found")
    return {"success": True, "message": "User deleted"}


@app.get("/api/admin/dbconn")
async def admin_list_all_dbconn(user=Depends(get_current_user)):
    """List ALL DB connections across all users (admin only)"""
    if user.get('role') not in ('ADMIN', 'MASTER'):
        raise HTTPException(403, "Admin required")
    if not db_connector_manager:
        return {"connections": [], "message": "DB Connector non configurato"}
    try:
        cursor = db_connector_manager.conn.cursor(cursor_factory=db_connector_manager.RealDictCursor)
        cursor.execute("""
            SELECT dc.id, dc.user_id, dc.name, dc.db_type, dc.default_schema,
                   dc.created_at, dc.last_used_at, dc.last_test_status, dc.last_test_message,
                   u.email as user_email, u.name as user_name
            FROM db_connections dc
            LEFT JOIN users u ON CAST(dc.user_id AS INTEGER) = u.id
            ORDER BY dc.created_at DESC
        """)
        return {"connections": [dict(r) for r in cursor.fetchall()]}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.delete("/api/admin/dbconn/{conn_id}")
async def admin_delete_dbconn(conn_id: str, user=Depends(get_current_user)):
    """Delete any DB connection (admin only)"""
    if user.get('role') not in ('ADMIN', 'MASTER'):
        raise HTTPException(403, "Admin required")
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    cursor = db_connector_manager.conn.cursor()
    cursor.execute("DELETE FROM db_connections WHERE id = %s", (conn_id,))
    if cursor.rowcount == 0:
        raise HTTPException(404, "Connessione non trovata")
    return {"success": True}


# ===========================================================================
# AUDIT LOG ENDPOINTS
# ===========================================================================

class AuditLevelRequest(BaseModel):
    level: str  # MINIMAL | STANDARD | FULL

@app.get("/api/admin/audit/logs")
async def get_audit_logs(
    request: Request,
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    outcome: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    resource_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    user = Depends(get_current_user)
):
    """Legge i log di audit con filtri."""
    if not audit_log:
        raise HTTPException(503, "Audit log not available (requires PostgreSQL)")
    result = audit_log.query(
        user_id=user_id, action=action, outcome=outcome,
        date_from=date_from, date_to=date_to, resource_type=resource_type,
        limit=limit, offset=offset,
        requester_role=user.get("role", "USER"),
        requester_id=str(user.get("id", ""))
    )
    if audit_log:
        audit_log.log(AuditAction.FILE_VIEW, user=user,
                      resource_type="audit_logs",
                      ip_address=request.client.host if request.client else None)
    return result


@app.get("/api/admin/audit/stats")
async def get_audit_stats(user = Depends(get_current_user)):
    """Statistiche dei log di audit."""
    if not audit_log:
        raise HTTPException(503, "Audit log not available")
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Admin required")
    return audit_log.get_stats()


@app.get("/api/admin/audit/export")
async def export_audit_logs(
    request: Request,
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    outcome: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    user = Depends(get_current_user)
):
    """Esporta log come CSV."""
    if not audit_log:
        raise HTTPException(503, "Audit log not available")
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Admin required")
    csv_data = audit_log.export_csv(
        user_id=user_id, action=action, outcome=outcome,
        date_from=date_from, date_to=date_to,
        requester_role=user.get("role", "MASTER"),
        requester_id=str(user.get("id", ""))
    )
    return Response(
        content=csv_data.encode("utf-8"),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=audit_log_{datetime.now().strftime('%Y%m%d')}.csv"}
    )


@app.get("/api/admin/audit/level")
async def get_audit_level(user = Depends(get_current_user)):
    """Legge il livello di log corrente."""
    if not audit_log:
        raise HTTPException(503, "Audit log not available")
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Admin required")
    return {"level": audit_log.get_level().value}


@app.put("/api/admin/audit/level")
async def set_audit_level(req: AuditLevelRequest, user = Depends(get_current_user)):
    """Cambia il livello di log (MINIMAL | STANDARD | FULL)."""
    if not audit_log:
        raise HTTPException(503, "Audit log not available")
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Admin required")
    try:
        level = AuditLevel(req.level.upper())
    except ValueError:
        raise HTTPException(400, f"Invalid level. Choose: MINIMAL, STANDARD, FULL")
    audit_log.set_level(level, updated_by=user.get("email"))
    if audit_log:
        audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                      resource_type="audit_level",
                      metadata={"new_level": level.value})
    return {"success": True, "level": level.value}


# ===========================================================================
# JOB ENGINE ENDPOINTS
# ===========================================================================

@app.get("/api/jobs")
async def list_jobs(
    status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    user = Depends(get_current_user)
):
    """Elenca i job dell'utente corrente."""
    if not job_engine:
        raise HTTPException(503, "Job engine not available")
    return job_engine.list_jobs(
        user_id=str(user.get("id", "")),
        is_admin=user.get("role") in ("MASTER", "ADMIN"),
        status=status, limit=limit, offset=offset
    )


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str, user = Depends(get_current_user)):
    """Stato di un job (polling)."""
    if not job_engine:
        raise HTTPException(503, "Job engine not available")
    job = job_engine.get_job(
        job_id,
        user_id=str(user.get("id", "")),
        is_admin=user.get("role") in ("MASTER", "ADMIN")
    )
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.delete("/api/jobs/{job_id}")
async def cancel_job(job_id: str, user = Depends(get_current_user)):
    """Cancella un job in esecuzione."""
    if not job_engine:
        raise HTTPException(503, "Job engine not available")
    ok = job_engine.cancel_job(
        job_id,
        user_id=str(user.get("id", "")),
        is_admin=user.get("role") in ("MASTER", "ADMIN")
    )
    if not ok:
        raise HTTPException(400, "Cannot cancel job (not found or already completed)")
    if audit_log:
        audit_log.log(AuditAction.JOB_CANCEL, user=user, resource_type="job", resource_id=job_id)
    return {"success": True}


@app.post("/api/transform/execute-async")
async def execute_transform_async(
    request: Request,
    file: UploadFile = File(...),
    output_format: str = Form("xml"),
    validate: bool = Form(False),
    mapping_rules: str = Form(None),
    input_schema: str = Form(None),    # Schema directory name (e.g. 'FatturaPA')
    output_schema: str = Form(None),   # Schema directory name (e.g. 'UBL-21')
    user = Depends(get_current_user)
):
    """
    Esegue una trasformazione in modo asincrono.
    Ritorna immediatamente un job_id; il frontend fa polling su /api/jobs/{id}.
    """
    if not job_engine:
        raise HTTPException(503, "Job engine not available")

    content = await file.read()
    file_size = len(content)

    # Piano check
    if billing_manager:
        ok, reason = billing_manager.check_limit(str(user.get("id", "")), "transform",
                                                   file_size_bytes=file_size)
        if not ok:
            raise HTTPException(402, reason)
        ok2, reason2 = billing_manager.check_limit(str(user.get("id", "")), "async_transform")
        if not ok2:
            raise HTTPException(402, reason2)
    fname = file.filename or "input"

    # Salva il file in una location temporanea accessibile al job
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(fname)[1] or ".dat")
    tmp.write(content)
    tmp.close()
    tmp_path = tmp.name

    rules = {}
    if mapping_rules:
        try:
            rules = json.loads(mapping_rules)
        except Exception:
            pass

    params = {
        "tmp_path": tmp_path,
        "file_name": fname,
        "output_format": output_format,
        "validate": validate,
        "mapping_rules": rules,
        "input_schema": input_schema,
        "output_schema": output_schema,
    }

    job_id = job_engine.create_job(
        JobType.TRANSFORM, user=user,
        input_params=params,
        input_file_name=fname,
        input_size_bytes=file_size
    )

    if audit_log:
        audit_log.log(AuditAction.TRANSFORM_ASYNC, user=user,
                      resource_type="transform", resource_id=job_id,
                      file_name=fname, file_size_bytes=file_size,
                      input_format="auto", output_format=output_format,
                      ip_address=request.client.host if request.client else None)

    async def transform_handler(jid, p, progress_cb):
        import time
        start = time.time()
        progress_cb(5)
        with open(p["tmp_path"], "r", encoding="utf-8", errors="replace") as f:
            file_content = f.read()
        progress_cb(20)

        stripped = file_content.strip()
        if stripped.startswith("<"):   input_fmt = "xml"
        elif stripped.startswith(("{", "[")): input_fmt = "json"
        else: input_fmt = "csv"

        # Get validation files (XSD and Schematron) using explicit schema names
        input_xsd, input_sch, output_xsd, output_sch = get_validation_files(
            input_fmt,
            p["output_format"],
            input_content=file_content,
            mapping_rules=p["mapping_rules"],
            input_schema=p.get("input_schema"),
            output_schema=p.get("output_schema")
        )

        engine_instance = TransformationEngine(
            input_xsd=input_xsd,
            output_xsd=output_xsd,
            input_schematron=input_sch,
            output_schematron=output_sch
        )
        progress_cb(40)

        # Normalize output_format for engine (expects 'xml', 'json', 'csv')
        _engine_fmt = p["output_format"].lower()
        if _engine_fmt not in ('xml', 'json', 'csv'):
            _engine_fmt = 'xml'  # UBL-21, FatturaPA, PEPPOL, etc. are all XML

        result = engine_instance.transform(
            input_content=file_content,
            input_format=input_fmt,
            output_format=_engine_fmt,
            mapping_rules=p["mapping_rules"],
            validate_input=p["validate"],
            validate_output=p["validate"]
        )
        progress_cb(80)

        # Salva output su file temporaneo
        _ext_map = {'json': 'json', 'csv': 'csv'}
        _out_ext = _ext_map.get(_engine_fmt, 'xml')
        out_tmp = p["tmp_path"] + f".output.{_out_ext}"
        output_content = result.output_content or ""
        with open(out_tmp, "w", encoding="utf-8") as f:
            f.write(output_content)
        progress_cb(95)

        os.unlink(p["tmp_path"])

        duration = int((time.time() - start) * 1000)
        # Usage tracking per billing
        if billing_manager and result.success and user:
            try:
                uid = user.get("id") if user else None
                if uid:
                    billing_manager.increment_usage(str(uid), "transforms_count")
                    billing_manager.increment_usage(str(uid), "bytes_processed", len(file_content))
            except Exception: pass
        if audit_log:
            audit_log.log(
                AuditAction.JOB_COMPLETE if result.success else AuditAction.JOB_FAIL,
                outcome=AuditOutcome.SUCCESS if result.success else AuditOutcome.FAILURE,
                user={"id": "system"},
                resource_type="transform", resource_id=jid,
                file_name=p["file_name"], file_size_bytes=len(file_content),
                input_format=input_fmt, output_format=p["output_format"],
                duration_ms=duration,
                output_preview=output_content[:500] if output_content else None
            )

        return {
            "success": result.success,
            "output_file_path": out_tmp,
            "output_file_name": f"output.{p['output_format']}",
            "output_size_bytes": len(output_content),
            "errors": result.validation_errors if hasattr(result, "validation_errors") else []
        }

    await job_engine.submit(job_id, transform_handler, params)
    return {"job_id": job_id, "status": "PENDING"}


@app.get("/api/jobs/{job_id}/download")
async def download_job_output(job_id: str, user = Depends(get_current_user)):
    """Scarica l'output di un job completato."""
    if not job_engine:
        raise HTTPException(503, "Job engine not available")
    job = job_engine.get_job(job_id, str(user.get("id", "")),
                              user.get("role") in ("MASTER", "ADMIN"))
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] != "COMPLETED":
        raise HTTPException(400, f"Job not completed (status: {job['status']})")
    out_path = job.get("output_file_path")
    if not out_path or not os.path.exists(out_path):
        raise HTTPException(404, "Output file not found")
    return FileResponse(
        out_path,
        filename=job.get("output_file_name", "output"),
        media_type="application/octet-stream"
    )


# ===========================================================================
# CODE GENERATION ENDPOINTS
# ===========================================================================

class CodeGenRequest(BaseModel):
    project_name: str
    mapping_rules: Dict
    save_engine: bool = True   # salva buddyliko_engine.py internamente


@app.post("/api/codegen/generate")
async def generate_code(
    request: Request,
    req: CodeGenRequest,
    user = Depends(get_current_user)
):
    """
    Genera codice Python standalone + Python engine + C# da una mappatura.
    Ritorna uno ZIP scaricabile.
    Salva anche buddyliko_engine.py internamente per uso come motore ad alta performance.
    """
    try:
        connections = req.mapping_rules.get("connections", [])
        if not connections:
            raise HTTPException(400, "No connections in mapping rules")

        # Piano check
        if billing_manager:
            ok, reason = billing_manager.check_limit(str(user.get("id", "")), "code_generation")
            if not ok:
                raise HTTPException(402, reason)

        # Genera ZIP
        zip_bytes = generate_zip(req.mapping_rules, req.project_name)

        # Salva engine module internamente
        engine_path = None
        if req.save_engine:
            try:
                engine_path = save_engine_module(
                    req.mapping_rules, req.project_name, GENERATED_ENGINES_DIR
                )
                print(f"✅ Engine saved: {engine_path}")
            except Exception as _se:
                print(f"⚠️  Engine save failed: {_se}")

        if audit_log:
            audit_log.log(
                AuditAction.CODE_GENERATE, user=user,
                resource_type="codegen",
                metadata={
                    "project_name": req.project_name,
                    "connections": len(connections),
                    "engine_saved": engine_path is not None,
                    "zip_size": len(zip_bytes)
                },
                ip_address=request.client.host if request.client else None
            )
        if billing_manager:
            try:
                uid = user.get("id") if user else None
                if uid:
                    billing_manager.increment_usage(str(uid), "codegen_count")
            except Exception: pass

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", req.project_name)
        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={safe_name}_transformer.zip"}
        )

    except HTTPException:
        raise
    except Exception as e:
        if audit_log:
            audit_log.log(AuditAction.CODE_GENERATE,
                          outcome=AuditOutcome.FAILURE,
                          user=user, error_message=str(e)[:300])
        raise HTTPException(500, f"Code generation failed: {str(e)}")


@app.get("/api/codegen/engines")
async def list_engines(user = Depends(get_current_user)):
    """Elenca i motori Python generati e salvati internamente."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Admin required")
    engines = []
    try:
        for fname in os.listdir(GENERATED_ENGINES_DIR):
            if fname.endswith("_engine.py"):
                path = os.path.join(GENERATED_ENGINES_DIR, fname)
                stat = os.stat(path)
                engines.append({
                    "name": fname,
                    "path": path,
                    "size_bytes": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
    except Exception:
        pass
    return {"engines": engines}


@app.post("/api/codegen/execute-engine")
async def execute_with_engine(
    request: Request,
    engine_name: str = Form(...),
    file: UploadFile = File(...),
    user = Depends(get_current_user)
):
    """
    Esegue una trasformazione usando un motore Python generato (alta performance).
    Il motore è pre-compilato dalla mappa: zero overhead di interpretazione.
    """
    import importlib.util
    engine_path = os.path.join(GENERATED_ENGINES_DIR, engine_name)
    if not os.path.exists(engine_path):
        raise HTTPException(404, f"Engine '{engine_name}' not found")

    content = (await file.read()).decode("utf-8", errors="replace")

    # Carica il modulo dinamicamente
    spec = importlib.util.spec_from_file_location("buddyliko_engine", engine_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Parse input
    stripped = content.strip()
    if stripped.startswith("<"):
        import xml.etree.ElementTree as ET
        root = ET.fromstring(content)
        def xml_to_dict(el):
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            children = list(el)
            if not children:
                return {tag: el.text or ""}
            d = {}
            for c in children:
                sub = xml_to_dict(c)
                for k, v in sub.items():
                    if k in d:
                        if not isinstance(d[k], list): d[k] = [d[k]]
                        d[k].append(v)
                    else:
                        d[k] = v
            return {tag: d}
        input_data = xml_to_dict(root)
    elif stripped.startswith(("{", "[")):
        input_data = json.loads(content)
    else:
        import csv, io
        input_data = list(csv.DictReader(io.StringIO(content)))

    import time
    start = time.time()
    result, warnings = module.transform(input_data)
    duration = int((time.time() - start) * 1000)

    if audit_log:
        audit_log.log(AuditAction.TRANSFORM, user=user,
                      resource_type="engine_transform",
                      file_name=file.filename, file_size_bytes=len(content),
                      duration_ms=duration,
                      metadata={"engine": engine_name, "warnings": len(warnings)},
                      ip_address=request.client.host if request.client else None)
    if billing_manager:
        try:
            uid = user.get("id") if user else None
            if uid:
                billing_manager.increment_usage(str(uid), "transforms_count")
                billing_manager.increment_usage(str(uid), "bytes_processed", len(content))
        except Exception: pass

    return {
        "success": True,
        "result": result,
        "warnings": warnings,
        "duration_ms": duration,
        "engine": engine_name,
        "metadata": module.get_metadata() if hasattr(module, "get_metadata") else {}
    }


# =============================================================================
# STRIPE BILLING ENDPOINTS
# =============================================================================

@app.get("/api/billing/config")
async def get_billing_config():
    """Public config: Stripe publishable key + plan prices (no auth needed)."""
    from stripe_billing import PLAN_PRICES_EUR, PLAN_LIMITS, Plan
    return {
        "stripe_publishable_key": APP_CONFIG.get("billing", {}).get("stripe_publishable_key", ""),
        "billing_enabled": billing_manager.enabled if billing_manager else False,
        "plans": {
            p.value: {
                "prices": PLAN_PRICES_EUR.get(p, {}),
                "limits": PLAN_LIMITS.get(p, {})
            } for p in [Plan.FREE, Plan.PRO, Plan.ENTERPRISE]
        }
    }


@app.get("/api/billing/status")
async def get_billing_status(user = Depends(get_current_user)):
    """Ritorna piano corrente, limiti e usage del mese."""
    if not billing_manager:
        return {"plan": "FREE", "status": "active", "limits": PLAN_LIMITS.get("FREE", {}),
                "usage": {}, "billing_enabled": False}
    try:
        data = billing_manager.get_user_plan(str(user['id']))
        data["billing_enabled"] = billing_manager.enabled
        return data
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/billing/checkout")
async def create_checkout(request: Request, user = Depends(get_current_user)):
    """Crea Stripe Checkout Session. Body: {plan, billing_period, coupon?}"""
    if not billing_manager or not billing_manager.enabled:
        raise HTTPException(400, "Stripe non configurato. Aggiungi stripe_secret_key a config.yaml")
    body = await request.json()
    plan = body.get("plan", "PRO").upper()
    period = body.get("billing_period", "monthly")
    coupon = body.get("coupon")
    try:
        result = billing_manager.create_checkout_session(
            str(user["id"]), user.get("email", ""), plan, period, coupon
        )
        if audit_log:
            try:
                audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                              resource_type="billing_checkout",
                              metadata={"plan": plan, "period": period})
            except Exception: pass
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/billing/portal")
async def create_portal(user = Depends(get_current_user)):
    """Crea Stripe Customer Portal session."""
    if not billing_manager or not billing_manager.enabled:
        raise HTTPException(400, "Stripe non configurato")
    try:
        url = billing_manager.create_portal_session(str(user["id"]))
        return {"portal_url": url}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/billing/webhook")
async def stripe_webhook(request: Request):
    """Riceve eventi Stripe (checkout.session.completed, subscription.updated, ecc.)"""
    if not billing_manager:
        return {"received": True}
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")
    try:
        result = billing_manager.handle_webhook(payload, sig)
        return result
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/billing/admin/override")
async def admin_billing_override(request: Request, user = Depends(get_current_user)):
    """Admin assegna piano gratuito permanente (bypass Stripe)."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not billing_manager:
        raise HTTPException(400, "Billing manager non disponibile")
    body = await request.json()
    target_user_id = body.get("user_id")
    plan = body.get("plan", "PRO")
    note = body.get("note", "")
    if not target_user_id:
        raise HTTPException(400, "user_id richiesto")
    try:
        result = billing_manager.admin_set_plan(
            str(target_user_id), plan, user.get("email", "admin"), note
        )
        if audit_log:
            try:
                audit_log.log(AuditAction.SETTINGS_CHANGE, user=user,
                              resource_type="billing_override",
                              metadata={"target": target_user_id, "plan": plan})
            except Exception: pass
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/billing/usage/history")
async def get_usage_history(months: int = 6, user = Depends(get_current_user)):
    """Storico usage mese per mese dell'utente corrente (max 24 mesi)."""
    if not billing_manager:
        return {"history": []}
    try:
        history = billing_manager.get_usage_history(str(user["id"]), min(months, 24))
        return {"history": history}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/billing/usage/alerts")
async def get_usage_alerts(user = Depends(get_current_user)):
    """Alert quando ci si avvicina ai limiti: warning 80%, critical 95%, exceeded 100%."""
    if not billing_manager:
        return {"alerts": []}
    try:
        alerts = billing_manager.check_usage_alerts(str(user["id"]))
        return {"alerts": alerts}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/billing/admin/usage")
async def get_all_users_usage(month: str = None, user = Depends(get_current_user)):
    """Usage aggregato di tutti gli utenti per un dato mese — admin only."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not billing_manager:
        return {"usage": [], "month": month}
    try:
        usage = billing_manager.get_all_users_usage(month)
        return {"usage": usage, "month": month or "current"}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/billing/admin/stats")
async def get_billing_stats(user = Depends(get_current_user)):
    """Statistiche revenue per admin dashboard."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not billing_manager:
        return {"mrr_eur": 0, "by_plan": [], "monthly_usage": []}
    try:
        return billing_manager.get_revenue_stats()
    except Exception as e:
        raise HTTPException(500, str(e))


# =============================================================================
# DB CONNECTOR ENDPOINTS
# =============================================================================

@app.post("/api/dbconn/save")
async def save_db_connection(request: Request, user = Depends(get_current_user)):
    """Salva nuova connessione DB. Body: {name, db_type, connection_params}"""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    if billing_manager:
        ok, reason = billing_manager.check_limit(str(user["id"]), "db_connector")
        if not ok:
            raise HTTPException(402, reason)
    body = await request.json()
    name = body.get("name")
    db_type = body.get("db_type")
    params = body.get("connection_params", {})
    if not name or not db_type:
        raise HTTPException(400, "name e db_type richiesti")
    try:
        conn_id = db_connector_manager.save_connection(str(user["id"]), name, db_type, params)
        if audit_log:
            try:
                audit_log.log(AuditAction.PROJECT_SAVE, user=user,
                              resource_type="db_connection", resource_id=conn_id,
                              metadata={"name": name, "db_type": db_type})
            except Exception: pass
        return {"success": True, "connection_id": conn_id}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/dbconn/list")
async def list_db_connections(user = Depends(get_current_user)):
    if not db_connector_manager:
        return {"connections": []}
    try:
        conns = db_connector_manager.list_connections(str(user["id"]))
        return {"connections": conns}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/api/dbconn/{conn_id}")
async def delete_db_connection(conn_id: str, user = Depends(get_current_user)):
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    ok = db_connector_manager.delete_connection(conn_id, str(user["id"]))
    if not ok:
        raise HTTPException(404, "Connessione non trovata")
    return {"success": True}

# ── AI BALANCE ESTIMATION ────────────────────────────────────────────────────

@app.get("/api/finance/ai-balance/estimated")
async def get_ai_balance_estimated(user = Depends(get_current_user)):
    """Saldo stimato Anthropic calcolato da ultimo saldo noto - spesa tracciata."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not ai_token_tracker:
        return {"available": False}
    result = ai_token_tracker.get_estimated_balance("anthropic")
    # Aggiungi stats del mese
    result["month_stats"] = ai_token_tracker.get_month_stats()
    result["today_spend"] = ai_token_tracker.get_today_spend()
    result["daily_history"] = ai_token_tracker.get_spend_by_day(days=30)
    return {"available": True, **result}

@app.post("/api/finance/ai-balance/calibrate")
async def calibrate_ai_balance(body: dict, user = Depends(get_current_user)):
    """Ricalibra il saldo — inserisci il valore letto da console.anthropic.com."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not ai_token_tracker:
        raise HTTPException(400, "Token tracker non disponibile")
    balance = body.get("balance_usd")
    if balance is None:
        raise HTTPException(400, "balance_usd richiesto")
    ai_token_tracker.save_balance(
        provider=body.get("provider", "anthropic"),
        balance_usd=float(balance),
        auto_recharge=body.get("auto_recharge", True),
        recharge_amount=body.get("recharge_amount", 15.0),
        recharge_threshold=body.get("recharge_threshold", 5.0),
        source="manual"
    )
    return {
        "success": True,
        "message": f"Saldo calibrato a ${float(balance):.2f}",
        "new_balance": ai_token_tracker.get_estimated_balance(body.get("provider", "anthropic"))
    }



@app.post("/api/dbconn/{conn_id}/test")
async def test_db_connection(conn_id: str, user = Depends(get_current_user)):
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    try:
        result = db_connector_manager.test_connection(conn_id, str(user["id"]))
        return result
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.get("/api/dbconn/{conn_id}/tables")
async def list_db_tables(conn_id: str, user = Depends(get_current_user)):
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    try:
        tables = db_connector_manager.list_tables(conn_id, str(user["id"]))
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/dbconn/{conn_id}/schema/{table_name}")
async def get_db_table_schema(conn_id: str, table_name: str, user = Depends(get_current_user)):
    """Ritorna schema tabella in formato Buddyliko (pronto per il mapper)."""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    try:
        schema = db_connector_manager.get_table_schema(conn_id, str(user["id"]), table_name)
        return schema
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/dbconn/{conn_id}/preview")
async def preview_db_table(conn_id: str, request: Request, user = Depends(get_current_user)):
    """Anteprima dati tabella (prime 50 righe)."""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    body = await request.json()
    table = body.get("table_name")
    limit = min(int(body.get("limit", 50)), 200)
    where = body.get("where_clause")
    if not table:
        raise HTTPException(400, "table_name richiesto")
    try:
        return db_connector_manager.preview_data(conn_id, str(user["id"]), table, limit, where)
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/dbconn/{conn_id}/execute-write")
async def execute_db_write(conn_id: str, request: Request, user = Depends(get_current_user)):
    """Scrivi righe trasformate nel DB di destinazione."""
    if not db_connector_manager:
        raise HTTPException(400, "DB Connector non disponibile")
    body = await request.json()
    table = body.get("table_name")
    rows = body.get("rows", [])
    mode = body.get("mode", "insert")
    pk_cols = body.get("pk_columns", [])
    if not table or not rows:
        raise HTTPException(400, "table_name e rows richiesti")
    try:
        result = db_connector_manager.execute_write(
            conn_id, str(user["id"]), table, rows, mode, pk_cols
        )
        if audit_log:
            try:
                audit_log.log(AuditAction.TRANSFORM, user=user,
                              resource_type="db_write",
                              metadata={"table": table, "rows": len(rows),
                                        "mode": mode, "inserted": result.get("inserted", 0)})
            except Exception: pass
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


# =============================================================================
# EDI ENDPOINTS (X12 / EDIFACT)
# =============================================================================

@app.post("/api/edi/parse")
async def parse_edi_file(
    file: UploadFile = File(...),
    user = Depends(get_current_user)
):
    """Parsa file EDI X12 o EDIFACT. Ritorna struttura navigabile + schema."""
    if not edi_available:
        raise HTTPException(400, "EDI Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_edi(content)
        schema = edi_to_schema(parsed, file.filename.split(".")[0])
        flat = edi_to_flat(parsed)
        if audit_log:
            try:
                audit_log.log(AuditAction.FILE_UPLOAD, user=user,
                              resource_type="edi",
                              file_name=file.filename,
                              metadata={"format": parsed.get("format"),
                                        "summary": parsed.get("_summary")})
            except Exception: pass
        return {
            "success": True,
            "format": parsed.get("format"),
            "summary": parsed.get("_summary"),
            "parsed": parsed,
            "schema": schema,
            "flat_records": flat[:10],   # Preview prime 10 righe
            "total_records": len(flat),
        }
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/edi/schema-from-file")
async def edi_schema_from_file(
    file: UploadFile = File(...),
    schema_name: str = Form(None),
    user = Depends(get_current_user)
):
    """Genera schema Buddyliko da file EDI — pronto per usare nel mapper."""
    if not edi_available:
        raise HTTPException(400, "EDI Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_edi(content)
        name = schema_name or file.filename.rsplit(".", 1)[0]
        schema = edi_to_schema(parsed, name)
        return schema
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/edi/build")
async def build_edi_endpoint(request: Request, user = Depends(get_current_user)):
    """Genera EDI X12 o EDIFACT da dict strutturato.
    Body: {format: 'X12'|'EDIFACT', transaction_type: '850', data: {...}}"""
    if not edi_available:
        raise HTTPException(400, "EDI Parser non disponibile")
    body = await request.json()
    fmt = body.get("format", "X12").upper()
    tx_type = body.get("transaction_type", "850")
    data = body.get("data", {})
    try:
        edi_content = build_edi(data, fmt=fmt, transaction_type=tx_type)
        return Response(
            content=edi_content,
            media_type="text/plain",
            headers={"Content-Disposition": f'attachment; filename="output.{fmt.lower()}.edi"'}
        )
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/edi/detect")
async def detect_edi_endpoint(file: UploadFile = File(...), user = Depends(get_current_user)):
    """Rileva formato EDI del file caricato."""
    content = (await file.read())[:512].decode("utf-8", errors="replace")
    fmt = detect_edi_format(content)
    return {"format": fmt, "filename": file.filename, "edi_available": edi_available}


# =============================================================================
# HL7 ENDPOINTS (v2 + FHIR)
# =============================================================================

@app.post("/api/hl7/parse")
async def parse_hl7_file(
    file: UploadFile = File(...),
    user = Depends(get_current_user)
):
    """Parsa file HL7 v2 o FHIR JSON/XML. Ritorna struttura + schema."""
    if not hl7_available:
        raise HTTPException(400, "HL7 Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_hl7(content)
        schema = to_buddyliko_schema_hl7(parsed, file.filename.split(".")[0])
        fmt = parsed.get("format", "")
        flat = hl7v2_to_flat(parsed) if fmt == "HL7v2" else fhir_to_flat(parsed)
        if audit_log:
            try:
                audit_log.log(AuditAction.FILE_UPLOAD, user=user,
                              resource_type="hl7",
                              file_name=file.filename,
                              metadata={"format": fmt})
            except Exception: pass
        return {
            "success": True,
            "format": fmt,
            "resource_type": parsed.get("resource_type", parsed.get("message_type", "")),
            "parsed": parsed,
            "schema": schema,
            "flat": flat,
        }
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/hl7/schema-from-file")
async def hl7_schema_from_file(
    file: UploadFile = File(...),
    schema_name: str = Form(None),
    user = Depends(get_current_user)
):
    """Genera schema Buddyliko da file HL7 — pronto per il mapper."""
    if not hl7_available:
        raise HTTPException(400, "HL7 Parser non disponibile")
    content = (await file.read()).decode("utf-8", errors="replace")
    try:
        parsed = parse_hl7(content)
        name = schema_name or file.filename.rsplit(".", 1)[0]
        schema = to_buddyliko_schema_hl7(parsed, name)
        return schema
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/hl7/build-v2")
async def build_hl7v2_endpoint(request: Request, user = Depends(get_current_user)):
    """Genera messaggio HL7 v2 da dict.
    Body: {message_type: 'ADT^A01', data: {...}}"""
    if not hl7_available:
        raise HTTPException(400, "HL7 Parser non disponibile")
    body = await request.json()
    msg_type = body.get("message_type", "ADT^A01")
    data = body.get("data", {})
    try:
        hl7_content = build_hl7v2(data, message_type=msg_type)
        return Response(
            content=hl7_content,
            media_type="text/plain",
            headers={"Content-Disposition": f'attachment; filename="output.hl7"'}
        )
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/hl7/detect")
async def detect_hl7_endpoint(file: UploadFile = File(...), user = Depends(get_current_user)):
    """Rileva formato HL7 del file caricato."""
    content = (await file.read())[:512].decode("utf-8", errors="replace")
    fmt = detect_hl7_format(content)
    return {"format": fmt, "filename": file.filename, "hl7_available": hl7_available}


# =============================================================================
# BILLING USAGE HISTORY & EXPORT
# =============================================================================

@app.get("/api/billing/usage/history")
async def get_usage_history(
    months: int = 6,
    user = Depends(get_current_user)
):
    """Storico usage mensile degli ultimi N mesi."""
    if not billing_manager:
        return {"history": []}
    from datetime import datetime, timezone, timedelta
    cur = billing_manager.conn.cursor(cursor_factory=billing_manager.RealDictCursor)
    # Genera lista mesi da oggi indietro
    now = datetime.now(timezone.utc)
    month_list = []
    for i in range(months):
        d = now.replace(day=1) - timedelta(days=i*28)
        month_list.append(d.strftime('%Y-%m'))

    cur.execute("""
        SELECT month, transforms_count, api_calls_count,
               bytes_processed, codegen_count
        FROM usage_counters
        WHERE user_id = %s AND month = ANY(%s)
        ORDER BY month DESC
    """, (str(user["id"]), month_list))
    rows = [dict(r) for r in cur.fetchall()]

    # Riempie i mesi senza dati con zero
    existing = {r['month'] for r in rows}
    for m in month_list:
        if m not in existing:
            rows.append({"month": m, "transforms_count": 0,
                         "api_calls_count": 0, "bytes_processed": 0, "codegen_count": 0})
    rows.sort(key=lambda r: r["month"], reverse=True)
    return {"history": rows, "user_id": str(user["id"])}


@app.get("/api/billing/usage/export")
async def export_usage_csv(
    months: int = 12,
    user = Depends(get_current_user)
):
    """Esporta usage CSV per l'utente corrente."""
    if not billing_manager:
        return Response("month,transforms,api_calls,bytes,codegen\n",
                        media_type="text/csv")
    from datetime import datetime, timezone, timedelta
    import io
    cur = billing_manager.conn.cursor(cursor_factory=billing_manager.RealDictCursor)
    now = datetime.now(timezone.utc)
    month_list = []
    for i in range(months):
        d = now.replace(day=1) - timedelta(days=i*28)
        month_list.append(d.strftime('%Y-%m'))

    cur.execute("""
        SELECT month, transforms_count, api_calls_count,
               bytes_processed, codegen_count
        FROM usage_counters
        WHERE user_id = %s AND month = ANY(%s)
        ORDER BY month DESC
    """, (str(user["id"]), month_list))
    rows = cur.fetchall()

    buf = io.StringIO()
    buf.write("month,transforms_count,api_calls_count,bytes_processed,codegen_count\n")
    existing = {r['month'] for r in rows}
    for row in rows:
        buf.write(f"{row['month']},{row['transforms_count']},"
                  f"{row['api_calls_count']},{row['bytes_processed']},"
                  f"{row['codegen_count']}\n")
    for m in sorted(month_list, reverse=True):
        if m not in existing:
            buf.write(f"{m},0,0,0,0\n")

    email = user.get("email", "user").split("@")[0]
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="usage_{email}_{now.strftime("%Y%m")}.csv"'}
    )


@app.get("/api/billing/admin/usage-all")
async def admin_usage_all(
    month: str = None,
    user = Depends(get_current_user)
):
    """Admin: usage di tutti gli utenti per un mese."""
    if user.get("role") not in ("MASTER", "ADMIN"):
        raise HTTPException(403, "Solo admin")
    if not billing_manager:
        return {"usage": []}
    from datetime import datetime, timezone
    target_month = month or datetime.now(timezone.utc).strftime('%Y-%m')
    cur = billing_manager.conn.cursor(cursor_factory=billing_manager.RealDictCursor)
    cur.execute("""
        SELECT u.user_id, us.email, us.name, us.plan,
               u.transforms_count, u.api_calls_count,
               u.bytes_processed, u.codegen_count
        FROM usage_counters u
        LEFT JOIN users us ON us.id::text = u.user_id
        WHERE u.month = %s
        ORDER BY u.transforms_count DESC
    """, (target_month,))
    rows = [dict(r) for r in cur.fetchall()]
    return {"month": target_month, "usage": rows, "total_users": len(rows)}


if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Visual Mapping System API...")
    print("📖 Swagger UI: http://localhost:8080/docs")
    print("🤖 AI Auto-Map: Requires ANTHROPIC_API_KEY or OPENAI_API_KEY in .env")
    print("🔄 Reverse Mapping: /api/mapping/reverse")
    print("👁️ Preview Extraction: /api/preview/extract (XML/JSON)")
    print("🏗️ Schema Editor: /api/schema/* (Visual structure builder)")
    uvicorn.run(app, host="0.0.0.0", port=8080)
