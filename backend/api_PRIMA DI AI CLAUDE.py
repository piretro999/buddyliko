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

from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, Response
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import json
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import httpx

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
try:
    from diagram_generator import generate_svg as _generate_svg
    _diagram_available = True
except ImportError:
    _diagram_available = False
from auth_system import AuthManager
import yaml
from pathlib import Path
from fastapi import Depends, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

app = FastAPI(title="Visual Mapping System API", version="1.0.0")

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
CONFIG_PATH = Path(__file__).parent.parent / 'config.yml'
APP_CONFIG = {}
if CONFIG_PATH.exists():
    with open(CONFIG_PATH) as f:
        APP_CONFIG = yaml.safe_load(f)

# Storage
try:
    storage = StorageFactory.get_storage(APP_CONFIG.get('database', {}))
    print(f"✅ Storage: {type(storage).__name__}")
except:
    storage = None
    print("⚠️ Using in-memory storage")

# Authentication
AUTH_ENABLED = APP_CONFIG.get('auth', {}).get('enabled', False)
auth_manager = None
security = HTTPBearer(auto_error=False)

if AUTH_ENABLED and storage:
    secret_key = APP_CONFIG.get('auth', {}).get('secret_key', 'default-secret-key')
    token_expiry = APP_CONFIG.get('auth', {}).get('token_expiry_hours', 24)
    auth_manager = AuthManager(storage, secret_key, token_expiry)
    print(f"🔐 Authentication: ENABLED")
else:
    print(f"🔓 Authentication: DISABLED")

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

def get_validation_files(input_format: str, output_format: str, input_content: str = None, mapping_rules: dict = None):
    """
    Get XSD and Schematron files for input and output formats
    Auto-detects XML type if content provided
    Auto-detects UBL document type (Invoice, CreditNote, etc.)
    
    Returns:
        (input_xsd, input_sch, output_xsd, output_sch)
    """
    # Auto-detect XML format from content
    input_schema_type = None
    output_schema_type = None
    document_type = None
    
    if input_format.lower() == 'xml' and input_content:
        # Try to detect if it's FatturaPA or UBL
        if '<FatturaElettronica' in input_content or 'FatturaPA' in input_content:
            input_schema_type = 'fatturapa'
            print(f"  🔍 Auto-detected input: FatturaPA")
        elif '<Invoice' in input_content or 'UBL' in input_content:
            input_schema_type = 'ubl'
            print(f"  🔍 Auto-detected input: UBL")
        else:
            input_schema_type = 'fatturapa'  # Default
            print(f"  ⚠️  Could not detect XML type, defaulting to FatturaPA")
    else:
        # Map format types to schema directory names
        format_map = {
            'xml': 'fatturapa',  # Default
            'fatturapa': 'fatturapa',
            'ubl': 'ubl',
            'peppol': 'ubl',
            'idoc': None,  # IDOC doesn't use XSD
            'json': None,
            'csv': None
        }
        input_schema_type = format_map.get(input_format.lower())
    
    # Output format
    format_map = {
        'xml': 'ubl',  # Assume output is UBL by default
        'fatturapa': 'fatturapa',
        'ubl': 'ubl',
        'peppol': 'ubl',
        'idoc': None,
        'json': None,
        'csv': None
    }
    output_schema_type = format_map.get(output_format.lower())
    
    # Auto-detect UBL document type for output
    if output_schema_type in ['ubl', 'peppol']:
        document_type = detect_ubl_document_type(
            xml_content=input_content,
            mapping_rules=mapping_rules
        )
        print(f"  📄 Document type: {document_type}")
    
    # Find XSD files with document type awareness
    input_xsd = find_xsd(input_schema_type, 'input') if input_schema_type else None
    input_sch = find_schematron(input_schema_type, 'input') if input_schema_type else None
    
    # For output, pass document type to find the right XSD
    output_xsd = find_xsd(output_schema_type, 'output', document_type=document_type) if output_schema_type else None
    output_sch = find_schematron(output_schema_type, 'output') if output_schema_type else None
    
    if input_xsd or input_sch or output_xsd or output_sch:
        print(f"\n📋 Validation files:")
        if input_xsd: print(f"  Input XSD: {input_xsd}")
        if input_sch: print(f"  Input Schematron: {input_sch}")
        if output_xsd: print(f"  Output XSD: {output_xsd}")
        if output_sch: print(f"  Output Schematron: {output_sch}")
    
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

@app.post("/api/ai/auto-map", response_model=List[AISuggestion])
async def ai_auto_map(request: AIAutoMapRequest):
    """
    AI-powered automatic mapping suggestions using Claude or OpenAI
    """
    if not ANTHROPIC_API_KEY and not OPENAI_API_KEY:
        raise HTTPException(
            status_code=400,
            detail="No AI API keys configured. Add ANTHROPIC_API_KEY or OPENAI_API_KEY to .env file"
        )
    
    try:
        # Prepare prompt
        prompt = f"""You are a data mapping expert. Analyze these schemas and suggest field mappings.

INPUT SCHEMA ({len(request.input_fields)} fields):
{json.dumps(request.input_fields, indent=2)}

OUTPUT SCHEMA ({len(request.output_fields)} fields):
{json.dumps(request.output_fields, indent=2)}

{"INPUT DATA SAMPLE:\n" + request.input_sample + "\n" if request.input_sample else ""}
{"OUTPUT DATA SAMPLE:\n" + request.output_sample + "\n" if request.output_sample else ""}

For each output field, suggest which input field(s) to map from. Return ONLY a JSON array like this:
[
  {{
    "source_field": "input field path",
    "target_field": "output field path",
    "confidence": 0.95,
    "reasoning": "why this mapping makes sense",
    "suggested_formula": "optional formula if transformation needed"
  }}
]

Rules:
- confidence: 0-1 (0.9+ = very confident, 0.7-0.9 = confident, 0.5-0.7 = uncertain)
- Only suggest mappings with confidence > 0.5
- If multiple inputs needed, use formula like: "field1 && field2"
- Consider field names, business terms, descriptions, and data types
- Return ONLY valid JSON, no markdown"""

        suggestions = []
        
        # Try Claude first
        if ANTHROPIC_API_KEY:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    'https://api.anthropic.com/v1/messages',
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': ANTHROPIC_API_KEY,
                        'anthropic-version': '2023-06-01'
                    },
                    json={
                        'model': 'claude-sonnet-4-20250514',
                        'max_tokens': 4000,
                        'messages': [{'role': 'user', 'content': prompt}]
                    },
                    timeout=60.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    text = data['content'][0]['text']
                    
                    # Extract JSON from response
                    import re
                    json_match = re.search(r'\[[\s\S]*\]', text)
                    if json_match:
                        suggestions = json.loads(json_match.group(0))
        
        # Fallback to OpenAI if Claude failed or not available
        if not suggestions and OPENAI_API_KEY:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    'https://api.openai.com/v1/chat/completions',
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {OPENAI_API_KEY}'
                    },
                    json={
                        'model': 'gpt-4-turbo-preview',
                        'messages': [
                            {'role': 'system', 'content': 'You are a data mapping expert. Always respond with valid JSON only.'},
                            {'role': 'user', 'content': prompt}
                        ],
                        'temperature': 0.3,
                        'max_tokens': 4000
                    },
                    timeout=60.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    text = data['choices'][0]['message']['content']
                    
                    # Extract JSON from response
                    import re
                    json_match = re.search(r'\[[\s\S]*\]', text)
                    if json_match:
                        suggestions = json.loads(json_match.group(0))
        
        return suggestions
    
    except Exception as e:
        raise HTTPException(500, f"AI error: {str(e)}")


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
    mapping_rules: str = Form(None)  # JSON string of mapping rules
):
    """Execute transformation with mapping rules"""
    try:
        content = (await file.read()).decode('utf-8')
        
        print(f"\n{'='*60}")
        print(f"🔄 TRANSFORM EXECUTE")
        print(f"{'='*60}")
        print(f"📁 File: {file.filename}")
        print(f"📏 Content length: {len(content)} chars")
        print(f"📝 First 200 chars: {content[:200]}")
        
        # Detect input format
        if content.strip().startswith('<'):
            input_fmt = 'xml'
        elif content.strip().startswith('{'):
            input_fmt = 'json'
        else:
            input_fmt = 'csv'
        
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
        
        # Get validation files (XSD and Schematron) - auto-detect XML type and document type
        input_xsd, input_sch, output_xsd, output_sch = get_validation_files(
            input_fmt, 
            output_format,
            input_content=content,  # Pass content for auto-detection
            mapping_rules=rules     # Pass mapping rules for document type detection
        )
        
        # Create TransformationEngine with validation files
        engine = TransformationEngine(
            input_xsd=input_xsd,
            output_xsd=output_xsd,
            input_schematron=input_sch,
            output_schematron=output_sch
        )
        
        print(f"\n🚀 Calling transformation_engine.transform...")
        
        # Transform with actual mapping
        result = engine.transform(
            input_content=content,
            input_format=input_fmt,
            output_format=output_format,
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
        
        if result.success:
            return Response(
                content=result.output_content,
                media_type='application/xml',
                headers={'Content-Disposition': f'attachment; filename=output.{output_format}'}
            )
        else:
            return {"success": False, "errors": result.validation_errors}
    except Exception as e:
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
# DIAGRAM ENDPOINT
# ============================================================================

@app.post("/api/mapping/diagram")
async def generate_mapping_diagram(request: Request):
    """Generate SVG diagram of the current mapping"""
    if not _diagram_available:
        raise HTTPException(500, "diagram_generator.py non trovato nel backend")
    
    try:
        body = await request.json()
        connections = body.get("connections", [])
        project_name = body.get("projectName", "Mappatura")
        input_name = body.get("inputSchemaName", "Input")
        output_name = body.get("outputSchemaName", "Output")
        
        if not connections:
            raise HTTPException(400, "Nessuna connessione fornita")
        
        svg_content = _generate_svg(
            connections=connections,
            project_name=project_name,
            input_schema_name=input_name,
            output_schema_name=output_name
        )
        
        return Response(
            content=svg_content,
            media_type="image/svg+xml",
            headers={"Content-Disposition": f'attachment; filename="mapping_diagram.svg"'}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Errore generazione diagramma: {str(e)}")


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

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to get current user from JWT token"""
    if not AUTH_ENABLED:
        return {"id": "anonymous", "email": "anonymous@local", "name": "Anonymous"}
    
    if not credentials:
        raise HTTPException(401, "Authentication required")
    
    token = credentials.credentials
    try:
        valid, payload = auth_manager.verify_token(token)
        if not valid:
            raise HTTPException(401, "Invalid or expired token")
        return payload
    except Exception as e:
        raise HTTPException(401, f"Authentication error: {str(e)}")

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
    """Register new user with email/password"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    success, message, user_data = auth_manager.register_user(
        request.email, 
        request.password, 
        request.name
    )
    
    if not success:
        raise HTTPException(400, message)
    
    return {"success": True, "message": message, "user": user_data}

@app.post("/api/auth/login")
async def login(request: LoginRequest):
    """Login with email/password"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    success, message, user_data = auth_manager.login(request.email, request.password)
    
    if not success:
        raise HTTPException(401, message)
    
    return {"success": True, "message": message, "user": user_data, "token": user_data['token']}

@app.get("/api/auth/google/login")
async def google_login():
    """Redirect to Google OAuth"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    google_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('google', {})
    if not google_config.get('enabled'):
        raise HTTPException(400, "Google OAuth is not enabled")
    
    client_id = google_config.get('client_id')
    redirect_uri = google_config.get('redirect_uri')
    
    auth_url = (
        f"https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={client_id}&"
        f"redirect_uri={redirect_uri}&"
        f"response_type=code&"
        f"scope=openid email profile&"
        f"access_type=offline"
    )
    
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
            raise HTTPException(400, "Failed to exchange code for token")
        
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
    
    # Login or register user
    oauth_data = {
        'provider_id': user_info.get('id'),
        'email': user_info.get('email'),
        'name': user_info.get('name'),
        'picture': user_info.get('picture')
    }
    
    success, message, user_data = auth_manager.oauth_login('google', oauth_data)
    
    if not success:
        raise HTTPException(400, message)
    
    # Redirect to frontend with token
    token = user_data.get('token')
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"http://localhost:8000/?token={token}")

# ALSO ADD WITHOUT /api/ PREFIX for Google OAuth compatibility
@app.get("/auth/google/callback")
async def google_callback_no_prefix(code: str, state: Optional[str] = None):
    """Handle Google OAuth callback (alternative path without /api/)"""
    return await google_callback(code, state)

@app.get("/api/auth/facebook/login")
async def facebook_login():
    """Redirect to Facebook OAuth"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    fb_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('facebook', {})
    if not fb_config.get('enabled'):
        raise HTTPException(400, "Facebook OAuth is not enabled")
    
    app_id = fb_config.get('app_id')
    redirect_uri = fb_config.get('redirect_uri')
    
    auth_url = (
        f"https://www.facebook.com/v12.0/dialog/oauth?"
        f"client_id={app_id}&"
        f"redirect_uri={redirect_uri}&"
        f"scope=email,public_profile"
    )
    
    return {"auth_url": auth_url}

@app.get("/api/auth/facebook/callback")
async def facebook_callback(code: str):
    """Handle Facebook OAuth callback"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    fb_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('facebook', {})
    
    # Exchange code for token
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
            raise HTTPException(400, "Failed to exchange code for token")
        
        tokens = token_response.json()
        access_token = tokens.get('access_token')
        
        # Get user info
        user_response = await client.get(
            "https://graph.facebook.com/me",
            params={
                "fields": "id,name,email,picture",
                "access_token": access_token
            }
        )
        
        if user_response.status_code != 200:
            raise HTTPException(400, "Failed to get user info")
        
        user_info = user_response.json()
    
    # Login or register user
    oauth_data = {
        'provider_id': user_info.get('id'),
        'email': user_info.get('email'),
        'name': user_info.get('name'),
        'picture': user_info.get('picture', {}).get('data', {}).get('url')
    }
    
    success, message, user_data = auth_manager.oauth_login('facebook', oauth_data)
    
    if not success:
        raise HTTPException(400, message)
    
    # Redirect to frontend with token
    token = user_data.get('token')
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"http://localhost:8000/?token={token}")

# ALSO ADD WITHOUT /api/ PREFIX for Facebook OAuth compatibility
@app.get("/auth/facebook/callback")
async def facebook_callback_no_prefix(code: str):
    """Handle Facebook OAuth callback (alternative path without /api/)"""
    return await facebook_callback(code)

@app.get("/api/auth/github/login")
async def github_login():
    """Redirect to GitHub OAuth"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    gh_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('github', {})
    if not gh_config.get('enabled'):
        raise HTTPException(400, "GitHub OAuth is not enabled")
    
    client_id = gh_config.get('client_id')
    redirect_uri = gh_config.get('redirect_uri')
    
    auth_url = (
        f"https://github.com/login/oauth/authorize?"
        f"client_id={client_id}&"
        f"redirect_uri={redirect_uri}&"
        f"scope=user:email"
    )
    
    return {"auth_url": auth_url}

@app.get("/api/auth/github/callback")
async def github_callback(code: str):
    """Handle GitHub OAuth callback"""
    if not AUTH_ENABLED:
        raise HTTPException(400, "Authentication is disabled")
    
    gh_config = APP_CONFIG.get('auth', {}).get('oauth', {}).get('github', {})
    
    # Exchange code for token
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
            raise HTTPException(400, "Failed to exchange code for token")
        
        tokens = token_response.json()
        access_token = tokens.get('access_token')
        
        # Get user info
        user_response = await client.get(
            "https://api.github.com/user",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if user_response.status_code != 200:
            raise HTTPException(400, "Failed to get user info")
        
        user_info = user_response.json()
        
        # Get email (might be in separate endpoint)
        email = user_info.get('email')
        if not email:
            email_response = await client.get(
                "https://api.github.com/user/emails",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            emails = email_response.json()
            primary_email = next((e for e in emails if e.get('primary')), None)
            email = primary_email.get('email') if primary_email else None
    
    # Login or register user
    oauth_data = {
        'provider_id': str(user_info.get('id')),
        'email': email,
        'name': user_info.get('name') or user_info.get('login'),
        'picture': user_info.get('avatar_url')
    }
    
    success, message, user_data = auth_manager.oauth_login('github', oauth_data)
    
    if not success:
        raise HTTPException(400, message)
    
    # Redirect to frontend with token
    token = user_data.get('token')
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"http://localhost:8000/?token={token}")

# ALSO ADD WITHOUT /api/ PREFIX for GitHub OAuth compatibility
@app.get("/auth/github/callback")
async def github_callback_no_prefix(code: str):
    """Handle GitHub OAuth callback (alternative path without /api/)"""
    return await github_callback(code)

@app.get("/api/auth/me")
async def get_me(user = Depends(get_current_user)):
    """Get current user info"""
    return {"user": user}

@app.post("/api/auth/logout")
async def logout():
    """Logout (client should delete token)"""
    return {"success": True, "message": "Logged out successfully"}


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


if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Visual Mapping System API...")
    print("📖 Swagger UI: http://localhost:8080/docs")
    print("🤖 AI Auto-Map: Requires ANTHROPIC_API_KEY or OPENAI_API_KEY in .env")
    print("🔄 Reverse Mapping: /api/mapping/reverse")
    print("👁️ Preview Extraction: /api/preview/extract (XML/JSON)")
    print("🏗️ Schema Editor: /api/schema/* (Visual structure builder)")
    uvicorn.run(app, host="0.0.0.0", port=8080)

