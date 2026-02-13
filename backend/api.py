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

from fastapi import FastAPI, File, UploadFile, HTTPException
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

# Load environment variables
load_dotenv()

# Import our modules
from schema_parser import SchemaParser
from mapper_engine import MappingDefinition, MappingRule, MappingEngine
from idoc_parser import IDOCParser, IDOCDefinition
from csv_parser import CSVSchemaParser, MappingCSVExporter

# === INTEGRATED COMPONENTS (AUTO-ADDED) ===
from storage_layer import StorageFactory
from transformation_engine import TransformationEngine, XSDValidator
import yaml
from pathlib import Path

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

# Schemas
SCHEMAS_DIR = Path(__file__).parent.parent / 'schemas'
SCHEMAS_DIR.mkdir(exist_ok=True)

# Transformation
transformation_engine = TransformationEngine()

def find_xsd(format_name, io='input'):
    d = SCHEMAS_DIR / io / format_name
    if d.exists():
        xsd = list(d.glob("*.xsd"))
        if xsd: return str(xsd[0])
    return None


# Storage
SCHEMAS_DIR = Path("schemas")
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
# SCHEMA ENDPOINTS
# ============================================================================

@app.post("/api/schemas/upload")
async def upload_schema(
    file: UploadFile = File(...),
    name: str = "",
    schema_type: str = "auto",
    direction: str = "input"
):
    """Upload and parse schema file"""
    try:
        content = await file.read()
        filename = name or file.filename
        
        # Save file
        schema_dir = SCHEMAS_DIR / direction
        file_path = schema_dir / filename
        
        with open(file_path, 'wb') as f:
            f.write(content)
        
        # Parse schema
        parser = SchemaParser()
        
        if schema_type == "auto":
            # Auto-detect
            if filename.endswith('.xsd'):
                schema_type = "xsd"
            elif filename.endswith('.json'):
                schema_type = "json_schema"
            elif filename.endswith('.xml'):
                schema_type = "sample_xml"
            elif filename.endswith('.csv'):
                schema_type = "csv"
        
        if schema_type == "xsd":
            schema = parser.parse_xsd(str(file_path))
        elif schema_type == "json_schema":
            schema = parser.parse_json_schema(str(file_path))
        elif schema_type == "sample_xml":
            schema = parser.parse_sample_xml(str(file_path), name)
        elif schema_type == "idoc":
            schema = parser.parse_idoc_definition(str(file_path))
        elif schema_type == "csv":
            # Use CSV parser
            csv_parser = CSVSchemaParser()
            is_output = (direction == "output")
            schema = csv_parser.parse_csv(str(file_path), is_output=is_output)
            parser = csv_parser  # Use for tree generation
        else:
            raise HTTPException(400, "Unknown schema type")
        
        # Cache
        schema_id = f"{direction}_{filename}"
        schemas_cache[schema_id] = schema
        
        return {
            "success": True,
            "schema_id": schema_id,
            "schema": schema,
            "tree": parser.to_tree_structure()
        }
    
    except Exception as e:
        raise HTTPException(500, str(e))


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
    output_format: str = 'xml',
    validate: bool = False
):
    """Execute transformation"""
    try:
        content = (await file.read()).decode('utf-8')
        
        # Detect input format
        if content.strip().startswith('<'):
            input_fmt = 'xml'
        elif content.strip().startswith('{'):
            input_fmt = 'json'
        else:
            input_fmt = 'csv'
        
        # Transform (TODO: use actual mapping)
        result = transformation_engine.transform(
            input_content=content,
            input_format=input_fmt,
            output_format=output_format,
            mapping_rules={'connections': []},
            validate_input=validate,
            validate_output=validate
        )
        
        if result.success:
            return Response(
                content=result.output_content,
                media_type='application/xml',
                headers={'Content-Disposition': f'attachment; filename=output.{output_format}'}
            )
        else:
            return {"success": False, "errors": result.validation_errors}
    except Exception as e:
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

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Visual Mapping System API...")
    print("📖 Swagger UI: http://localhost:8080/docs")
    print("🤖 AI Auto-Map: Requires ANTHROPIC_API_KEY or OPENAI_API_KEY in .env")
    print("🔄 Reverse Mapping: /api/mapping/reverse")
    print("👁️ Preview Extraction: /api/preview/extract (XML/JSON)")
    print("🏗️ Schema Editor: /api/schema/* (Visual structure builder)")
    uvicorn.run(app, host="0.0.0.0", port=8080)

