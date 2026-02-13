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
from fastapi.responses import JSONResponse, FileResponse
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


# Run server
if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Visual Mapping System API...")
    print("📖 Swagger UI: http://localhost:8080/docs")
    print("🤖 AI Auto-Map: Requires ANTHROPIC_API_KEY or OPENAI_API_KEY in .env")
    uvicorn.run(app, host="0.0.0.0", port=8080)

