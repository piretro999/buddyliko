# Buddyliko — Mapping System
### IDoc / CSV / XML → UBL / FatturaPA / PEPPOL / EN16931

A local integration tool for creating, managing, and executing structured data mappings between enterprise formats and international e-invoicing standards.

Designed for system integrators, e-invoicing specialists, and technical teams who need to transform data between formats in a controlled, repeatable, and fully offline environment.

---

## Main Capabilities

- Load and parse input/output schemas (XML, IDoc, CSV, JSON)
- Visual field-to-field mapping editor
- Apply transformation rules (direct, formula-based, concatenation, etc.)
- Save and version mappings as JSON files
- Execute conversions on real data files
- Live hover preview — shows actual extracted values from example files on both input and output fields
- Export mapping as CSV, JSON, or SVG diagram
- Forward and reverse mapping support
- AI-assisted mapping suggestions
- Sequential automatic mapping discovery
- XSD-aware output ordering (UBL 2.1 compliant element sequence)
- Authentication layer (optional, configurable)

---

## Supported Formats

**Input**
- IDoc (positional flat structures)
- CSV
- XML (FatturaPA and custom structures)
- ERP export datasets

**Output**
- UBL 2.1
- FatturaPA
- PEPPOL BIS
- EN16931-aligned structures
- Custom XML formats

---

## Architecture

```
Frontend (app.html)
       ↓
Python API backend (api.py)
       ↓
Mapping & transformation engine
       ↓
Local storage (JSON + SQLite)
```

No cloud dependency. All operations run locally.

---

## Components

### Frontend
**File:** `frontend/app.html`

- Schema selection (input and output, with ID-based resolution)
- Visual canvas for drag-and-drop mapping creation
- Hover popup preview: shows extracted values from loaded example files for both input and output fields (with transformation applied)
- AI-assisted and sequential mapping suggestions
- Formula editor with built-in transformation types
- Export: CSV table, JSON project file, SVG diagram
- Session persistence (save/load project)
- Reverse mapping
- Hide/show connected fields toggle
- Schema editor (visual field structure builder)
- Search across fields

### Backend

| Module | Description |
|---|---|
| `api.py` | Main FastAPI layer, all endpoints |
| `transformation_engine.py` | Executes field mappings, applies transformations, orders output elements per XSD |
| `mapper_engine.py` | Core field matching and mapping logic |
| `schema_parser.py` | Parses input/output schema definitions |
| `idoc_parser.py` | Positional IDoc parsing using segment definitions |
| `csv_parser.py` | CSV ingestion |
| `preview_extractor.py` | Extracts field values from XML/JSON example files (XPath-based, path-precise) |
| `diagram_generator.py` | Generates SVG mapping diagrams (pure Python, no external dependencies) |
| `reverse_mapper.py` | Generates reverse mappings from existing configurations |
| `storage_layer.py` | Persists mappings, sessions, and metadata |
| `formulas.py` | Formula library for transformation rules |

---

## Storage Structure

```
backend/data/database.sqlite     # Internal metadata
backend/mappings/*.json          # Versioned mapping files
backend/sessions/                # Temporary session data
schemas/input/                   # Input schema definitions
schemas/output/                  # Output schema definitions + XSD files
```

Mappings are stored as timestamped JSON files:
```
mapping_name_YYYYMMDD_HHMMSS.json
```

---

## Mapping File Format

Each mapping connection includes:

```json
{
  "id": "conn_2",
  "source": "TipoDocumento",
  "target": "cbc_InvoiceTypeCode",
  "sourcePath": "FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/TipoDocumento",
  "targetPath": "cbc:InvoiceTypeCode",
  "businessTerm": "BT-3",
  "transformation": {
    "type": "DIRECT",
    "formula": null,
    "description": "Direct 1:1 mapping"
  }
}
```

The `sourcePath` and `targetPath` fields are required for correct value extraction and XSD-ordered output generation.

---

## Typical Use Cases

- IDoc → UBL 2.1 conversion
- FatturaPA → UBL transformation
- CSV → EN16931 mapping
- UBL → FatturaPA reverse mapping
- ERP integration prototyping
- Mapping design for international e-invoicing flows
- Data migration projects

---

## Preview System

Each field in the canvas shows a hover popup with the actual value extracted from a loaded example file.

- **Input fields**: value extracted from the input example via XPath or positional offset (IDoc)
- **Output fields (connected)**: value extracted from the input example, routed through the connection's `sourcePath` and transformation applied
- **Output fields (not connected)**: structure placeholder from output example

The preview extractor uses strict path navigation — it never returns concatenated values from multiple XML nodes with the same tag name.

---

## SVG Diagram Export

The mapping can be exported as a clean SVG diagram showing:

- Source fields and paths (left column)
- Target fields and paths (right column)
- Transformation type badges
- Business term (BT-xx) labels
- Logical grouping by source section

Generated by `diagram_generator.py` — no external libraries required.

---

## XSD Output Ordering

When an output schema ID is configured (e.g. `UBL-21`), the transformation engine automatically reorders output XML elements to comply with the XSD sequence. The schema is resolved by fuzzy-matching the `outputSchemaId` field in the mapping rules against the available XSD directories.

---

## AI-Assisted Mapping

The system can automatically discover mappings by:

1. Starting from the first unmapped input field
2. Searching for the most probable output field match
3. Continuing sequentially
4. Stopping when confidence drops below threshold

Running the process again preserves existing mappings and suggests new candidates.

---

## IDoc Support

Supports positional IDoc structures:

- Segment definitions
- Technical field names and qualifiers
- Field lengths and offsets
- Handles both E1 and E2 segment prefixes

---

## Installation

```bash
git clone <repository_url>
cd buddyliko

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt

python backend/api.py
```

Or use `run.bat` on Windows.

Open `frontend/app.html` in a browser.

---

## Configuration

| File | Purpose |
|---|---|
| `.env` | API keys, runtime parameters, auth settings |
| `backend/config.yml` | System-level configuration |

---

## Security Model

- Fully local execution
- No automatic data transmission
- Optional authentication layer (token-based)
- Suitable for sensitive enterprise data

---

## Repository Structure

```
backend/
  api.py
  transformation_engine.py
  mapper_engine.py
  schema_parser.py
  idoc_parser.py
  csv_parser.py
  preview_extractor.py
  diagram_generator.py
  reverse_mapper.py
  storage_layer.py
  formulas.py

frontend/
  app.html

schemas/
  input/
  output/
    UBL-21/
      xsd/

mappings/
examples/
run.bat
.env
```

---

## Project Status

Advanced prototype / integration laboratory.

Suitable for:
- System integrators
- E-invoicing solution providers
- ERP integration teams
- Data transformation specialists

---

## Author

**Paolo Forte**  
e-Invoicing Program Manager  
International Integration & Compliance Systems

---

## License

Internal use / experimental project.
