
### Main Components

**Frontend**
- `frontend/index.html`
- Visual editor for:
  - Schema selection
  - Mapping creation
  - AI-assisted suggestions
  - Sequential mapping execution

**Backend**
- `backend/api.py` → main API layer
- `mapper_engine.py` → mapping logic
- `transformation_engine.py` → data transformations
- `reverse_mapper.py` → reverse mapping logic
- `schema_parser.py` → schema interpretation
- `idoc_parser.py` → positional IDoc handling
- `csv_parser.py` → CSV handling
- `storage_layer.py` → mapping/session persistence
- `preview_extractor.py` → data preview generation

**Storage**
- `backend/data/database.sqlite`
- `backend/mappings/*.json`
- `backend/sessions/`

---

## Primary Use Cases

- IDoc → UBL (France, PEPPOL, etc.)
- FatturaPA → UBL
- CSV → EN16931
- UBL → FatturaPA (reverse mapping)
- Reusable mapping design for ERP integrations

---

## Key Features

- Sequential automatic mapping
- AI-assisted field matching suggestions
- Versioned mapping persistence
- Support for positional IDoc structures
- Support for multiple schema types:
  - FatturaPA
  - UBL
  - PEPPOL
  - EN16931
- Reverse engineering of mappings
- Data preview support

---

## Repository Structure

backend/
api.py
mapper_engine.py
transformation_engine.py
idoc_parser.py
schema_parser.py
storage_layer.py
reverse_mapper.py
...

frontend/
index.html

schemas/
input/
output/

examples/
Sample IDoc, CSV, UBL test files

mappings/
Saved mapping configurations

run.bat
.env


---

## Requirements

- Python 3.10+
- Windows (primary tested environment)
- Modern web browser

---

## Installation

1) Clone the repository

```bash
git clone <repo>
cd mapping_system
Create Python virtual environment

python -m venv venv
venv\Scripts\activate
Install dependencies (if not already available)

pip install flask pyyaml pandas
Start the backend

python backend/api.py
or:

run.bat
Open the frontend

Open in your browser:

frontend/index.html
Configuration
Main configuration files:

.env/.env → API keys and runtime settings

backend/config.yml → system parameters

Operational Flow
Load input schema

Load output schema

Create mappings manually or with AI assistance

Save mapping (versioned JSON)

Execute transformation

Validate output

Sequential AI-Assisted Mapping
The system can:

Start from the first input field

Automatically search for the most probable output match

Continue sequentially until confidence drops

When re-running the process:

Existing mappings are preserved

New potential mappings are suggested

IDoc Support
Supports:

Positional IDoc formats

Parsing based on:

Segment definitions

Qualifiers

Field lengths

Offsets (if available)

Example files:

schemas/input/IDOC.txt
Mapping Versioning
Each saved mapping generates a timestamped file:

name_YYYYMMDD_HHMMSS.json
This enables:

History tracking

Rollback capability

Version comparison

Security
No external data transmission required

Fully local execution

Optional API keys via .env

Project Status
Advanced prototype / integration laboratory.

Suitable for:

System integrators

E-invoicing teams

IDoc → UBL migration projects

ERP integration design

Possible Roadmap
Advanced visual editor

EN16931 validation module

Direct XML export engine

ERP connector plugins

Improved AI mapping accuracy

Author
Paolo Forte
e-Invoicing Program Manager
International Integration & Compliance Systems

License
Internal use / experimental project.
