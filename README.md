# Mapping System – IDoc / CSV → UBL / FatturaPA / PEPPOL

## Overview

Mapping System is a local integration tool designed to create, manage, and execute structured data mappings between enterprise formats and international e-invoicing standards.

It is intended for system integrators, e-invoicing specialists, and technical teams who need to transform data between formats such as IDoc, CSV, FatturaPA, UBL, PEPPOL, and EN16931.

The application provides a visual environment to define field-to-field relationships, apply transformations, save reusable mappings, and execute conversions in a controlled and repeatable way.

All operations run locally. No external services are required.

---

## Main Capabilities

- Load and parse input schemas
- Load and parse output schemas
- Create field-to-field mappings
- Apply transformation rules
- Save mappings with versioning
- Execute conversions on real data
- Preview mapped data
- Support forward and reverse mapping
- AI-assisted mapping suggestions
- Sequential automatic mapping discovery

---

## Supported Formats

Input sources may include:
- IDoc (positional structures)
- CSV files
- Structured XML sources
- ERP export datasets

Output targets may include:
- UBL
- FatturaPA
- PEPPOL BIS
- EN16931-aligned structures
- Custom XML formats

---

## Architecture

The system follows a simple local architecture:

Frontend (HTML interface)  
↓  
Python API backend  
↓  
Mapping & transformation engine  
↓  
Local storage (JSON + SQLite)

There is no cloud dependency.

---

## Components

### Frontend

File:
- `frontend/index.html`

Features:
- Schema selection
- Visual mapping creation
- AI-assisted suggestions
- Sequential mapping execution
- Mapping preview

### Backend

Core modules:

- `backend/api.py`  
  Main API layer used by the frontend.

- `mapper_engine.py`  
  Core logic for field matching and mapping execution.

- `transformation_engine.py`  
  Applies data transformations between source and target fields.

- `reverse_mapper.py`  
  Generates reverse mappings from existing configurations.

- `schema_parser.py`  
  Parses input and output schema definitions.

- `idoc_parser.py`  
  Handles positional IDoc parsing using segment definitions.

- `csv_parser.py`  
  Handles CSV ingestion.

- `preview_extractor.py`  
  Extracts preview data for mapping validation.

- `storage_layer.py`  
  Persists mappings, sessions, and metadata.

---

## Storage Structure

Local storage is used for persistence:

- `backend/data/database.sqlite`  
  Internal metadata database.

- `backend/mappings/*.json`  
  Versioned mapping files.

- `backend/sessions/`  
  Temporary session data.

Mappings are stored as timestamped JSON files.

---

## Typical Use Cases

- IDoc → UBL conversion
- FatturaPA → UBL transformation
- CSV → EN16931 mapping
- UBL → FatturaPA reverse mapping
- ERP integration prototyping
- Mapping design for international e-invoicing flows
- Data migration projects

---

## Sequential AI-Assisted Mapping

The system can automatically attempt to discover mappings by:

1. Starting from the first input field
2. Searching for the most probable output field
3. Continuing sequentially
4. Stopping when confidence drops

If the process is executed again:
- Existing mappings are preserved
- New potential matches are suggested

This allows gradual enrichment of mapping quality over time.

---

## IDoc Support

The system supports positional IDoc structures and can interpret:

- Segment definitions
- Technical field names
- Qualifiers
- Field lengths
- Offsets (if available)

This allows structured extraction even from rigid flat IDoc text files.

---

## Repository Structure

backend/
api.py
mapper_engine.py
transformation_engine.py
idoc_parser.py
schema_parser.py
reverse_mapper.py
storage_layer.py
preview_extractor.py

frontend/
index.html

schemas/
input/
output/

examples/
Sample IDoc, CSV, and XML test files

mappings/
Saved mapping configurations

run.bat
.env
--

## Requirements

- Python 3.10 or newer
- Windows environment (primary tested platform)
- Modern web browser

---

## Installation

Clone the repository:

git clone <repository_url>
cd mapping_system


Create a virtual environment:

python -m venv venv
venv\Scripts\activate


Install dependencies:

pip install flask pyyaml pandas


Start the backend:

python backend/api.py


or use:

run.bat


Open the frontend:

frontend/index.html


---

## Configuration

Configuration files:

- `.env/.env`  
  Optional API keys and runtime parameters.

- `backend/config.yml`  
  System-level configuration.

---

## Operational Flow

1. Load an input schema
2. Load an output schema
3. Create mappings manually or with AI assistance
4. Save mapping (JSON versioned file)
5. Execute transformation
6. Validate the result
7. Iterate and refine

---

## Mapping Versioning

Each saved mapping generates a timestamped file:

mapping_name_YYYYMMDD_HHMMSS.json


This allows:

- History tracking
- Rollback capability
- Version comparison
- Reuse across projects

---

## Security Model

- Fully local execution
- No automatic data transmission
- API keys optional
- Suitable for sensitive enterprise data

---

## Project Status

Advanced prototype / integration laboratory.

This project is suitable for:

- System integrators
- E-invoicing solution providers
- ERP integration teams
- Data transformation specialists

---

## Roadmap (Possible Extensions)

- Advanced visual mapping editor
- EN16931 validation module
- Direct XML export engine
- ERP connector plugins
- Improved AI matching accuracy
- Mapping quality scoring

---

## Author

Paolo Forte  
e-Invoicing Program Manager  
International Integration & Compliance Systems

---

## License

Internal use / experimental project.
