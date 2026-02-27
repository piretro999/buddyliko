#!/usr/bin/env python3
"""
Buddyliko - HL7 Parser (v2 + FHIR R4)
Zero dipendenze esterne.

Supporta:
  HL7 v2.x:  MSH envelope + segmenti ADT, ORU, ORM, ORL, SIU, MDM, DFT
  FHIR R4:   JSON e XML (Patient, Observation, Encounter, DiagnosticReport,
             MedicationRequest, Condition, Procedure, Bundle)

Funzioni principali:
  parse_hl7v2(content)   → dict strutturato con envelope + segmenti
  parse_fhir(content)    → dict normalizzato da JSON/XML FHIR
  to_buddyliko_schema()  → schema navigabile nel mapper
  hl7v2_to_flat()        → record flat {SEGMENT.FIELD.COMPONENT: value}
  fhir_to_flat()         → record flat da risorsa FHIR
  build_hl7v2()          → genera messaggio HL7 v2 da dict
"""

import json
import re
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple


# ===========================================================================
# HL7 v2 FIELD DEFINITIONS (principali)
# ===========================================================================

HL7V2_SEGMENT_NAMES = {
    'MSH': 'Message Header',
    'EVN': 'Event Type',
    'PID': 'Patient Identification',
    'PV1': 'Patient Visit',
    'PV2': 'Patient Visit Additional Info',
    'OBR': 'Observation Request',
    'OBX': 'Observation Result',
    'NTE': 'Notes and Comments',
    'ORC': 'Common Order',
    'RXO': 'Pharmacy/Treatment Order',
    'RXR': 'Pharmacy/Treatment Route',
    'DG1': 'Diagnosis',
    'PR1': 'Procedures',
    'IN1': 'Insurance',
    'IN2': 'Insurance Additional Info',
    'GT1': 'Guarantor',
    'AL1': 'Patient Allergy Info',
    'NK1': 'Next of Kin / Associated Parties',
    'ACC': 'Accident',
    'DB1': 'Disability Info',
    'PD1': 'Patient Additional Demographics',
    'ROL': 'Role',
    'SCH': 'Scheduling Activity Info',
    'AIS': 'Appointment Information Service',
    'ZPD': 'Custom Segment (Z-segment)',
}

# PID field names (HL7 v2.5)
HL7V2_PID_FIELDS = {
    1: 'SetID', 2: 'PatientID', 3: 'PatientIdentifierList',
    4: 'AlternatePatientID', 5: 'PatientName', 6: 'MothersMaidenName',
    7: 'DateTimeOfBirth', 8: 'AdministrativeSex', 9: 'PatientAlias',
    10: 'Race', 11: 'PatientAddress', 12: 'CountyCode',
    13: 'PhoneNumberHome', 14: 'PhoneNumberBusiness', 15: 'PrimaryLanguage',
    16: 'MaritalStatus', 17: 'Religion', 18: 'PatientAccountNumber',
    19: 'SSNNumber', 22: 'EthnicGroup', 29: 'PatientDeathIndicator',
}

HL7V2_MSH_FIELDS = {
    1: 'FieldSeparator', 2: 'EncodingCharacters', 3: 'SendingApplication',
    4: 'SendingFacility', 5: 'ReceivingApplication', 6: 'ReceivingFacility',
    7: 'DateTimeOfMessage', 8: 'Security', 9: 'MessageType',
    10: 'MessageControlID', 11: 'ProcessingID', 12: 'VersionID',
}


# ===========================================================================
# HL7 v2 PARSER
# ===========================================================================

class HL7v2Parser:
    """
    Parser HL7 v2.x — supporta tutti i message types.
    Gestisce separatori personalizzati dal MSH.
    """

    def __init__(self, content: str):
        self.content = content
        # Default HL7v2 separators
        self.field_sep = '|'
        self.component_sep = '^'
        self.repetition_sep = '~'
        self.escape_char = '\\'
        self.subcomponent_sep = '&'
        self._parse_msh_separators()

    def _parse_msh_separators(self):
        """MSH-1 e MSH-2 contengono i separatori."""
        stripped = self.content.strip()
        if not stripped.startswith('MSH'):
            return
        # MSH|^~\&|...
        # MSH-1 = campo 1 = il carattere dopo 'MSH'
        if len(stripped) > 3:
            self.field_sep = stripped[3]
        if len(stripped) > 7:
            encoding = stripped[4:8]
            if len(encoding) >= 1: self.component_sep = encoding[0]
            if len(encoding) >= 2: self.repetition_sep = encoding[1]
            if len(encoding) >= 3: self.escape_char = encoding[2]
            if len(encoding) >= 4: self.subcomponent_sep = encoding[3]

    def parse(self) -> Dict:
        """
        Ritorna:
        {
          "format": "HL7v2",
          "version": "2.5",
          "message_type": "ADT^A01",
          "sending_app": "...",
          "sending_facility": "...",
          "message_control_id": "...",
          "datetime": "...",
          "segments": [
            {"id": "MSH", "name": "Message Header", "fields": [...], "raw": "..."},
            {"id": "PID", "name": "Patient Identification", "fields": [...], "raw": "..."},
            ...
          ]
        }
        """
        lines = [l.strip() for l in self.content.strip().splitlines()
                 if l.strip() and not l.strip().startswith('#')]

        segments = []
        msh_data = {}

        for line in lines:
            seg = self._parse_segment(line)
            if seg:
                segments.append(seg)
                if seg['id'] == 'MSH':
                    msh_data = self._extract_msh(seg)

        return {
            "format": "HL7v2",
            "version": msh_data.get('version', '2.5'),
            "message_type": msh_data.get('message_type', ''),
            "sending_app": msh_data.get('sending_app', ''),
            "sending_facility": msh_data.get('sending_facility', ''),
            "receiving_app": msh_data.get('receiving_app', ''),
            "receiving_facility": msh_data.get('receiving_facility', ''),
            "datetime": msh_data.get('datetime', ''),
            "message_control_id": msh_data.get('message_control_id', ''),
            "processing_id": msh_data.get('processing_id', 'P'),
            "segments": segments,
        }

    def _parse_segment(self, line: str) -> Optional[Dict]:
        if not line or len(line) < 3:
            return None
        parts = line.split(self.field_sep)
        seg_id = parts[0].strip().upper()

        fields = []
        for part in parts[1:]:
            # Gestisci ripetizioni
            repetitions = part.split(self.repetition_sep)
            parsed_reps = []
            for rep in repetitions:
                # Gestisci componenti
                components = rep.split(self.component_sep)
                if len(components) > 1:
                    parsed_comps = []
                    for comp in components:
                        sub = comp.split(self.subcomponent_sep)
                        parsed_comps.append(sub if len(sub) > 1 else comp)
                    parsed_reps.append(parsed_comps)
                else:
                    parsed_reps.append(rep)
            fields.append(parsed_reps[0] if len(parsed_reps) == 1 else parsed_reps)

        return {
            'id': seg_id,
            'name': HL7V2_SEGMENT_NAMES.get(seg_id, seg_id),
            'fields': fields,
            'raw': line,
        }

    def _extract_msh(self, seg: Dict) -> Dict:
        def get(idx, comp=0):
            fields = seg['fields']
            if idx >= len(fields):
                return ''
            f = fields[idx]
            if isinstance(f, list):
                if comp < len(f):
                    c = f[comp]
                    return c[0] if isinstance(c, list) else c
            return str(f) if not isinstance(f, list) else ''

        return {
            'sending_app': get(1),        # MSH-3
            'sending_facility': get(2),   # MSH-4
            'receiving_app': get(3),      # MSH-5
            'receiving_facility': get(4), # MSH-6
            'datetime': get(5),           # MSH-7
            'message_type': f"{get(7)}" + (f"^{get(7, 1)}" if get(7, 1) else ''),  # MSH-9
            'message_control_id': get(8), # MSH-10
            'processing_id': get(10),     # MSH-11 (nota: 0-indexed da fields[])
            'version': get(11),           # MSH-12
        }


class HL7v2Writer:
    """Genera messaggio HL7 v2 da dict strutturato."""

    def __init__(self):
        self.fs = '|'
        self.cs = '^'
        self.rs = '~'
        self.ec = '\\'
        self.sc = '&'

    def build(self, data: Dict, message_type: str = 'ADT^A01') -> str:
        now = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        ctrl_id = str(uuid.uuid4()).replace('-', '')[:20]

        lines = []
        # MSH
        lines.append(
            f"MSH{self.fs}{self.cs}{self.rs}{self.ec}{self.sc}{self.fs}"
            f"{data.get('sending_app', 'BUDDYLIKO')}{self.fs}"
            f"{data.get('sending_facility', 'FACILITY')}{self.fs}"
            f"{data.get('receiving_app', '')}{self.fs}"
            f"{data.get('receiving_facility', '')}{self.fs}"
            f"{now}{self.fs}{self.fs}{message_type}{self.fs}"
            f"{ctrl_id}{self.fs}P{self.fs}2.5"
        )

        # Segmenti aggiuntivi
        for seg in data.get('segments', []):
            seg_id = seg.get('id', '')
            fields = seg.get('fields', [])
            encoded = []
            for f in fields:
                if isinstance(f, list):
                    # Componenti
                    encoded.append(self.cs.join(
                        (self.sc.join(s) if isinstance(s, list) else str(s or ''))
                        for s in f
                    ))
                else:
                    encoded.append(str(f or ''))
            lines.append(seg_id + self.fs + self.fs.join(encoded))

        return '\r\n'.join(lines) + '\r\n'


# ===========================================================================
# FHIR R4 PARSER
# ===========================================================================

class FHIRParser:
    """
    Parser FHIR R4 — supporta JSON e XML.
    Estrae le risorse principali in formato normalizzato.
    """

    RESOURCE_TYPES = {
        'Patient', 'Observation', 'Encounter', 'DiagnosticReport',
        'MedicationRequest', 'Condition', 'Procedure', 'Bundle',
        'Practitioner', 'Organization', 'Location', 'ServiceRequest',
        'AllergyIntolerance', 'Immunization', 'Coverage', 'Claim',
        'ExplanationOfBenefit', 'DocumentReference', 'CarePlan',
    }

    def parse(self, content: str) -> Dict:
        """Auto-detect JSON o XML e parsa la risorsa FHIR."""
        stripped = content.strip()
        if stripped.startswith('{') or stripped.startswith('['):
            return self._parse_json(stripped)
        elif stripped.startswith('<'):
            return self._parse_xml(stripped)
        else:
            raise ValueError("FHIR content non riconosciuto: deve essere JSON ({) o XML (<)")

    def _parse_json(self, content: str) -> Dict:
        data = json.loads(content)
        resource_type = data.get('resourceType', 'Unknown')

        result = {
            "format": "FHIR_JSON",
            "resource_type": resource_type,
            "id": data.get('id', ''),
            "_raw": data,
        }

        if resource_type == 'Bundle':
            result["entries"] = []
            for entry in data.get('entry', []):
                resource = entry.get('resource', {})
                rt = resource.get('resourceType', '')
                result["entries"].append({
                    "resource_type": rt,
                    "id": resource.get('id', ''),
                    "data": self._normalize_resource(rt, resource),
                })
            result["total"] = data.get('total', len(result["entries"]))
            result["bundle_type"] = data.get('type', 'collection')
        else:
            result["data"] = self._normalize_resource(resource_type, data)

        return result

    def _parse_xml(self, content: str) -> Dict:
        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            raise ValueError(f"XML FHIR non valido: {e}")

        # Namespace FHIR
        ns = ''
        if root.tag.startswith('{'):
            ns = root.tag.split('}')[0] + '}'

        tag = root.tag.replace(ns, '')
        data = self._xml_to_dict(root, ns)

        result = {
            "format": "FHIR_XML",
            "resource_type": tag,
            "id": self._get_xml_value(root, ns + 'id', 'value') or data.get('id', ''),
            "_raw": data,
        }

        if tag == 'Bundle':
            result["entries"] = []
            for entry_el in root.findall(f'{ns}entry'):
                resource_el = entry_el.find(f'{ns}resource')
                if resource_el is None:
                    continue
                children = list(resource_el)
                if not children:
                    continue
                resource_root = children[0]
                rt = resource_root.tag.replace(ns, '')
                resource_data = self._xml_to_dict(resource_root, ns)
                result["entries"].append({
                    "resource_type": rt,
                    "id": self._get_xml_value(resource_root, ns + 'id', 'value'),
                    "data": self._normalize_resource(rt, resource_data),
                })
            result["total"] = len(result["entries"])
        else:
            result["data"] = self._normalize_resource(tag, data)

        return result

    def _xml_to_dict(self, element, ns: str) -> Dict:
        result = {}
        tag = element.tag.replace(ns, '')
        # Attributo value
        if 'value' in element.attrib:
            return element.attrib['value']
        for child in element:
            child_tag = child.tag.replace(ns, '')
            val = self._xml_to_dict(child, ns)
            if child_tag in result:
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(val)
            else:
                result[child_tag] = val
        return result

    def _get_xml_value(self, element, tag: str, attr: str = 'value') -> str:
        el = element.find(tag)
        if el is not None:
            return el.attrib.get(attr, '')
        return ''

    def _normalize_resource(self, resource_type: str, data: Dict) -> Dict:
        """
        Estrae i campi più importanti in struttura flat per ogni resource type.
        """
        if resource_type == 'Patient':
            name = data.get('name', [{}])
            name = name[0] if isinstance(name, list) else name
            return {
                "id": data.get('id', ''),
                "family": name.get('family', '') if isinstance(name, dict) else '',
                "given": ' '.join(name.get('given', [])) if isinstance(name, dict) and isinstance(name.get('given'), list) else '',
                "birthDate": data.get('birthDate', ''),
                "gender": data.get('gender', ''),
                "active": data.get('active', True),
                "identifier": self._extract_identifier(data),
                "address": self._extract_address(data),
                "telecom": self._extract_telecom(data),
                "_full": data,
            }

        elif resource_type == 'Observation':
            coding = {}
            code = data.get('code', {})
            if isinstance(code, dict):
                codings = code.get('coding', [{}])
                coding = codings[0] if codings else {}
            return {
                "id": data.get('id', ''),
                "status": data.get('status', ''),
                "code_system": coding.get('system', '') if isinstance(coding, dict) else '',
                "code_code": coding.get('code', '') if isinstance(coding, dict) else '',
                "code_display": coding.get('display', '') if isinstance(coding, dict) else '',
                "value_quantity": data.get('valueQuantity', {}).get('value', '') if isinstance(data.get('valueQuantity'), dict) else '',
                "value_unit": data.get('valueQuantity', {}).get('unit', '') if isinstance(data.get('valueQuantity'), dict) else '',
                "value_string": data.get('valueString', ''),
                "effectiveDateTime": data.get('effectiveDateTime', ''),
                "subject_ref": data.get('subject', {}).get('reference', '') if isinstance(data.get('subject'), dict) else '',
                "_full": data,
            }

        elif resource_type == 'Encounter':
            return {
                "id": data.get('id', ''),
                "status": data.get('status', ''),
                "class_code": data.get('class', {}).get('code', '') if isinstance(data.get('class'), dict) else '',
                "subject_ref": data.get('subject', {}).get('reference', '') if isinstance(data.get('subject'), dict) else '',
                "period_start": data.get('period', {}).get('start', '') if isinstance(data.get('period'), dict) else '',
                "period_end": data.get('period', {}).get('end', '') if isinstance(data.get('period'), dict) else '',
                "_full": data,
            }

        elif resource_type == 'Condition':
            coding = {}
            code = data.get('code', {})
            if isinstance(code, dict):
                codings = code.get('coding', [{}])
                coding = codings[0] if codings else {}
            return {
                "id": data.get('id', ''),
                "clinical_status": data.get('clinicalStatus', {}).get('coding', [{}])[0].get('code', '') if isinstance(data.get('clinicalStatus', {}).get('coding', None), list) else '',
                "code": coding.get('code', '') if isinstance(coding, dict) else '',
                "code_display": coding.get('display', '') if isinstance(coding, dict) else '',
                "subject_ref": data.get('subject', {}).get('reference', '') if isinstance(data.get('subject'), dict) else '',
                "onsetDateTime": data.get('onsetDateTime', ''),
                "_full": data,
            }

        elif resource_type == 'MedicationRequest':
            med = data.get('medicationCodeableConcept', {})
            coding = med.get('coding', [{}])[0] if isinstance(med.get('coding'), list) else {}
            return {
                "id": data.get('id', ''),
                "status": data.get('status', ''),
                "intent": data.get('intent', ''),
                "medication_code": coding.get('code', '') if isinstance(coding, dict) else '',
                "medication_display": coding.get('display', '') if isinstance(coding, dict) else '',
                "subject_ref": data.get('subject', {}).get('reference', '') if isinstance(data.get('subject'), dict) else '',
                "authoredOn": data.get('authoredOn', ''),
                "_full": data,
            }

        # Per tutti gli altri resource type — restituisce la struttura raw
        return {"id": data.get('id', ''), "_full": data}

    def _extract_identifier(self, data: Dict) -> str:
        ids = data.get('identifier', [])
        if isinstance(ids, list) and ids:
            return ids[0].get('value', '') if isinstance(ids[0], dict) else str(ids[0])
        return ''

    def _extract_address(self, data: Dict) -> Dict:
        addrs = data.get('address', [])
        if isinstance(addrs, list) and addrs:
            a = addrs[0]
            if isinstance(a, dict):
                return {
                    "line": ' '.join(a.get('line', [])) if isinstance(a.get('line'), list) else a.get('line', ''),
                    "city": a.get('city', ''),
                    "state": a.get('state', ''),
                    "postalCode": a.get('postalCode', ''),
                    "country": a.get('country', ''),
                }
        return {}

    def _extract_telecom(self, data: Dict) -> List[Dict]:
        telecoms = data.get('telecom', [])
        if isinstance(telecoms, list):
            return [{"system": t.get('system', ''), "value": t.get('value', ''), "use": t.get('use', '')}
                    for t in telecoms if isinstance(t, dict)]
        return []


# ===========================================================================
# INTERFACCIA UNIFICATA
# ===========================================================================

def detect_hl7_format(content: str) -> str:
    stripped = content.strip()
    if stripped.startswith('MSH'):
        return 'HL7v2'
    if stripped.startswith('{') or stripped.startswith('['):
        try:
            data = json.loads(stripped)
            if 'resourceType' in data:
                return 'FHIR_JSON'
        except: pass
    if stripped.startswith('<'):
        if 'xmlns' in stripped[:200] and ('fhir' in stripped[:200].lower() or
           any(rt in stripped[:100] for rt in ['Patient', 'Bundle', 'Observation'])):
            return 'FHIR_XML'
    return 'UNKNOWN'


def parse_hl7(content: str) -> Dict:
    """Auto-detect e parsa HL7 v2 o FHIR."""
    fmt = detect_hl7_format(content)
    if fmt == 'HL7v2':
        return HL7v2Parser(content).parse()
    elif fmt in ('FHIR_JSON', 'FHIR_XML'):
        return FHIRParser().parse(content)
    else:
        raise ValueError("Formato HL7 non riconosciuto. Assicurati che inizi con MSH (HL7v2) o sia un JSON/XML FHIR valido.")


def hl7v2_to_flat(parsed: Dict) -> Dict:
    """Converte HL7v2 parsato in dict flat {SEGMENT.FIELD: value}."""
    flat = {
        '_message_type': parsed.get('message_type', ''),
        '_sending_app': parsed.get('sending_app', ''),
        '_sending_facility': parsed.get('sending_facility', ''),
        '_datetime': parsed.get('datetime', ''),
        '_control_id': parsed.get('message_control_id', ''),
        '_version': parsed.get('version', ''),
    }
    for seg in parsed.get('segments', []):
        sid = seg['id']
        for i, field in enumerate(seg.get('fields', []), 1):
            key_base = f"{sid}.{i:02d}"
            if isinstance(field, list):
                for j, comp in enumerate(field, 1):
                    if isinstance(comp, list):
                        for k, sub in enumerate(comp, 1):
                            flat[f"{key_base}.{j:02d}.{k:02d}"] = str(sub or '')
                    else:
                        flat[f"{key_base}.{j:02d}"] = str(comp or '')
            elif isinstance(field, list):  # repetitions
                flat[key_base] = str(field[0] if field else '')
            else:
                flat[key_base] = str(field or '')
    return flat


def fhir_to_flat(parsed: Dict) -> Dict:
    """Converte FHIR parsato in dict flat."""
    flat = {
        '_format': parsed.get('format', ''),
        '_resource_type': parsed.get('resource_type', ''),
        '_id': parsed.get('id', ''),
    }
    if parsed.get('resource_type') == 'Bundle':
        for i, entry in enumerate(parsed.get('entries', []), 1):
            prefix = f"entry.{i:03d}"
            flat[f"{prefix}._resource_type"] = entry.get('resource_type', '')
            flat[f"{prefix}._id"] = entry.get('id', '')
            for k, v in entry.get('data', {}).items():
                if k != '_full' and not isinstance(v, (dict, list)):
                    flat[f"{prefix}.{k}"] = str(v or '')
    else:
        for k, v in parsed.get('data', {}).items():
            if k != '_full' and not isinstance(v, (dict, list)):
                flat[k] = str(v or '')
            elif isinstance(v, dict):
                for k2, v2 in v.items():
                    if not isinstance(v2, (dict, list)):
                        flat[f"{k}.{k2}"] = str(v2 or '')
    return flat


def to_buddyliko_schema_hl7(parsed: Dict, name: str = None) -> Dict:
    """Converte HL7/FHIR parsato in schema Buddyliko."""
    fmt = parsed.get('format', 'UNKNOWN')
    schema_name = name or f"HL7_{fmt}"

    fields = []

    if fmt == 'HL7v2':
        # MSH envelope
        for fname, path in [
            ('MSH_MessageType', 'MSH/MessageType'),
            ('MSH_SendingApp', 'MSH/SendingApp'),
            ('MSH_SendingFacility', 'MSH/SendingFacility'),
            ('MSH_DateTime', 'MSH/DateTime'),
            ('MSH_ControlID', 'MSH/ControlID'),
            ('MSH_Version', 'MSH/Version'),
        ]:
            fields.append({"id": str(uuid.uuid4()), "name": fname, "type": "string",
                           "path": f"{schema_name}/{path}",
                           "xml_path": f"{schema_name}/{path}"})

        # Segmenti body
        seen = {}
        for seg in parsed.get('segments', []):
            sid = seg['id']
            if sid in seen:
                continue
            seen[sid] = True
            seg_name = seg.get('name', sid)
            for i, field in enumerate(seg.get('fields', []), 1):
                field_defs = HL7V2_PID_FIELDS if sid == 'PID' else {}
                fname_hint = field_defs.get(i, f"F{i:02d}")
                if isinstance(field, list):
                    for j, comp in enumerate(field, 1):
                        fields.append({
                            "id": str(uuid.uuid4()),
                            "name": f"{sid}_{fname_hint}_{j:02d}" if isinstance(comp, list) else f"{sid}_{fname_hint}",
                            "type": "string",
                            "path": f"{schema_name}/{sid}/{fname_hint}",
                            "xml_path": f"{schema_name}/{sid}/{fname_hint}",
                            "description": f"{seg_name} field {i}",
                        })
                        break  # Solo prima ripetizione per schema
                else:
                    fields.append({
                        "id": str(uuid.uuid4()),
                        "name": f"{sid}_{fname_hint}",
                        "type": "string",
                        "path": f"{schema_name}/{sid}/{fname_hint}",
                        "xml_path": f"{schema_name}/{sid}/{fname_hint}",
                        "description": f"{seg_name} field {i}",
                    })

    elif fmt in ('FHIR_JSON', 'FHIR_XML'):
        rt = parsed.get('resource_type', 'Resource')
        data = parsed.get('data', {})
        if not data and parsed.get('resource_type') == 'Bundle':
            # Usa il primo entry
            entries = parsed.get('entries', [])
            if entries:
                rt = entries[0].get('resource_type', 'Resource')
                data = entries[0].get('data', {})

        for k, v in data.items():
            if k == '_full':
                continue
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    if not isinstance(v2, (dict, list)):
                        fields.append({
                            "id": str(uuid.uuid4()), "name": f"{k}_{k2}",
                            "type": "string",
                            "path": f"{schema_name}/{rt}/{k}/{k2}",
                            "xml_path": f"{schema_name}/{rt}/{k}/{k2}",
                        })
            elif not isinstance(v, list):
                fields.append({
                    "id": str(uuid.uuid4()), "name": k,
                    "type": "string",
                    "path": f"{schema_name}/{rt}/{k}",
                    "xml_path": f"{schema_name}/{rt}/{k}",
                })

    return {
        "id": str(uuid.uuid4()),
        "name": schema_name,
        "source": "hl7",
        "hl7_format": fmt,
        "fields": fields,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def build_hl7v2(data: Dict, message_type: str = 'ADT^A01') -> str:
    """Genera messaggio HL7 v2 da dict."""
    return HL7v2Writer().build(data, message_type)
