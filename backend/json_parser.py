#!/usr/bin/env python3
"""
Buddyliko - JSON Parser & Writer
Zero dipendenze esterne (jsonschema opzionale per validazione avanzata).

Equivalente di edi_parser / hl7_parser / csv_parser per formato JSON.

Parità funzionale con XML:
  ┌──────────────────────┬─────────────────────────────────────────────┐
  │ XML                  │ JSON (questo file)                          │
  ├──────────────────────┼─────────────────────────────────────────────┤
  │ XSDValidator         │ JSONSchemaValidator                         │
  │ SchematronValidator  │ JSONBusinessRulesValidator (regole custom)  │
  │ BusinessRulesValidator│ (integrato in JSONBusinessRulesValidator)  │
  │ _parse_xml_to_dict   │ parse_json()                               │
  │ _dict_to_xml         │ build_json()                               │
  │ parse_xsd            │ parse_json_schema_file()                   │
  │ parse_sample_xml     │ detect_schema_from_sample()                │
  │ XPath navigation     │ JSONPathNavigator                          │
  │ to_buddyliko_schema  │ to_buddyliko_schema()                      │
  │ edi_to_flat          │ json_to_flat()                             │
  │ detect_edi_format    │ detect_json_format()                       │
  │ validate_input/output│ validate_json()                            │
  │ validate_file        │ validate_json_file()                       │
  └──────────────────────┴─────────────────────────────────────────────┘

Interfaccia pubblica:
  detect_json_format(content)           → str
  parse_json(content)                   → dict normalizzato
  to_buddyliko_schema(parsed, name)     → schema Buddyliko per il mapper
  json_to_flat(parsed)                  → lista record flat {path: value}
  build_json(data, **opts)              → stringa JSON formattata
  validate_json(content, schema, ...)   → (bool, errors)
  validate_json_file(path, schema, ...) → (bool, errors)
  detect_schema_from_sample(content)    → JSON Schema inferito
  parse_json_schema_file(path)          → schema Buddyliko da JSON Schema
"""

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple, Union


# ===========================================================================
# RILEVAMENTO FORMATO
# ===========================================================================

def detect_json_format(content: str) -> str:
    """
    Rileva il tipo di JSON.
    Ritorna: 'JSON_SCHEMA', 'OPENAPI', 'FHIR', 'GEOJSON',
             'JSON_ARRAY', 'JSON', 'UNKNOWN'
    """
    stripped = content.strip()
    if not stripped:
        return 'UNKNOWN'
    try:
        data = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return 'UNKNOWN'

    if isinstance(data, list):
        return 'JSON_ARRAY'
    if not isinstance(data, dict):
        return 'JSON'
    if '$schema' in data or ('type' in data and 'properties' in data):
        return 'JSON_SCHEMA'
    if 'openapi' in data or 'swagger' in data:
        return 'OPENAPI'
    if 'resourceType' in data:
        return 'FHIR'
    if data.get('type') in ('Feature', 'FeatureCollection', 'Point',
                             'MultiPoint', 'LineString', 'MultiLineString',
                             'Polygon', 'MultiPolygon', 'GeometryCollection'):
        return 'GEOJSON'
    return 'JSON'


# ===========================================================================
# JSON PARSER
# ===========================================================================

class JSONParser:
    """
    Parser JSON strutturale.
    Equivalente di _parse_xml_to_dict nel transformation_engine.
    """

    def __init__(self, content: str):
        self.content = content
        self.raw_data = None
        self.format_type = 'UNKNOWN'

    def parse(self) -> Dict:
        self.raw_data = json.loads(self.content)
        self.format_type = detect_json_format(self.content)
        root_type = 'array' if isinstance(self.raw_data, list) else 'object'
        structure = self._analyze(self.raw_data, '$')
        summary = self._summary(structure)
        return {
            'format': self.format_type,
            'root_type': root_type,
            'data': self.raw_data,
            'structure': structure,
            '_summary': summary,
        }

    def _analyze(self, data, path, depth=0, max_depth=30):
        if depth > max_depth:
            return {'type': 'truncated', 'path': path}
        if data is None:
            return {'type': 'null', 'path': path}
        if isinstance(data, bool):
            return {'type': 'boolean', 'path': path, 'value': data}
        if isinstance(data, int):
            return {'type': 'integer', 'path': path, 'value': data}
        if isinstance(data, float):
            return {'type': 'decimal', 'path': path, 'value': data}
        if isinstance(data, str):
            return {'type': _infer_string_subtype(data), 'path': path, 'value': data}
        if isinstance(data, list):
            items = [self._analyze(item, f"{path}[{i}]", depth + 1)
                     for i, item in enumerate(data[:5])]
            return {
                'type': 'array', 'path': path, 'length': len(data),
                'items': items,
                'item_type': items[0]['type'] if items else 'unknown',
            }
        if isinstance(data, dict):
            props = {k: self._analyze(v, f"{path}.{k}", depth + 1)
                     for k, v in data.items()}
            return {
                'type': 'object', 'path': path,
                'properties': props, 'keys': list(data.keys()),
            }
        return {'type': 'unknown', 'path': path}

    def _summary(self, structure):
        c = {'fields': 0, 'objects': 0, 'arrays': 0, 'depth': 0}
        self._count(structure, 0, c)
        return {
            'format': self.format_type,
            'root_type': structure.get('type', 'unknown'),
            'total_fields': c['fields'],
            'total_objects': c['objects'],
            'total_arrays': c['arrays'],
            'max_depth': c['depth'],
        }

    def _count(self, node, depth, c):
        c['depth'] = max(c['depth'], depth)
        t = node.get('type', '')
        if t == 'object':
            c['objects'] += 1
            for p in node.get('properties', {}).values():
                self._count(p, depth + 1, c)
        elif t == 'array':
            c['arrays'] += 1
            for item in node.get('items', []):
                self._count(item, depth + 1, c)
        else:
            c['fields'] += 1


def _infer_string_subtype(value):
    if not value:
        return 'string'
    if re.match(r'^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})', value):
        return 'datetime' if 'T' in value else 'date'
    if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', value, re.I):
        return 'uuid'
    if re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', value):
        return 'email'
    if re.match(r'^https?://', value):
        return 'uri'
    if re.match(r'^-?\d+(\.\d+)?$', value):
        return 'numeric_string'
    return 'string'


# ===========================================================================
# JSON WRITER  (equivalente _dict_to_xml)
# ===========================================================================

class JSONWriter:
    def __init__(self, indent=2, sort_keys=False, ensure_ascii=False):
        self.indent = indent
        self.sort_keys = sort_keys
        self.ensure_ascii = ensure_ascii

    def build(self, data, compact=False):
        if compact:
            return json.dumps(data, ensure_ascii=self.ensure_ascii,
                              sort_keys=self.sort_keys, separators=(',', ':'))
        return json.dumps(data, indent=self.indent,
                          ensure_ascii=self.ensure_ascii,
                          sort_keys=self.sort_keys)

    def build_lines(self, records):
        """JSON Lines (NDJSON)."""
        return '\n'.join(
            json.dumps(r, ensure_ascii=self.ensure_ascii) for r in records
        )


# ===========================================================================
# JSONPATH NAVIGATOR  (equivalente XPath per XML)
# ===========================================================================

class JSONPathNavigator:
    """
    $.key, $.key.nested, $.arr[0], $.arr[*], $.arr[*].field
    """

    @staticmethod
    def get(data, path):
        if not path or path == '$':
            return data
        current = data
        for tok in JSONPathNavigator._tokenize(path.lstrip('$').lstrip('.')):
            if current is None:
                return None
            if isinstance(tok, int):
                if isinstance(current, list) and -len(current) <= tok < len(current):
                    current = current[tok]
                else:
                    return None
            elif tok == '*':
                if isinstance(current, list):
                    pass  # current already is the list
                elif isinstance(current, dict):
                    current = list(current.values())
                else:
                    return None
            elif isinstance(tok, str):
                if isinstance(current, dict):
                    current = current.get(tok)
                elif isinstance(current, list):
                    current = [i.get(tok) for i in current
                               if isinstance(i, dict) and tok in i]
                else:
                    return None
        return current

    @staticmethod
    def set(data, path, value):
        if not path or path == '$':
            return value
        tokens = JSONPathNavigator._tokenize(path.lstrip('$').lstrip('.'))
        if not tokens:
            return data
        if data is None:
            data = {} if isinstance(tokens[0], str) else []

        cur = data
        for i, tok in enumerate(tokens[:-1]):
            nxt = tokens[i + 1]
            if isinstance(tok, int) and isinstance(cur, list):
                while len(cur) <= tok:
                    cur.append(None)
                if cur[tok] is None:
                    cur[tok] = {} if isinstance(nxt, str) else []
                cur = cur[tok]
            elif isinstance(tok, str) and isinstance(cur, dict):
                if tok not in cur or cur[tok] is None:
                    cur[tok] = {} if isinstance(nxt, str) else []
                cur = cur[tok]

        last = tokens[-1]
        if isinstance(last, int) and isinstance(cur, list):
            while len(cur) <= last:
                cur.append(None)
            cur[last] = value
        elif isinstance(last, str) and isinstance(cur, dict):
            cur[last] = value
        return data

    @staticmethod
    def list_paths(data, prefix='$', max_depth=20):
        paths = []
        JSONPathNavigator._walk(data, prefix, paths, 0, max_depth)
        return paths

    @staticmethod
    def _walk(data, path, paths, depth, max_depth):
        if depth > max_depth:
            return
        if isinstance(data, dict):
            if not data:
                paths.append(path)
            for k, v in data.items():
                JSONPathNavigator._walk(v, f"{path}.{k}", paths, depth + 1, max_depth)
        elif isinstance(data, list):
            if not data:
                paths.append(path)
            for i, item in enumerate(data[:10]):
                JSONPathNavigator._walk(item, f"{path}[{i}]", paths, depth + 1, max_depth)
        else:
            paths.append(path)

    @staticmethod
    def _tokenize(path):
        tokens = []
        i = 0
        while i < len(path):
            if path[i] == '.':
                i += 1
            elif path[i] == '[':
                end = path.index(']', i)
                inside = path[i + 1:end]
                tokens.append('*' if inside == '*' else
                              (int(inside) if inside.lstrip('-').isdigit()
                               else inside.strip("'\"")))
                i = end + 1
            else:
                end = len(path)
                for c in '.[]':
                    p = path.find(c, i)
                    if p != -1:
                        end = min(end, p)
                tokens.append(path[i:end])
                i = end
        return tokens


# ===========================================================================
# JSON SCHEMA VALIDATOR  (equivalente XSDValidator)
# ===========================================================================

class JSONSchemaValidator:
    """
    Valida JSON contro JSON Schema.
    Stessa interfaccia di XSDValidator: __init__(path), validate(content), validate_file(path).
    Usa jsonschema se installato, altrimenti validazione built-in.
    """

    def __init__(self, schema=None, schema_path=None):
        self.schema = None
        self._lib_available = False

        if schema_path:
            import os
            if os.path.exists(schema_path):
                try:
                    with open(schema_path, 'r', encoding='utf-8') as f:
                        self.schema = json.load(f)
                    print(f"✅ JSON Schema loaded: {schema_path}")
                except Exception as e:
                    print(f"⚠️ JSON Schema load error: {e}")
            else:
                print(f"⚠️ JSON Schema not found: {schema_path}")
        elif schema:
            self.schema = json.loads(schema) if isinstance(schema, str) else schema

        try:
            import jsonschema as _js
            self._lib_available = True
        except ImportError:
            pass

    def validate(self, content):
        """Ritorna (is_valid, errors) — stessa interfaccia di XSDValidator."""
        if self.schema is None:
            return True, ["JSON Schema validation skipped (no schema loaded)"]
        if isinstance(content, str):
            try:
                data = json.loads(content)
            except json.JSONDecodeError as e:
                return False, [f"JSON parse error: {e}"]
        else:
            data = content
        if self._lib_available:
            return self._val_lib(data)
        return self._val_builtin(data, self.schema, '$')

    def validate_file(self, path):
        """Stessa interfaccia di XSDValidator.validate_file."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return self.validate(f.read())
        except Exception as e:
            return False, [f"File error: {e}"]

    # --- jsonschema library ---
    def _val_lib(self, data):
        import jsonschema
        errors = []
        try:
            cls = jsonschema.validators.validator_for(self.schema)
            v = cls(self.schema)
            for err in sorted(v.iter_errors(data), key=lambda e: list(e.path)):
                p = '.'.join(str(x) for x in err.absolute_path)
                errors.append(f"$.{p}: {err.message}" if p else f"$: {err.message}")
        except jsonschema.exceptions.SchemaError as e:
            errors.append(f"Schema error: {e.message}")
        except Exception as e:
            errors.append(f"Validation error: {e}")
        return len(errors) == 0, errors

    # --- built-in fallback ---
    def _val_builtin(self, data, schema, path):
        errors = []

        # $ref
        if '$ref' in schema:
            resolved = self._resolve_ref(schema['$ref'])
            if resolved is None:
                return False, [f"{path}: unresolvable $ref: {schema['$ref']}"]
            schema = resolved

        # allOf / anyOf / oneOf
        for sub in schema.get('allOf', []):
            _, e = self._val_builtin(data, sub, path); errors.extend(e)
        if 'anyOf' in schema:
            if not any(self._val_builtin(data, s, path)[0] for s in schema['anyOf']):
                errors.append(f"{path}: no match in anyOf")
        if 'oneOf' in schema:
            n = sum(1 for s in schema['oneOf'] if self._val_builtin(data, s, path)[0])
            if n != 1:
                errors.append(f"{path}: must match exactly 1 in oneOf (matched {n})")

        # type
        exp = schema.get('type')
        if exp and not self._chk_type(data, exp):
            return False, errors + [f"{path}: expected '{exp}', got '{type(data).__name__}'"]

        # enum / const
        if 'enum' in schema and data not in schema['enum']:
            errors.append(f"{path}: not in enum {schema['enum']}")
        if 'const' in schema and data != schema['const']:
            errors.append(f"{path}: expected const '{schema['const']}'")

        # string
        if isinstance(data, str):
            if 'minLength' in schema and len(data) < schema['minLength']:
                errors.append(f"{path}: len {len(data)} < minLength {schema['minLength']}")
            if 'maxLength' in schema and len(data) > schema['maxLength']:
                errors.append(f"{path}: len {len(data)} > maxLength {schema['maxLength']}")
            if 'pattern' in schema and not re.search(schema['pattern'], data):
                errors.append(f"{path}: pattern mismatch")

        # number
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            if 'minimum' in schema and data < schema['minimum']:
                errors.append(f"{path}: {data} < min {schema['minimum']}")
            if 'maximum' in schema and data > schema['maximum']:
                errors.append(f"{path}: {data} > max {schema['maximum']}")
            if 'exclusiveMinimum' in schema and data <= schema['exclusiveMinimum']:
                errors.append(f"{path}: {data} <= exclusiveMin")
            if 'exclusiveMaximum' in schema and data >= schema['exclusiveMaximum']:
                errors.append(f"{path}: {data} >= exclusiveMax")
            if 'multipleOf' in schema and schema['multipleOf']:
                if data % schema['multipleOf'] != 0:
                    errors.append(f"{path}: not multipleOf {schema['multipleOf']}")

        # object
        if isinstance(data, dict):
            for r in schema.get('required', []):
                if r not in data:
                    errors.append(f"{path}: required missing: '{r}'")
            props = schema.get('properties', {})
            for k, v in data.items():
                if k in props:
                    _, e = self._val_builtin(v, props[k], f"{path}.{k}")
                    errors.extend(e)
                elif 'additionalProperties' in schema:
                    ap = schema['additionalProperties']
                    if ap is False:
                        errors.append(f"{path}: extra property: '{k}'")
                    elif isinstance(ap, dict):
                        _, e = self._val_builtin(v, ap, f"{path}.{k}")
                        errors.extend(e)
            if 'minProperties' in schema and len(data) < schema['minProperties']:
                errors.append(f"{path}: {len(data)} props < min {schema['minProperties']}")
            if 'maxProperties' in schema and len(data) > schema['maxProperties']:
                errors.append(f"{path}: {len(data)} props > max {schema['maxProperties']}")

        # array
        if isinstance(data, list):
            if 'minItems' in schema and len(data) < schema['minItems']:
                errors.append(f"{path}: {len(data)} items < min {schema['minItems']}")
            if 'maxItems' in schema and len(data) > schema['maxItems']:
                errors.append(f"{path}: {len(data)} items > max {schema['maxItems']}")
            if schema.get('uniqueItems'):
                seen = set()
                for item in data:
                    s = json.dumps(item, sort_keys=True)
                    if s in seen:
                        errors.append(f"{path}: duplicate items")
                        break
                    seen.add(s)
            items_s = schema.get('items')
            if items_s and isinstance(items_s, dict):
                for i, item in enumerate(data):
                    _, e = self._val_builtin(item, items_s, f"{path}[{i}]")
                    errors.extend(e)

        return len(errors) == 0, errors

    def _chk_type(self, data, expected):
        if isinstance(expected, list):
            return any(self._chk_type(data, t) for t in expected)
        m = {'string': str, 'integer': int, 'number': (int, float),
             'boolean': bool, 'null': type(None), 'object': dict, 'array': list}
        t = m.get(expected)
        if t is None:
            return True
        if expected == 'integer' and isinstance(data, bool):
            return False
        return isinstance(data, t)

    def _resolve_ref(self, ref):
        if not ref.startswith('#/'):
            return None
        cur = self.schema
        for part in ref[2:].split('/'):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur if isinstance(cur, dict) else None


# ===========================================================================
# JSON BUSINESS RULES VALIDATOR  (equivalente SchematronValidator)
# ===========================================================================

class JSONBusinessRulesValidator:
    """
    Regole business custom per JSON.
    Equivalente di SchematronValidator per XML.
    Stessa interfaccia: add_rule(), validate() → (bool, errors).
    """

    def __init__(self):
        self.rules = []

    def add_rule(self, rule_func, description):
        """func(data) → (bool, error_msg)"""
        self.rules.append({'function': rule_func, 'description': description})

    def add_required_path(self, json_path, description=None):
        desc = description or f"Required: {json_path}"
        def check(data):
            val = JSONPathNavigator.get(data, json_path)
            if val is None:
                return False, f"{desc} — '{json_path}' is null or missing"
            return True, ""
        self.rules.append({'function': check, 'description': desc})

    def add_value_rule(self, json_path, condition, value, description=None):
        """condition: eq, neq, gt, gte, lt, lte, in, not_in, matches"""
        desc = description or f"{json_path} {condition} {value}"
        ops = {
            'eq': lambda a, b: a == b, 'neq': lambda a, b: a != b,
            'gt': lambda a, b: a > b, 'gte': lambda a, b: a >= b,
            'lt': lambda a, b: a < b, 'lte': lambda a, b: a <= b,
            'in': lambda a, b: a in b, 'not_in': lambda a, b: a not in b,
            'matches': lambda a, b: bool(re.search(b, str(a))),
        }
        op = ops.get(condition)
        if not op:
            return
        def check(data):
            actual = JSONPathNavigator.get(data, json_path)
            if actual is None:
                return False, f"{desc} — path not found"
            try:
                return (True, "") if op(actual, value) else (False, f"{desc} — got '{actual}'")
            except Exception as e:
                return False, f"{desc} — error: {e}"
        self.rules.append({'function': check, 'description': desc})

    def validate(self, data):
        """Stessa interfaccia di SchematronValidator.validate()."""
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                return False, [f"JSON parse error: {e}"]
        if not self.rules:
            return True, []
        errors = []
        for rule in self.rules:
            try:
                ok, msg = rule['function'](data)
                if not ok:
                    errors.append(f"Business Rule: {msg}")
            except Exception as e:
                errors.append(f"Business Rule '{rule['description']}': {e}")
        return len(errors) == 0, errors


# ===========================================================================
# SCHEMA DETECTION DA SAMPLE  (equivalente parse_sample_xml)
# ===========================================================================

def detect_schema_from_sample(content):
    """Inferisce JSON Schema da un JSON di esempio."""
    data = json.loads(content)
    schema = _infer_schema(data)
    schema['$schema'] = 'https://json-schema.org/draft/2020-12/schema'
    return schema


def _infer_schema(data):
    if data is None:
        return {'type': 'null'}
    if isinstance(data, bool):
        return {'type': 'boolean'}
    if isinstance(data, int):
        return {'type': 'integer'}
    if isinstance(data, float):
        return {'type': 'number'}
    if isinstance(data, str):
        s = {'type': 'string'}
        if re.match(r'^\d{4}-\d{2}-\d{2}T', data):
            s['format'] = 'date-time'
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', data):
            s['format'] = 'date'
        elif re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', data):
            s['format'] = 'email'
        elif re.match(r'^https?://', data):
            s['format'] = 'uri'
        elif re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-', data, re.I):
            s['format'] = 'uuid'
        return s
    if isinstance(data, list):
        s = {'type': 'array'}
        if data:
            items = [_infer_schema(i) for i in data[:10]]
            types = list({x.get('type') for x in items})
            if len(types) == 1:
                s['items'] = _merge_object_schemas(items) if types[0] == 'object' else items[0]
            else:
                s['items'] = {'anyOf': items}
        return s
    if isinstance(data, dict):
        return {
            'type': 'object',
            'properties': {k: _infer_schema(v) for k, v in data.items()},
            'required': list(data.keys()),
        }
    return {}


def _merge_object_schemas(schemas):
    merged = {}
    required = None
    for s in schemas:
        props = s.get('properties', {})
        keys = set(props.keys())
        required = keys.copy() if required is None else required & keys
        for k, v in props.items():
            if k not in merged:
                merged[k] = v
    result = {'type': 'object', 'properties': merged}
    if required:
        result['required'] = sorted(required)
    return result


# ===========================================================================
# PARSE JSON SCHEMA FILE  (equivalente schema_parser.parse_xsd)
# ===========================================================================

def parse_json_schema_file(schema_path):
    """Converte JSON Schema file in lista campi Buddyliko."""
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    fields = []
    name = schema.get('title', 'JSONSchema')
    _fields_from_schema(schema, '$', name, fields, schema)
    return {
        'id': str(uuid.uuid4()),
        'name': name,
        'source': 'json_schema',
        'fields': fields,
        'field_count': len(fields),
        'original_schema': schema,
        'created_at': datetime.now(timezone.utc).isoformat(),
    }


def _fields_from_schema(schema, path, root, fields, root_schema, depth=0):
    if depth > 20:
        return
    if '$ref' in schema:
        ref = schema['$ref']
        if ref.startswith('#/'):
            cur = root_schema
            for p in ref[2:].split('/'):
                cur = cur.get(p, {})
            if cur:
                _fields_from_schema(cur, path, root, fields, root_schema, depth)
        return

    st = schema.get('type', 'object')
    if st == 'object':
        for key, ps in schema.get('properties', {}).items():
            cp = f"{path}.{key}"
            pt = ps.get('type', 'string')
            req = key in schema.get('required', [])
            if pt == 'object':
                _fields_from_schema(ps, cp, root, fields, root_schema, depth + 1)
            elif pt == 'array':
                bp = f"{root}/{cp.replace('$.', '').replace('.', '/')}"
                fields.append({
                    'id': str(uuid.uuid4()), 'name': key, 'type': 'array',
                    'path': bp, 'json_path': cp, 'xml_path': cp,
                    'required': req,
                    'description': ps.get('description', f"Array of {key}"),
                })
                items = ps.get('items', {})
                if items.get('type') == 'object' or 'properties' in items:
                    _fields_from_schema(items, f"{cp}[*]", root, fields,
                                        root_schema, depth + 1)
            else:
                bp = f"{root}/{cp.replace('$.', '').replace('.', '/')}"
                fields.append({
                    'id': str(uuid.uuid4()), 'name': key, 'type': pt,
                    'path': bp, 'json_path': cp, 'xml_path': cp,
                    'required': req,
                    'description': ps.get('description', ''),
                    'enum': ps.get('enum'),
                    'format': ps.get('format'),
                    'default': ps.get('default'),
                })
    elif st == 'array':
        items = schema.get('items', {})
        if items:
            _fields_from_schema(items, f"{path}[*]", root, fields,
                                root_schema, depth + 1)


# ===========================================================================
# INTERFACCIA UNIFICATA  (come parse_edi, parse_hl7)
# ===========================================================================

def parse_json(content):
    """Parsa JSON → struttura normalizzata."""
    return JSONParser(content).parse()


# ===========================================================================
# TO BUDDYLIKO SCHEMA  (come edi_to_schema, to_buddyliko_schema_hl7)
# ===========================================================================

def to_buddyliko_schema(parsed, name=None):
    """Converte JSON parsato in schema Buddyliko per il mapper."""
    data = parsed.get('data', parsed)
    fmt = parsed.get('format', 'JSON')
    schema_name = name or f"JSON_{fmt}"
    fields = []
    _fields_from_data(data, '$', schema_name, fields, 0)
    return {
        'id': str(uuid.uuid4()),
        'name': schema_name,
        'source': 'json',
        'json_format': fmt,
        'fields': fields,
        'field_count': len(fields),
        'created_at': datetime.now(timezone.utc).isoformat(),
    }


def _fields_from_data(data, jp, sn, fields, depth, max_d=20):
    if depth > max_d:
        return
    if isinstance(data, dict):
        for k, v in data.items():
            cp = f"{jp}.{k}"
            if isinstance(v, dict):
                _fields_from_data(v, cp, sn, fields, depth + 1)
            elif isinstance(v, list):
                bp = f"{sn}/{cp.replace('$.', '').replace('.', '/')}"
                fields.append({
                    'id': str(uuid.uuid4()), 'name': k, 'type': 'array',
                    'path': bp, 'xml_path': cp, 'json_path': cp,
                    'description': f"Array ({len(v)} items)",
                })
                if v and isinstance(v[0], dict):
                    _fields_from_data(v[0], f"{cp}[0]", sn, fields, depth + 1)
            else:
                bp = f"{sn}/{cp.replace('$.', '').replace('.', '/')}"
                fields.append({
                    'id': str(uuid.uuid4()), 'name': k,
                    'type': _py_type(v), 'path': bp,
                    'xml_path': cp, 'json_path': cp,
                    'description': f"{type(v).__name__} field",
                    'sample_value': str(v)[:100] if v is not None else None,
                })
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        _fields_from_data(data[0], f"{jp}[0]", sn, fields, depth + 1)


def _py_type(v):
    if v is None: return 'string'
    if isinstance(v, bool): return 'boolean'
    if isinstance(v, int): return 'integer'
    if isinstance(v, float): return 'decimal'
    if isinstance(v, str):
        return 'date' if re.match(r'^\d{4}-\d{2}-\d{2}', v) else 'string'
    return 'string'


# ===========================================================================
# FLATTEN  (come edi_to_flat, hl7v2_to_flat)
# ===========================================================================

def json_to_flat(parsed):
    """JSON parsato → lista record flat {path: value}."""
    data = parsed.get('data', parsed)
    if isinstance(data, list):
        records = []
        for i, item in enumerate(data):
            if isinstance(item, dict):
                rec = {'_index': i}
                _flatten(item, '$', rec)
                records.append(rec)
            else:
                records.append({'_index': i, '_value': item})
        return records
    if isinstance(data, dict):
        rec = {}
        _flatten(data, '$', rec)
        return [rec]
    return [{'_value': data}]


def _flatten(data, prefix, rec):
    for k, v in data.items():
        p = f"{prefix}.{k}"
        if isinstance(v, dict):
            _flatten(v, p, rec)
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                for i, item in enumerate(v[:50]):
                    _flatten(item, f"{p}[{i}]", rec)
            else:
                rec[p] = json.dumps(v)
        else:
            rec[p] = v


# ===========================================================================
# BUILD JSON  (come build_edi, build_hl7v2)
# ===========================================================================

def build_json(data, indent=2, compact=False, sort_keys=False,
               ensure_ascii=False, ndjson=False):
    """Genera JSON da dati strutturati."""
    w = JSONWriter(indent=indent, sort_keys=sort_keys, ensure_ascii=ensure_ascii)
    if ndjson and isinstance(data, list):
        return w.build_lines(data)
    return w.build(data, compact=compact)


# ===========================================================================
# VALIDATE JSON  (come _validate_input/_validate_output per XML)
# ===========================================================================

def validate_json(content, schema=None, schema_path=None, business_rules=None):
    """
    Validazione completa: JSON Schema + Business Rules.
    Equivalente di validate_input/output(XSD + Schematron + Business Rules) per XML.
    Ritorna (is_valid, errors).
    """
    all_errors = []

    if isinstance(content, str):
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            return False, [f"JSON parse error: {e}"]
    else:
        data = content

    # JSON Schema (= XSD)
    if schema or schema_path:
        v = JSONSchemaValidator(schema=schema, schema_path=schema_path)
        ok, errs = v.validate(data)
        if not ok:
            all_errors.extend([f"Schema: {e}" for e in errs])

    # Business rules (= Schematron)
    if business_rules:
        ok, errs = business_rules.validate(data)
        if not ok:
            all_errors.extend(errs)

    return len(all_errors) == 0, all_errors


def validate_json_file(file_path, schema=None, schema_path=None):
    """Valida file JSON. Equivalente di XSDValidator.validate_file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return validate_json(f.read(), schema=schema, schema_path=schema_path)
    except Exception as e:
        return False, [f"File error: {e}"]
