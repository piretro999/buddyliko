#!/usr/bin/env python3
"""
Buddyliko - Code Generator
Genera codice ottimizzato Python e C# da una mappatura validata.

Output:
  - transformer.py         → Python standalone, zero dipendenze, CLI
  - buddyliko_engine.py    → Python module per uso interno Buddyliko
  - BudlylikoTransformer/  → Progetto C# (libreria + CLI wrapper)
  - README.md
  - buddyliko_transformer.zip (tutto insieme)

Il codice generato è COMPILATO dalla mappa: nessuna interpretazione a runtime,
solo if/else e assegnazioni dirette → più veloce del motore dinamico.
"""

import json
import zipfile
import io
import os
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple


# ===========================================================================
# UTILITIES
# ===========================================================================

def _safe_id(s: str) -> str:
    """Converte un path/nome in identificatore valido Python/C#."""
    s = re.sub(r'[^a-zA-Z0-9_]', '_', str(s))
    if s and s[0].isdigit():
        s = '_' + s
    return s or '_field'


def _py_str(v) -> str:
    """Rappresentazione Python di un literal."""
    if v is None:
        return 'None'
    if isinstance(v, bool):
        return 'True' if v else 'False'
    if isinstance(v, (int, float)):
        return str(v)
    return repr(str(v))


def _cs_str(v) -> str:
    """Rappresentazione C# di un literal."""
    if v is None:
        return 'null'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, (int, float)):
        return str(v)
    return f'"{str(v).replace(chr(34), chr(92)+chr(34))}"'


def _get_path_parts(path: str) -> List[str]:
    """Estrae parti di un path XML/JSON (rimuove namespace, attributi)."""
    if not path:
        return []
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip() and not p.strip().startswith('@')]
    # Rimuovi prefissi namespace
    return [p.split(':')[-1] if ':' in p else p for p in parts if p]


# ===========================================================================
# FORMULA CODE GENERATORS
# ===========================================================================

def _gen_py_formula(trans: Any, src_var: str, ctx_var: str = 'input_data') -> str:
    """Genera codice Python per una trasformazione."""
    if not trans:
        return src_var

    if isinstance(trans, str):
        # Vecchio formato stringa
        t = trans.upper()
        if t == 'UPPER':     return f'str({src_var}).upper() if {src_var} else ""'
        if t == 'LOWER':     return f'str({src_var}).lower() if {src_var} else ""'
        if t == 'TRIM':      return f'str({src_var}).strip() if {src_var} else ""'
        if t == 'DIRECT':    return src_var
        return src_var

    if not isinstance(trans, dict):
        return src_var

    t = trans.get('type', 'DIRECT').upper()
    formula = trans.get('formula', {}) or {}
    if isinstance(formula, str):
        formula = {}

    if t == 'DIRECT':
        return src_var

    elif t in ('UPPER', 'LOWER', 'TRIM'):
        ops = {'UPPER': 'upper()', 'LOWER': 'lower()', 'TRIM': 'strip()'}
        return f'str({src_var}).{ops[t]} if {src_var} is not None else ""'

    elif t == 'CONCAT':
        sep = _py_str(formula.get('separator', ''))
        inputs = formula.get('inputs', [])
        if inputs:
            parts = []
            for inp in inputs:
                if 'literal' in inp:
                    parts.append(_py_str(inp['literal']))
                elif 'field' in inp:
                    field = inp['field']
                    parts.append(f'str(_get({ctx_var}, {_py_str(field)}) or "")')
            return f'{sep}.join([p for p in [{", ".join(parts)}] if p])'
        return f'{sep}.join([str(v) for v in ([{src_var}] if not isinstance({src_var}, list) else {src_var}) if v is not None])'

    elif t == 'SPLIT':
        delim = formula.get('delimiter', '')
        regex = formula.get('regex', '')
        idx = formula.get('index', 0)
        if regex:
            return f'(re.split({_py_str(regex)}, str({src_var} or "")) or [""])[{idx}]'
        return f'(str({src_var} or "").split({_py_str(delim)}) or [""])[{idx}]'

    elif t == 'MATH':
        op = formula.get('operation', 'round')
        operand = formula.get('operand', 1)
        decimals = formula.get('decimals', 2)
        ops = {
            'add':      f'float({src_var} or 0) + {operand}',
            'subtract': f'float({src_var} or 0) - {operand}',
            'multiply': f'float({src_var} or 0) * {operand}',
            'divide':   f'float({src_var} or 0) / {operand} if {operand} != 0 else 0',
            'abs':      f'abs(float({src_var} or 0))',
            'negate':   f'-float({src_var} or 0)',
            'round':    f'round(float({src_var} or 0), {decimals})',
        }
        return ops.get(op, src_var)

    elif t == 'LOOKUP':
        table = formula.get('table', {})
        default = formula.get('default')
        case_sensitive = formula.get('case_sensitive', False)
        table_repr = repr(table)
        if case_sensitive:
            return f'{table_repr}.get(str({src_var} or ""), {_py_str(default) if default is not None else src_var})'
        return f'{{{", ".join([repr(k.lower())+": "+repr(v) for k,v in table.items()])}}}.get(str({src_var} or "").lower(), {_py_str(default) if default is not None else src_var})'

    elif t in ('STRING_OP', 'REPLACE', 'SUBSTRING', 'PAD_LEFT', 'PAD_RIGHT'):
        op = formula.get('operation', t.lower())
        if op == 'replace':
            find = formula.get('find', '')
            repl = formula.get('replace_with') or formula.get('replace', '')
            return f'str({src_var} or "").replace({_py_str(find)}, {_py_str(repl)})'
        elif op == 'substring':
            start = formula.get('start', 0)
            end = formula.get('end')
            return f'str({src_var} or "")[{start}:{end}]' if end is not None else f'str({src_var} or "")[{start}:]'
        elif op == 'pad_left':
            width = formula.get('width', 0)
            char = formula.get('char', ' ')
            return f'str({src_var} or "").rjust({width}, {_py_str(char)})'
        elif op == 'pad_right':
            width = formula.get('width', 0)
            char = formula.get('char', ' ')
            return f'str({src_var} or "").ljust({width}, {_py_str(char)})'
        elif op == 'regex_replace':
            pattern = formula.get('pattern', '')
            repl = formula.get('repl', '')
            return f're.sub({_py_str(pattern)}, {_py_str(repl)}, str({src_var} or ""))'
        return f'str({src_var} or "").strip()'

    elif t == 'DATE_FORMAT':
        fmt_in = formula.get('input_format', '%Y%m%d')
        fmt_out = formula.get('output_format', '%Y-%m-%d')
        return (f'(lambda _d: datetime.strptime(_d, {_py_str(fmt_in)}).strftime({_py_str(fmt_out)}) '
                f'if _d else "")({src_var} if isinstance({src_var}, str) else str({src_var} or ""))')

    elif t == 'CONDITIONAL':
        conditions = formula.get('conditions', [])
        default = formula.get('default', 'pass_through')
        if not conditions:
            return src_var
        lines = []
        for cond in conditions:
            cond_expr = cond.get('if', '')
            then_val = cond.get('then', {})
            if 'starts_with' in cond_expr:
                m = re.search(r'starts_with\("(.+?)"\)', cond_expr)
                if m:
                    prefix = m.group(1)
                    op = then_val.get('operation', 'pass_through')
                    if op == 'pass_through':
                        lines.append(f'str({src_var}).startswith({_py_str(prefix)}): {src_var}')
                    else:
                        lines.append(f'str({src_var}).startswith({_py_str(prefix)}): {src_var}')
        if lines:
            result = ' else '.join([f'({v} if {k})' for part in lines for k, v in [part.split(': ', 1)]])
            return result + f' else {src_var}'
        return src_var

    elif t == 'HARDCODE':
        value = formula.get('value', '')
        return _py_str(value)

    elif t == 'DEFAULT':
        default = formula.get('default', '')
        return f'{src_var} if {src_var} is not None and {src_var} != "" else {_py_str(default)}'

    elif t == 'COALESCE':
        fields = formula.get('fields', [])
        parts = [f'_get({ctx_var}, {_py_str(f)})' for f in fields]
        parts.append(src_var)
        return f'next((v for v in [{", ".join(parts)}] if v is not None and v != ""), None)'

    elif t == 'SUM_MULTI':
        decimals = formula.get('decimals', 2)
        fields = formula.get('fields', [])
        extra = ' + '.join([f'float(_get({ctx_var}, {_py_str(f)}) or 0)' for f in fields])
        base = f'(float(v) for v in ({src_var} if isinstance({src_var}, list) else [{src_var}]) if v is not None)'
        if extra:
            return f'round(sum({base}) + {extra}, {decimals})'
        return f'round(sum({base}), {decimals})'

    elif t == 'NOOP':
        # NOOP: pass-through, valore invariato
        return src_var

    elif t == 'CUSTOM':
        expr = formula.get('expression', '')
        if expr:
            safe_expr = expr.replace('value', src_var)
            return f'(lambda value: {safe_expr})({src_var})'
        return src_var

    return src_var


def _gen_cs_formula(trans: Any, src_var: str) -> str:
    """Genera codice C# per una trasformazione."""
    if not trans:
        return src_var

    if isinstance(trans, str):
        t = trans.upper()
        if t == 'UPPER': return f'({src_var} ?? "").ToUpper()'
        if t == 'LOWER': return f'({src_var} ?? "").ToLower()'
        if t == 'TRIM':  return f'({src_var} ?? "").Trim()'
        return src_var

    if not isinstance(trans, dict):
        return src_var

    t = trans.get('type', 'DIRECT').upper()
    formula = trans.get('formula', {}) or {}
    if isinstance(formula, str):
        formula = {}

    if t == 'DIRECT':
        return src_var

    elif t == 'UPPER': return f'({src_var} ?? "").ToUpper()'
    elif t == 'LOWER': return f'({src_var} ?? "").ToLower()'
    elif t == 'TRIM':  return f'({src_var} ?? "").Trim()'

    elif t == 'CONCAT':
        sep = formula.get('separator', '')
        inputs = formula.get('inputs', [])
        if inputs:
            parts = []
            for inp in inputs:
                if 'literal' in inp:
                    parts.append(_cs_str(inp['literal']))
                elif 'field' in inp:
                    field = inp['field']
                    parts.append(f'GetValue(input, {_cs_str(field)}) ?? ""')
            return f'string.Join({_cs_str(sep)}, new[]{{ {", ".join(parts)} }}.Where(p => p != ""))'
        return f'string.Join({_cs_str(sep)}, new[]{{ {src_var} ?? "" }})'

    elif t == 'MATH':
        op = formula.get('operation', 'round')
        operand = formula.get('operand', 1)
        decimals = formula.get('decimals', 2)
        base = f'double.TryParse({src_var}, out var __n) ? __n : 0'
        ops = {
            'add':      f'(({base}) + {operand})',
            'subtract': f'(({base}) - {operand})',
            'multiply': f'(({base}) * {operand})',
            'divide':   f'({operand} != 0 ? (({base}) / {operand}) : 0)',
            'abs':      f'Math.Abs(({base}))',
            'negate':   f'-(({base}))',
            'round':    f'Math.Round(({base}), {decimals})',
        }
        return f'({ops.get(op, base)}).ToString()'

    elif t == 'LOOKUP':
        table = formula.get('table', {})
        default = formula.get('default', '')
        entries = ', '.join([f'{{ {_cs_str(k)}, {_cs_str(v)} }}' for k, v in table.items()])
        default_cs = _cs_str(default) if default is not None else src_var
        return (f'(new Dictionary<string,string>{{ {entries} }})'
                f'.TryGetValue(({src_var} ?? "").ToLower(), out var __lv) ? __lv : {default_cs}')

    elif t in ('STRING_OP', 'REPLACE', 'SUBSTRING', 'PAD_LEFT', 'PAD_RIGHT'):
        op = formula.get('operation', 'trim')
        if op == 'replace':
            find = formula.get('find', '')
            repl = formula.get('replace_with') or formula.get('replace', '')
            return f'({src_var} ?? "").Replace({_cs_str(find)}, {_cs_str(repl)})'
        elif op == 'substring':
            start = formula.get('start', 0)
            end = formula.get('end')
            if end is not None:
                return f'({src_var} ?? "").Substring({start}, Math.Min({end-start}, ({src_var} ?? "").Length - {start}))'
            return f'({src_var} ?? "").Substring(Math.Min({start}, ({src_var} ?? "").Length))'
        elif op == 'pad_left':
            width = formula.get('width', 0)
            char = formula.get('char', ' ')
            return f'({src_var} ?? "").PadLeft({width}, {_cs_str(char)}[0])'
        elif op == 'pad_right':
            width = formula.get('width', 0)
            char = formula.get('char', ' ')
            return f'({src_var} ?? "").PadRight({width}, {_cs_str(char)}[0])'
        return f'({src_var} ?? "").Trim()'

    elif t == 'HARDCODE':
        value = formula.get('value', '')
        return _cs_str(value)

    elif t == 'DEFAULT':
        default = formula.get('default', '')
        return f'(string.IsNullOrEmpty({src_var}) ? {_cs_str(default)} : {src_var})'

    elif t == 'SUM_MULTI':
        decimals = formula.get('decimals', 2)
        fields = formula.get('fields', [])
        extra = ' + '.join([f'(double.TryParse(GetValue(inputData, {_cs_str(f)}), out var __sf{i}) ? __sf{i} : 0)' for i, f in enumerate(fields)])
        base = f'(double.TryParse({src_var}, out var __sm) ? __sm : 0)'
        if extra:
            return f'Math.Round({base} + {extra}, {decimals}).ToString()'
        return f'Math.Round({base}, {decimals}).ToString()'

    elif t == 'NOOP':
        return src_var

    return src_var


# ===========================================================================
# PYTHON STANDALONE GENERATOR
# ===========================================================================

def generate_python_standalone(mapping_rules: Dict, project_name: str = "transformer") -> str:
    """
    Genera transformer.py: script Python standalone, zero dipendenze esterne.
    Accetta input da file o stdin, produce output su file o stdout.
    """
    connections = mapping_rules.get('connections', [])
    input_schema = mapping_rules.get('inputSchema', {})
    output_schema = mapping_rules.get('outputSchema', {})
    proj = _safe_id(project_name)
    ts = datetime.utcnow().strftime('%Y-%m-%d %Human:%M:%S UTC')

    lines = [f'''#!/usr/bin/env python3
"""
{project_name} - Generated by Buddyliko Code Generator
Generated: {ts}
Connections: {len(connections)}
Input:  {input_schema.get("name", "?")}
Output: {output_schema.get("name", "?")}

USAGE:
    python transformer.py input.xml output.xml
    python transformer.py input.json output.json
    python transformer.py input.csv output.json
    cat input.xml | python transformer.py - -  (stdin/stdout)

No external dependencies required (Python 3.8+).
"""

import sys
import json
import re
import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional
try:
    import xml.etree.ElementTree as ET
    HAS_ET = True
except ImportError:
    HAS_ET = False

# ===========================================================================
# HELPERS
# ===========================================================================

def _get(data: Any, path: str, default=None) -> Any:
    """Legge un valore da un dict annidato tramite path (/ o .)."""
    if data is None or not path:
        return default
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip() and not p.strip().startswith('@')]
    parts = [p.split(':')[-1] if ':' in p else p for p in parts]
    current = data
    for part in parts:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                # Prova senza namespace, entra nel root se singolo
                found = False
                for k in current:
                    clean_k = k.split(':')[-1] if ':' in k else k
                    if clean_k == part:
                        current = current[k]
                        found = True
                        break
                if not found:
                    if len(current) == 1:
                        current = list(current.values())[0]
                        if isinstance(current, dict) and part in current:
                            current = current[part]
                        else:
                            return default
                    else:
                        return default
        elif isinstance(current, list):
            current = current[0] if current else default
            if isinstance(current, dict):
                current = current.get(part, default)
            else:
                return default
        else:
            return default
    return current


def _set(data: Dict, path: str, value: Any):
    """Scrive un valore in un dict annidato, creando la struttura se necessario."""
    if not path or value is None:
        return
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip() and not p.strip().startswith('@')]
    parts = [p.split(':')[-1] if ':' in p else p for p in parts]
    current = data
    for part in parts[:-1]:
        if part not in current:
            current[part] = {{}}
        current = current[part]
    if parts:
        current[parts[-1]] = value


# ===========================================================================
# PARSER
# ===========================================================================

def parse_xml(content: str) -> Dict:
    if not HAS_ET:
        raise RuntimeError("xml.etree.ElementTree not available")
    root = ET.fromstring(content)
    return _xml_elem_to_dict(root)

def _xml_elem_to_dict(elem) -> Any:
    tag = elem.tag.split('}}')[-1] if '}}' in elem.tag else elem.tag
    children = list(elem)
    if not children:
        return {{tag: elem.text or ''}}
    child_dict = {{}}
    for child in children:
        c = _xml_elem_to_dict(child)
        for k, v in c.items():
            if k in child_dict:
                if not isinstance(child_dict[k], list):
                    child_dict[k] = [child_dict[k]]
                child_dict[k].append(v)
            else:
                child_dict[k] = v
    return {{tag: child_dict}}

def parse_json(content: str) -> Dict:
    return json.loads(content)

def parse_csv(content: str) -> List[Dict]:
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


# ===========================================================================
# SERIALIZER
# ===========================================================================

def to_xml(data: Dict, root_name: str = "root") -> str:
    def _dict_to_xml(d, parent_tag):
        if isinstance(d, dict):
            elem = ET.Element(parent_tag)
            for k, v in d.items():
                child = _dict_to_xml(v, k)
                elem.append(child)
            return elem
        elif isinstance(d, list):
            elem = ET.Element(parent_tag)
            for item in d:
                child = _dict_to_xml(item, parent_tag + "_item")
                elem.append(child)
            return elem
        else:
            elem = ET.Element(parent_tag)
            elem.text = str(d) if d is not None else ''
            return elem
    root_tag = list(data.keys())[0] if len(data) == 1 else root_name
    root_data = data[root_tag] if root_tag in data else data
    root_elem = _dict_to_xml(root_data, root_tag)
    ET.indent(root_elem, space='  ')
    return '<?xml version="1.0" encoding="UTF-8"?>\\n' + ET.tostring(root_elem, encoding='unicode')

def to_json(data: Dict) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)

def to_csv(data: Any) -> str:
    if isinstance(data, list) and data:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()
    elif isinstance(data, dict):
        flat = {{k: str(v) for k, v in _flatten(data).items()}}
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=flat.keys())
        writer.writeheader()
        writer.writerow(flat)
        return output.getvalue()
    return ""

def _flatten(d: Dict, prefix: str = '') -> Dict:
    result = {{}}
    for k, v in d.items():
        key = f"{{prefix}}.{{k}}" if prefix else k
        if isinstance(v, dict):
            result.update(_flatten(v, key))
        else:
            result[key] = v
    return result


# ===========================================================================
# TRANSFORM (GENERATED FROM MAPPING)
# ===========================================================================

def transform(input_data: Any) -> Dict:
    """
    Trasformazione generata dalla mappa Buddyliko.
    {len(connections)} connessioni applicate in sequenza ottimizzata.
    """
    # Normalizza input come lista di record
    if isinstance(input_data, list):
        records = input_data
    elif isinstance(input_data, dict):
        # Entra nel root se singolo elemento
        vals = list(input_data.values())
        records = vals if isinstance(vals[0], list) else [input_data]
    else:
        records = [input_data]

    results = []
    for record in records:
        results.append(_transform_record(record))

    return results[0] if len(results) == 1 else results


def _transform_record(input_data: Dict) -> Dict:
    """Trasforma un singolo record."""
    output = {{}}

''']

    # Genera il corpo di _transform_record
    indent = '    '
    for i, conn in enumerate(connections):
        source_path = conn.get('sourcePath') or conn.get('source', '')
        target_path = conn.get('targetPath') or conn.get('target', '')
        trans = conn.get('transformation')

        if not source_path or not target_path:
            continue

        src_var = f'_v{i}'
        lines.append(f"{indent}# Connessione {i+1}: {source_path} → {target_path}")
        lines.append(f"{indent}{src_var} = _get(input_data, {_py_str(source_path)})")

        py_expr = _gen_py_formula(trans, src_var, 'input_data')
        if py_expr != src_var:
            lines.append(f"{indent}{src_var} = {py_expr}")

        lines.append(f"{indent}_set(output, {_py_str(target_path)}, {src_var})")
        lines.append('')

    lines.append(f"{indent}return output")

    lines.append('''

# ===========================================================================
# CLI ENTRY POINT
# ===========================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Buddyliko Transformer')
    parser.add_argument('input',  help='Input file path (- for stdin)')
    parser.add_argument('output', help='Output file path (- for stdout)', nargs='?', default='-')
    parser.add_argument('--input-format',  '-if', choices=['auto','xml','json','csv'], default='auto')
    parser.add_argument('--output-format', '-of', choices=['xml','json','csv'], default='json')
    args = parser.parse_args()

    # Leggi input
    if args.input == '-':
        content = sys.stdin.read()
    else:
        with open(args.input, 'r', encoding='utf-8') as f:
            content = f.read()

    # Detect formato
    fmt = args.input_format
    if fmt == 'auto':
        s = content.strip()
        if s.startswith('<'):   fmt = 'xml'
        elif s.startswith('{') or s.startswith('['): fmt = 'json'
        else: fmt = 'csv'

    # Parse
    if fmt == 'xml':       input_data = parse_xml(content)
    elif fmt == 'json':    input_data = parse_json(content)
    elif fmt == 'csv':     input_data = parse_csv(content)
    else:                  input_data = parse_json(content)

    # Trasforma
    result = transform(input_data)

    # Serializza
    ofmt = args.output_format
    if ofmt == 'xml':     output = to_xml(result)
    elif ofmt == 'csv':   output = to_csv(result)
    else:                 output = to_json(result)

    # Scrivi output
    if args.output == '-':
        sys.stdout.write(output)
    else:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f"✅ Output scritto in: {args.output}", file=sys.stderr)


if __name__ == '__main__':
    main()
''')

    return '\n'.join(lines)


# ===========================================================================
# PYTHON ENGINE MODULE GENERATOR (per uso interno Buddyliko)
# ===========================================================================

def generate_python_engine_module(mapping_rules: Dict, project_name: str = "transformer") -> str:
    """
    Genera buddyliko_engine.py: modulo Python da importare internamente.
    Espone transform(input_data, input_format, output_format) → result_dict
    Ottimizzato per velocità: nessun overhead di parsing della mappa a runtime.
    """
    connections = mapping_rules.get('connections', [])
    input_schema = mapping_rules.get('inputSchema', {})
    output_schema = mapping_rules.get('outputSchema', {})
    proj = _safe_id(project_name)
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

    lines = [f'''#!/usr/bin/env python3
"""
{project_name} - Buddyliko Internal Engine Module
Generated: {ts}
Connessioni: {len(connections)}

Importato da Buddyliko come motore ad alta performance.
NON modificare manualmente: rigenerare dalla mappa.
"""

import json
import re
import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# Metadati mappatura
MAPPING_METADATA = {{
    "name": {_py_str(project_name)},
    "generated": {_py_str(ts)},
    "connections": {len(connections)},
    "input_schema": {_py_str(input_schema.get("name", ""))},
    "output_schema": {_py_str(output_schema.get("name", ""))},
}}


def _get(data: Any, path: str, default=None) -> Any:
    if data is None or not path:
        return default
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip() and not p.strip().startswith('@')]
    parts = [p.split(':')[-1] if ':' in p else p for p in parts]
    current = data
    for part in parts:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                found = False
                for k in current:
                    if (k.split(':')[-1] if ':' in k else k) == part:
                        current = current[k]; found = True; break
                if not found:
                    if len(current) == 1:
                        current = list(current.values())[0]
                        if isinstance(current, dict) and part in current:
                            current = current[part]
                        else:
                            return default
                    else:
                        return default
        elif isinstance(current, list):
            current = current[0] if current else default
            if isinstance(current, dict):
                current = current.get(part, default)
            else:
                return default
        else:
            return default
    return current


def _set(data: Dict, path: str, value: Any):
    if not path or value is None:
        return
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip() and not p.strip().startswith('@')]
    parts = [p.split(':')[-1] if ':' in p else p for p in parts]
    current = data
    for part in parts[:-1]:
        if part not in current:
            current[part] = {{}}
        current = current[part]
    if parts:
        current[parts[-1]] = value


def transform_record(input_data: Dict) -> Tuple[Dict, List[str]]:
    """
    Trasforma un singolo record. Ritorna (output, warnings).
    Ottimizzata: nessun loop, nessuna lookup della mappa.
    """
    output: Dict = {{}}
    warnings: List[str] = []

''']

    indent = '    '
    for i, conn in enumerate(connections):
        source_path = conn.get('sourcePath') or conn.get('source', '')
        target_path = conn.get('targetPath') or conn.get('target', '')
        trans = conn.get('transformation')
        if not source_path or not target_path:
            continue
        src_var = f'_v{i}'
        lines.append(f"{indent}# [{i+1}] {source_path} → {target_path}")
        lines.append(f"{indent}try:")
        lines.append(f"{indent}    {src_var} = _get(input_data, {_py_str(source_path)})")
        py_expr = _gen_py_formula(trans, src_var, 'input_data')
        if py_expr != src_var:
            lines.append(f"{indent}    {src_var} = {py_expr}")
        lines.append(f"{indent}    _set(output, {_py_str(target_path)}, {src_var})")
        lines.append(f"{indent}except Exception as _e{i}:")
        lines.append(f"{indent}    warnings.append(f'Connection {i+1} failed: {{_e{i}}}')")
        lines.append('')

    lines.append(f"{indent}return output, warnings")

    lines.append(f'''

def transform(input_data: Any, batch: bool = False) -> Tuple[Any, List[str]]:
    """
    Entry point principale.
    - batch=False: input_data è un singolo record, ritorna (dict, warnings)
    - batch=True:  input_data è una lista, ritorna (list[dict], all_warnings)
    """
    if batch or isinstance(input_data, list):
        records = input_data if isinstance(input_data, list) else [input_data]
        results, all_warnings = [], []
        for rec in records:
            out, warns = transform_record(rec)
            results.append(out)
            all_warnings.extend(warns)
        return results, all_warnings
    else:
        return transform_record(input_data)


def get_metadata() -> Dict:
    return MAPPING_METADATA
''')

    return '\n'.join(lines)


# ===========================================================================
# C# GENERATOR
# ===========================================================================

def generate_csharp(mapping_rules: Dict, project_name: str = "transformer") -> Dict[str, str]:
    """
    Genera progetto C# completo:
    - BudlylikoTransformer.cs  (libreria)
    - Program.cs               (CLI wrapper)
    - BudlylikoTransformer.csproj
    """
    connections = mapping_rules.get('connections', [])
    input_schema = mapping_rules.get('inputSchema', {})
    output_schema = mapping_rules.get('outputSchema', {})
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    ns = re.sub(r'[^a-zA-Z0-9]', '', project_name.title()) or 'BudlylikoTransformer'

    # ---- Library ----
    lib_lines = [f'''// Generated by Buddyliko Code Generator
// {ts}
// Connections: {len(connections)}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace {ns}
{{
    public class TransformResult
    {{
        public Dictionary<string, object?> Output {{ get; set; }} = new();
        public List<string> Warnings {{ get; set; }} = new();
        public bool Success {{ get; set; }} = true;
        public string? Error {{ get; set; }}
    }}

    public static class Transformer
    {{
        public static readonly Dictionary<string, string> Metadata = new()
        {{
            ["name"]          = {_cs_str(project_name)},
            ["generated"]     = {_cs_str(ts)},
            ["connections"]   = {_cs_str(str(len(connections)))},
            ["input_schema"]  = {_cs_str(input_schema.get("name", ""))},
            ["output_schema"] = {_cs_str(output_schema.get("name", ""))},
        }};

        // ── Helpers ──────────────────────────────────────────────────────

        private static string? GetValue(Dictionary<string, object?> data, string path)
        {{
            var parts = path.Contains('/') ? path.Split('/') : path.Split('.');
            object? current = data;
            foreach (var raw in parts)
            {{
                var part = raw.Contains(':') ? raw.Split(':')[1] : raw;
                if (string.IsNullOrEmpty(part) || part.StartsWith("@")) continue;
                if (current is Dictionary<string, object?> dict)
                {{
                    if (dict.TryGetValue(part, out var val)) {{ current = val; continue; }}
                    // Try without namespace prefix
                    var found = dict.FirstOrDefault(kv =>
                        (kv.Key.Contains(':') ? kv.Key.Split(':')[1] : kv.Key) == part);
                    if (found.Key != null) {{ current = found.Value; continue; }}
                    // Auto-enter single-key root
                    if (dict.Count == 1) {{ current = dict.Values.First(); }}
                    else return null;
                }}
                else if (current is List<object?> list) {{ current = list.FirstOrDefault(); }}
                else return null;
            }}
            return current?.ToString();
        }}

        private static void SetValue(Dictionary<string, object?> data, string path, object? value)
        {{
            if (value == null) return;
            var parts = (path.Contains('/') ? path.Split('/') : path.Split('.'))
                .Where(p => !string.IsNullOrEmpty(p) && !p.StartsWith("@"))
                .Select(p => p.Contains(':') ? p.Split(':')[1] : p)
                .ToArray();
            var current = data;
            for (int i = 0; i < parts.Length - 1; i++)
            {{
                if (!current.ContainsKey(parts[i]))
                    current[parts[i]] = new Dictionary<string, object?>();
                current = (Dictionary<string, object?>)current[parts[i]]!;
            }}
            if (parts.Length > 0) current[parts[^1]] = value;
        }}

        // ── Transform ────────────────────────────────────────────────────

        public static TransformResult Transform(Dictionary<string, object?> inputData)
        {{
            var result = new TransformResult();
            var output = new Dictionary<string, object?>();
            string? _v;

            try
            {{
''']

    indent = '                '
    for i, conn in enumerate(connections):
        source_path = conn.get('sourcePath') or conn.get('source', '')
        target_path = conn.get('targetPath') or conn.get('target', '')
        trans = conn.get('transformation')
        if not source_path or not target_path:
            continue
        src_var = f'_v{i}'
        cs_expr = _gen_cs_formula(trans, f'GetValue(inputData, {_cs_str(source_path)})')
        lib_lines.append(f'{indent}// [{i+1}] {source_path} → {target_path}')
        lib_lines.append(f'{indent}try {{')
        lib_lines.append(f'{indent}    SetValue(output, {_cs_str(target_path)}, {cs_expr});')
        lib_lines.append(f'{indent}}} catch (Exception _e{i}) {{')
        lib_lines.append(f'{indent}    result.Warnings.Add($"Connection {i+1} failed: {{_e{i}.Message}}");')
        lib_lines.append(f'{indent}}}')
        lib_lines.append('')

    lib_lines.append(f'''            }}
            catch (Exception ex)
            {{
                result.Success = false;
                result.Error = ex.Message;
            }}

            result.Output = output;
            return result;
        }}

        // ── Parsers ──────────────────────────────────────────────────────

        public static Dictionary<string, object?> ParseJson(string json)
        {{
            var doc = JsonDocument.Parse(json);
            return ParseJsonElement(doc.RootElement) as Dictionary<string, object?> ?? new();
        }}

        private static object? ParseJsonElement(JsonElement el) => el.ValueKind switch
        {{
            JsonValueKind.Object => el.EnumerateObject()
                .ToDictionary(p => p.Name, p => ParseJsonElement(p.Value)),
            JsonValueKind.Array  => el.EnumerateArray().Select(ParseJsonElement).ToList(),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var i) ? (object?)i : el.GetDouble(),
            JsonValueKind.True   => true,
            JsonValueKind.False  => false,
            _                    => null
        }};

        public static Dictionary<string, object?> ParseXml(string xml)
        {{
            var doc = XDocument.Parse(xml);
            return new Dictionary<string, object?>
            {{
                [doc.Root!.Name.LocalName] = XmlToDictionary(doc.Root!)
            }};
        }}

        private static object? XmlToDictionary(XElement el)
        {{
            if (!el.HasElements) return el.Value;
            var dict = new Dictionary<string, object?>();
            foreach (var child in el.Elements())
            {{
                var key = child.Name.LocalName;
                var val = XmlToDictionary(child);
                if (dict.ContainsKey(key))
                {{
                    if (dict[key] is List<object?> list) list.Add(val);
                    else dict[key] = new List<object?> {{ dict[key], val }};
                }}
                else dict[key] = val;
            }}
            return dict;
        }}

        // ── Serializers ──────────────────────────────────────────────────

        public static string ToJson(Dictionary<string, object?> data, bool pretty = true)
        {{
            var opts = new JsonSerializerOptions {{ WriteIndented = pretty }};
            return JsonSerializer.Serialize(data, opts);
        }}

        public static string ToXml(Dictionary<string, object?> data, string rootName = "root")
        {{
            var rootKey = data.Count == 1 ? data.Keys.First() : rootName;
            var rootVal = data.Count == 1 ? data.Values.First() : data;
            var root = DictToXml(rootKey, rootVal);
            return $"<?xml version=\\"1.0\\" encoding=\\"UTF-8\\"?>\\n{{root}}";
        }}

        private static string DictToXml(string tag, object? value)
        {{
            if (value is Dictionary<string, object?> dict)
            {{
                var children = string.Join("\\n  ", dict.Select(kv => DictToXml(kv.Key, kv.Value)));
                return $"<{{tag}}>\\n  {{children}}\\n</{{tag}}>";
            }}
            return $"<{{tag}}>{{System.Security.SecurityElement.Escape(value?.ToString() ?? "")}}</{{tag}}>";
        }}
    }}
}}
''')

    # ---- CLI Program.cs ----
    program_cs = f'''// Buddyliko Transformer - CLI Entry Point
// {ts}

using System;
using System.IO;
using {ns};

var args = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (args.Length < 1)
{{
    Console.Error.WriteLine("Usage: BudlylikoTransformer <input> [output] [--if xml|json] [--of xml|json]");
    Environment.Exit(1);
}}

string inputPath = args[0];
string outputPath = args.Length > 1 && !args[1].StartsWith("--") ? args[1] : null!;
string inputFmt = "auto";
string outputFmt = "json";

for (int i = 0; i < args.Length - 1; i++)
{{
    if (args[i] == "--if") inputFmt = args[i+1];
    if (args[i] == "--of") outputFmt = args[i+1];
}}

// Read input
string content = inputPath == "-" ? Console.In.ReadToEnd() : File.ReadAllText(inputPath);

// Detect format
if (inputFmt == "auto")
{{
    var s = content.TrimStart();
    inputFmt = s.StartsWith("<") ? "xml" : "json";
}}

// Parse
var inputData = inputFmt == "xml"
    ? Transformer.ParseXml(content)
    : Transformer.ParseJson(content);

// Transform
var result = Transformer.Transform(inputData);

if (!result.Success)
{{
    Console.Error.WriteLine($"❌ Transform failed: {{result.Error}}");
    Environment.Exit(1);
}}

foreach (var w in result.Warnings)
    Console.Error.WriteLine($"⚠️  {{w}}");

// Serialize
string output = outputFmt == "xml"
    ? Transformer.ToXml(result.Output)
    : Transformer.ToJson(result.Output);

// Write output
if (string.IsNullOrEmpty(outputPath) || outputPath == "-")
    Console.WriteLine(output);
else
{{
    File.WriteAllText(outputPath, output);
    Console.Error.WriteLine($"✅ Output written to: {{outputPath}}");
}}
'''

    # ---- .csproj ----
    csproj = f'''<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <AssemblyName>BudlylikoTransformer</AssemblyName>
    <RootNamespace>{ns}</RootNamespace>
    <Description>Generated by Buddyliko - {project_name}</Description>
    <Version>1.0.0</Version>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="System.Text.Json" Version="8.*" />
  </ItemGroup>
</Project>
'''

    return {
        'BudlylikoTransformer.cs': '\n'.join(lib_lines),
        'Program.cs': program_cs,
        'BudlylikoTransformer.csproj': csproj,
    }


# ===========================================================================
# README GENERATOR
# ===========================================================================

def generate_readme(mapping_rules: Dict, project_name: str) -> str:
    connections = mapping_rules.get('connections', [])
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    return f'''# {project_name} — Buddyliko Generated Transformer

Generated: {ts}
Connections: {len(connections)}

---

## Python Standalone (transformer.py)

**Requirements:** Python 3.8+ (zero external dependencies)

```bash
# XML → JSON
python transformer.py input.xml output.json --of json

# JSON → XML
python transformer.py input.json output.xml --of xml

# CSV → JSON
python transformer.py input.csv output.json --if csv --of json

# stdin → stdout
cat input.xml | python transformer.py - - --of json
```

---

## Python Engine Module (buddyliko_engine.py)

Per integrare in un'applicazione Python esistente:

```python
from buddyliko_engine import transform, get_metadata

# Singolo record
output, warnings = transform(input_dict)

# Batch
outputs, warnings = transform(list_of_dicts, batch=True)

print(get_metadata())
```

---

## C# — BudlylikoTransformer

**Requirements:** .NET 8 SDK

```bash
# Build
cd BudlylikoTransformer
dotnet build -c Release

# Run CLI
dotnet run -- input.xml output.json --of json
dotnet run -- input.json output.xml --of xml

# Use as library in your project
dotnet add reference /path/to/BudlylikoTransformer.csproj
```

```csharp
using BudlylikoTransformer;

var inputData = Transformer.ParseXml(xmlContent);
var result = Transformer.Transform(inputData);

if (result.Success)
{{
    string json = Transformer.ToJson(result.Output);
    // use json...
}}
```

---

## Connessioni mappate ({len(connections)})

| # | Sorgente | Destinazione | Trasformazione |
|---|----------|--------------|----------------|
'''

def generate_readme(mapping_rules: Dict, project_name: str) -> str:
    connections = mapping_rules.get('connections', [])
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    conn_rows = ''
    for i, conn in enumerate(connections):
        src = (conn.get('sourcePath') or conn.get('source', '?'))[:50]
        tgt = (conn.get('targetPath') or conn.get('target', '?'))[:50]
        trans = conn.get('transformation')
        t_type = trans.get('type', 'DIRECT') if isinstance(trans, dict) else (str(trans) if trans else 'DIRECT')
        conn_rows += f'| {i+1} | `{src}` | `{tgt}` | {t_type} |\n'

    return f'''# {project_name} — Buddyliko Generated Transformer

Generated: {ts} | Connections: {len(connections)}

---

## Python Standalone (transformer.py)

**Requirements:** Python 3.8+ (zero external dependencies)

```bash
python transformer.py input.xml output.json --of json
python transformer.py input.json output.xml --of xml
cat input.xml | python transformer.py - - --of json
```

---

## Python Engine Module (buddyliko_engine.py)

```python
from buddyliko_engine import transform, get_metadata

output, warnings = transform(input_dict)
outputs, warnings = transform(list_of_dicts, batch=True)
```

---

## C# — BudlylikoTransformer

**Requirements:** .NET 8 SDK

```bash
cd BudlylikoTransformer
dotnet run -- input.xml output.json --of json
```

---

## Connections ({len(connections)})

| # | Source | Target | Transform |
|---|--------|--------|-----------|
{conn_rows}
'''


# ===========================================================================
# ZIP ASSEMBLER
# ===========================================================================

def generate_zip(mapping_rules: Dict, project_name: str = "transformer") -> bytes:
    """
    Assembla lo ZIP completo con tutti i file generati.
    Ritorna i bytes dello ZIP.
    """
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', project_name)

    py_standalone = generate_python_standalone(mapping_rules, project_name)
    py_engine     = generate_python_engine_module(mapping_rules, project_name)
    cs_files      = generate_csharp(mapping_rules, project_name)
    readme        = generate_readme(mapping_rules, project_name)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        # Python standalone
        zf.writestr(f'{safe_name}/transformer.py', py_standalone)
        # Python engine module
        zf.writestr(f'{safe_name}/buddyliko_engine.py', py_engine)
        # C# project
        for filename, content in cs_files.items():
            zf.writestr(f'{safe_name}/BudlylikoTransformer/{filename}', content)
        # README
        zf.writestr(f'{safe_name}/README.md', readme)
        # Mapping rules originali (per riferimento)
        zf.writestr(f'{safe_name}/mapping_rules.json',
                    json.dumps(mapping_rules, indent=2, ensure_ascii=False))

    return buf.getvalue()


def save_engine_module(mapping_rules: Dict, project_name: str,
                       output_dir: str = 'generated_engines') -> str:
    """
    Salva buddyliko_engine.py nella cartella dei motori interni.
    Ritorna il path del file salvato.
    """
    os.makedirs(output_dir, exist_ok=True)
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', project_name)
    module_path = os.path.join(output_dir, f'{safe_name}_engine.py')
    content = generate_python_engine_module(mapping_rules, project_name)
    with open(module_path, 'w', encoding='utf-8') as f:
        f.write(content)
    return module_path
