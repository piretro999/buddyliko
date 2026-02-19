"""
formulas.py — Buddyliko Transformation Formulas
================================================
Registro centralizzato di tutte le formule di trasformazione.

Ogni formula è una funzione con firma:
    execute(value, params, context) -> Any

    - value   : valore del campo sorgente (o lista di valori se multi-source)
    - params  : dict con i parametri della formula (dal JSON del mapping)
    - context : dict con {'input_data': ..., 'mapping_rules': ..., 'all_values': ...}

Aggiungere una formula = aggiungere una funzione e registrarla nel REGISTRY.

Tutte le formule gestiscono None/empty in modo sicuro.
"""

from __future__ import annotations
import re
import datetime
from typing import Any, Dict, List, Optional


# ── helpers interni ──────────────────────────────────────────────────────────

def _str(v) -> str:
    """Converti in stringa sicura."""
    if v is None:
        return ''
    return str(v).strip()


def _num(v, fallback=0):
    """Converti in float sicuro."""
    try:
        return float(str(v).replace(',', '.'))
    except (TypeError, ValueError):
        return fallback


def _first(v):
    """Restituisce il primo elemento se lista, altrimenti v stesso."""
    if isinstance(v, list):
        return v[0] if v else None
    return v


# ============================================================================
# FORMULE
# ============================================================================

def formula_direct(value, params, context):
    """
    DIRECT — Passa il valore senza modifiche.
    Nessun parametro.
    """
    return value


def formula_concat(value, params, context):
    """
    CONCAT — Concatena più valori con un separatore.

    params:
        separator (str, default '')  : separatore tra i valori
        inputs    (list, optional)   : lista di {field: "..."} o {literal: "..."}
        null_as   (str, default '')  : cosa mettere al posto di None/empty

    Esempi JSON:
        {"type": "CONCAT", "separator": " ", "inputs": [{"field": "NAME1"}, {"field": "NAME2"}]}
        {"type": "CONCAT", "separator": "-", "inputs": [{"literal": "IT"}, {"field": "VAT"}]}
    """
    separator = params.get('separator', '')
    null_as   = params.get('null_as', '')
    inputs    = params.get('inputs', [])

    # Se non ci sono inputs espliciti, usa il valore direttamente
    if not inputs:
        if isinstance(value, list):
            parts = [_str(v) or null_as for v in value]
        else:
            parts = [_str(value)]
        return separator.join(p for p in parts if p)

    # Con inputs espliciti
    all_values = context.get('all_values', {})
    input_data = context.get('input_data', {})
    parts = []

    for inp in inputs:
        if 'literal' in inp:
            parts.append(str(inp['literal']))
        elif 'field' in inp:
            field_name = inp['field']
            v = all_values.get(field_name) or _get_nested(input_data, field_name)
            parts.append(_str(v) or null_as)

    return separator.join(p for p in parts if p != null_as or null_as != '')


def formula_split(value, params, context):
    """
    SPLIT — Estrae una parte da una stringa con regex o delimitatore.

    params:
        delimiter (str)        : separatore semplice
        regex     (str)        : pattern regex (alternativo a delimiter)
        index     (int, def 0) : quale parte prendere (0-based)
        group     (int, def 1) : quale gruppo regex restituire

    Esempi:
        {"type": "SPLIT", "delimiter": "/", "index": 0}     → "2024" da "2024/01/15"
        {"type": "SPLIT", "regex": "(\\d{4})-(\\d{2})", "group": 2}  → "01" da "2024-01"
    """
    s = _str(_first(value))
    if not s:
        return value

    regex = params.get('regex')
    if regex:
        match = re.search(regex, s)
        if match:
            group = params.get('group', 1)
            try:
                return match.group(group)
            except IndexError:
                return None
        return None

    delimiter = params.get('delimiter', '')
    if delimiter:
        parts = s.split(delimiter)
        index = params.get('index', 0)
        try:
            return parts[index].strip()
        except IndexError:
            return None

    return value


def formula_date_format(value, params, context):
    """
    DATE_FORMAT — Converte formato data.

    params:
        from_format (str) : formato input  (strftime, es: "%Y%m%d")
        to_format   (str) : formato output (strftime, es: "%Y-%m-%d")
        from_iso    (bool): se True, assume input ISO 8601 (YYYY-MM-DD o YYYYMMDD)

    Esempi:
        {"type": "DATE_FORMAT", "from_format": "%Y%m%d", "to_format": "%d/%m/%Y"}
        {"type": "DATE_FORMAT", "from_iso": true, "to_format": "%d.%m.%Y"}

    Formati comuni:
        %Y  = anno 4 cifre       %y = anno 2 cifre
        %m  = mese               %d = giorno
        %H  = ora                %M = minuti  %S = secondi
    """
    s = _str(_first(value))
    if not s:
        return value

    # Rimuovi eventuale parte time se non serve
    if 'T' in s and params.get('date_only', True):
        s = s.split('T')[0]

    from_format = params.get('from_format')
    to_format   = params.get('to_format', '%Y-%m-%d')
    from_iso    = params.get('from_iso', False)

    try:
        if from_iso or not from_format:
            # Prova vari formati ISO comuni
            for fmt in ('%Y-%m-%d', '%Y%m%d', '%d/%m/%Y', '%d.%m.%Y',
                        '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ'):
                try:
                    dt = datetime.datetime.strptime(s, fmt)
                    return dt.strftime(to_format)
                except ValueError:
                    continue
            return value  # nessun formato trovato
        else:
            dt = datetime.datetime.strptime(s, from_format)
            return dt.strftime(to_format)
    except (ValueError, TypeError):
        return value


def formula_math(value, params, context):
    """
    MATH — Operazioni aritmetiche su un campo numerico.

    params:
        operation (str)    : add | subtract | multiply | divide | abs | round | negate
        operand   (float)  : secondo operando (non serve per abs/negate/round)
        decimals  (int)    : cifre decimali per round (default 2)
        fallback  (float)  : valore se conversione fallisce (default 0)

    Esempi:
        {"type": "MATH", "operation": "multiply", "operand": 100}   → converti 0.22 → 22 (%)
        {"type": "MATH", "operation": "round", "decimals": 2}
        {"type": "MATH", "operation": "divide", "operand": 100}     → converti 22 → 0.22
    """
    fallback  = params.get('fallback', 0)
    v         = _num(_first(value), fallback)
    operation = params.get('operation', 'round')
    operand   = _num(params.get('operand', 1), 1)

    try:
        if operation == 'add':
            return v + operand
        elif operation == 'subtract':
            return v - operand
        elif operation == 'multiply':
            return v * operand
        elif operation == 'divide':
            return (v / operand) if operand != 0 else fallback
        elif operation == 'abs':
            return abs(v)
        elif operation == 'negate':
            return -v
        elif operation == 'round':
            decimals = int(params.get('decimals', 2))
            return round(v, decimals)
        else:
            return v
    except (TypeError, ZeroDivisionError):
        return fallback


def formula_lookup(value, params, context):
    """
    LOOKUP — Traduce un codice in un altro tramite tabella.

    params:
        table    (dict)  : {"valore_input": "valore_output", ...}
        default  (any)   : valore se non trovato (default: valore originale)
        case_sensitive (bool, default False)

    Esempi:
        {
          "type": "LOOKUP",
          "table": {"IT": "380", "DE": "380", "FR": "380"},
          "default": "380"
        }
        {
          "type": "LOOKUP",
          "table": {"MWST": "S", "KEIN": "Z", "STEUA": "AE"},
          "default": "S"
        }
    """
    table = params.get('table', {})
    default = params.get('default', value)
    case_sensitive = params.get('case_sensitive', False)

    s = _str(_first(value))
    if not s:
        return default

    if not case_sensitive:
        table_lower = {k.lower(): v for k, v in table.items()}
        return table_lower.get(s.lower(), default)
    else:
        return table.get(s, default)


def formula_conditional(value, params, context):
    """
    CONDITIONAL — Valore basato su condizioni if/elif/else.

    params:
        conditions (list): lista di {if: {...}, then: ...}
            if può essere:
                {equals: "X"}
                {not_equals: "X"}
                {starts_with: "X"}
                {contains: "X"}
                {is_empty: true}
                {gt: N}, {lt: N}, {gte: N}, {lte: N}
        default (any): valore se nessuna condizione soddisfatta

    Esempio:
        {
          "type": "CONDITIONAL",
          "conditions": [
            {"if": {"equals": "RE"}, "then": "381"},
            {"if": {"equals": "FZ"}, "then": "380"}
          ],
          "default": "380"
        }
    """
    conditions = params.get('conditions', [])
    default    = params.get('default', value)
    s          = _str(_first(value))
    n          = _num(s)

    for cond in conditions:
        if_clause = cond.get('if', {})
        then_val  = cond.get('then', value)

        match = False
        if 'equals'      in if_clause: match = s == str(if_clause['equals'])
        elif 'not_equals' in if_clause: match = s != str(if_clause['not_equals'])
        elif 'starts_with' in if_clause: match = s.startswith(str(if_clause['starts_with']))
        elif 'ends_with'   in if_clause: match = s.endswith(str(if_clause['ends_with']))
        elif 'contains'    in if_clause: match = str(if_clause['contains']) in s
        elif 'is_empty'    in if_clause: match = (not s) == bool(if_clause['is_empty'])
        elif 'gt'  in if_clause: match = n >  _num(if_clause['gt'])
        elif 'lt'  in if_clause: match = n <  _num(if_clause['lt'])
        elif 'gte' in if_clause: match = n >= _num(if_clause['gte'])
        elif 'lte' in if_clause: match = n <= _num(if_clause['lte'])
        elif 'regex' in if_clause: match = bool(re.search(str(if_clause['regex']), s))

        if match:
            return then_val

    return default


def formula_string_op(value, params, context):
    """
    STRING_OP — Operazioni su stringa.

    params:
        operation (str): upper | lower | title | trim | strip | lstrip | rstrip |
                         replace | substring | pad_left | pad_right | remove_prefix |
                         remove_suffix | regex_replace
        # per replace:
        find    (str)
        replace (str, default '')
        # per regex_replace:
        pattern (str)
        repl    (str, default '')
        # per substring:
        start (int, default 0)
        end   (int, optional)
        # per pad_left / pad_right:
        width (int)
        char  (str, default ' ')
        # per remove_prefix / remove_suffix:
        prefix/suffix (str)

    Esempi:
        {"type": "STRING_OP", "operation": "upper"}
        {"type": "STRING_OP", "operation": "replace", "find": ".", "replace": ","}
        {"type": "STRING_OP", "operation": "substring", "start": 0, "end": 8}
        {"type": "STRING_OP", "operation": "pad_left", "width": 10, "char": "0"}
    """
    s         = _str(_first(value))
    operation = params.get('operation', 'trim')

    if not s and operation not in ('pad_left', 'pad_right'):
        return value

    if   operation == 'upper':   return s.upper()
    elif operation == 'lower':   return s.lower()
    elif operation == 'title':   return s.title()
    elif operation == 'trim':    return s.strip()
    elif operation == 'lstrip':  return s.lstrip()
    elif operation == 'rstrip':  return s.rstrip()

    elif operation == 'replace':
        find    = str(params.get('find', ''))
        replace = str(params.get('replace_with') or params.get('replace', ''))
        return s.replace(find, replace)

    elif operation == 'regex_replace':
        pattern = str(params.get('pattern', ''))
        repl    = str(params.get('repl', ''))
        return re.sub(pattern, repl, s) if pattern else s

    elif operation == 'substring':
        start = int(params.get('start', 0))
        end   = params.get('end')
        return s[start:end] if end is not None else s[start:]

    elif operation == 'pad_left':
        width = int(params.get('width', len(s)))
        char  = str(params.get('char', ' '))[:1] or ' '
        return s.rjust(width, char)

    elif operation == 'pad_right':
        width = int(params.get('width', len(s)))
        char  = str(params.get('char', ' '))[:1] or ' '
        return s.ljust(width, char)

    elif operation == 'remove_prefix':
        prefix = str(params.get('prefix', ''))
        return s[len(prefix):] if s.startswith(prefix) else s

    elif operation == 'remove_suffix':
        suffix = str(params.get('suffix', ''))
        return s[:-len(suffix)] if suffix and s.endswith(suffix) else s

    return s


def formula_default(value, params, context):
    """
    DEFAULT — Usa un valore di fallback se il campo è vuoto/None.

    params:
        value (any)          : valore di default
        also_whitespace (bool, default True): considera spazi come empty

    Esempio:
        {"type": "DEFAULT", "value": "EUR"}
        {"type": "DEFAULT", "value": "380", "also_whitespace": true}
    """
    also_ws  = params.get('also_whitespace', True)
    default  = params.get('value', '')
    v        = _first(value)

    if v is None:
        return default
    if also_ws and _str(v) == '':
        return default
    return v


def formula_coalesce(value, params, context):
    """
    COALESCE — Restituisce il primo valore non-vuoto tra più campi.

    params:
        fields (list of str): nomi dei campi da controllare in ordine

    Esempio:
        {"type": "COALESCE", "fields": ["VAT_ID", "TAX_ID", "COMPANY_ID"]}
    """
    all_values = context.get('all_values', {})
    input_data = context.get('input_data', {})
    # Support both list and comma-separated string from UI
    fields_raw = params.get('fields_raw', '')
    fields = params.get('fields', [])
    if not fields and fields_raw:
        fields = [f.strip() for f in fields_raw.split(',') if f.strip()]

    # Controlla prima il valore primario
    if value is not None and _str(value) != '':
        return value

    for field in fields:
        v = all_values.get(field) or _get_nested(input_data, field)
        if v is not None and _str(v) != '':
            return v

    return None


def formula_hardcode(value, params, context):
    """
    HARDCODE — Ignora il valore sorgente e restituisce sempre un valore fisso.

    params:
        value (any): valore fisso da inserire

    Esempio:
        {"type": "HARDCODE", "value": "380"}
        {"type": "HARDCODE", "value": "EUR"}
    """
    return params.get('value', '')


def formula_sum_multi(value, params, context):
    """
    SUM_MULTI — Somma i valori di più campi sorgente.

    params:
        fields   (list of str, optional): campi extra da sommare
        decimals (int, default 2)

    Esempio:
        {"type": "SUM_MULTI", "decimals": 2}
        (il valore sorgente è già una lista se multi-source)
    """
    decimals = int(params.get('decimals', 2))

    if isinstance(value, list):
        total = sum(_num(v) for v in value)
    else:
        total = _num(value)

    # Aggiungi campi extra se specificati
    all_values = context.get('all_values', {})
    for field in params.get('fields', []):
        total += _num(all_values.get(field))

    return round(total, decimals)


def formula_noop(value, params, context):
    """
    NOOP — Non fa nulla (placeholder per formule non ancora implementate).
    Restituisce il valore inalterato con un warning nel log.
    """
    formula_name = params.get('_formula_name', 'UNKNOWN')
    print(f"⚠️  NOOP: formula '{formula_name}' non implementata, valore passato inalterato")
    return value


def formula_custom(value, params, context):
    """
    CUSTOM — Espressione Python libera scritta dall'utente.

    params:
        expression (str): espressione Python, usa 'value' come variabile sorgente.

    Esempi:
        value.upper()
        float(value) * 100
        value.split('/')[0]
        value[0:4] + '-' + value[4:6]
    """
    expression = params.get('expression', '').strip()
    if not expression:
        return value
    try:
        result = eval(expression, {"__builtins__": {
            'str': str, 'int': int, 'float': float, 'round': round,
            'len': len, 'abs': abs, 'bool': bool, 'list': list,
            'dict': dict, 'tuple': tuple, 'range': range,
            'min': min, 'max': max, 'sum': sum,
            'isinstance': isinstance, 'hasattr': hasattr,
            '__import__': __import__,
        }}, {"value": _str(value)})
        return result
    except Exception as e:
        print(f"❌ CUSTOM formula error: {e}  expression: {expression!r}")
        return value


# ============================================================================
# REGISTRY
# ============================================================================

REGISTRY: Dict[str, callable] = {
    # Core
    'DIRECT':       formula_direct,
    'direct':       formula_direct,

    # Concatenazione / splitting
    'CONCAT':       formula_concat,
    'SPLIT':        formula_split,

    # Date
    'DATE_FORMAT':  formula_date_format,
    'date_format':  formula_date_format,

    # Numerica
    'MATH':         formula_math,
    'SUM_MULTI':    formula_sum_multi,

    # Logica
    'LOOKUP':       formula_lookup,
    'CONDITIONAL':  formula_conditional,

    # Stringa
    'STRING_OP':    formula_string_op,
    'UPPER':        lambda v, p, c: formula_string_op(v, {**p, 'operation': 'upper'}, c),
    'LOWER':        lambda v, p, c: formula_string_op(v, {**p, 'operation': 'lower'}, c),
    'TRIM':         lambda v, p, c: formula_string_op(v, {**p, 'operation': 'trim'}, c),
    'REPLACE':      lambda v, p, c: formula_string_op(v, {**p, 'operation': 'replace'}, c),
    'SUBSTRING':    lambda v, p, c: formula_string_op(v, {**p, 'operation': 'substring'}, c),
    'PAD_LEFT':     lambda v, p, c: formula_string_op(v, {**p, 'operation': 'pad_left'}, c),

    # Fallback
    'DEFAULT':      formula_default,
    'COALESCE':     formula_coalesce,
    'HARDCODE':     formula_hardcode,

    # Custom expression
    'CUSTOM':       formula_custom,
    'custom':       formula_custom,

    # Placeholder
    'NOOP':         formula_noop,
}


# ============================================================================
# ENTRY POINT PUBBLICO
# ============================================================================

def execute_formula(transformation: Dict, value: Any,
                    input_data: Dict = None,
                    mapping_rules: Dict = None,
                    all_values: Dict = None) -> Any:
    """
    Esegui una formula dato il suo oggetto transformation JSON.

    Args:
        transformation : dict con 'type' e parametri specifici della formula
        value          : valore del/dei campo/i sorgente
        input_data     : dizionario dell'input completo (per COALESCE, CONCAT con fields, ecc.)
        mapping_rules  : regole di mapping complete (per accesso allo schema)
        all_values     : valori già risolti di tutti i campi (cache opzionale)

    Returns:
        Valore trasformato, qualsiasi tipo.

    Esempio:
        result = execute_formula(
            {"type": "DATE_FORMAT", "from_format": "%Y%m%d", "to_format": "%d/%m/%Y"},
            "20240115"
        )
        # → "15/01/2024"
    """
    if not isinstance(transformation, dict):
        # Compatibilità con vecchio formato stringa
        return value

    trans_type = transformation.get('type', 'DIRECT').upper()
    params     = {k: v for k, v in transformation.items() if k != 'type'}

    context = {
        'input_data':    input_data    or {},
        'mapping_rules': mapping_rules or {},
        'all_values':    all_values    or {},
    }

    formula_fn = REGISTRY.get(trans_type) or REGISTRY.get(transformation.get('type', ''))

    if not formula_fn:
        print(f"⚠️  Formula sconosciuta: '{trans_type}', passaggio diretto")
        return value

    try:
        return formula_fn(value, params, context)
    except Exception as e:
        print(f"❌ Errore formula '{trans_type}': {e}")
        return value


def list_formulas() -> List[Dict]:
    """
    Restituisce la lista delle formule disponibili con descrizione.
    Usato dall'API per popolare il selettore formule nell'UI.
    """
    seen = set()
    result = []
    for name, fn in REGISTRY.items():
        if fn in seen:
            continue
        seen.add(fn)
        result.append({
            'id':          name,
            'label':       name.replace('_', ' ').title(),
            'description': (fn.__doc__ or '').strip().split('\n')[0],
        })
    return result


# ── utility interna ──────────────────────────────────────────────────────────

def _get_nested(data: Dict, path: str) -> Any:
    """Accede a un dict annidato con path separato da / o ."""
    if not data or not path:
        return None
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip()]
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (IndexError, ValueError):
                return None
        else:
            return None
    return current
