#!/usr/bin/env python3
"""
Buddyliko - EDI Parser (X12 + EDIFACT)
Zero dipendenze esterne.

Supporta:
  X12:     ISA/GS/ST envelope, segmenti 850 (Purchase Order), 810 (Invoice),
           856 (ASN), 270/271 (Eligibility), 837 (Claims)
  EDIFACT: UNB/UNH envelope, ORDERS, INVOIC, DESADV, PRICAT

Funzioni principali:
  parse_edi(content)  → dict normalizzato (funziona per X12 e EDIFACT)
  to_schema(parsed)   → Buddyliko schema da struttura EDI
  edi_to_json(parsed) → rappresentazione JSON navigabile
  build_x12(data, transaction_type) → genera X12 da dict
  build_edifact(data, message_type) → genera EDIFACT da dict
"""

import re
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple


# ===========================================================================
# RILEVAMENTO FORMATO
# ===========================================================================

def detect_edi_format(content: str) -> str:
    """Ritorna 'X12', 'EDIFACT' o 'UNKNOWN'."""
    stripped = content.strip()
    if stripped.startswith('ISA'):
        return 'X12'
    if stripped.startswith('UNB') or stripped.startswith('UNA'):
        return 'EDIFACT'
    return 'UNKNOWN'


# ===========================================================================
# X12 PARSER
# ===========================================================================

class X12Parser:
    """
    Parser EDI X12.
    Supporta qualsiasi transaction set — estrae tutti i segmenti
    e li organizza in envelope ISA > GS > ST > segmenti > SE > GE > IEA.
    """

    # Nomi dei segmenti X12 più comuni
    SEGMENT_NAMES = {
        'ISA': 'Interchange Control Header',
        'GS':  'Functional Group Header',
        'ST':  'Transaction Set Header',
        'SE':  'Transaction Set Trailer',
        'GE':  'Functional Group Trailer',
        'IEA': 'Interchange Control Trailer',
        'BEG': 'Beginning Segment for PO',
        'REF': 'Reference Identification',
        'DTM': 'Date/Time Reference',
        'N1':  'Name',
        'N2':  'Additional Name',
        'N3':  'Address',
        'N4':  'Geographic Location',
        'PO1': 'Baseline Item Data',
        'PID': 'Product/Item Description',
        'QTY': 'Quantity',
        'SCH': 'Line Item Schedule',
        'CTT': 'Transaction Totals',
        'BIG': 'Beginning Segment for Invoice',
        'IT1': 'Baseline Item Data (Invoice)',
        'TDS': 'Total Monetary Value Summary',
        'TXI': 'Tax Information',
        'SAC': 'Service/Promotion/Allowance',
        'BSN': 'Beginning Segment for Ship Notice',
        'HL':  'Hierarchical Level',
        'MAN': 'Marks and Numbers',
        'PRF': 'Purchase Order Reference',
        'TD1': 'Carrier Details - Quantity/Weight',
        'TD5': 'Carrier Details - Routing',
        'BGN': 'Beginning Segment',
        'INS': 'Insured Benefit',
        'CLM': 'Claim',
        'NM1': 'Name (Individual/Organization)',
    }

    def __init__(self, content: str):
        self.content = content
        self.element_sep = '~'   # segment terminator
        self.field_sep = '*'     # element separator
        self.sub_sep = ':'       # sub-element separator
        self._parse_envelope()

    def _parse_envelope(self):
        """Estrae i separatori dall'ISA header."""
        stripped = self.content.strip()
        if len(stripped) < 106:
            return
        # ISA ha lunghezza fissa: ISA + 15 elementi da 2-15 chars
        # Il field_sep è il carattere alla posizione 3
        self.field_sep = stripped[3]
        # Il sub_sep è alla posizione 104
        self.sub_sep = stripped[104]
        # Il segment terminator è alla posizione 105
        self.element_sep = stripped[105]
        if self.element_sep == '\n':
            self.element_sep = '\n'
        elif self.element_sep.strip() == '':
            self.element_sep = '~'

    def parse(self) -> Dict:
        """
        Ritorna struttura normalizzata:
        {
          "format": "X12",
          "interchanges": [{
            "isa": {...},
            "groups": [{
              "gs": {...},
              "transactions": [{
                "st": {...},
                "segments": [{"id": "BEG", "name": "...", "elements": [...]}],
                "transaction_type": "850"
              }]
            }]
          }]
        }
        """
        # Split in segmenti
        raw = self.content.strip()
        if self.element_sep == '\n':
            raw_segments = [s.strip() for s in raw.splitlines() if s.strip()]
        else:
            raw_segments = [s.strip() for s in raw.split(self.element_sep) if s.strip()]

        segments = []
        for seg in raw_segments:
            parts = seg.split(self.field_sep)
            seg_id = parts[0].strip().upper()
            elements = []
            for p in parts[1:]:
                if self.sub_sep in p:
                    elements.append(p.split(self.sub_sep))
                else:
                    elements.append(p)
            segments.append({'id': seg_id, 'elements': elements,
                             'name': self.SEGMENT_NAMES.get(seg_id, seg_id),
                             'raw': seg})

        # Organizza in envelope
        result = {"format": "X12", "interchanges": [], "_raw_segments": segments}
        interchange = None
        group = None
        transaction = None

        for seg in segments:
            sid = seg['id']
            if sid == 'ISA':
                interchange = {
                    "isa": self._parse_isa(seg),
                    "groups": [],
                    "sender_id": (seg['elements'][5] if len(seg['elements']) > 5 else '').strip(),
                    "receiver_id": (seg['elements'][7] if len(seg['elements']) > 7 else '').strip(),
                    "date": seg['elements'][8] if len(seg['elements']) > 8 else '',
                    "control_number": seg['elements'][12] if len(seg['elements']) > 12 else '',
                }
                result["interchanges"].append(interchange)
                group = None
                transaction = None

            elif sid == 'GS' and interchange:
                group = {
                    "gs": {k: v for k, v in zip(
                        ['functional_id', 'sender', 'receiver', 'date', 'time',
                         'control_number', 'responsible_agency', 'version'],
                        seg['elements']
                    )},
                    "transactions": [],
                    "functional_id": seg['elements'][0] if seg['elements'] else '',
                }
                interchange["groups"].append(group)
                transaction = None

            elif sid == 'ST' and group:
                transaction_type = seg['elements'][0] if seg['elements'] else 'UNKNOWN'
                transaction = {
                    "st": {"transaction_type": transaction_type,
                           "control_number": seg['elements'][1] if len(seg['elements']) > 1 else ''},
                    "transaction_type": transaction_type,
                    "segments": [],
                }
                group["transactions"].append(transaction)

            elif sid == 'SE' and transaction:
                transaction["segment_count"] = seg['elements'][0] if seg['elements'] else 0
                transaction = None

            elif sid == 'GE':
                group = None

            elif sid == 'IEA':
                interchange = None

            elif transaction is not None:
                transaction["segments"].append(seg)

        return result

    def _parse_isa(self, seg: Dict) -> Dict:
        fields = ['auth_qualifier', 'auth_info', 'security_qualifier', 'security_info',
                  'sender_qualifier', 'sender_id', 'receiver_qualifier', 'receiver_id',
                  'date', 'time', 'repetition_sep', 'version', 'control_number',
                  'ack_requested', 'usage_indicator']
        return {f: (seg['elements'][i] if i < len(seg['elements']) else '')
                for i, f in enumerate(fields)}


class X12Writer:
    """Genera EDI X12 da un dizionario strutturato."""

    def __init__(self, field_sep='*', element_sep='~\n', sub_sep=':'):
        self.fs = field_sep
        self.es = element_sep
        self.ss = sub_sep

    def build(self, data: Dict, transaction_type: str = '850') -> str:
        now = datetime.now(timezone.utc)
        date_str = now.strftime('%y%m%d')
        time_str = now.strftime('%H%M')
        ctrl = '000000001'

        sender = data.get('sender_id', 'SENDER         ')
        receiver = data.get('receiver_id', 'RECEIVER       ')
        segments = []

        # ISA
        segments.append(
            f"ISA{self.fs}00{self.fs}          {self.fs}00{self.fs}          "
            f"{self.fs}ZZ{self.fs}{sender:<15}{self.fs}ZZ{self.fs}{receiver:<15}"
            f"{self.fs}{date_str}{self.fs}{time_str}{self.fs}^{self.fs}00501"
            f"{self.fs}{ctrl.zfill(9)}{self.fs}0{self.fs}P{self.fs}:"
        )
        # GS
        func_id = {'850': 'PO', '810': 'IN', '856': 'SH', '270': 'HS', '837': 'HC'}.get(transaction_type, 'FA')
        segments.append(
            f"GS{self.fs}{func_id}{self.fs}{sender.strip()}{self.fs}"
            f"{receiver.strip()}{self.fs}{now.strftime('%Y%m%d')}{self.fs}"
            f"{time_str}{self.fs}1{self.fs}X{self.fs}005010"
        )
        # ST
        segments.append(f"ST{self.fs}{transaction_type}{self.fs}0001")

        # Body segments da data['segments']
        seg_count = 1  # ST conta come 1
        for seg in data.get('segments', []):
            seg_id = seg.get('id', '')
            elements = seg.get('elements', [])
            line = seg_id + self.fs + self.fs.join(
                (self.ss.join(e) if isinstance(e, list) else str(e or ''))
                for e in elements
            )
            segments.append(line)
            seg_count += 1

        # SE
        seg_count += 1  # SE stessa conta
        segments.append(f"SE{self.fs}{seg_count}{self.fs}0001")
        # GE
        segments.append(f"GE{self.fs}1{self.fs}1")
        # IEA
        segments.append(f"IEA{self.fs}1{self.fs}{ctrl.zfill(9)}")

        return self.es.join(segments) + self.es[-1] if '\n' in self.es else \
               self.es.join(segments) + '~'


# ===========================================================================
# EDIFACT PARSER
# ===========================================================================

class EDIFACTParser:
    """
    Parser EDIFACT UN/EDIFACT.
    Supporta UNA (service string advice) + UNB/UNH envelope.
    """

    SEGMENT_NAMES = {
        'UNA': 'Service String Advice',
        'UNB': 'Interchange Header',
        'UNZ': 'Interchange Trailer',
        'UNH': 'Message Header',
        'UNT': 'Message Trailer',
        'BGM': 'Beginning of Message',
        'DTM': 'Date/Time/Period',
        'NAD': 'Name and Address',
        'LOC': 'Place/Location Identification',
        'RFF': 'Reference',
        'CTA': 'Contact Information',
        'COM': 'Communication Contact',
        'TAX': 'Duty/Tax/Fee Details',
        'CUX': 'Currencies',
        'PAT': 'Payment Terms Basis',
        'LIN': 'Line Item',
        'PIA': 'Additional Product Id',
        'IMD': 'Item Description',
        'QTY': 'Quantity',
        'PRI': 'Price Details',
        'MOA': 'Monetary Amount',
        'TOD': 'Terms of Delivery/Transport',
        'ALC': 'Allowance or Charge',
        'UNS': 'Section Control',
        'CNT': 'Control Total',
        'SCC': 'Scheduling Conditions',
        'TDT': 'Transport Information',
        'FTX': 'Free Text',
        'ERC': 'Application Error Information',
    }

    def __init__(self, content: str):
        self.content = content
        # Default separatori EDIFACT
        self.component_sep = ':'
        self.element_sep = '+'
        self.decimal_mark = '.'
        self.release_char = '?'
        self.segment_term = "'"
        self._parse_una()

    def _parse_una(self):
        """UNA contiene la definizione dei separatori personalizzati."""
        stripped = self.content.strip()
        if stripped.startswith('UNA'):
            una = stripped[3:9]
            if len(una) >= 6:
                self.component_sep = una[0]
                self.element_sep = una[1]
                self.decimal_mark = una[2]
                self.release_char = una[3]
                # una[4] è riservato
                self.segment_term = una[5]

    def parse(self) -> Dict:
        """
        Ritorna:
        {
          "format": "EDIFACT",
          "interchanges": [{
            "unb": {...},
            "messages": [{
              "unh": {...},
              "message_type": "ORDERS",
              "segments": [{"id": "BGM", "name": "...", "elements": [...]}]
            }]
          }]
        }
        """
        # Rimuovi UNA se presente
        raw = self.content.strip()
        if raw.startswith('UNA'):
            raw = raw[9:].strip()

        # Split in segmenti usando segment terminator
        # Gestisce release character (?)
        raw_segments = self._split_segments(raw)

        result = {"format": "EDIFACT", "interchanges": []}
        interchange = None
        message = None

        for seg_str in raw_segments:
            seg = self._parse_segment(seg_str)
            if not seg:
                continue
            sid = seg['id']

            if sid == 'UNB':
                interchange = {
                    "unb": self._parse_unb(seg),
                    "messages": [],
                    "sender": self._get_element(seg, 1, 0),
                    "receiver": self._get_element(seg, 2, 0),
                    "date": self._get_element(seg, 3, 0),
                    "control_ref": self._get_element(seg, 4),
                }
                result["interchanges"].append(interchange)

            elif sid == 'UNH' and interchange is not None:
                msg_ref = self._get_element(seg, 0)
                msg_type = self._get_element(seg, 1, 0)
                message = {
                    "unh": {"ref": msg_ref, "type": msg_type,
                            "version": self._get_element(seg, 1, 1),
                            "release": self._get_element(seg, 1, 2)},
                    "message_type": msg_type,
                    "ref": msg_ref,
                    "segments": [],
                }
                interchange["messages"].append(message)

            elif sid == 'UNT' and message is not None:
                message["segment_count"] = self._get_element(seg, 0)
                message = None

            elif sid == 'UNZ':
                interchange = None

            elif message is not None:
                message["segments"].append(seg)

        return result

    def _split_segments(self, raw: str) -> List[str]:
        """Split rispettando il release character."""
        segments = []
        current = ''
        i = 0
        while i < len(raw):
            ch = raw[i]
            if ch == self.release_char and i + 1 < len(raw):
                current += raw[i + 1]  # carattere escaped, non è separatore
                i += 2
                continue
            if ch == self.segment_term:
                seg = current.strip()
                if seg:
                    segments.append(seg)
                current = ''
            else:
                current += ch
            i += 1
        if current.strip():
            segments.append(current.strip())
        return segments

    def _parse_segment(self, seg_str: str) -> Optional[Dict]:
        if not seg_str:
            return None
        # Split by element separator
        parts = seg_str.split(self.element_sep)
        seg_id = parts[0].strip().upper()
        elements = []
        for p in parts[1:]:
            if self.component_sep in p:
                elements.append(p.split(self.component_sep))
            else:
                elements.append(p)
        return {
            'id': seg_id,
            'elements': elements,
            'name': self.SEGMENT_NAMES.get(seg_id, seg_id),
            'raw': seg_str,
        }

    def _parse_unb(self, seg: Dict) -> Dict:
        return {
            'syntax_identifier': self._get_element(seg, 0, 0),
            'syntax_version': self._get_element(seg, 0, 1),
            'sender': self._get_element(seg, 1, 0),
            'receiver': self._get_element(seg, 2, 0),
            'date': self._get_element(seg, 3, 0),
            'time': self._get_element(seg, 3, 1),
            'interchange_ref': self._get_element(seg, 4),
            'application_ref': self._get_element(seg, 6),
        }

    def _get_element(self, seg: Dict, idx: int, sub_idx: int = None):
        elems = seg.get('elements', [])
        if idx >= len(elems):
            return ''
        elem = elems[idx]
        if sub_idx is None:
            return elem if isinstance(elem, str) else (elem[0] if elem else '')
        if isinstance(elem, list):
            return elem[sub_idx] if sub_idx < len(elem) else ''
        return elem if sub_idx == 0 else ''


class EDIFACTWriter:
    """Genera EDIFACT da dizionario strutturato."""

    def __init__(self):
        self.cs = ':'   # component separator
        self.es = '+'   # element separator
        self.st = "'\n" # segment terminator + newline

    def build(self, data: Dict, message_type: str = 'ORDERS') -> str:
        now = datetime.now(timezone.utc)
        date_str = now.strftime('%y%m%d')
        time_str = now.strftime('%H%M')
        ctrl_ref = '1'
        sender = data.get('sender_id', 'SENDER')
        receiver = data.get('receiver_id', 'RECEIVER')

        lines = []
        # UNA
        lines.append("UNA:+.? '")
        # UNB
        lines.append(
            f"UNB+UNOA{self.cs}1+{sender}+{receiver}+{date_str}{self.cs}{time_str}+{ctrl_ref}"
        )
        # UNH
        msg_ref = '1'
        lines.append(f"UNH+{msg_ref}+{message_type}{self.cs}D{self.cs}96A{self.cs}UN")

        # Body segments
        seg_count = 2  # UNH + UNT
        for seg in data.get('segments', []):
            seg_id = seg.get('id', '')
            elements = seg.get('elements', [])
            parts = []
            for e in elements:
                if isinstance(e, list):
                    parts.append(self.cs.join(str(x) for x in e))
                else:
                    parts.append(str(e or ''))
            lines.append(seg_id + self.es + self.es.join(parts))
            seg_count += 1

        # UNT
        lines.append(f"UNT+{seg_count}+{msg_ref}")
        # UNZ
        lines.append(f"UNZ+1+{ctrl_ref}")

        return self.st.join(lines) + self.st[-1]


# ===========================================================================
# INTERFACCIA UNIFICATA
# ===========================================================================

def parse_edi(content: str) -> Dict:
    """
    Auto-detect X12 o EDIFACT e parsa il contenuto.
    Ritorna struttura normalizzata con campi comuni aggiuntivi.
    """
    fmt = detect_edi_format(content)
    if fmt == 'X12':
        parsed = X12Parser(content).parse()
    elif fmt == 'EDIFACT':
        parsed = EDIFACTParser(content).parse()
    else:
        raise ValueError(f"Formato EDI non riconosciuto. Assicurati che inizi con ISA (X12) o UNB/UNA (EDIFACT)")

    # Aggiungi summary
    parsed['_summary'] = _build_summary(parsed)
    return parsed


def _build_summary(parsed: Dict) -> Dict:
    fmt = parsed.get('format', 'UNKNOWN')
    summary = {"format": fmt, "interchanges": 0, "messages": 0, "segments": 0}
    interchanges = parsed.get('interchanges', [])
    summary["interchanges"] = len(interchanges)
    for ic in interchanges:
        groups_or_msgs = ic.get('groups', ic.get('messages', []))
        for gm in groups_or_msgs:
            transactions = gm.get('transactions', [gm])  # EDIFACT: messages è già la lista
            for tx in (transactions if 'transactions' in gm else [gm]):
                summary["messages"] += 1
                segs = tx.get('segments', [])
                summary["segments"] += len(segs)
    return summary


def to_buddyliko_schema(parsed: Dict, name: str = None) -> Dict:
    """
    Converte un EDI parsato in uno schema Buddyliko navigabile nel mapper.
    Ogni segmento diventa un gruppo di campi.
    """
    fmt = parsed.get('format', 'UNKNOWN')
    schema_name = name or f"EDI_{fmt}"

    # Raccoglie tutti i segmenti unici dalla prima transazione
    seen_segments = {}
    interchanges = parsed.get('interchanges', [])
    for ic in interchanges:
        for gm in ic.get('groups', ic.get('messages', [])):
            transactions = gm.get('transactions', [gm]) if 'transactions' in gm else [gm]
            for tx in transactions:
                for seg in tx.get('segments', []):
                    sid = seg['id']
                    if sid not in seen_segments:
                        seen_segments[sid] = seg

    fields = []

    # Envelope fields
    if fmt == 'X12':
        fields += [
            {"id": str(uuid.uuid4()), "name": "ISA_SenderID", "type": "string",
             "path": f"{schema_name}/ISA/SenderID", "xml_path": f"{schema_name}/ISA/SenderID"},
            {"id": str(uuid.uuid4()), "name": "ISA_ReceiverID", "type": "string",
             "path": f"{schema_name}/ISA/ReceiverID", "xml_path": f"{schema_name}/ISA/ReceiverID"},
            {"id": str(uuid.uuid4()), "name": "ISA_Date", "type": "date",
             "path": f"{schema_name}/ISA/Date", "xml_path": f"{schema_name}/ISA/Date"},
            {"id": str(uuid.uuid4()), "name": "GS_TransactionType", "type": "string",
             "path": f"{schema_name}/GS/TransactionType",
             "xml_path": f"{schema_name}/GS/TransactionType"},
        ]
    elif fmt == 'EDIFACT':
        fields += [
            {"id": str(uuid.uuid4()), "name": "UNB_Sender", "type": "string",
             "path": f"{schema_name}/UNB/Sender", "xml_path": f"{schema_name}/UNB/Sender"},
            {"id": str(uuid.uuid4()), "name": "UNB_Receiver", "type": "string",
             "path": f"{schema_name}/UNB/Receiver", "xml_path": f"{schema_name}/UNB/Receiver"},
            {"id": str(uuid.uuid4()), "name": "UNH_MessageType", "type": "string",
             "path": f"{schema_name}/UNH/MessageType",
             "xml_path": f"{schema_name}/UNH/MessageType"},
        ]

    # Segmenti body
    for sid, seg in seen_segments.items():
        seg_name = seg.get('name', sid)
        for i, elem in enumerate(seg.get('elements', []), 1):
            field_name = f"{sid}_{i:02d}"
            if isinstance(elem, list):
                for j, sub in enumerate(elem, 1):
                    fields.append({
                        "id": str(uuid.uuid4()),
                        "name": f"{sid}_{i:02d}_{j:02d}",
                        "type": "string",
                        "path": f"{schema_name}/{sid}/{i:02d}/{j:02d}",
                        "xml_path": f"{schema_name}/{sid}/{i:02d}/{j:02d}",
                        "description": f"{seg_name} - element {i} sub {j}",
                    })
            else:
                fields.append({
                    "id": str(uuid.uuid4()),
                    "name": field_name,
                    "type": "string",
                    "path": f"{schema_name}/{sid}/{i:02d}",
                    "xml_path": f"{schema_name}/{sid}/{i:02d}",
                    "description": f"{seg_name} - element {i}",
                })

    return {
        "id": str(uuid.uuid4()),
        "name": schema_name,
        "source": "edi",
        "edi_format": fmt,
        "fields": fields,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def edi_to_flat(parsed: Dict) -> List[Dict]:
    """
    Converte EDI parsato in lista di record flat {segment_id.element_n: value}.
    Utile per la trasformazione diretta.
    """
    records = []
    interchanges = parsed.get('interchanges', [])
    for ic in interchanges:
        for gm in ic.get('groups', ic.get('messages', [])):
            transactions = gm.get('transactions', [gm]) if 'transactions' in gm else [gm]
            for tx in transactions:
                record = {}
                # Envelope metadata
                record['_sender'] = ic.get('sender_id', ic.get('sender', ''))
                record['_receiver'] = ic.get('receiver_id', ic.get('receiver', ''))
                record['_transaction_type'] = tx.get('transaction_type', tx.get('message_type', ''))
                record['_format'] = parsed.get('format', '')

                # Segmenti → appiattiti
                for seg in tx.get('segments', []):
                    sid = seg['id']
                    for i, elem in enumerate(seg.get('elements', []), 1):
                        if isinstance(elem, list):
                            for j, sub in enumerate(elem, 1):
                                record[f"{sid}.{i:02d}.{j:02d}"] = sub
                        else:
                            record[f"{sid}.{i:02d}"] = elem
                records.append(record)
    return records


def build_edi(data: Dict, fmt: str = 'X12', transaction_type: str = '850') -> str:
    """Genera EDI (X12 o EDIFACT) da dizionario strutturato."""
    fmt = fmt.upper()
    if fmt == 'X12':
        return X12Writer().build(data, transaction_type)
    elif fmt == 'EDIFACT':
        return EDIFACTWriter().build(data, transaction_type)
    else:
        raise ValueError(f"Formato non supportato: {fmt}")
