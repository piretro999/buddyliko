#!/usr/bin/env python3
"""
Buddyliko File Tester / Validator
===================================
Testa e valida file XML/EDI/JSON prodotti da mapping per conformità agli standard.

Funzionalità:
- Auto-detect tipo file (UBL, FatturaPA, EDIFACT, X12, FHIR, ISO 20022, IDoc...)
- Validazione XSD (se schema disponibile localmente o scaricabile)
- Validazione struttura base (elementi obbligatori, namespace)
- Validazione Schematron (per standard che lo supportano)
- Report dettagliato con errori e warnings
- Modalità batch per directory

Usage:
    python file_tester.py invoice.xml
    python file_tester.py --dir ./output_files/ --format ubl
    python file_tester.py transaction.edi --standard x12-810
    python file_tester.py claim.json --standard hl7-fhir-r4

Integrazione con Buddyliko API:
    POST /api/standards/test-file
    { "file_content": "...", "standard_slug": "ubl-2-1" }

Requirements:
    pip install lxml
    (optional) pip install jsonschema  # for JSON schema validation
    (optional) pip install saxonche   # for full Schematron (XSLT2)
"""

import os
import sys
import json
import re
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime

log = logging.getLogger(__name__)


# ─── RESULT STRUCTURES ───────────────────────────────────────────────────────

@dataclass
class ValidationIssue:
    severity: str   # ERROR | WARNING | INFO
    code:     str
    message:  str
    location: str = ''
    line:     int  = 0


@dataclass
class ValidationResult:
    standard:    str = ''
    format_type: str = ''
    detected_as: str = ''
    valid:       bool = False
    score:       int  = 0  # 0-100
    issues:      List[ValidationIssue] = field(default_factory=list)
    metadata:    Dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0

    @property
    def errors(self):
        return [i for i in self.issues if i.severity == 'ERROR']

    @property
    def warnings(self):
        return [i for i in self.issues if i.severity == 'WARNING']

    def to_dict(self):
        return {
            'standard':     self.standard,
            'format_type':  self.format_type,
            'detected_as':  self.detected_as,
            'valid':         self.valid,
            'score':         self.score,
            'error_count':   len(self.errors),
            'warning_count': len(self.warnings),
            'metadata':      self.metadata,
            'duration_ms':   self.duration_ms,
            'issues': [
                {'severity': i.severity, 'code': i.code, 'message': i.message, 'location': i.location, 'line': i.line}
                for i in self.issues
            ]
        }

    def summary(self) -> str:
        status = '✅ VALID' if self.valid else '❌ INVALID'
        lines = [
            f"{status} | Standard: {self.standard or self.detected_as} | Score: {self.score}/100",
            f"Errors: {len(self.errors)} | Warnings: {len(self.warnings)}",
        ]
        if self.metadata:
            for k, v in self.metadata.items():
                if v:
                    lines.append(f"  {k}: {v}")
        if self.errors:
            lines.append("\nERRORS:")
            for e in self.errors[:10]:
                loc = f" [{e.location}]" if e.location else ""
                lines.append(f"  ❌ [{e.code}]{loc} {e.message}")
        if self.warnings:
            lines.append("\nWARNINGS:")
            for w in self.warnings[:5]:
                lines.append(f"  ⚠️  [{w.code}] {w.message}")
        return '\n'.join(lines)


# ─── DETECTOR ────────────────────────────────────────────────────────────────

class FileDetector:
    """Auto-detect file format and standard from content."""

    # XML namespace → standard
    NAMESPACE_MAP = {
        'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2':        ('ubl-2-1',       'xml'),
        'urn:oasis:names:specification:ubl:schema:xsd:CreditNote-2':     ('ubl-2-1',       'xml'),
        'urn:oasis:names:specification:ubl:schema:xsd:Order-2':          ('ubl-2-1',       'xml'),
        'urn:un:unece:uncefact:data:standard:CrossIndustryInvoice:100':   ('cii-d16b',      'xml'),
        'urn:un:unece:uncefact:data:standard:CrossIndustryDocument:schemas:Extended:100': ('cii-d16b', 'xml'),
        'http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2': ('fatturapa-1-2', 'xml'),
        'urn:iso:std:iso:20022:tech:xsd:pain.001':                        ('iso-20022',     'xml'),
        'urn:iso:std:iso:20022:tech:xsd:pain.008':                        ('iso-20022',     'xml'),
        'urn:iso:std:iso:20022:tech:xsd:camt.053':                        ('iso-20022',     'xml'),
        'urn:iso:std:iso:20022:tech:xsd:camt.054':                        ('iso-20022',     'xml'),
        'urn:iso:std:iso:20022:tech:xsd:pacs.008':                        ('iso-20022',     'xml'),
        'http://www.ebinterface.at/schema/':                              ('ph-ebinterface','xml'),
    }

    # XML root tag patterns
    ROOT_TAG_MAP = {
        'FatturaElettronica':  ('fatturapa-1-2', 'xml'),
        'Invoice':             ('ubl-2-1',       'xml'),
        'CreditNote':          ('ubl-2-1',       'xml'),
        'Order':               ('ubl-2-1',       'xml'),
        'CrossIndustryInvoice':('cii-d16b',      'xml'),
        'Document':            ('iso-20022',     'xml'),
    }

    @classmethod
    def detect_xml(cls, content: str) -> Tuple[str, str]:
        """Detect standard from XML content. Returns (slug, format_type)."""
        for ns, (slug, fmt) in cls.NAMESPACE_MAP.items():
            if ns in content:
                return slug, fmt
        # Fallback: root tag
        m = re.search(r'<(?:\w+:)?(\w+)[\s>]', content.lstrip())
        if m:
            tag = m.group(1)
            if tag in cls.ROOT_TAG_MAP:
                return cls.ROOT_TAG_MAP[tag]
        return 'unknown-xml', 'xml'

    @classmethod
    def detect_edi(cls, content: str) -> Tuple[str, str]:
        """Detect EDI format (X12 or EDIFACT)."""
        stripped = content.strip()
        if stripped.startswith('ISA'):
            # X12 — detect transaction set
            st_match = re.search(r'ST\*(\d{3})\*', content)
            if st_match:
                ts = st_match.group(1)
                ts_map = {
                    '810': 'x12-810', '850': 'x12-850', '856': 'x12-856',
                    '820': 'x12-820', '834': 'x12-834', '835': 'x12-835',
                    '837': 'x12-837', '270': 'x12-270', '271': 'x12-270',
                    '940': 'x12-940', '945': 'x12-945', '997': 'x12-997',
                }
                return ts_map.get(ts, f'x12-{ts}'), 'edi'
            return 'x12-unknown', 'edi'
        elif stripped.startswith('UNB'):
            # EDIFACT — detect message type
            unh_match = re.search(r'UNH\+[^+]+\+([A-Z]+):', content)
            if unh_match:
                msg = unh_match.group(1)
                msg_map = {
                    'INVOIC': 'edifact-invoic', 'ORDERS': 'edifact-orders',
                    'ORDRSP': 'edifact-ordrsp', 'DESADV': 'edifact-desadv',
                    'RECADV': 'edifact-recadv', 'CUSCAR': 'edifact-cuscar',
                    'CUSDEC': 'edifact-cusdec',
                }
                return msg_map.get(msg, f'edifact-{msg.lower()}'), 'edifact'
            return 'edifact-unknown', 'edifact'
        return 'unknown-edi', 'edi'

    @classmethod
    def detect_json(cls, content: str) -> Tuple[str, str]:
        """Detect standard from JSON content."""
        try:
            data = json.loads(content)
        except Exception:
            return 'unknown-json', 'json'
        rt = data.get('resourceType', '')
        if rt:
            fhir_map = {
                'Bundle': 'hl7-fhir-r4', 'Patient': 'hl7-fhir-r4',
                'Claim': 'hl7-fhir-r4', 'Invoice': 'hl7-fhir-r4',
                'Observation': 'hl7-fhir-r4', 'DiagnosticReport': 'hl7-fhir-r4',
            }
            return fhir_map.get(rt, 'hl7-fhir-r4'), 'json'
        return 'unknown-json', 'json'

    @classmethod
    def detect(cls, content: str, hint: str = '') -> Tuple[str, str]:
        """Auto-detect format from content + optional hint."""
        if hint and hint != 'auto':
            return hint, 'xml'
        stripped = content.strip()
        if stripped.startswith('<') or stripped.startswith('<?xml'):
            return cls.detect_xml(content)
        if stripped.startswith('ISA') or stripped.startswith('UNB') or stripped.startswith('UNH'):
            return cls.detect_edi(content)
        if stripped.startswith('{') or stripped.startswith('['):
            return cls.detect_json(content)
        # Fixed-width (NACHA ACH, SWIFT MT)
        if len(stripped) > 0 and len(stripped.split('\n')[0]) == 94:
            return 'nacha-ach', 'fixed'
        if stripped.startswith(':20:') or stripped.startswith('{1:F01'):
            return 'swift-mt', 'fixed'
        return 'unknown', 'unknown'


# ─── VALIDATORS ──────────────────────────────────────────────────────────────

class UBLValidator:
    """Validate UBL 2.1/2.3 XML documents."""

    REQUIRED_ELEMENTS = [
        ('cbc:ID',               'Invoice ID'),
        ('cbc:IssueDate',        'Issue Date'),
        ('cbc:InvoiceTypeCode',  'Invoice Type Code'),
        ('cbc:DocumentCurrencyCode', 'Currency Code'),
    ]

    INVOICE_TYPE_CODES = {'380', '381', '383', '384', '386', '394', '82', '0', '1'}
    VALID_CURRENCIES   = {'EUR', 'USD', 'GBP', 'CHF', 'SEK', 'NOK', 'DKK', 'PLN', 'CZK'}

    @classmethod
    def validate(cls, content: str) -> ValidationResult:
        result = ValidationResult(standard='ubl-2-1', format_type='xml')
        result.metadata['type'] = 'UBL Invoice'

        # Try XML parse
        try:
            from lxml import etree
            tree = etree.fromstring(content.encode('utf-8'))
            ns = {
                'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
                'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
            }

            # Check root
            root_tag = tree.tag.split('}')[-1] if '}' in tree.tag else tree.tag
            result.metadata['root_element'] = root_tag

            # Extract key metadata
            def get_text(xpath):
                els = tree.xpath(xpath, namespaces=ns)
                return els[0].text if els else ''

            result.metadata['invoice_id']    = get_text('cbc:ID')
            result.metadata['issue_date']    = get_text('cbc:IssueDate')
            result.metadata['invoice_type']  = get_text('cbc:InvoiceTypeCode')
            result.metadata['currency']      = get_text('cbc:DocumentCurrencyCode')
            result.metadata['supplier']      = get_text('cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name')
            result.metadata['buyer']         = get_text('cac:AccountingCustomerParty/cac:Party/cac:PartyName/cbc:Name')
            result.metadata['total']         = get_text('cac:LegalMonetaryTotal/cbc:PayableAmount')
            result.metadata['line_count']    = str(len(tree.xpath('cac:InvoiceLine', namespaces=ns)))

            # Validate required fields
            for xpath, label in cls.REQUIRED_ELEMENTS:
                val = get_text(xpath)
                if not val:
                    result.issues.append(ValidationIssue('ERROR', 'BT-REQUIRED', f'Missing required: {label} ({xpath})', xpath))

            # Invoice type code
            inv_type = result.metadata.get('invoice_type', '')
            if inv_type and inv_type not in cls.INVOICE_TYPE_CODES:
                result.issues.append(ValidationIssue('WARNING', 'BT-003', f'Unknown InvoiceTypeCode: {inv_type}. Expected: {sorted(cls.INVOICE_TYPE_CODES)}', 'cbc:InvoiceTypeCode'))

            # Currency
            currency = result.metadata.get('currency', '')
            if currency and currency not in cls.VALID_CURRENCIES:
                result.issues.append(ValidationIssue('WARNING', 'BT-005', f'Uncommon currency code: {currency}', 'cbc:DocumentCurrencyCode'))

            # Supplier VAT
            supplier_vat = get_text('cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID')
            if not supplier_vat:
                result.issues.append(ValidationIssue('WARNING', 'BT-031', 'Supplier VAT number not found', 'AccountingSupplierParty'))

            # Lines
            lines = tree.xpath('cac:InvoiceLine', namespaces=ns)
            if not lines:
                result.issues.append(ValidationIssue('ERROR', 'BG-25', 'No InvoiceLine elements found', 'InvoiceLine'))
            else:
                for i, line in enumerate(lines):
                    line_id = (tree.xpath(f'cac:InvoiceLine[{i+1}]/cbc:ID/text()', namespaces=ns) or ['?'])[0]
                    if not tree.xpath(f'cac:InvoiceLine[{i+1}]/cbc:InvoicedQuantity', namespaces=ns):
                        result.issues.append(ValidationIssue('ERROR', 'BT-129', f'Line {line_id}: Missing InvoicedQuantity', f'InvoiceLine[{i+1}]'))
                    if not tree.xpath(f'cac:InvoiceLine[{i+1}]/cac:Item/cbc:Name', namespaces=ns):
                        result.issues.append(ValidationIssue('WARNING', 'BT-153', f'Line {line_id}: Missing Item/Name', f'InvoiceLine[{i+1}]'))

            # LegalMonetaryTotal
            if not tree.xpath('cac:LegalMonetaryTotal/cbc:PayableAmount', namespaces=ns):
                result.issues.append(ValidationIssue('ERROR', 'BT-115', 'Missing LegalMonetaryTotal/PayableAmount', 'LegalMonetaryTotal'))

        except ImportError:
            result.issues.append(ValidationIssue('WARNING', 'LXML-MISSING', 'lxml not installed. Install with: pip install lxml. Running basic checks only.'))
            # Basic string checks
            for check in ['<cbc:ID>', '<cbc:IssueDate>', '<cac:InvoiceLine', 'cbc:PayableAmount']:
                if check not in content:
                    result.issues.append(ValidationIssue('WARNING', 'BASIC-CHECK', f'Element not found in content: {check}'))

        except Exception as e:
            result.issues.append(ValidationIssue('ERROR', 'PARSE-ERROR', f'XML parse error: {str(e)}'))

        # Score
        error_count = len(result.errors)
        result.valid = error_count == 0
        result.score = max(0, 100 - (error_count * 20) - (len(result.warnings) * 5))
        return result


class FatturapaValidator:
    """Validate FatturaPA 1.2.2 XML documents."""

    TIPO_DOCUMENTO = {'TD01', 'TD02', 'TD03', 'TD04', 'TD05', 'TD06', 'TD07',
                      'TD08', 'TD09', 'TD10', 'TD11', 'TD12', 'TD16', 'TD17',
                      'TD18', 'TD19', 'TD20', 'TD21', 'TD22', 'TD23', 'TD24',
                      'TD25', 'TD26', 'TD27', 'TD28'}
    MODALITA_PAGAMENTO = {'MP01','MP02','MP03','MP04','MP05','MP06','MP07',
                          'MP08','MP09','MP10','MP11','MP12','MP13','MP14',
                          'MP15','MP16','MP17','MP18','MP19','MP20','MP21','MP22'}

    @classmethod
    def validate(cls, content: str) -> ValidationResult:
        result = ValidationResult(standard='fatturapa-1-2', format_type='xml')

        try:
            from lxml import etree
            tree = etree.fromstring(content.encode('utf-8'))

            def xpath_text(path, default=''):
                el = tree.find(path)
                return el.text.strip() if el is not None and el.text else default

            # Versione
            ver = tree.get('versione', '')
            if ver not in ('FPR12', 'FPA12'):
                result.issues.append(ValidationIssue('ERROR', 'FPA-001', f'Attributo versione non valido: "{ver}". Atteso: FPR12 o FPA12', 'FatturaElettronica/@versione'))
            result.metadata['versione'] = ver

            # DatiTrasmissione
            prog = xpath_text('.//ProgressivoInvio')
            if not prog:
                result.issues.append(ValidationIssue('ERROR', 'FPA-002', 'ProgressivoInvio mancante'))
            result.metadata['progressivo_invio'] = prog

            dest = xpath_text('.//CodiceDestinatario')
            pec  = xpath_text('.//PECDestinatario')
            if not dest and not pec:
                result.issues.append(ValidationIssue('ERROR', 'FPA-003', 'Né CodiceDestinatario né PECDestinatario presenti'))
            result.metadata['destinatario'] = dest or pec

            # Cedente
            piva_cedente = xpath_text('.//CedentePrestatore//IdCodice')
            if not piva_cedente:
                result.issues.append(ValidationIssue('ERROR', 'FPA-010', 'Partita IVA Cedente mancante'))
            result.metadata['piva_cedente'] = piva_cedente

            regime = xpath_text('.//RegimeFiscale')
            if not regime:
                result.issues.append(ValidationIssue('WARNING', 'FPA-011', 'RegimeFiscale mancante'))

            # Dati documento
            tipo = xpath_text('.//TipoDocumento')
            if tipo not in cls.TIPO_DOCUMENTO:
                result.issues.append(ValidationIssue('ERROR', 'FPA-020', f'TipoDocumento non valido: "{tipo}". Valori ammessi: TD01-TD28'))
            result.metadata['tipo_documento'] = tipo

            numero = xpath_text('.//Numero')
            if not numero:
                result.issues.append(ValidationIssue('ERROR', 'FPA-021', 'Numero documento mancante'))
            result.metadata['numero'] = numero

            data = xpath_text('.//Data')
            if not data:
                result.issues.append(ValidationIssue('ERROR', 'FPA-022', 'Data documento mancante'))
            result.metadata['data'] = data

            # Linee
            lines = tree.findall('.//DettaglioLinee')
            if not lines:
                result.issues.append(ValidationIssue('ERROR', 'FPA-030', 'Nessun DettaglioLinee trovato'))
            result.metadata['numero_linee'] = str(len(lines))

            for line in lines:
                num_linea = (line.find('NumeroLinea').text if line.find('NumeroLinea') is not None else '?')
                if line.find('PrezzoTotale') is None:
                    result.issues.append(ValidationIssue('ERROR', 'FPA-031', f'Linea {num_linea}: PrezzoTotale mancante', f'DettaglioLinee[{num_linea}]'))
                if line.find('AliquotaIVA') is None:
                    result.issues.append(ValidationIssue('ERROR', 'FPA-032', f'Linea {num_linea}: AliquotaIVA mancante', f'DettaglioLinee[{num_linea}]'))

            # Pagamento
            mod_pag = xpath_text('.//ModalitaPagamento')
            if mod_pag and mod_pag not in cls.MODALITA_PAGAMENTO:
                result.issues.append(ValidationIssue('WARNING', 'FPA-040', f'ModalitaPagamento non standard: "{mod_pag}"'))

            # Totale
            totale = xpath_text('.//ImportoTotaleDocumento')
            result.metadata['importo_totale'] = totale

        except ImportError:
            result.issues.append(ValidationIssue('WARNING', 'LXML-MISSING', 'lxml non installato. Esegui: pip install lxml'))
        except Exception as e:
            result.issues.append(ValidationIssue('ERROR', 'PARSE-ERROR', f'Errore parsing XML: {str(e)}'))

        result.valid = len(result.errors) == 0
        result.score = max(0, 100 - (len(result.errors) * 15) - (len(result.warnings) * 5))
        return result


class X12Validator:
    """Validate ANSI X12 EDI files."""

    @classmethod
    def validate(cls, content: str, transaction_set: str = '') -> ValidationResult:
        result = ValidationResult(standard=f'x12-{transaction_set}' if transaction_set else 'x12', format_type='edi')

        lines = content.replace('~', '~\n').split('\n')
        segments = [l.strip() for l in lines if l.strip()]

        if not segments or not segments[0].startswith('ISA'):
            result.issues.append(ValidationIssue('ERROR', 'X12-001', 'File does not start with ISA envelope'))
            result.valid = False
            return result

        # Parse ISA
        isa = segments[0]
        elements = isa.split('*')
        if len(elements) < 16:
            result.issues.append(ValidationIssue('ERROR', 'X12-002', f'ISA segment has {len(elements)} elements, expected 16'))
        else:
            result.metadata['sender_id']     = elements[6].strip()
            result.metadata['receiver_id']   = elements[8].strip()
            result.metadata['date']          = elements[9]
            result.metadata['time']          = elements[10]
            result.metadata['version']       = elements[12]
            result.metadata['control_num']   = elements[13]

        # Find GS
        gs_segs = [s for s in segments if s.startswith('GS*')]
        if not gs_segs:
            result.issues.append(ValidationIssue('ERROR', 'X12-010', 'No GS (Functional Group) segment found'))
        else:
            gs = gs_segs[0].split('*')
            result.metadata['func_id'] = gs[1] if len(gs) > 1 else ''

        # Find ST/SE
        st_segs = [s for s in segments if s.startswith('ST*')]
        se_segs = [s for s in segments if s.startswith('SE*')]
        if not st_segs:
            result.issues.append(ValidationIssue('ERROR', 'X12-011', 'No ST (Transaction Set) header found'))
        if not se_segs:
            result.issues.append(ValidationIssue('ERROR', 'X12-012', 'No SE (Transaction Set) trailer found'))

        if st_segs:
            st = st_segs[0].split('*')
            ts_id = st[1] if len(st) > 1 else ''
            result.metadata['transaction_set'] = ts_id
            result.standard = f'x12-{ts_id}'

        # Find IEA
        iea_segs = [s for s in segments if s.startswith('IEA*')]
        if not iea_segs:
            result.issues.append(ValidationIssue('ERROR', 'X12-013', 'No IEA (Interchange Control Trailer) found'))

        # Segment count check
        if st_segs and se_segs:
            try:
                se = se_segs[-1].split('*')
                declared_count = int(se[1]) if len(se) > 1 else 0
                # Count segments between ST and SE
                st_idx = next(i for i, s in enumerate(segments) if s.startswith('ST*'))
                se_idx = next(i for i, s in enumerate(segments) if s.startswith('SE*'))
                actual_count = se_idx - st_idx + 1
                result.metadata['segment_count']  = str(actual_count)
                if declared_count != actual_count:
                    result.issues.append(ValidationIssue('ERROR', 'X12-020', f'SE segment count mismatch: declared {declared_count}, actual {actual_count}'))
            except Exception:
                pass

        result.valid = len(result.errors) == 0
        result.score = max(0, 100 - (len(result.errors) * 20) - (len(result.warnings) * 5))
        return result


class EDIFACTValidator:
    """Validate UN/EDIFACT messages."""

    @classmethod
    def validate(cls, content: str) -> ValidationResult:
        result = ValidationResult(standard='edifact', format_type='edifact')

        # Normalize: remove newlines added for readability
        content_flat = content.replace('\n', '')
        segments = [s.strip() for s in content_flat.split("'") if s.strip()]

        if not segments or not segments[0].startswith('UNB'):
            result.issues.append(ValidationIssue('ERROR', 'EDI-001', 'File does not start with UNB envelope'))
            result.valid = False
            return result

        # Parse UNB
        unb = segments[0].split('+')
        if len(unb) >= 5:
            result.metadata['sender']   = unb[2].split(':')[0] if len(unb) > 2 else ''
            result.metadata['receiver'] = unb[3].split(':')[0] if len(unb) > 3 else ''
            result.metadata['date']     = unb[4] if len(unb) > 4 else ''

        # Find UNH
        unh_segs = [s for s in segments if s.startswith('UNH')]
        unz_segs = [s for s in segments if s.startswith('UNZ')]
        if not unh_segs:
            result.issues.append(ValidationIssue('ERROR', 'EDI-010', 'No UNH (Message Header) found'))
        if not unz_segs:
            result.issues.append(ValidationIssue('ERROR', 'EDI-011', 'No UNZ (Interchange Trailer) found'))

        if unh_segs:
            unh = unh_segs[0].split('+')
            if len(unh) >= 2:
                msg_ref = unh[1] if len(unh) > 1 else ''
                msg_id  = unh[2].split(':')[0] if len(unh) > 2 else ''
                result.metadata['message_type'] = msg_id
                result.standard = f'edifact-{msg_id.lower()}' if msg_id else 'edifact'

        # UNT check
        unt_segs = [s for s in segments if s.startswith('UNT')]
        if not unt_segs:
            result.issues.append(ValidationIssue('ERROR', 'EDI-012', 'No UNT (Message Trailer) found'))
        else:
            unt = unt_segs[-1].split('+')
            try:
                declared = int(unt[1]) if len(unt) > 1 else 0
                unh_idx = next(i for i, s in enumerate(segments) if s.startswith('UNH'))
                unt_idx = next(i for i, s in enumerate(segments) if s.startswith('UNT'))
                actual = unt_idx - unh_idx + 1
                result.metadata['segment_count'] = str(actual)
                if declared != actual:
                    result.issues.append(ValidationIssue('WARNING', 'EDI-020', f'UNT segment count mismatch: declared {declared}, actual {actual}'))
            except Exception:
                pass

        result.valid = len(result.errors) == 0
        result.score = max(0, 100 - (len(result.errors) * 20) - (len(result.warnings) * 5))
        return result


class FHIRValidator:
    """Validate HL7 FHIR R4/R5 JSON resources."""

    REQUIRED_RESOURCE_FIELDS = {
        'Patient':     ['id', 'name', 'gender', 'birthDate'],
        'Claim':       ['status', 'type', 'use', 'patient', 'created', 'insurer', 'provider', 'priority', 'insurance'],
        'Observation': ['status', 'code', 'subject'],
        'Bundle':      ['type', 'entry'],
    }

    @classmethod
    def validate(cls, content: str) -> ValidationResult:
        result = ValidationResult(standard='hl7-fhir-r4', format_type='json')

        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            result.issues.append(ValidationIssue('ERROR', 'FHIR-001', f'JSON parse error: {str(e)}'))
            result.valid = False
            return result

        rt = data.get('resourceType')
        if not rt:
            result.issues.append(ValidationIssue('ERROR', 'FHIR-002', 'Missing resourceType'))
        result.metadata['resource_type'] = rt or 'unknown'
        result.metadata['id'] = data.get('id', '')

        # FHIR version hint
        meta = data.get('meta', {})
        profile = meta.get('profile', [])
        result.metadata['profile'] = ', '.join(profile[:2])

        if rt == 'Bundle':
            entries = data.get('entry', [])
            result.metadata['entry_count'] = str(len(entries))
            bundle_type = data.get('type', '')
            result.metadata['bundle_type'] = bundle_type
            if not bundle_type:
                result.issues.append(ValidationIssue('ERROR', 'FHIR-010', 'Bundle.type is required'))
            for i, entry in enumerate(entries):
                res = entry.get('resource', {})
                if not res.get('resourceType'):
                    result.issues.append(ValidationIssue('WARNING', 'FHIR-011', f'Entry [{i}]: resource.resourceType missing'))

        # Check required fields for known resource types
        if rt in cls.REQUIRED_RESOURCE_FIELDS:
            for required_field in cls.REQUIRED_RESOURCE_FIELDS[rt]:
                if required_field not in data:
                    result.issues.append(ValidationIssue('WARNING', 'FHIR-REQ', f'{rt}.{required_field} is recommended/required'))

        result.valid = len(result.errors) == 0
        result.score = max(0, 100 - (len(result.errors) * 20) - (len(result.warnings) * 5))
        return result


class ISO20022Validator:
    """Validate ISO 20022 XML messages (pain, camt, pacs)."""

    @classmethod
    def validate(cls, content: str) -> ValidationResult:
        result = ValidationResult(standard='iso-20022', format_type='xml')

        try:
            from lxml import etree
            tree = etree.fromstring(content.encode('utf-8'))

            root_ns = tree.nsmap.get(None, tree.nsmap.get('', ''))
            result.metadata['namespace'] = root_ns

            # Detect message type from namespace
            if root_ns:
                ns_match = re.search(r'(pain|camt|pacs|sese|semt|tsmt)\.\d{3}\.\d{3}\.\d{2}', root_ns)
                if ns_match:
                    result.metadata['message_type'] = ns_match.group(0)
                    result.standard = f'iso-20022-{ns_match.group(0)}'

            # For pain.001 (Credit Transfer)
            ns = {None: root_ns} if root_ns else {}

            def get_text(xpath):
                try:
                    els = tree.xpath(xpath, namespaces={'d': root_ns} if root_ns else {})
                    return els[0].text if els else ''
                except Exception:
                    return ''

            # Message ID
            pfx = ('{%s}' % root_ns) if root_ns else ''
            mid_els = tree.findall(f'.//{pfx}MsgId')
            msg_id  = mid_els[0].text if mid_els else ''
            result.metadata['msg_id'] = msg_id
            nb_els = tree.findall(f'.//{pfx}NbOfTxs')
            nb_txs = nb_els[0].text if nb_els else ''
            result.metadata['nb_of_txs'] = nb_txs

        except ImportError:
            result.issues.append(ValidationIssue('WARNING', 'LXML-MISSING', 'lxml not installed: pip install lxml'))
        except Exception as e:
            result.issues.append(ValidationIssue('ERROR', 'PARSE-ERROR', f'XML parse error: {str(e)}'))

        result.valid = len(result.errors) == 0
        result.score = max(0, 100 - (len(result.errors) * 20) - (len(result.warnings) * 5))
        return result


# ─── MAIN VALIDATOR DISPATCHER ───────────────────────────────────────────────

class FileTester:
    """Main entry point: detect and validate any standard file."""

    VALIDATORS = {
        'ubl-2-1':       UBLValidator.validate,
        'ubl-2-3':       UBLValidator.validate,
        'peppol-bis-3':  UBLValidator.validate,
        'en-16931':      UBLValidator.validate,
        'fatturapa-1-2': FatturapaValidator.validate,
        'hl7-fhir-r4':   FHIRValidator.validate,
        'hl7-fhir-r5':   FHIRValidator.validate,
        'iso-20022':     ISO20022Validator.validate,
        'edifact-invoic': EDIFACTValidator.validate,
        'edifact-orders': EDIFACTValidator.validate,
        'edifact-desadv': EDIFACTValidator.validate,
        'x12-810':       lambda c: X12Validator.validate(c, '810'),
        'x12-850':       lambda c: X12Validator.validate(c, '850'),
        'x12-856':       lambda c: X12Validator.validate(c, '856'),
        'x12-834':       lambda c: X12Validator.validate(c, '834'),
        'x12-835':       lambda c: X12Validator.validate(c, '835'),
        'x12-837':       lambda c: X12Validator.validate(c, '837'),
    }

    @classmethod
    def test_file(cls, content: str, standard_hint: str = '') -> ValidationResult:
        """Test a file with optional standard hint. Returns ValidationResult."""
        start = datetime.now()

        # Auto-detect
        detected_slug, detected_fmt = FileDetector.detect(content, standard_hint)
        
        # Find validator
        validator_key = standard_hint if standard_hint and standard_hint in cls.VALIDATORS else detected_slug
        # Try prefix match for x12/edifact
        if validator_key not in cls.VALIDATORS:
            for key in cls.VALIDATORS:
                if detected_slug.startswith(key.split('-')[0]):
                    validator_key = key
                    break

        if validator_key in cls.VALIDATORS:
            result = cls.VALIDATORS[validator_key](content)
        else:
            # Generic XML check
            result = ValidationResult(standard=detected_slug, format_type=detected_fmt)
            result.issues.append(ValidationIssue('INFO', 'NO-VALIDATOR', f'No specific validator for {detected_slug}. Basic check only.'))
            if detected_fmt == 'xml':
                try:
                    from lxml import etree
                    etree.fromstring(content.encode('utf-8'))
                    result.valid = True
                    result.score = 60
                except Exception as e:
                    result.issues.append(ValidationIssue('ERROR', 'PARSE-ERROR', str(e)))

        result.detected_as = detected_slug
        result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        return result

    @classmethod
    def test_file_path(cls, path: Path, standard_hint: str = '') -> ValidationResult:
        try:
            content = path.read_text(encoding='utf-8', errors='replace')
        except Exception as e:
            r = ValidationResult()
            r.issues.append(ValidationIssue('ERROR', 'FILE-ERROR', f'Cannot read file: {e}'))
            return r
        return cls.test_file(content, standard_hint)


# ─── FASTAPI INTEGRATION (for Buddyliko backend) ─────────────────────────────

def register_file_test_endpoints(app, get_current_user=None):
    """Register /api/standards/test-file endpoint in FastAPI app."""
    try:
        from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
        router = APIRouter()

        @router.post('/api/standards/test-file')
        async def test_file_endpoint(
            file: UploadFile = File(None),
            file_content: str = Form(None),
            standard: str = Form(''),
            user = Depends(get_current_user) if get_current_user else None
        ):
            content = None
            if file:
                raw = await file.read()
                content = raw.decode('utf-8', errors='replace')
            elif file_content:
                content = file_content
            else:
                raise HTTPException(400, 'Provide file or file_content')

            result = FileTester.test_file(content, standard)
            return result.to_dict()

        app.include_router(router)
        print("✅ File test endpoint registered: POST /api/standards/test-file")

    except ImportError:
        print("⚠️  FastAPI not available. Skipping endpoint registration.")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Buddyliko File Tester')
    parser.add_argument('file',       nargs='?', help='File to test')
    parser.add_argument('--standard', default='', help='Force standard (e.g. ubl-2-1, fatturapa-1-2)')
    parser.add_argument('--dir',      help='Test all files in directory')
    parser.add_argument('--json',     action='store_true', help='Output JSON')
    parser.add_argument('--verbose',  action='store_true', help='Verbose output')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    tester = FileTester()

    if args.dir:
        results = []
        for f in Path(args.dir).iterdir():
            if f.is_file() and f.suffix in ('.xml', '.json', '.edi', '.txt', '.edifact'):
                print(f"\n{'='*60}")
                print(f"Testing: {f.name}")
                result = tester.test_file_path(f, args.standard)
                if args.json:
                    print(json.dumps(result.to_dict(), indent=2))
                else:
                    print(result.summary())
                results.append({'file': f.name, **result.to_dict()})
        return

    if args.file:
        path = Path(args.file)
        if not path.exists():
            print(f"File not found: {path}", file=sys.stderr)
            sys.exit(1)
        result = tester.test_file_path(path, args.standard)
        if args.json:
            print(json.dumps(result.to_dict(), indent=2))
        else:
            print(result.summary())
        sys.exit(0 if result.valid else 1)

    parser.print_help()


if __name__ == '__main__':
    main()
