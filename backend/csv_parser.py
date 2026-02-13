#!/usr/bin/env python3
"""
CSV Schema Parser
Parse CSV files with business metadata (business term, description, cardinality, conditions, calculations, offset, length, xmlpath, json_path)

CSV Format (supports both comma and pipe delimiters):
INPUT: campo,business_term,spiegazione,obbligatorio,numerosità,condizionalità,calcolo,offset,lunghezza,xmlpath,json_path
OUTPUT: campo,business_term,spiegazione,obbligatorio,numerosità,condizionalità,calcolo,offset,lunghezza,xmlpath,json_path

OR with pipe separator:
INPUT: campo|business_term|spiegazione|obbligatorio|numerosità|condizionalità|calcolo|offset|lunghezza|xmlpath|json_path
OUTPUT: campo|business_term|spiegazione|obbligatorio|numerosità|condizionalità|calcolo|offset|lunghezza|xmlpath|json_path

Field Descriptions:
- campo: Field path (e.g., Invoice.InvoiceNumber)
- business_term: Business term/label
- spiegazione: Description
- obbligatorio: Required (SI/NO)
- numerosità: Cardinality (1..1, 0..1, 1..N, 0..N)
- condizionalità: Conditional rules
- calcolo: Calculation formula (for output)
- offset: Character offset for IDOC flat files
- lunghezza: Length for IDOC flat files
- xmlpath: XPath for XML files (e.g., /Invoice/cbc:ID)
- json_path: JSONPath for JSON files (e.g., $.Invoice.ID)

The parser automatically detects which delimiter is used by counting occurrences in the first line.
"""

import csv
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class CSVField:
    """Field definition from CSV"""
    campo: str
    business_term: str
    spiegazione: str
    obbligatorio: str  # SI/NO
    numerosità: str  # 1..1, 0..1, 1..N, 0..N
    condizionalità: str
    calcolo: str = ""  # Only for output
    offset: str = ""  # Position offset for IDoc
    lunghezza: str = ""  # Length for IDoc
    xmlpath: str = ""  # XML path (XPath)
    json_path: str = ""  # JSON path (JSONPath)
    
    def to_schema_field(self) -> Dict:
        """Convert to unified schema format"""
        # Parse path
        parts = self.campo.split('.')
        field_id = self.campo.replace('.', '_')
        
        # Determine type from business term or default
        field_type = self._infer_type()
        
        # Parse cardinality
        is_required = self.obbligatorio.upper() == 'SI'
        is_array = 'N' in self.numerosità or '*' in self.numerosità
        
        return {
            'id': field_id,
            'name': parts[-1] if len(parts) > 1 else self.campo,
            'path': self.campo,
            'type': field_type,
            'cardinality': self.numerosità,
            'description': self.spiegazione,
            'business_term': self.business_term,
            'required': is_required,
            'is_array': is_array,
            'condition': self.condizionalità,
            'calculation': self.calcolo,
            'offset': self.offset,
            'length': self.lunghezza,
            'xml_path': self.xmlpath,
            'json_path': self.json_path,
            'parent': '.'.join(parts[:-1]) if len(parts) > 1 else None
        }
    
    def _infer_type(self) -> str:
        """Infer field type from business term or name"""
        term_lower = (self.business_term + ' ' + self.campo).lower()
        
        if any(word in term_lower for word in ['date', 'data', 'datum']):
            return 'date'
        elif any(word in term_lower for word in ['amount', 'total', 'price', 'quantity', 'importo', 'prezzo', 'quantità', 'menge', 'preis']):
            return 'number'
        elif any(word in term_lower for word in ['percent', 'rate', 'percentuale', 'aliquota']):
            return 'number'
        elif any(word in term_lower for word in ['code', 'id', 'number', 'codice', 'numero']):
            return 'string'
        else:
            return 'string'


class CSVSchemaParser:
    """Parse CSV schema files"""
    
    def __init__(self):
        self.fields: Dict[str, CSVField] = {}
        self.hierarchy: Dict[str, List[str]] = {}
    
    def parse_csv(self, csv_path: str, is_output: bool = False) -> Dict[str, Any]:
        """Parse CSV file into schema with auto-detection of delimiter (comma or pipe)"""
        self.fields = {}
        self.hierarchy = {}
        
        # Auto-detect delimiter
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline()
            # Count delimiters in first line
            comma_count = first_line.count(',')
            pipe_count = first_line.count('|')
            
            # Choose delimiter with more occurrences
            delimiter = '|' if pipe_count > comma_count else ','
        
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            
            for row in reader:
                # Skip empty rows
                if not row.get('campo') or not row['campo'].strip():
                    continue
                
                # Create field
                field_data = {
                    'campo': (row['campo'] or '').strip(),
                    'business_term': (row.get('business_term') or '').strip(),
                    'spiegazione': (row.get('spiegazione') or '').strip(),
                    'obbligatorio': (row.get('obbligatorio') or 'NO').strip(),
                    'numerosità': (row.get('numerosità') or '0..1').strip(),
                    'condizionalità': (row.get('condizionalità') or '').strip(),
                    'calcolo': (row.get('calcolo') or '').strip(),
                    'offset': (row.get('offset') or '').strip(),
                    'lunghezza': (row.get('lunghezza') or '').strip(),
                    'xmlpath': (row.get('xmlpath') or '').strip(),
                    'json_path': (row.get('json_path') or '').strip(),
                }
                
                field = CSVField(**field_data)
                field_id = field.campo.replace('.', '_')
                self.fields[field_id] = field
                
                # Build hierarchy
                parts = field.campo.split('.')
                if len(parts) > 1:
                    parent = '.'.join(parts[:-1])
                    parent_id = parent.replace('.', '_')
                    if parent_id not in self.hierarchy:
                        self.hierarchy[parent_id] = []
                    self.hierarchy[parent_id].append(field_id)
        
        return self._build_schema_output(Path(csv_path).stem)
    
    def _build_schema_output(self, name: str) -> Dict[str, Any]:
        """Build unified schema output"""
        schema_fields = {}
        root_fields = []
        
        for field_id, field in self.fields.items():
            schema_field = field.to_schema_field()
            
            # Add children
            if field_id in self.hierarchy:
                schema_field['children'] = self.hierarchy[field_id]
            else:
                schema_field['children'] = []
            
            schema_fields[field_id] = schema_field
            
            # Track root fields
            if not schema_field['parent']:
                root_fields.append(field_id)
        
        return {
            'name': name,
            'type': 'csv',
            'fields': schema_fields,
            'root_fields': root_fields,
            'field_count': len(schema_fields)
        }
    
    def to_tree_structure(self) -> List[Dict]:
        """Convert to tree structure for UI"""
        def build_node(field_id: str) -> Dict:
            field = self.fields[field_id]
            schema_field = field.to_schema_field()
            
            node = {
                'id': schema_field['id'],
                'label': f"{schema_field['name']} ({field.business_term})",
                'path': schema_field['path'],
                'type': schema_field['type'],
                'cardinality': schema_field['cardinality'],
                'description': schema_field['description'],
                'business_term': field.business_term,
                'required': schema_field['required'],
                'condition': field.condizionalità,
                'calculation': field.calcolo,
                'children': []
            }
            
            if field_id in self.hierarchy:
                for child_id in self.hierarchy[field_id]:
                    node['children'].append(build_node(child_id))
            
            return node
        
        # Build tree from root fields
        tree = []
        for field_id, field in self.fields.items():
            schema_field = field.to_schema_field()
            if not schema_field['parent']:
                tree.append(build_node(field_id))
        
        return tree


class MappingCSVExporter:
    """Export mapping results to CSV"""
    
    def __init__(self, mapping_data: Dict, input_schema: Dict, output_schema: Dict):
        self.mapping = mapping_data
        self.input_schema = input_schema
        self.output_schema = output_schema
    
    def export_to_csv(self, output_path: str):
        """Export mapping to CSV file"""
        rows = []
        
        # Process each mapping rule
        for rule in self.mapping.get('rules', []):
            source_path = rule.get('source', '')
            target_path = rule.get('target', '')
            transformation = rule.get('transformation', {})
            
            # Get source field info
            source_field = self._get_field_info(source_path, self.input_schema)
            
            # Get target field info
            target_field = self._get_field_info(target_path, self.output_schema)
            
            # Build transformation rule description
            rule_desc = self._build_rule_description(transformation)
            
            row = {
                'campo_input': source_path,
                'campo_output': target_path,
                'business_term': target_field.get('business_term', ''),
                'spiegazione': target_field.get('description', ''),
                'obbligatorio': 'SI' if target_field.get('required') else 'NO',
                'numerosità': target_field.get('cardinality', ''),
                'condizionalità': target_field.get('condition', ''),
                'regola_trasformazione': rule_desc
            }
            
            rows.append(row)
        
        # Add calculated fields (no source mapping)
        for field_id, field in self.output_schema.get('fields', {}).items():
            if field.get('calculation') and not self._has_mapping(field['path']):
                row = {
                    'campo_input': '',
                    'campo_output': field['path'],
                    'business_term': field.get('business_term', ''),
                    'spiegazione': field.get('description', ''),
                    'obbligatorio': 'SI' if field.get('required') else 'NO',
                    'numerosità': field.get('cardinality', ''),
                    'condizionalità': field.get('condition', ''),
                    'regola_trasformazione': field['calculation']
                }
                rows.append(row)
        
        # Write CSV
        if rows:
            fieldnames = [
                'campo_input', 'campo_output', 'business_term', 'spiegazione',
                'obbligatorio', 'numerosità', 'condizionalità', 'regola_trasformazione'
            ]
            
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
    
    def _get_field_info(self, path: str, schema: Dict) -> Dict:
        """Get field information from schema"""
        field_id = path.replace('.', '_')
        return schema.get('fields', {}).get(field_id, {})
    
    def _has_mapping(self, target_path: str) -> bool:
        """Check if field has mapping"""
        for rule in self.mapping.get('rules', []):
            if rule.get('target') == target_path:
                return True
        return False
    
    def _build_rule_description(self, transformation: Dict) -> str:
        """Build human-readable transformation description"""
        trans_type = transformation.get('type', 'direct')
        
        if trans_type == 'direct':
            return 'direct'
        
        elif trans_type == 'function':
            func_name = transformation.get('function', '')
            params = transformation.get('params', {})
            
            if func_name == 'concat':
                sep = params.get('separator', '')
                return f"concat(separator='{sep}')"
            elif func_name == 'format_date':
                from_fmt = params.get('from_format', '')
                to_fmt = params.get('to_format', '')
                return f"format_date({from_fmt} -> {to_fmt})"
            elif func_name == 'lookup':
                table = params.get('table', '')
                return f"lookup(table={table})"
            else:
                return f"{func_name}({params})"
        
        elif trans_type == 'constant':
            value = transformation.get('value', '')
            return f"constant({value})"
        
        elif trans_type == 'conditional':
            return f"conditional: {transformation}"
        
        else:
            return str(transformation)


def create_sample_input_csv(output_path: str):
    """Create sample input CSV"""
    rows = [
        {
            'campo': 'EDI_DC40.DOCNUM',
            'business_term': 'Document Number',
            'spiegazione': 'Numero documento IDOC',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '0',
            'lunghezza': '16',
            'xmlpath': '/IDOC/EDI_DC40/DOCNUM'
        },
        {
            'campo': 'E1EDK01.BELNR',
            'business_term': 'Invoice Number',
            'spiegazione': 'Numero fattura',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '16',
            'lunghezza': '35',
            'xmlpath': '/IDOC/E1EDK01/BELNR'
        },
        {
            'campo': 'E1EDK01.DATUM',
            'business_term': 'Invoice Date',
            'spiegazione': 'Data emissione fattura',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '51',
            'lunghezza': '8',
            'xmlpath': '/IDOC/E1EDK01/DATUM'
        },
        {
            'campo': 'E1EDK01.WKURS',
            'business_term': 'Exchange Rate',
            'spiegazione': 'Tasso di cambio',
            'obbligatorio': 'NO',
            'numerosità': '0..1',
            'condizionalità': 'IF foreign currency',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '59',
            'lunghezza': '12',
            'xmlpath': '/IDOC/E1EDK01/WKURS'
        },
        {
            'campo': 'E1EDP01.POSEX',
            'business_term': 'Line Item Number',
            'spiegazione': 'Numero riga',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '71',
            'lunghezza': '6',
            'xmlpath': '/IDOC/E1EDP01/POSEX'
        },
        {
            'campo': 'E1EDP01.MENGE',
            'business_term': 'Quantity',
            'spiegazione': 'Quantità articolo',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '77',
            'lunghezza': '15',
            'xmlpath': '/IDOC/E1EDP01/MENGE'
        },
        {
            'campo': 'E1EDP01.PREIS',
            'business_term': 'Unit Price',
            'spiegazione': 'Prezzo unitario',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '92',
            'lunghezza': '15',
            'xmlpath': '/IDOC/E1EDP01/PREIS'
        },
        {
            'campo': 'E1EDP19.MWSKZ',
            'business_term': 'Tax Code',
            'spiegazione': 'Codice IVA',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '107',
            'lunghezza': '2',
            'xmlpath': '/IDOC/E1EDP19/MWSKZ'
        },
        {
            'campo': 'E1EDP19.MSATZ',
            'business_term': 'Tax Rate',
            'spiegazione': 'Aliquota IVA percentuale',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': '',
            'offset': '109',
            'lunghezza': '5',
            'xmlpath': '/IDOC/E1EDP19/MSATZ'
        }
    ]
    
    fieldnames = ['campo', 'business_term', 'spiegazione', 'obbligatorio', 'numerosità', 'condizionalità', 'calcolo', 'offset', 'lunghezza', 'xmlpath']
    
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def create_sample_output_csv(output_path: str):
    """Create sample output CSV"""
    rows = [
        {
            'campo': 'Invoice.ID',
            'business_term': 'Invoice Number',
            'spiegazione': 'Numero identificativo fattura',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'Invoice.IssueDate',
            'business_term': 'Issue Date',
            'spiegazione': 'Data emissione',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'Invoice.DocumentCurrencyCode',
            'business_term': 'Currency Code',
            'spiegazione': 'Codice valuta',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'InvoiceLine.ID',
            'business_term': 'Line ID',
            'spiegazione': 'Identificativo riga',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'InvoiceLine.InvoicedQuantity',
            'business_term': 'Invoiced Quantity',
            'spiegazione': 'Quantità fatturata',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'InvoiceLine.LineExtensionAmount',
            'business_term': 'Line Total',
            'spiegazione': 'Totale riga (senza IVA)',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': 'InvoicedQuantity * Price', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'InvoiceLine.Price.PriceAmount',
            'business_term': 'Price',
            'spiegazione': 'Prezzo unitario',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'InvoiceLine.TaxTotal.TaxAmount',
            'business_term': 'Tax Amount',
            'spiegazione': 'Importo IVA riga',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': 'LineExtensionAmount * (TaxPercent / 100)', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'InvoiceLine.TaxTotal.TaxSubtotal.TaxCategory.ID',
            'business_term': 'Tax Category',
            'spiegazione': 'Categoria IVA',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'InvoiceLine.TaxTotal.TaxSubtotal.TaxCategory.Percent',
            'business_term': 'Tax Percent',
            'spiegazione': 'Percentuale IVA',
            'obbligatorio': 'SI',
            'numerosità': '1..N',
            'condizionalità': '',
            'calcolo': '', 'offset': '', 'lunghezza': '', 'xmlpath': '', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'TaxTotal.TaxAmount',
            'business_term': 'Total Tax Amount',
            'spiegazione': 'Importo IVA totale documento',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': 'SUM(InvoiceLine.TaxTotal.TaxAmount)', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'LegalMonetaryTotal.TaxExclusiveAmount',
            'business_term': 'Total Excluding Tax',
            'spiegazione': 'Totale imponibile',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': 'SUM(InvoiceLine.LineExtensionAmount)', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'LegalMonetaryTotal.TaxInclusiveAmount',
            'business_term': 'Total Including Tax',
            'spiegazione': 'Totale con IVA',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': 'TaxExclusiveAmount + TaxTotal.TaxAmount', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        },
        {
            'campo': 'LegalMonetaryTotal.PayableAmount',
            'business_term': 'Payable Amount',
            'spiegazione': 'Importo da pagare',
            'obbligatorio': 'SI',
            'numerosità': '1..1',
            'condizionalità': '',
            'calcolo': 'TaxInclusiveAmount', 'offset': '', 'lunghezza': '', 'xmlpath': ''
        }
    ]
    
    fieldnames = ['campo', 'business_term', 'spiegazione', 'obbligatorio', 'numerosità', 'condizionalità', 'calcolo', 'offset', 'lunghezza', 'xmlpath']
    
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    # Create sample files
    create_sample_input_csv('sample_input.csv')
    create_sample_output_csv('sample_output.csv')
    
    print("✓ Created sample_input.csv")
    print("✓ Created sample_output.csv")
    
    # Test parsing
    parser = CSVSchemaParser()
    input_schema = parser.parse_csv('sample_input.csv', is_output=False)
    
    print(f"\n✓ Parsed input schema: {input_schema['field_count']} fields")
    
    parser2 = CSVSchemaParser()
    output_schema = parser2.parse_csv('sample_output.csv', is_output=True)
    
    print(f"✓ Parsed output schema: {output_schema['field_count']} fields")
    print(f"  - {sum(1 for f in output_schema['fields'].values() if f.get('calculation'))} calculated fields")
