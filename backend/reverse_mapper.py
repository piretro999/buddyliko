#!/usr/bin/env python3
"""
Reverse Mapping Engine
Inverts input↔output schemas and transformations

Handles:
- Schema swap (input becomes output, vice versa)
- Connection reversal (source→target becomes target→source)
- Transformation inversion where possible
- Warns about non-invertible transformations
"""

from typing import Dict, List, Any, Optional, Tuple
import json


class TransformationInverter:
    """Invert transformations where possible"""
    
    @staticmethod
    def can_invert(transformation: Dict) -> Tuple[bool, str]:
        """
        Check if transformation can be inverted
        Returns: (can_invert: bool, reason: str)
        """
        trans_type = transformation.get('type', 'direct')
        
        if trans_type == 'direct':
            return True, "Direct mapping is always invertible"
        
        if trans_type == 'formula':
            formula = transformation.get('formula', '').lower()
            
            # CONCAT is invertible to SPLIT
            if 'concat' in formula or '+' in formula:
                return True, "CONCAT can be inverted to SPLIT"
            
            # Math operations are invertible
            if any(op in formula for op in ['*', '/', '+', '-']):
                return True, "Math operations are invertible"
            
            # DATE_FORMAT is invertible if formats are known
            if 'date_format' in formula:
                return True, "DATE_FORMAT can be inverted"
            
            # TRIM, UPPERCASE, LOWERCASE are NOT invertible (data loss)
            if any(fn in formula for fn in ['trim', 'uppercase', 'lowercase', 'substr']):
                return False, "Lossy transformation (TRIM/CASE/SUBSTR) cannot be inverted"
            
            return False, "Unknown formula - cannot determine invertibility"
        
        return False, f"Transformation type '{trans_type}' not supported for inversion"
    
    @staticmethod
    def invert(transformation: Dict, source_field: str, target_field: str) -> Dict:
        """
        Invert transformation
        source_field: original source (will become target after reverse)
        target_field: original target (will become source after reverse)
        """
        trans_type = transformation.get('type', 'direct')
        
        if trans_type == 'direct':
            return {'type': 'direct'}
        
        if trans_type == 'formula':
            formula = transformation.get('formula', '')
            inverted_formula = TransformationInverter._invert_formula(
                formula, source_field, target_field
            )
            
            return {
                'type': 'formula',
                'formula': inverted_formula
            }
        
        return transformation
    
    @staticmethod
    def _invert_formula(formula: str, source: str, target: str) -> str:
        """Invert formula logic"""
        formula_lower = formula.lower()
        
        # CONCAT(A, B, ...) → SPLIT(result, separator)
        if 'concat' in formula_lower or '+' in formula:
            # Detect separator
            if ',' in formula:
                separator = ','
            elif ' + ' in formula:
                separator = ' '
            else:
                separator = ''
            
            return f"SPLIT({target}, '{separator}')"
        
        # Math operations
        # A * 2 → result / 2
        if '*' in formula:
            import re
            match = re.search(r'\*\s*(\d+\.?\d*)', formula)
            if match:
                factor = match.group(1)
                return f"{target} / {factor}"
        
        # A / 2 → result * 2
        if '/' in formula:
            import re
            match = re.search(r'/\s*(\d+\.?\d*)', formula)
            if match:
                factor = match.group(1)
                return f"{target} * {factor}"
        
        # A + 10 → result - 10
        if '+' in formula and not 'concat' in formula_lower:
            import re
            match = re.search(r'\+\s*(\d+\.?\d*)', formula)
            if match:
                value = match.group(1)
                return f"{target} - {value}"
        
        # A - 10 → result + 10
        if '-' in formula:
            import re
            match = re.search(r'-\s*(\d+\.?\d*)', formula)
            if match:
                value = match.group(1)
                return f"{target} + {value}"
        
        # DATE_FORMAT(field, "from", "to") → DATE_FORMAT(field, "to", "from")
        if 'date_format' in formula_lower:
            # Swap from/to formats
            import re
            match = re.search(r'DATE_FORMAT\((.*?),\s*"([^"]+)",\s*"([^"]+)"\)', formula, re.IGNORECASE)
            if match:
                field, from_fmt, to_fmt = match.groups()
                return f'DATE_FORMAT({target}, "{to_fmt}", "{from_fmt}")'
        
        # Fallback: cannot invert
        return f"REVERSE_OF({formula})"


class MappingReverser:
    """Reverse complete mapping"""
    
    def __init__(self):
        self.inverter = TransformationInverter()
        self.warnings: List[Dict] = []
        self.errors: List[Dict] = []
    
    def reverse_mapping(self, project: Dict) -> Dict:
        """
        Reverse entire project mapping
        
        Input project structure:
        {
            "projectName": "...",
            "inputSchema": {...},
            "outputSchema": {...},
            "inputExample": "...",
            "outputExample": "...",
            "connections": [...]
        }
        
        Returns reversed project
        """
        self.warnings = []
        self.errors = []
        
        reversed_project = {
            "projectName": f"{project['projectName']}_REVERSED",
            "version": project.get("version", "1.0"),
            "created": project.get("created"),
            
            # SWAP SCHEMAS
            "inputSchema": project["outputSchema"],
            "outputSchema": project["inputSchema"],
            
            # SWAP EXAMPLES
            "inputExample": project.get("outputExample", ""),
            "outputExample": project.get("inputExample", ""),
            
            # REVERSE CONNECTIONS
            "connections": []
        }
        
        # Reverse each connection
        for conn in project.get("connections", []):
            reversed_conn = self._reverse_connection(conn)
            if reversed_conn:
                reversed_project["connections"].append(reversed_conn)
        
        return reversed_project
    
    def _reverse_connection(self, connection: Dict) -> Optional[Dict]:
        """
        Reverse single connection
        
        Original: source → target (with transformation)
        Reversed: target → source (with inverted transformation)
        """
        transformation = connection.get('transformation', {'type': 'direct'})
        
        # Check if invertible
        can_invert, reason = self.inverter.can_invert(transformation)
        
        if not can_invert:
            self.warnings.append({
                'connection_id': connection.get('id'),
                'source': connection.get('source'),
                'target': connection.get('target'),
                'reason': reason,
                'action': 'Skipped or converted to direct mapping'
            })
            
            # Convert to direct mapping (best effort)
            transformation = {'type': 'direct'}
        
        # Invert transformation
        inverted_transformation = self.inverter.invert(
            transformation,
            connection.get('source'),
            connection.get('target')
        )
        
        # Create reversed connection
        reversed_conn = {
            'id': f"rev_{connection.get('id')}",
            
            # SWAP source ↔ target
            'source': connection.get('target'),
            'target': connection.get('source'),
            'sourceName': connection.get('targetName'),
            'targetName': connection.get('sourceName'),
            
            # INVERTED transformation
            'transformation': inverted_transformation
        }
        
        return reversed_conn
    
    def get_report(self) -> Dict:
        """Get reversal report"""
        return {
            'warnings': self.warnings,
            'errors': self.errors,
            'total_warnings': len(self.warnings),
            'total_errors': len(self.errors)
        }


# Test
if __name__ == '__main__':
    # Example project
    test_project = {
        "projectName": "IDOC_to_UBL",
        "inputSchema": {
            "name": "IDOC",
            "fields": {
                "E1EDK01_CURCY": {
                    "name": "CURCY",
                    "path": "E1EDK01.CURCY",
                    "business_term": "Currency"
                }
            }
        },
        "outputSchema": {
            "name": "UBL",
            "fields": {
                "Invoice_Currency": {
                    "name": "Currency",
                    "path": "Invoice.Currency",
                    "business_term": "Currency"
                }
            }
        },
        "connections": [
            {
                "id": "conn_1",
                "source": "E1EDK01.CURCY",
                "target": "Invoice.Currency",
                "sourceName": "CURCY",
                "targetName": "Currency",
                "transformation": {"type": "direct"}
            },
            {
                "id": "conn_2",
                "source": "E1EDK01.MWSKZ",
                "target": "Invoice.TaxPercent",
                "sourceName": "MWSKZ",
                "targetName": "TaxPercent",
                "transformation": {
                    "type": "formula",
                    "formula": "E1EDK01.MWSKZ * 100"
                }
            }
        ]
    }
    
    reverser = MappingReverser()
    reversed = reverser.reverse_mapping(test_project)
    
    print("✅ Original:")
    print(f"  Input: {test_project['inputSchema']['name']}")
    print(f"  Output: {test_project['outputSchema']['name']}")
    print(f"  Connections: {len(test_project['connections'])}")
    
    print("\n✅ Reversed:")
    print(f"  Input: {reversed['inputSchema']['name']}")
    print(f"  Output: {reversed['outputSchema']['name']}")
    print(f"  Connections: {len(reversed['connections'])}")
    
    print("\n📋 Report:")
    report = reverser.get_report()
    print(f"  Warnings: {report['total_warnings']}")
    for warn in report['warnings']:
        print(f"    - {warn['source']} → {warn['target']}: {warn['reason']}")
