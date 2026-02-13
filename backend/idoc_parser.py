#!/usr/bin/env python3
"""
IDOC Parser - Elastic parser for SAP IDOC files
Supports positional formats, automatic segment detection, hierarchical structures

Usage:
    parser = IDOCParser()
    parser.load_definition('INVOIC02.json')  # Or auto-detect
    data = parser.parse('idoc_file.txt')
"""

import re
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class IDOCField:
    """IDOC field definition"""
    name: str
    offset: int
    length: int
    type: str  # char, num, date, time
    description: str = ""
    
    def extract(self, line: str) -> str:
        """Extract field value from line"""
        value = line[self.offset:self.offset + self.length].strip()
        return value


@dataclass
class IDOCSegment:
    """IDOC segment definition"""
    segment_id: str
    technical_name: str
    min_occurs: int = 0
    max_occurs: int = 999999
    parent: Optional[str] = None
    fields: List[IDOCField] = field(default_factory=list)
    
    def matches(self, line: str) -> bool:
        """Check if line matches this segment"""
        # Segment ID is typically first field (positions 0-8)
        return line[:8].strip() == self.segment_id
    
    def parse_line(self, line: str) -> Dict[str, Any]:
        """Parse line into field dict"""
        data = {
            'segment_id': self.segment_id,
            'technical_name': self.technical_name,
            'fields': {}
        }
        
        for field in self.fields:
            data['fields'][field.name] = field.extract(line)
        
        return data


class IDOCDefinition:
    """Complete IDOC type definition"""
    
    def __init__(self, idoc_type: str):
        self.idoc_type = idoc_type
        self.segments: Dict[str, IDOCSegment] = {}
        self.hierarchy: Dict[str, List[str]] = {}  # parent -> [children]
    
    def add_segment(self, segment: IDOCSegment):
        """Add segment definition"""
        self.segments[segment.segment_id] = segment
        
        # Build hierarchy
        if segment.parent:
            if segment.parent not in self.hierarchy:
                self.hierarchy[segment.parent] = []
            self.hierarchy[segment.parent].append(segment.segment_id)
    
    def get_segment(self, segment_id: str) -> Optional[IDOCSegment]:
        """Get segment by ID"""
        return self.segments.get(segment_id)
    
    @classmethod
    def from_json(cls, json_path: str) -> 'IDOCDefinition':
        """Load definition from JSON file"""
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        definition = cls(data['idoc_type'])
        
        for seg_data in data['segments']:
            fields = [
                IDOCField(
                    name=f['name'],
                    offset=f['offset'],
                    length=f['length'],
                    type=f.get('type', 'char'),
                    description=f.get('description', '')
                )
                for f in seg_data.get('fields', [])
            ]
            
            segment = IDOCSegment(
                segment_id=seg_data['segment_id'],
                technical_name=seg_data['technical_name'],
                min_occurs=seg_data.get('min_occurs', 0),
                max_occurs=seg_data.get('max_occurs', 999999),
                parent=seg_data.get('parent'),
                fields=fields
            )
            
            definition.add_segment(segment)
        
        return definition
    
    def to_json(self, output_path: str):
        """Save definition to JSON file"""
        data = {
            'idoc_type': self.idoc_type,
            'segments': [
                {
                    'segment_id': seg.segment_id,
                    'technical_name': seg.technical_name,
                    'min_occurs': seg.min_occurs,
                    'max_occurs': seg.max_occurs,
                    'parent': seg.parent,
                    'fields': [
                        {
                            'name': f.name,
                            'offset': f.offset,
                            'length': f.length,
                            'type': f.type,
                            'description': f.description
                        }
                        for f in seg.fields
                    ]
                }
                for seg in self.segments.values()
            ]
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)


class IDOCParser:
    """Parse IDOC files"""
    
    def __init__(self, definition: Optional[IDOCDefinition] = None):
        self.definition = definition
        self.auto_detect = definition is None
    
    def load_definition(self, json_path: str):
        """Load IDOC definition"""
        self.definition = IDOCDefinition.from_json(json_path)
        self.auto_detect = False
    
    def parse_file(self, idoc_path: str) -> Dict[str, Any]:
        """Parse IDOC file"""
        with open(idoc_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        return self.parse_lines(lines)
    
    def parse_lines(self, lines: List[str]) -> Dict[str, Any]:
        """Parse IDOC from lines"""
        if self.auto_detect:
            self._auto_detect_structure(lines)
        
        result = {
            'idoc_type': self.definition.idoc_type if self.definition else 'UNKNOWN',
            'segments': [],
            'hierarchy': {}
        }
        
        current_hierarchy = []
        
        for line_num, line in enumerate(lines, 1):
            if not line.strip():
                continue
            
            # Identify segment
            segment_id = line[:8].strip()
            
            if self.definition and segment_id in self.definition.segments:
                segment_def = self.definition.segments[segment_id]
                parsed_segment = segment_def.parse_line(line)
                parsed_segment['line_number'] = line_num
                
                # Track hierarchy
                if segment_def.parent:
                    # Find parent in current hierarchy
                    parent_found = False
                    for i in range(len(current_hierarchy) - 1, -1, -1):
                        if current_hierarchy[i]['segment_id'] == segment_def.parent:
                            parent_found = True
                            current_hierarchy = current_hierarchy[:i+1]
                            break
                    
                    if not parent_found:
                        current_hierarchy = []
                
                current_hierarchy.append(parsed_segment)
                result['segments'].append({
                    'data': parsed_segment,
                    'hierarchy_level': len(current_hierarchy) - 1
                })
            else:
                # Unknown segment - try to parse anyway
                result['segments'].append({
                    'segment_id': segment_id,
                    'line_number': line_num,
                    'raw': line.strip(),
                    'warning': 'Unknown segment type'
                })
        
        return result
    
    def _auto_detect_structure(self, lines: List[str]):
        """Auto-detect IDOC structure from file"""
        # Analyze patterns
        segment_patterns = {}
        
        for line in lines[:100]:  # Sample first 100 lines
            if not line.strip():
                continue
            
            segment_id = line[:8].strip()
            if segment_id not in segment_patterns:
                segment_patterns[segment_id] = {
                    'count': 0,
                    'sample_line': line,
                    'length': len(line.rstrip())
                }
            segment_patterns[segment_id]['count'] += 1
        
        # Create basic definition
        self.definition = IDOCDefinition('AUTO_DETECTED')
        
        for seg_id, info in segment_patterns.items():
            # Auto-detect fields (basic heuristic)
            fields = self._detect_fields(info['sample_line'])
            
            segment = IDOCSegment(
                segment_id=seg_id,
                technical_name=seg_id,
                fields=fields
            )
            
            self.definition.add_segment(segment)
    
    def _detect_fields(self, sample_line: str) -> List[IDOCField]:
        """Auto-detect field boundaries (basic heuristic)"""
        # Skip segment ID (first 8 chars)
        data_part = sample_line[8:]
        
        fields = []
        current_offset = 8
        
        # Detect fields by spaces or fixed patterns
        # This is simplified - real implementation would be more sophisticated
        parts = re.split(r'(\s{2,})', data_part)
        
        for i, part in enumerate(parts):
            if part.strip():
                fields.append(IDOCField(
                    name=f'FIELD_{i+1:02d}',
                    offset=current_offset,
                    length=len(part),
                    type='char'
                ))
            current_offset += len(part)
        
        return fields
    
    def generate_mapping_schema(self) -> Dict[str, Any]:
        """Generate schema for visual mapper"""
        if not self.definition:
            return {}
        
        schema = {
            'type': 'idoc',
            'idoc_type': self.definition.idoc_type,
            'nodes': []
        }
        
        for segment in self.definition.segments.values():
            node = {
                'id': f'{segment.segment_id}',
                'type': 'segment',
                'label': f'{segment.segment_id} - {segment.technical_name}',
                'cardinality': f'{segment.min_occurs}..{segment.max_occurs}',
                'fields': []
            }
            
            for field in segment.fields:
                node['fields'].append({
                    'id': f'{segment.segment_id}.{field.name}',
                    'name': field.name,
                    'type': field.type,
                    'description': field.description,
                    'path': f'{segment.segment_id}.{field.name}'
                })
            
            schema['nodes'].append(node)
        
        return schema


# ============================================================================
# IDOC DEFINITION BUILDER
# ============================================================================

class IDOCDefinitionBuilder:
    """Interactive builder for IDOC definitions"""
    
    def __init__(self):
        self.definition = None
    
    def create_from_sample(self, sample_file: str, idoc_type: str) -> IDOCDefinition:
        """Create definition by analyzing sample file"""
        parser = IDOCParser()
        
        with open(sample_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        parser._auto_detect_structure(lines)
        parser.definition.idoc_type = idoc_type
        
        return parser.definition
    
    def refine_definition(self, definition: IDOCDefinition, 
                         segment_id: str, 
                         field_updates: Dict[str, Any]) -> IDOCDefinition:
        """Refine auto-detected definition with manual corrections"""
        segment = definition.get_segment(segment_id)
        
        if not segment:
            return definition
        
        # Update field definitions
        for field_name, updates in field_updates.items():
            for field in segment.fields:
                if field.name == field_name:
                    for key, value in updates.items():
                        setattr(field, key, value)
        
        return definition


# ============================================================================
# EXAMPLE INVOIC02 DEFINITION
# ============================================================================

def create_invoic02_definition() -> IDOCDefinition:
    """Create example INVOIC02 definition"""
    definition = IDOCDefinition('INVOIC02')
    
    # EDI_DC40 - Control Record
    definition.add_segment(IDOCSegment(
        segment_id='EDI_DC40',
        technical_name='IDOC_CONTROL',
        fields=[
            IDOCField('TABNAM', 0, 10, 'char', 'Table name'),
            IDOCField('MANDT', 10, 3, 'char', 'Client'),
            IDOCField('DOCNUM', 13, 16, 'char', 'Document number'),
            IDOCField('IDOCTYP', 29, 30, 'char', 'IDOC type'),
            IDOCField('MESTYP', 59, 30, 'char', 'Message type'),
        ]
    ))
    
    # E1EDK01 - Document Header
    definition.add_segment(IDOCSegment(
        segment_id='E1EDK01',
        technical_name='DOC_HEADER',
        parent='EDI_DC40',
        fields=[
            IDOCField('BELNR', 8, 35, 'char', 'Document number'),
            IDOCField('DATUM', 43, 8, 'date', 'Document date'),
            IDOCField('WKURS', 51, 12, 'num', 'Exchange rate'),
        ]
    ))
    
    # E1EDP01 - Item Data
    definition.add_segment(IDOCSegment(
        segment_id='E1EDP01',
        technical_name='ITEM_DATA',
        parent='E1EDK01',
        max_occurs=999999,
        fields=[
            IDOCField('POSEX', 8, 6, 'char', 'Item number'),
            IDOCField('MENGE', 14, 15, 'num', 'Quantity'),
            IDOCField('MENEE', 29, 3, 'char', 'Unit'),
            IDOCField('NTGEW', 32, 18, 'num', 'Net weight'),
        ]
    ))
    
    return definition


# ============================================================================
# MAIN / TESTING
# ============================================================================

if __name__ == '__main__':
    # Create example definition
    definition = create_invoic02_definition()
    definition.to_json('invoic02_definition.json')
    
    print("✓ Created INVOIC02 definition")
    print(f"  Segments: {len(definition.segments)}")
    
    # Example: Parse with definition
    # parser = IDOCParser(definition)
    # data = parser.parse_file('example.idoc')
    
    # Example: Auto-detect
    # parser = IDOCParser()
    # data = parser.parse_file('unknown.idoc')
    
    # Generate mapping schema
    schema = IDOCParser(definition).generate_mapping_schema()
    print(f"\n✓ Generated mapping schema with {len(schema['nodes'])} nodes")
