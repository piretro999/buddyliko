#!/usr/bin/env python3
"""
Universal Schema Parser
Parses input/output schemas from: XSD, JSON Schema, IDOC definitions, sample files

Generates unified schema format for visual mapper
"""

import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class SchemaField:
    """Universal field representation"""
    id: str
    name: str
    path: str
    type: str  # string, number, date, boolean, object, array
    cardinality: str  # 0..1, 1..1, 0..N, 1..N
    description: str = ""
    parent: Optional[str] = None
    children: List[str] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
    
    def to_dict(self):
        return asdict(self)


class SchemaParser:
    """Parse various schema formats into unified format"""
    
    def __init__(self):
        self.fields: Dict[str, SchemaField] = {}
        self.root_fields: List[str] = []
    
    def parse_xsd(self, xsd_path: str) -> Dict[str, Any]:
        """Parse XSD schema"""
        tree = ET.parse(xsd_path)
        root = tree.getroot()
        
        # Remove namespace for easier parsing
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}')[1]
        
        # Find root element
        for element in root.findall('.//element'):
            if element.get('name'):
                self._parse_xsd_element(element, None)
        
        return self._build_schema_output()
    
    def _parse_xsd_element(self, element, parent_path):
        """Parse XSD element recursively"""
        name = element.get('name')
        element_type = element.get('type', 'string')
        min_occurs = element.get('minOccurs', '1')
        max_occurs = element.get('maxOccurs', '1')
        
        path = f"{parent_path}.{name}" if parent_path else name
        field_id = path.replace('.', '_')
        
        # Determine cardinality
        if max_occurs == 'unbounded':
            cardinality = f"{min_occurs}..N"
        else:
            cardinality = f"{min_occurs}..{max_occurs}"
        
        field = SchemaField(
            id=field_id,
            name=name,
            path=path,
            type=self._map_xsd_type(element_type),
            cardinality=cardinality,
            parent=parent_path
        )
        
        self.fields[field_id] = field
        
        if parent_path is None:
            self.root_fields.append(field_id)
        else:
            parent_id = parent_path.replace('.', '_')
            if parent_id in self.fields:
                self.fields[parent_id].children.append(field_id)
        
        # Parse child elements
        complex_type = element.find('.//complexType')
        if complex_type is not None:
            for child in complex_type.findall('.//element'):
                self._parse_xsd_element(child, path)
    
    def _map_xsd_type(self, xsd_type: str) -> str:
        """Map XSD types to universal types"""
        type_map = {
            'string': 'string',
            'int': 'number',
            'integer': 'number',
            'decimal': 'number',
            'double': 'number',
            'float': 'number',
            'date': 'date',
            'dateTime': 'datetime',
            'boolean': 'boolean',
            'bool': 'boolean'
        }
        
        for xsd, universal in type_map.items():
            if xsd in xsd_type.lower():
                return universal
        
        return 'string'
    
    def parse_json_schema(self, json_path: str) -> Dict[str, Any]:
        """Parse JSON Schema"""
        with open(json_path, 'r') as f:
            schema = json.load(f)
        
        if 'properties' in schema:
            self._parse_json_properties(schema['properties'], None, schema.get('required', []))
        
        return self._build_schema_output()
    
    def _parse_json_properties(self, properties: Dict, parent_path: Optional[str], required: List[str]):
        """Parse JSON schema properties recursively"""
        for name, prop in properties.items():
            path = f"{parent_path}.{name}" if parent_path else name
            field_id = path.replace('.', '_')
            
            prop_type = prop.get('type', 'string')
            is_required = name in required
            
            # Determine cardinality
            if prop_type == 'array':
                min_occurs = '1' if is_required else '0'
                cardinality = f"{min_occurs}..N"
                actual_type = prop.get('items', {}).get('type', 'object')
            else:
                cardinality = '1..1' if is_required else '0..1'
                actual_type = prop_type
            
            field = SchemaField(
                id=field_id,
                name=name,
                path=path,
                type=actual_type,
                cardinality=cardinality,
                description=prop.get('description', ''),
                parent=parent_path
            )
            
            self.fields[field_id] = field
            
            if parent_path is None:
                self.root_fields.append(field_id)
            else:
                parent_id = parent_path.replace('.', '_')
                if parent_id in self.fields:
                    self.fields[parent_id].children.append(field_id)
            
            # Parse nested properties
            if 'properties' in prop:
                self._parse_json_properties(
                    prop['properties'], 
                    path, 
                    prop.get('required', [])
                )
            elif prop_type == 'array' and 'items' in prop and 'properties' in prop['items']:
                self._parse_json_properties(
                    prop['items']['properties'],
                    path,
                    prop['items'].get('required', [])
                )
    
    def parse_sample_xml(self, xml_path: str, schema_name: str = "CustomXML") -> Dict[str, Any]:
        """Parse sample XML to infer schema"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        self._infer_from_xml_element(root, None)
        
        return self._build_schema_output(schema_name)
    
    def _infer_from_xml_element(self, element: ET.Element, parent_path: Optional[str]):
        """Infer schema from XML element"""
        # Remove namespace
        tag = element.tag
        if '}' in tag:
            tag = tag.split('}')[1]
        
        path = f"{parent_path}.{tag}" if parent_path else tag
        field_id = path.replace('.', '_')
        
        # Infer type from content
        inferred_type = 'string'
        if element.text and element.text.strip():
            text = element.text.strip()
            if text.isdigit():
                inferred_type = 'number'
            elif text.lower() in ('true', 'false'):
                inferred_type = 'boolean'
            elif self._looks_like_date(text):
                inferred_type = 'date'
        
        # Check if has children
        if len(element):
            inferred_type = 'object'
        
        field = SchemaField(
            id=field_id,
            name=tag,
            path=path,
            type=inferred_type,
            cardinality='1..1',  # Can't infer without multiple samples
            parent=parent_path
        )
        
        self.fields[field_id] = field
        
        if parent_path is None:
            self.root_fields.append(field_id)
        else:
            parent_id = parent_path.replace('.', '_')
            if parent_id in self.fields:
                self.fields[parent_id].children.append(field_id)
        
        # Parse children
        for child in element:
            self._infer_from_xml_element(child, path)
    
    def _looks_like_date(self, text: str) -> bool:
        """Check if text looks like a date"""
        import re
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{4}\d{2}\d{2}'
        ]
        return any(re.match(pattern, text) for pattern in date_patterns)
    
    def parse_idoc_definition(self, idoc_json: str) -> Dict[str, Any]:
        """Parse IDOC definition JSON"""
        with open(idoc_json, 'r') as f:
            idoc_def = json.load(f)
        
        idoc_type = idoc_def.get('idoc_type', 'IDOC')
        
        for segment in idoc_def.get('segments', []):
            segment_id = segment['segment_id']
            parent = segment.get('parent')
            
            # Create segment node
            seg_path = f"{parent}.{segment_id}" if parent else segment_id
            seg_field_id = seg_path.replace('.', '_')
            
            segment_field = SchemaField(
                id=seg_field_id,
                name=segment_id,
                path=seg_path,
                type='object',
                cardinality=f"{segment.get('min_occurs', 0)}..{segment.get('max_occurs', 'N')}",
                description=segment.get('technical_name', ''),
                parent=parent
            )
            
            self.fields[seg_field_id] = segment_field
            
            if parent is None:
                self.root_fields.append(seg_field_id)
            else:
                parent_id = parent.replace('.', '_')
                if parent_id in self.fields:
                    self.fields[parent_id].children.append(seg_field_id)
            
            # Add fields
            for field_def in segment.get('fields', []):
                field_path = f"{seg_path}.{field_def['name']}"
                field_id = field_path.replace('.', '_')
                
                field = SchemaField(
                    id=field_id,
                    name=field_def['name'],
                    path=field_path,
                    type=field_def.get('type', 'string'),
                    cardinality='1..1',
                    description=field_def.get('description', ''),
                    parent=seg_path
                )
                
                self.fields[field_id] = field
                segment_field.children.append(field_id)
        
        return self._build_schema_output(idoc_type)
    
    def _build_schema_output(self, name: str = "Schema") -> Dict[str, Any]:
        """Build final schema output"""
        return {
            'name': name,
            'fields': {fid: field.to_dict() for fid, field in self.fields.items()},
            'root_fields': self.root_fields,
            'field_count': len(self.fields)
        }
    
    def to_tree_structure(self) -> List[Dict]:
        """Convert to tree structure for UI"""
        def build_node(field_id: str) -> Dict:
            field = self.fields[field_id]
            node = {
                'id': field.id,
                'label': field.name,
                'path': field.path,
                'type': field.type,
                'cardinality': field.cardinality,
                'description': field.description,
                'children': []
            }
            
            for child_id in field.children:
                node['children'].append(build_node(child_id))
            
            return node
        
        return [build_node(root_id) for root_id in self.root_fields]


# Quick test
if __name__ == '__main__':
    parser = SchemaParser()
    
    # Test with sample
    print("Schema Parser initialized")
    print("Ready to parse: XSD, JSON Schema, IDOC JSON, Sample XML")
