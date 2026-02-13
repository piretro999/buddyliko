#!/usr/bin/env python3
"""
Schema Editor Engine
Build and modify schema structures visually for CSV/XML/JSON/Excel
"""

import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class FieldType(Enum):
    """Field data types"""
    STRING = "string"
    NUMBER = "number"
    DATE = "date"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"


class SchemaFormat(Enum):
    """Output format types"""
    CSV = "csv"
    XML = "xml"
    JSON = "json"
    EXCEL = "excel"
    FLAT = "flat"  # Fixed-width like IDOC


@dataclass
class SchemaField:
    """Field definition in schema"""
    id: str
    name: str
    field_type: str
    path: str  # Hierarchical path: Invoice.Lines.Line.Price
    
    # Optional attributes
    description: str = ""
    required: bool = False
    cardinality: str = "0..1"  # 0..1, 1..1, 0..N, 1..N
    default_value: str = ""
    
    # Format-specific
    xml_path: str = ""  # XPath for XML
    json_path: str = ""  # JSONPath for JSON
    offset: int = 0  # Position for fixed-width
    length: int = 0  # Length for fixed-width
    excel_column: str = ""  # Excel column (A, B, C...)
    
    # Validation
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    pattern: str = ""  # Regex pattern
    enum_values: List[str] = None
    
    # Children (for nested structures)
    children: List[str] = None  # List of child field IDs
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
        if self.enum_values is None:
            self.enum_values = []


class SchemaEditor:
    """Visual schema builder and editor"""
    
    def __init__(self):
        self.schema = {
            'name': 'New Schema',
            'format': SchemaFormat.CSV.value,
            'version': '1.0',
            'fields': {},
            'root_fields': [],
            'field_count': 0
        }
    
    def create_schema(self, name: str, format_type: str) -> Dict:
        """Create new empty schema"""
        self.schema = {
            'name': name,
            'format': format_type,
            'version': '1.0',
            'fields': {},
            'root_fields': [],
            'field_count': 0
        }
        return self.schema
    
    def add_field(self, name: str, field_type: str, 
                  parent_path: Optional[str] = None,
                  **kwargs) -> SchemaField:
        """
        Add new field to schema
        
        Args:
            name: Field name
            field_type: string, number, date, boolean, array, object
            parent_path: Parent path (None for root)
            **kwargs: Additional field properties
        
        Returns:
            Created SchemaField
        """
        # Generate path
        if parent_path:
            path = f"{parent_path}.{name}"
        else:
            path = name
        
        # Generate unique ID
        field_id = path.replace('.', '_')
        
        # Check if exists
        if field_id in self.schema['fields']:
            raise ValueError(f"Field {field_id} already exists")
        
        # Create field
        field = SchemaField(
            id=field_id,
            name=name,
            field_type=field_type,
            path=path,
            **kwargs
        )
        
        # Auto-generate format-specific paths
        if self.schema['format'] == SchemaFormat.XML.value:
            field.xml_path = self._generate_xpath(path)
        elif self.schema['format'] == SchemaFormat.JSON.value:
            field.json_path = self._generate_jsonpath(path)
        
        # Add to schema
        self.schema['fields'][field_id] = asdict(field)
        
        # Update parent's children or root
        if parent_path:
            parent_id = parent_path.replace('.', '_')
            if parent_id in self.schema['fields']:
                self.schema['fields'][parent_id]['children'].append(field_id)
        else:
            self.schema['root_fields'].append(field_id)
        
        self.schema['field_count'] += 1
        
        return field
    
    def remove_field(self, field_id: str) -> bool:
        """Remove field and all its children"""
        if field_id not in self.schema['fields']:
            return False
        
        field = self.schema['fields'][field_id]
        
        # Remove all children recursively
        for child_id in field['children']:
            self.remove_field(child_id)
        
        # Remove from parent's children
        parent_path = '.'.join(field['path'].split('.')[:-1])
        if parent_path:
            parent_id = parent_path.replace('.', '_')
            if parent_id in self.schema['fields']:
                self.schema['fields'][parent_id]['children'].remove(field_id)
        else:
            if field_id in self.schema['root_fields']:
                self.schema['root_fields'].remove(field_id)
        
        # Remove field
        del self.schema['fields'][field_id]
        self.schema['field_count'] -= 1
        
        return True
    
    def update_field(self, field_id: str, **updates) -> bool:
        """Update field properties"""
        if field_id not in self.schema['fields']:
            return False
        
        field = self.schema['fields'][field_id]
        
        # Update allowed properties
        for key, value in updates.items():
            if key in field and key not in ['id', 'path', 'children']:
                field[key] = value
        
        return True
    
    def reorder_fields(self, parent_id: Optional[str], 
                      new_order: List[str]) -> bool:
        """Reorder children of a parent (or root fields)"""
        if parent_id:
            if parent_id not in self.schema['fields']:
                return False
            self.schema['fields'][parent_id]['children'] = new_order
        else:
            self.schema['root_fields'] = new_order
        
        return True
    
    def move_field(self, field_id: str, new_parent_id: Optional[str]) -> bool:
        """Move field to new parent"""
        if field_id not in self.schema['fields']:
            return False
        
        field = self.schema['fields'][field_id]
        old_path = field['path']
        
        # Remove from old parent
        old_parent_path = '.'.join(old_path.split('.')[:-1])
        if old_parent_path:
            old_parent_id = old_parent_path.replace('.', '_')
            self.schema['fields'][old_parent_id]['children'].remove(field_id)
        else:
            self.schema['root_fields'].remove(field_id)
        
        # Update path
        if new_parent_id:
            new_parent = self.schema['fields'][new_parent_id]
            new_path = f"{new_parent['path']}.{field['name']}"
            
            # Add to new parent
            new_parent['children'].append(field_id)
        else:
            new_path = field['name']
            self.schema['root_fields'].append(field_id)
        
        # Update field and all descendants' paths
        self._update_paths_recursive(field_id, old_path, new_path)
        
        return True
    
    def _update_paths_recursive(self, field_id: str, old_path: str, new_path: str):
        """Update path for field and all children"""
        field = self.schema['fields'][field_id]
        field['path'] = new_path
        
        # Update ID if needed
        new_id = new_path.replace('.', '_')
        if new_id != field_id:
            # Update references
            self._rename_field_id(field_id, new_id)
            field_id = new_id
        
        # Regenerate format-specific paths
        if self.schema['format'] == SchemaFormat.XML.value:
            field['xml_path'] = self._generate_xpath(new_path)
        elif self.schema['format'] == SchemaFormat.JSON.value:
            field['json_path'] = self._generate_jsonpath(new_path)
        
        # Update children
        for child_id in field['children']:
            child = self.schema['fields'][child_id]
            old_child_path = child['path']
            new_child_path = old_child_path.replace(old_path, new_path, 1)
            self._update_paths_recursive(child_id, old_child_path, new_child_path)
    
    def _rename_field_id(self, old_id: str, new_id: str):
        """Rename field ID throughout schema"""
        # Move field
        self.schema['fields'][new_id] = self.schema['fields'].pop(old_id)
        
        # Update root_fields
        if old_id in self.schema['root_fields']:
            idx = self.schema['root_fields'].index(old_id)
            self.schema['root_fields'][idx] = new_id
        
        # Update all parent references
        for field in self.schema['fields'].values():
            if old_id in field['children']:
                idx = field['children'].index(old_id)
                field['children'][idx] = new_id
    
    def import_from_xsd(self, xsd_content: str) -> Dict:
        """Import schema from XSD (XML Schema Definition)"""
        # TODO: Parse XSD and create fields
        # This is complex - would need lxml
        raise NotImplementedError("XSD import not yet implemented")
    
    def import_from_json_schema(self, json_schema: Dict) -> Dict:
        """Import from JSON Schema"""
        # Parse JSON Schema format
        if 'properties' in json_schema:
            for prop_name, prop_def in json_schema['properties'].items():
                field_type = self._json_type_to_field_type(prop_def.get('type', 'string'))
                required = prop_name in json_schema.get('required', [])
                
                self.add_field(
                    name=prop_name,
                    field_type=field_type,
                    description=prop_def.get('description', ''),
                    required=required
                )
        
        return self.schema
    
    def import_from_sample_csv(self, csv_header: str, delimiter: str = ',') -> Dict:
        """Import schema from CSV header"""
        headers = csv_header.strip().split(delimiter)
        
        for header in headers:
            header = header.strip()
            if header:
                self.add_field(
                    name=header,
                    field_type=FieldType.STRING.value,
                    description=f"Column {header}"
                )
        
        return self.schema
    
    def export_to_csv_schema(self) -> str:
        """Export schema as CSV definition file"""
        lines = []
        
        # Header
        lines.append("campo,business_term,spiegazione,obbligatorio,numerosità,condizionalità,calcolo,offset,lunghezza,xmlpath,json_path")
        
        # Fields
        for field_id in self._get_fields_in_order():
            field = self.schema['fields'][field_id]
            
            line = [
                field['path'],
                field['name'],
                field['description'],
                'SI' if field['required'] else 'NO',
                field['cardinality'],
                '',  # condizionalità
                '',  # calcolo
                str(field['offset']) if field['offset'] else '',
                str(field['length']) if field['length'] else '',
                field['xml_path'],
                field['json_path']
            ]
            
            lines.append(','.join(f'"{v}"' if ',' in str(v) else str(v) for v in line))
        
        return '\n'.join(lines)
    
    def export_sample_xml(self) -> str:
        """Generate sample XML from schema"""
        if self.schema['format'] != SchemaFormat.XML.value:
            raise ValueError("Schema must be XML format")
        
        xml_lines = ['<?xml version="1.0" encoding="UTF-8"?>']
        
        def build_element(field_id: str, indent: int = 0) -> List[str]:
            field = self.schema['fields'][field_id]
            ind = '  ' * indent
            lines = []
            
            if field['field_type'] == FieldType.ARRAY.value:
                # Array container
                for child_id in field['children']:
                    lines.extend(build_element(child_id, indent))
            elif field['field_type'] == FieldType.OBJECT.value:
                # Object with children
                lines.append(f'{ind}<{field["name"]}>')
                for child_id in field['children']:
                    lines.extend(build_element(child_id, indent + 1))
                lines.append(f'{ind}</{field["name"]}>')
            else:
                # Simple field
                sample_val = self._get_sample_value(field)
                lines.append(f'{ind}<{field["name"]}>{sample_val}</{field["name"]}>')
            
            return lines
        
        # Build from root
        for root_id in self.schema['root_fields']:
            xml_lines.extend(build_element(root_id, 0))
        
        return '\n'.join(xml_lines)
    
    def export_sample_json(self) -> str:
        """Generate sample JSON from schema"""
        def build_object(field_ids: List[str]) -> Dict:
            obj = {}
            for field_id in field_ids:
                field = self.schema['fields'][field_id]
                
                if field['field_type'] == FieldType.ARRAY.value:
                    # Array of objects
                    if field['children']:
                        obj[field['name']] = [build_object(field['children'])]
                    else:
                        obj[field['name']] = []
                elif field['field_type'] == FieldType.OBJECT.value:
                    obj[field['name']] = build_object(field['children'])
                else:
                    obj[field['name']] = self._get_sample_value(field)
            
            return obj
        
        result = build_object(self.schema['root_fields'])
        return json.dumps(result, indent=2)
    
    def export_sample_csv(self) -> str:
        """Generate sample CSV from schema"""
        # Get all leaf fields (no children)
        leaf_fields = [
            field for field in self.schema['fields'].values()
            if not field['children']
        ]
        
        # Header
        headers = [field['name'] for field in leaf_fields]
        
        # Sample row
        values = [self._get_sample_value(field) for field in leaf_fields]
        
        return ','.join(headers) + '\n' + ','.join(str(v) for v in values)
    
    def _get_fields_in_order(self) -> List[str]:
        """Get all field IDs in hierarchical order"""
        result = []
        
        def traverse(field_ids: List[str]):
            for field_id in field_ids:
                result.append(field_id)
                field = self.schema['fields'][field_id]
                if field['children']:
                    traverse(field['children'])
        
        traverse(self.schema['root_fields'])
        return result
    
    def _generate_xpath(self, path: str) -> str:
        """Generate XPath from hierarchical path"""
        parts = path.split('.')
        return '/' + '/'.join(parts)
    
    def _generate_jsonpath(self, path: str) -> str:
        """Generate JSONPath from hierarchical path"""
        parts = path.split('.')
        return '$.' + '.'.join(parts)
    
    def _json_type_to_field_type(self, json_type: str) -> str:
        """Convert JSON Schema type to FieldType"""
        mapping = {
            'string': FieldType.STRING.value,
            'number': FieldType.NUMBER.value,
            'integer': FieldType.NUMBER.value,
            'boolean': FieldType.BOOLEAN.value,
            'array': FieldType.ARRAY.value,
            'object': FieldType.OBJECT.value
        }
        return mapping.get(json_type, FieldType.STRING.value)
    
    def _get_sample_value(self, field: Dict) -> Any:
        """Generate sample value for field"""
        if field['default_value']:
            return field['default_value']
        
        if field['field_type'] == FieldType.NUMBER.value:
            return 0
        elif field['field_type'] == FieldType.DATE.value:
            return '2024-01-01'
        elif field['field_type'] == FieldType.BOOLEAN.value:
            return True
        elif field['field_type'] == FieldType.ARRAY.value:
            return []
        elif field['field_type'] == FieldType.OBJECT.value:
            return {}
        else:
            return f'Sample {field["name"]}'
    
    def get_tree_structure(self) -> List[Dict]:
        """Get schema as tree structure for UI"""
        def build_node(field_id: str) -> Dict:
            field = self.schema['fields'][field_id]
            
            node = {
                'id': field_id,
                'name': field['name'],
                'type': field['field_type'],
                'path': field['path'],
                'description': field['description'],
                'required': field['required'],
                'children': []
            }
            
            for child_id in field['children']:
                node['children'].append(build_node(child_id))
            
            return node
        
        tree = []
        for root_id in self.schema['root_fields']:
            tree.append(build_node(root_id))
        
        return tree
    
    def validate_schema(self) -> List[str]:
        """Validate schema completeness"""
        errors = []
        
        if not self.schema['name']:
            errors.append("Schema name is required")
        
        if self.schema['field_count'] == 0:
            errors.append("Schema must have at least one field")
        
        # Check for circular references
        visited = set()
        def check_circular(field_id: str, ancestors: set):
            if field_id in ancestors:
                errors.append(f"Circular reference detected: {field_id}")
                return
            
            if field_id in visited:
                return
            
            visited.add(field_id)
            field = self.schema['fields'].get(field_id)
            if field:
                for child_id in field['children']:
                    check_circular(child_id, ancestors | {field_id})
        
        for root_id in self.schema['root_fields']:
            check_circular(root_id, set())
        
        return errors
