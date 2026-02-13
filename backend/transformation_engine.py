#!/usr/bin/env python3
"""
Transformation Engine - Complete Data Transformation Pipeline
Supports: XSD validation, Schematron validation, Business rules, Multi-format I/O

Features:
- Input validation (XSD + Schematron)
- Data transformation (mapping rules)
- Output validation (XSD + Schematron)
- Multiple input sources (API, SFTP, Queue)
- Multiple output targets (API, SFTP, Queue)
"""

import os
import json
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
import re


# ===========================================================================
# XSD VALIDATION
# ===========================================================================

class XSDValidator:
    """XML Schema (XSD) validation"""
    
    def __init__(self, xsd_path: Optional[str] = None):
        self.xsd_path = xsd_path
        self.schema = None
        
        if xsd_path and os.path.exists(xsd_path):
            try:
                from lxml import etree
                self.lxml_available = True
                schema_doc = etree.parse(xsd_path)
                self.schema = etree.XMLSchema(schema_doc)
                print(f"✅ XSD loaded: {xsd_path}")
            except ImportError:
                self.lxml_available = False
                print("⚠️ lxml not installed. Install with: pip install lxml")
            except Exception as e:
                print(f"⚠️ XSD load error: {e}")
    
    def validate(self, xml_content: str) -> Tuple[bool, List[str]]:
        """
        Validate XML against XSD
        Returns: (is_valid, error_messages)
        """
        if not self.lxml_available or not self.schema:
            return True, ["XSD validation skipped (schema not loaded)"]
        
        try:
            from lxml import etree
            doc = etree.fromstring(xml_content.encode('utf-8'))
            
            if self.schema.validate(doc):
                return True, []
            else:
                errors = [str(e) for e in self.schema.error_log]
                return False, errors
        
        except Exception as e:
            return False, [f"Validation error: {str(e)}"]
    
    def validate_file(self, xml_file_path: str) -> Tuple[bool, List[str]]:
        """Validate XML file against XSD"""
        with open(xml_file_path, 'r', encoding='utf-8') as f:
            return self.validate(f.read())


# ===========================================================================
# SCHEMATRON VALIDATION
# ===========================================================================

class SchematronValidator:
    """Schematron business rules validation"""
    
    def __init__(self, schematron_path: Optional[str] = None):
        self.schematron_path = schematron_path
        self.rules = []
        
        if schematron_path and os.path.exists(schematron_path):
            self._load_schematron(schematron_path)
    
    def _load_schematron(self, path: str):
        """Load Schematron rules"""
        try:
            from lxml import etree, isoschematron
            
            schematron_doc = etree.parse(path)
            self.schematron = isoschematron.Schematron(schematron_doc)
            print(f"✅ Schematron loaded: {path}")
        
        except ImportError:
            print("⚠️ lxml not installed. Install with: pip install lxml")
        except Exception as e:
            print(f"⚠️ Schematron load error: {e}")
    
    def validate(self, xml_content: str) -> Tuple[bool, List[str]]:
        """
        Validate XML against Schematron rules
        Returns: (is_valid, error_messages)
        """
        if not hasattr(self, 'schematron'):
            return True, ["Schematron validation skipped"]
        
        try:
            from lxml import etree
            doc = etree.fromstring(xml_content.encode('utf-8'))
            
            if self.schematron.validate(doc):
                return True, []
            else:
                errors = []
                for error in self.schematron.error_log:
                    errors.append(f"Line {error.line}: {error.message}")
                return False, errors
        
        except Exception as e:
            return False, [f"Schematron validation error: {str(e)}"]


# ===========================================================================
# BUSINESS RULES VALIDATOR
# ===========================================================================

class BusinessRulesValidator:
    """Custom business rules validation (Python-based)"""
    
    def __init__(self):
        self.rules = []
    
    def add_rule(self, rule_func, description: str):
        """Add custom validation rule"""
        self.rules.append({
            'function': rule_func,
            'description': description
        })
    
    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        """
        Run all business rules
        Returns: (all_valid, error_messages)
        """
        errors = []
        
        for rule in self.rules:
            try:
                is_valid, message = rule['function'](data)
                if not is_valid:
                    errors.append(f"{rule['description']}: {message}")
            except Exception as e:
                errors.append(f"{rule['description']}: Error - {str(e)}")
        
        return len(errors) == 0, errors


# ===========================================================================
# TRANSFORMATION ENGINE
# ===========================================================================

@dataclass
class TransformationResult:
    """Result of transformation"""
    success: bool
    output_content: Optional[str]
    output_format: str
    validation_errors: List[str]
    transformation_errors: List[str]
    warnings: List[str]
    metadata: Dict[str, Any]


class TransformationEngine:
    """Main transformation engine"""
    
    def __init__(self, 
                 input_xsd: Optional[str] = None,
                 output_xsd: Optional[str] = None,
                 input_schematron: Optional[str] = None,
                 output_schematron: Optional[str] = None):
        
        self.input_xsd_validator = XSDValidator(input_xsd) if input_xsd else None
        self.output_xsd_validator = XSDValidator(output_xsd) if output_xsd else None
        self.input_schematron_validator = SchematronValidator(input_schematron) if input_schematron else None
        self.output_schematron_validator = SchematronValidator(output_schematron) if output_schematron else None
        self.business_rules = BusinessRulesValidator()
    
    def transform(self,
                  input_content: str,
                  input_format: str,
                  output_format: str,
                  mapping_rules: Dict,
                  validate_input: bool = True,
                  validate_output: bool = True) -> TransformationResult:
        """
        Complete transformation pipeline
        
        Args:
            input_content: Input file content
            input_format: xml, json, csv, edi
            output_format: xml, json, csv
            mapping_rules: Transformation rules from visual mapper
            validate_input: Run input validation
            validate_output: Run output validation
        
        Returns:
            TransformationResult with output or errors
        """
        
        result = TransformationResult(
            success=False,
            output_content=None,
            output_format=output_format,
            validation_errors=[],
            transformation_errors=[],
            warnings=[],
            metadata={
                'input_format': input_format,
                'output_format': output_format,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        # STEP 1: Input Validation
        if validate_input:
            validation_ok, errors = self._validate_input(input_content, input_format)
            if not validation_ok:
                result.validation_errors.extend(errors)
                return result
        
        # STEP 2: Parse Input
        try:
            parsed_data = self._parse_input(input_content, input_format)
        except Exception as e:
            result.transformation_errors.append(f"Parse error: {str(e)}")
            return result
        
        # STEP 3: Apply Transformations
        try:
            transformed_data = self._apply_transformations(parsed_data, mapping_rules)
        except Exception as e:
            result.transformation_errors.append(f"Transformation error: {str(e)}")
            return result
        
        # STEP 4: Generate Output
        try:
            output_content = self._generate_output(transformed_data, output_format, mapping_rules)
        except Exception as e:
            result.transformation_errors.append(f"Output generation error: {str(e)}")
            return result
        
        # STEP 5: Output Validation
        if validate_output:
            validation_ok, errors = self._validate_output(output_content, output_format)
            if not validation_ok:
                result.validation_errors.extend(errors)
                # Still return output but mark as invalid
                result.output_content = output_content
                return result
        
        # SUCCESS
        result.success = True
        result.output_content = output_content
        return result
    
    def _validate_input(self, content: str, format_type: str) -> Tuple[bool, List[str]]:
        """Validate input with XSD and Schematron"""
        all_errors = []
        
        if format_type == 'xml':
            # XSD validation
            if self.input_xsd_validator:
                valid, errors = self.input_xsd_validator.validate(content)
                if not valid:
                    all_errors.extend([f"XSD: {e}" for e in errors])
            
            # Schematron validation
            if self.input_schematron_validator:
                valid, errors = self.input_schematron_validator.validate(content)
                if not valid:
                    all_errors.extend([f"Schematron: {e}" for e in errors])
        
        return len(all_errors) == 0, all_errors
    
    def _validate_output(self, content: str, format_type: str) -> Tuple[bool, List[str]]:
        """Validate output with XSD and Schematron"""
        all_errors = []
        
        if format_type == 'xml':
            # XSD validation
            if self.output_xsd_validator:
                valid, errors = self.output_xsd_validator.validate(content)
                if not valid:
                    all_errors.extend([f"XSD: {e}" for e in errors])
            
            # Schematron validation
            if self.output_schematron_validator:
                valid, errors = self.output_schematron_validator.validate(content)
                if not valid:
                    all_errors.extend([f"Schematron: {e}" for e in errors])
        
        # Business rules
        try:
            # Parse for business rules check
            if format_type == 'xml':
                parsed = self._parse_xml_to_dict(content)
            elif format_type == 'json':
                parsed = json.loads(content)
            else:
                parsed = {}
            
            valid, errors = self.business_rules.validate(parsed)
            if not valid:
                all_errors.extend([f"Business Rule: {e}" for e in errors])
        except:
            pass
        
        return len(all_errors) == 0, all_errors
    
    def _parse_input(self, content: str, format_type: str) -> Dict:
        """Parse input to internal dictionary format"""
        if format_type == 'xml':
            return self._parse_xml_to_dict(content)
        elif format_type == 'json':
            return json.loads(content)
        elif format_type == 'csv':
            return self._parse_csv_to_dict(content)
        else:
            raise ValueError(f"Unsupported input format: {format_type}")
    
    def _parse_xml_to_dict(self, xml_content: str) -> Dict:
        """Parse XML to dictionary"""
        root = ET.fromstring(xml_content)
        
        def element_to_dict(elem):
            result = {}
            
            # Attributes
            if elem.attrib:
                result['@attributes'] = elem.attrib
            
            # Text content
            if elem.text and elem.text.strip():
                if len(elem) == 0:  # Leaf node
                    return elem.text.strip()
                else:
                    result['#text'] = elem.text.strip()
            
            # Children
            for child in elem:
                child_data = element_to_dict(child)
                
                # Remove namespace from tag
                tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                
                if tag in result:
                    # Multiple children with same tag -> array
                    if not isinstance(result[tag], list):
                        result[tag] = [result[tag]]
                    result[tag].append(child_data)
                else:
                    result[tag] = child_data
            
            return result if result else None
        
        # Get root tag name
        root_tag = root.tag.split('}')[-1] if '}' in root.tag else root.tag
        
        return {root_tag: element_to_dict(root)}
    
    def _parse_csv_to_dict(self, csv_content: str) -> Dict:
        """Parse CSV to dictionary"""
        import csv
        from io import StringIO
        
        reader = csv.DictReader(StringIO(csv_content))
        rows = list(reader)
        
        return {
            'rows': rows,
            'count': len(rows)
        }
    
    def _apply_transformations(self, input_data: Dict, mapping_rules: Dict) -> Dict:
        """Apply transformation rules with support for structured formulas"""
        output_data = {}
        
        connections = mapping_rules.get('connections', [])
        
        for connection in connections:
            transformation = connection.get('transformation', {})
            
            # Handle different connection types
            if 'sources' in connection:
                # Multiple sources (CONCAT)
                source_values = []
                for source_id in connection['sources']:
                    # Get value by source field ID
                    val = self._get_value_by_field_id(input_data, source_id, mapping_rules)
                    source_values.append(val)
                
                # Apply transformation
                if transformation and isinstance(transformation, dict):
                    transformed_value = self._execute_structured_formula(
                        transformation,
                        source_values,
                        input_data,
                        mapping_rules
                    )
                else:
                    # Fallback: join values
                    transformed_value = ' '.join([str(v) for v in source_values if v])
                
            elif 'source' in connection:
                # Single source
                source_value = self._get_value_by_field_id(input_data, connection['source'], mapping_rules)
                
                # Apply transformation
                if transformation:
                    if isinstance(transformation, dict):
                        transformed_value = self._execute_structured_formula(
                            transformation,
                            source_value,
                            input_data,
                            mapping_rules
                        )
                    elif isinstance(transformation, str):
                        # Old string-based formula (backward compatibility)
                        transformed_value = self._apply_transformation_formula(
                            source_value,
                            transformation,
                            input_data
                        )
                    else:
                        transformed_value = source_value
                else:
                    transformed_value = source_value
            else:
                continue
            
            # Set target value
            target_path = connection.get('targetPath', '')
            if target_path:
                self._set_value_by_path(output_data, target_path, transformed_value)
        
        return output_data
    
    def _get_value_by_field_id(self, data: Dict, field_id: str, mapping_rules: Dict) -> Any:
        """Get value by field ID from mapping"""
        # Try to get field path from schema
        input_schema = mapping_rules.get('inputSchema', {})
        fields = input_schema.get('fields', {})
        
        for field_data in fields.values():
            if field_data.get('id') == field_id:
                path = field_data.get('path', '')
                if path:
                    return self._get_value_by_path(data, path)
        
        # Fallback: try field_id as path
        return self._get_value_by_path(data, field_id)
    
    def _execute_structured_formula(self, transformation: Dict, source_value: Any, 
                                   input_data: Dict, mapping_rules: Dict) -> Any:
        """Execute structured JSON formula"""
        trans_type = transformation.get('type', 'DIRECT')
        formula = transformation.get('formula')
        
        if not formula:
            return source_value
        
        # DIRECT - pass through
        if trans_type == 'DIRECT':
            return source_value
        
        # CONCAT - concatenate multiple inputs
        elif trans_type == 'CONCAT':
            if isinstance(source_value, list):
                # Multiple sources provided
                values = source_value
            else:
                values = [source_value]
            
            result = []
            inputs = formula.get('inputs', [])
            
            for i, input_spec in enumerate(inputs):
                if 'literal' in input_spec:
                    result.append(input_spec['literal'])
                elif 'field' in input_spec:
                    # Get value from sources
                    if i < len(values):
                        result.append(str(values[i]) if values[i] else '')
                    else:
                        result.append('')
            
            separator = formula.get('separator', '')
            if separator:
                return separator.join(result)
            else:
                return ''.join(result)
        
        # SPLIT - extract parts from single value
        elif trans_type == 'SPLIT':
            import re
            pattern = formula.get('regex', '')
            if not pattern:
                return source_value
            
            match = re.match(pattern, str(source_value))
            if match:
                outputs = formula.get('outputs', [])
                result = {}
                for output in outputs:
                    group_num = output.get('group', 1)
                    field_name = output.get('name', '')
                    result[field_name] = match.group(group_num)
                return result
            
            # Fallback
            fallback = formula.get('fallback')
            if fallback:
                return self._execute_structured_formula(
                    {'type': fallback.get('operation', 'DIRECT'), 'formula': fallback},
                    source_value,
                    input_data,
                    mapping_rules
                )
            return source_value
        
        # CONDITIONAL - if/then logic
        elif trans_type == 'CONDITIONAL':
            conditions = formula.get('conditions', [])
            value_str = str(source_value)
            
            for condition in conditions:
                condition_check = condition.get('if', '')
                
                # Simple condition evaluation
                if 'starts_with' in condition_check:
                    import re
                    prefix_match = re.search(r'starts_with\("(.+?)"\)', condition_check)
                    if prefix_match and value_str.startswith(prefix_match.group(1)):
                        then_action = condition.get('then', {})
                        return self._execute_conditional_action(then_action, source_value)
            
            # Default action
            default = formula.get('default', 'pass_through')
            if default == 'pass_through':
                return source_value
            return None
        
        # Unknown type
        return source_value
    
    def _execute_conditional_action(self, action: Dict, source_value: str) -> Any:
        """Execute conditional action (extract, etc)"""
        import re
        
        operation = action.get('operation', 'extract')
        
        if operation == 'extract':
            pattern = action.get('regex', '')
            if pattern:
                match = re.match(pattern, source_value)
                if match:
                    if 'target' in action:
                        # Single target
                        group_num = action.get('group', 1)
                        return match.group(group_num)
                    elif 'targets' in action:
                        # Multiple targets
                        result = {}
                        for i, target in enumerate(action['targets'], 1):
                            result[target] = match.group(i)
                        return result
        
        return source_value
    
    def _get_value_by_path(self, data: Dict, path: str) -> Any:
        """Get value from dictionary by dot-notation path"""
        if not path:
            return None
        
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                current = current[int(part)]
            else:
                return None
        
        return current
    
    def _set_value_by_path(self, data: Dict, path: str, value: Any):
        """Set value in dictionary by dot-notation path"""
        if not path:
            return
        
        parts = path.split('.')
        current = data
        
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                # Create intermediate dict or list
                next_part = parts[i + 1]
                if next_part.isdigit():
                    current[part] = []
                else:
                    current[part] = {}
            current = current[part]
        
        # Set final value
        final_key = parts[-1]
        if isinstance(current, dict):
            current[final_key] = value
        elif isinstance(current, list):
            if final_key.isdigit():
                idx = int(final_key)
                while len(current) <= idx:
                    current.append(None)
                current[idx] = value
    
    def _apply_transformation_formula(self, value: Any, formula: str, context: Dict) -> Any:
        """Apply transformation formula to value"""
        # Simple formula evaluation
        # TODO: Implement full formula engine
        
        if not formula:
            return value
        
        # CONCAT
        if formula.startswith('CONCAT('):
            parts = formula[7:-1].split(',')
            return ''.join([str(self._get_value_by_path(context, p.strip())) or '' for p in parts])
        
        # UPPER
        if formula == 'UPPER()':
            return str(value).upper() if value else ''
        
        # LOWER
        if formula == 'LOWER()':
            return str(value).lower() if value else ''
        
        # Mathematical
        if '+' in formula or '-' in formula or '*' in formula or '/' in formula:
            try:
                # Replace field references with values
                eval_formula = formula
                for match in re.findall(r'\{([^}]+)\}', formula):
                    field_value = self._get_value_by_path(context, match)
                    eval_formula = eval_formula.replace(f'{{{match}}}', str(field_value or 0))
                
                return eval(eval_formula)
            except:
                return value
        
        return value
    
    def _generate_output(self, data: Dict, format_type: str, mapping_rules: Dict) -> str:
        """Generate output in specified format"""
        if format_type == 'xml':
            return self._dict_to_xml(data, mapping_rules.get('outputSchema', {}))
        elif format_type == 'json':
            return json.dumps(data, indent=2)
        elif format_type == 'csv':
            return self._dict_to_csv(data)
        else:
            raise ValueError(f"Unsupported output format: {format_type}")
    
    def _dict_to_xml(self, data: Dict, schema: Dict) -> str:
        """Convert dictionary to XML"""
        def dict_to_element(parent, data_dict, tag_name=None):
            if tag_name:
                elem = ET.SubElement(parent, tag_name)
            else:
                elem = parent
            
            if isinstance(data_dict, dict):
                for key, value in data_dict.items():
                    if key.startswith('@'):
                        # Attribute
                        continue
                    elif key == '#text':
                        elem.text = str(value)
                    else:
                        dict_to_element(elem, value, key)
            elif isinstance(data_dict, list):
                for item in data_dict:
                    dict_to_element(parent, item, tag_name)
            else:
                elem.text = str(data_dict) if data_dict is not None else ''
        
        # Get root element name
        root_name = list(data.keys())[0] if data else 'root'
        root_data = data.get(root_name, {})
        
        root = ET.Element(root_name)
        dict_to_element(root, root_data)
        
        # Pretty print
        self._indent_xml(root)
        
        return ET.tostring(root, encoding='unicode', xml_declaration=True)
    
    def _indent_xml(self, elem, level=0):
        """Add pretty-print indentation to XML"""
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for child in elem:
                self._indent_xml(child, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i
    
    def _dict_to_csv(self, data: Dict) -> str:
        """Convert dictionary to CSV"""
        # Assume data has 'rows' key with list of dicts
        if 'rows' in data:
            rows = data['rows']
        else:
            # Flatten single object
            rows = [data]
        
        if not rows:
            return ''
        
        # Get headers
        headers = list(rows[0].keys())
        
        # Build CSV
        lines = [','.join(headers)]
        for row in rows:
            values = [str(row.get(h, '')) for h in headers]
            lines.append(','.join(f'"{v}"' if ',' in v else v for v in values))
        
        return '\n'.join(lines)


# ===========================================================================
# USAGE EXAMPLES
# ===========================================================================

if __name__ == '__main__':
    # Example: Transform FatturaPA (IT) to UBL (FR)
    
    engine = TransformationEngine(
        input_xsd='schemas/FatturaPA_v1.2.1.xsd',
        output_xsd='schemas/UBL-Invoice-2.1.xsd',
        input_schematron='schemas/FatturaPA.sch',
        output_schematron='schemas/UBL-Invoice-2.1.sch'
    )
    
    # Load input
    with open('input/fattura_001.xml', 'r') as f:
        input_xml = f.read()
    
    # Load mapping rules (from visual mapper)
    with open('mappings/FatturaPA_to_UBL_FR.json', 'r') as f:
        mapping_rules = json.load(f)
    
    # Transform
    result = engine.transform(
        input_content=input_xml,
        input_format='xml',
        output_format='xml',
        mapping_rules=mapping_rules,
        validate_input=True,
        validate_output=True
    )
    
    # Check result
    if result.success:
        print("✅ Transformation successful!")
        with open('output/ubl_invoice_001.xml', 'w') as f:
            f.write(result.output_content)
    else:
        print("❌ Transformation failed:")
        for error in result.validation_errors + result.transformation_errors:
            print(f"  - {error}")
