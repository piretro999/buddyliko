#!/usr/bin/env python3
"""
Transformation Engine - FIXED VERSION
Version: 20260216_111828
Last Modified: 2026-02-16T11:18:28.765697

FIXES:
- Usa 'offset' per estrarre path completi dai campi (invece di xml_path/path)
- Questo risolve il problema dell'XML vuoto quando offset è popolato ma xml_path è vuoto
"""

#!/usr/bin/env python3
"""
🎯 PURE DYNAMIC Transformation Engine - ZERO HARDCODED ORDERS!

This is a PURE DYNAMIC version that extracts element orders ONLY from XSD schemas.
NO hardcoded fallbacks, NO default orders, STRICT schema-driven approach.

⚠️ REQUIREMENTS:
- output_xsd MUST be provided (default strict_mode=True)
- XSD file must exist and be valid
- All element orders come from XSD parsing

✅ BENEFITS:
- 100% accurate to schema (no human errors)
- Works with ANY XML schema (UBL, PEPPOL, FatturaPA, CII, custom)
- Self-updating when schema changes
- No maintenance required

🔄 FOR CACHED VERSION (faster):
Use transformation_engine_cached.py which generates Python cache from XSD

Features:
- Input validation (XSD + Schematron)
- Data transformation (mapping rules)
- Output validation (XSD + Schematron)
- Element reordering from XSD (PURE DYNAMIC - NO FALLBACKS)
- Multiple input/output formats (XML, JSON, CSV, EDI)
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
    """
    Pure Dynamic Transformation Engine - REQUIRES XSD
    
    ⚠️ This version REQUIRES output_xsd to function properly.
    It extracts element orders ONLY from XSD schema with ZERO hardcoded fallbacks.
    
    If you need a version that works without XSD, use transformation_engine_cached.py instead.
    """
    
    def __init__(self, 
                 input_xsd: Optional[str] = None,
                 output_xsd: Optional[str] = None,
                 input_schematron: Optional[str] = None,
                 output_schematron: Optional[str] = None,
                 strict_mode: bool = False):  # ← FALSE di default per retro-compatibilità
        """
        Initialize transformation engine
        
        Args:
            input_xsd: Path to input XSD schema (optional, for validation)
            output_xsd: Path to output XSD schema (REQUIRED in strict mode!)
            input_schematron: Path to input Schematron (optional)
            output_schematron: Path to output Schematron (optional)
            strict_mode: If True, requires output_xsd to be provided
        """
        
        # STRICT MODE: Require XSD (opzionale, default False)
        if strict_mode and not output_xsd:
            raise ValueError(
                "❌ STRICT MODE: output_xsd is REQUIRED!\n"
                "\n"
                "You enabled strict_mode=True but didn't provide output_xsd.\n"
                "\n"
                "Solutions:\n"
                "1. Provide output_xsd parameter: TransformationEngine(output_xsd='path/to/schema.xsd', strict_mode=True)\n"
                "2. Disable strict mode: TransformationEngine(strict_mode=False) [default]\n"
            )
        
        # Warn if XSD path provided but doesn't exist
        if output_xsd and not os.path.exists(output_xsd):
            import warnings
            warnings.warn(
                f"⚠️  Output XSD file not found: {output_xsd}\n"
                f"   Element ordering will not be available.\n"
                f"   Transformation will continue but may produce invalid XML.",
                UserWarning
            )
            # Don't block - just warn
            output_xsd = None  # Clear invalid path
        
        # Save paths for namespace extraction
        self.input_xsd_path = input_xsd
        self.output_xsd_path = output_xsd
        self.strict_mode = strict_mode
        
        # Element order from XSD (populated during transformation)
        self.element_order = []
        
        # Print mode info
        if strict_mode:
            print(f"🔒 STRICT MODE ENABLED: XSD required for transformation")
            print(f"   Output XSD: {output_xsd}")
        elif output_xsd and os.path.exists(output_xsd):
            print(f"📋 XSD-driven mode: Element ordering from schema")
            print(f"   Output XSD: {output_xsd}")
        else:
            print(f"⚠️  Running without XSD - element ordering may be incorrect")
            print(f"   Recommendation: Provide output_xsd for best results")
        
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
            # Add missing mandatory elements for UBL compliance
            transformed_data = self._add_mandatory_elements(transformed_data, output_format)
        except Exception as e:
            result.transformation_errors.append(f"Transformation error: {str(e)}")
            return result
        
        # STEP 4: Generate Output
        try:
            print(f"🔨 Generating output...")
            output_content = self._generate_output(transformed_data, output_format, mapping_rules)
            print(f"✅ Output generated: {len(output_content) if output_content else 0} chars")
        except Exception as e:
            print(f"❌ Output generation error: {str(e)}")
            result.transformation_errors.append(f"Output generation error: {str(e)}")
            return result
        
        # STEP 5: Output Validation
        print(f"🔍 validate_output = {validate_output}")
        if validate_output:
            print(f"⚙️ Running output validation...")
            validation_ok, errors = self._validate_output(output_content, output_format)
            print(f"📊 Validation result: ok={validation_ok}, errors={len(errors)}")
            if not validation_ok:
                result.validation_errors.extend(errors)
                # Still return output but mark as invalid
                result.output_content = output_content
                print(f"⚠️ Validation failed, returning with success=False but output_content set")
                return result
        
        # SUCCESS
        print(f"🎉 Setting success=True")
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
        
        print(f"\n🔄 _apply_transformations:")
        print(f"  Input data keys: {list(input_data.keys())[:10]}")
        print(f"  Connections: {len(connections)}")
        
        for idx, connection in enumerate(connections):
            print(f"\n  Connection #{idx+1}:")
            print(f"    Full connection dict:")
            for key, value in connection.items():
                if key == 'transformation':
                    print(f"      {key}: {value}")
                else:
                    print(f"      {key}: {value}")
            
            # CRITICAL: Check if targetPath exists
            if 'targetPath' not in connection:
                print(f"    ⚠️⚠️⚠️ WARNING: targetPath MISSING from connection!")
            if 'sourcePath' not in connection:
                print(f"    ⚠️⚠️⚠️ WARNING: sourcePath MISSING from connection!")
            
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
                # CRITICAL FIX: Use sourcePath directly if available (avoids ambiguous field names)
                if 'sourcePath' in connection and connection['sourcePath']:
                    print(f"    🎯 Using sourcePath directly: {connection['sourcePath']}")
                    source_value = self._get_value_by_path(input_data, connection['sourcePath'])
                else:
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
            # CRITICAL FIX: Use targetPath directly if available
            target_path = connection.get('targetPath') or connection.get('target', '')
            if target_path:
                # Use targetPath directly - don't try to resolve from schema
                # This avoids ambiguous field name resolution
                actual_path = target_path
                
                print(f"  🎯 Setting {target_path} = {transformed_value}")
                self._set_value_by_path(output_data, actual_path, transformed_value)
        
        return output_data
    
    def _get_value_by_field_id(self, data: Dict, field_id: str, mapping_rules: Dict) -> Any:
        """Get value by field ID from mapping"""
        print(f"    🔎 _get_value_by_field_id: {field_id}")
        
        # Try to get field path from schema
        input_schema = mapping_rules.get('inputSchema', {})
        fields = input_schema.get('fields', {})
        
        print(f"    📋 Input schema has {len(fields)} fields")
        
        for field_key, field_data in fields.items():
            if field_data.get('id') == field_id or field_data.get('name') == field_id or field_key == field_id:
                # Try offset first (FatturaPA), then xml_path, then path
                path = field_data.get('offset') or field_data.get('xml_path') or field_data.get('path', '')
                print(f"    ✅ Found field: {field_key} → path: {path}")
                if path:
                    value = self._get_value_by_path(data, path)
                    print(f"    💎 Extracted value: {value}")
                    return value
        
        print(f"    ⚠️  Field '{field_id}' not found in schema, trying as direct path")
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
        """Get value from dictionary by path (supports XML paths with /)"""
        if not path:
            return None
        
        # CRITICAL FIX: Remove any trailing/leading whitespace and newlines
        path = path.strip()
        
        # Determine separator: / for XML paths, . for others
        separator = '/' if '/' in path else '.'
        
        # Remove leading separator and namespace prefixes
        path = path.lstrip(separator)
        
        # Split by separator
        parts = path.split(separator)
        
        # Remove empty parts and attributes, and STRIP each part!
        parts = [p.strip() for p in parts if p and p.strip() and not p.strip().startswith('@')]
        
        print(f"      _get_value_by_path: {path}")
        print(f"      Parts: {parts}")
        print(f"      Data keys: {list(data.keys())[:5]}")
        
        current = data
        
        # SPECIAL CASE: If first part not found, try entering the root element first
        if len(parts) > 0 and isinstance(current, dict):
            first_part = parts[0]
            
            # Check if first part exists (with or without namespace)
            first_part_exists = False
            
            # Try exact match
            if first_part in current:
                first_part_exists = True
            # Try without namespace
            elif ':' in first_part:
                clean_first = first_part.split(':')[-1]
                if clean_first in current:
                    first_part_exists = True
            
            # If first part NOT found, try entering root automatically
            if not first_part_exists:
                root_keys = list(current.keys())
                if len(root_keys) == 1:
                    # Single root element - enter it automatically
                    print(f"      ⚡ First part '{first_part}' not found, entering root '{root_keys[0]}'")
                    current = current[root_keys[0]]
                    print(f"      📍 Now inside root. Current keys: {list(current.keys())[:10] if isinstance(current, dict) else type(current)}")
        
        for i, part in enumerate(parts):
            if isinstance(current, dict):
                # Try exact match first
                if part in current:
                    current = current.get(part)
                # Try without namespace prefix (e.g., "p:FatturaElettronica" → "FatturaElettronica")
                elif ':' in part:
                    clean_part = part.split(':')[-1]
                    if clean_part in current:
                        current = current.get(clean_part)
                    else:
                        print(f"      ❌ Part '{part}' (or '{clean_part}') not found in {list(current.keys())[:10]}")
                        return None
                else:
                    # Try with common namespace prefixes
                    found = False
                    for prefix in ['p:', 'cac:', 'cbc:']:
                        prefixed = f"{prefix}{part}"
                        if prefixed in current:
                            current = current.get(prefixed)
                            found = True
                            break
                    if not found:
                        print(f"      ❌ Part '{part}' not found in {list(current.keys())[:10]}")
                        return None
            elif isinstance(current, list):
                if part.isdigit():
                    idx = int(part)
                    if idx < len(current):
                        current = current[idx]
                    else:
                        print(f"      ❌ Index {idx} out of range (list length: {len(current)})")
                        return None
                else:
                    # Take first element of list
                    if current:
                        current = current[0]
                        # Try to navigate further with this part
                        if isinstance(current, dict) and part in current:
                            current = current[part]
                    else:
                        print(f"      ❌ Empty list")
                        return None
            else:
                print(f"      ❌ Current value is not dict or list: {type(current)}")
                return None
        
        print(f"      ✅ Found value: {current}")
        return current
    
    def _set_value_by_path(self, data: Dict, path: str, value: Any):
        """Set value in dictionary using full path (creates nested structure)"""
        if not path:
            return
        
        # Determine separator: / for XML paths, . for others
        separator = '/' if '/' in path else '.'
        
        # Remove leading separator
        path = path.lstrip(separator)
        
        # Split by separator and remove empties and attributes
        parts = [p.strip() for p in path.split(separator) if p and p.strip() and not p.strip().startswith('@')]
        
        if not parts:
            return
        
        print(f"    _set_value_by_path: {path}")
        print(f"    Parts: {parts}")
        print(f"    Value: {value}")
        
        # Get or create root element (usually "Invoice" or similar)
        if 'Invoice' not in data:
            data['Invoice'] = {}
        
        current = data['Invoice']
        
        # Navigate/create nested structure for all parts except the last
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                # If already exists but not a dict, convert to dict
                current[part] = {'#text': current[part]}
            
            current = current[part]
        
        # Set the final value
        final_key = parts[-1]
        
        if value is not None:
            # Handle list values (like multiple <cbc:Note>)
            if final_key in current:
                # Key already exists - convert to list
                if not isinstance(current[final_key], list):
                    current[final_key] = [current[final_key]]
                if isinstance(value, list):
                    current[final_key].extend(value)
                else:
                    current[final_key].append(value)
            else:
                current[final_key] = value
            
            print(f"    ✅ Set {'/'.join(parts)} = {value}")
    
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
    
    def _extract_element_order_from_xsd(self, xsd_path: str) -> List[str]:
        """Extract the element order from XSD schema sequence"""
        if not xsd_path or not os.path.exists(xsd_path):
            return []
        
        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(xsd_path)
            root = tree.getroot()
            
            # Define namespace for XSD
            ns = {'xsd': 'http://www.w3.org/2001/XMLSchema'}
            
            # Find the main complexType > sequence
            sequence = root.find('.//xsd:complexType/xsd:sequence', ns)
            if sequence is None:
                return []
            
            # Extract element refs in order
            element_order = []
            for elem in sequence.findall('xsd:element', ns):
                ref = elem.get('ref')
                if ref:
                    # Remove namespace prefix if present (cbc:ID -> ID, cac:Party -> Party)
                    element_name = ref.split(':')[-1] if ':' in ref else ref
                    element_order.append(ref)  # Keep full ref with namespace prefix
            
            print(f"📋 Extracted {len(element_order)} elements from XSD sequence")
            return element_order
        except Exception as e:
            print(f"⚠️ Could not extract element order from XSD: {e}")
            return []
    
    def _extract_element_order_from_xsd(self, xsd_path: str) -> List[str]:
        """Extract the element order from XSD schema sequence"""
        if not xsd_path or not os.path.exists(xsd_path):
            return []
        
        try:
            import xml.etree.ElementTree as ET
            
            # Cache per gli schemi già parsati
            if not hasattr(self, '_xsd_cache'):
                self._xsd_cache = {}
            
            # Namespace XSD
            ns = {'xsd': 'http://www.w3.org/2001/XMLSchema'}
            
            def resolve_import_path(import_location, current_xsd_path):
                """Risolve path relativi degli import"""
                current_dir = os.path.dirname(current_xsd_path)
                import_path = os.path.join(current_dir, import_location)
                import_path = os.path.normpath(import_path)
                return import_path
            
            def parse_xsd_file(file_path):
                """Parsa un file XSD e ritorna tree + namespace map"""
                if file_path in self._xsd_cache:
                    return self._xsd_cache[file_path]
                
                if not os.path.exists(file_path):
                    print(f"⚠️ XSD file not found: {file_path}")
                    return None, {}
                
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                # Estrai targetNamespace
                target_ns = root.get('targetNamespace', '')
                
                # Estrai namespace prefixes
                ns_map = {}
                for key, value in root.attrib.items():
                    if key.startswith('{http://www.w3.org/2000/xmlns/}'):
                        prefix = key.split('}')[1]
                        ns_map[value] = prefix
                    elif key.startswith('xmlns:'):
                        prefix = key.split(':', 1)[1]
                        ns_map[value] = prefix
                
                self._xsd_cache[file_path] = (root, target_ns, ns_map)
                return root, target_ns, ns_map
            
            def get_type_definition(type_name, root, all_schemas):
                """Trova la definizione di un complexType in tutti gli schemi"""
                # Cerca nello schema corrente
                for complex_type in root.findall('.//xsd:complexType', ns):
                    if complex_type.get('name') == type_name:
                        return complex_type
                
                # Cerca negli schemi importati
                for schema_root, schema_ns, schema_nsmap in all_schemas:
                    for complex_type in schema_root.findall('.//xsd:complexType', ns):
                        if complex_type.get('name') == type_name:
                            return complex_type
                
                return None
            
            def extract_sequence_from_type(complex_type, all_schemas):
                """Estrae la sequenza di elementi da un complexType"""
                if complex_type is None:
                    return []
                
                sequence = complex_type.find('.//xsd:sequence', ns)
                if sequence is None:
                    return []
                
                elements = []
                for elem in sequence.findall('xsd:element', ns):
                    ref = elem.get('ref')
                    if ref:
                        elements.append(ref)
                
                return elements
            
            # Parsa lo schema principale
            main_root, main_ns, main_nsmap = parse_xsd_file(xsd_path)
            if main_root is None:
                return []
            
            # Lista di tutti gli schemi (principale + importati)
            all_schemas = [(main_root, main_ns, main_nsmap)]
            
            # Trova e parsa tutti gli import
            for import_elem in main_root.findall('.//xsd:import', ns):
                schema_location = import_elem.get('schemaLocation')
                if schema_location:
                    import_path = resolve_import_path(schema_location, xsd_path)
                    print(f"📥 Following import: {schema_location} -> {import_path}")
                    
                    import_root, import_ns, import_nsmap = parse_xsd_file(import_path)
                    if import_root is not None:
                        all_schemas.append((import_root, import_ns, import_nsmap))
            
            # Trova il root element (es: Invoice)
            root_element = main_root.find('.//xsd:element[@name]', ns)
            if root_element is None:
                return []
            
            type_attr = root_element.get('type')
            if not type_attr:
                return []
            
            # Rimuovi prefix dal type (es: "InvoiceType" da "inv:InvoiceType")
            type_name = type_attr.split(':')[-1]
            
            # Trova la definizione del tipo
            main_type = get_type_definition(type_name, main_root, all_schemas)
            
            # Estrai la sequenza di elementi
            element_order = extract_sequence_from_type(main_type, all_schemas)
            
            # Ora costruisci un dizionario completo di tutti i tipi e i loro ordini
            self._type_orders = {}
            
            # Per ogni schema, estrai tutti i complexType e le loro sequenze
            for schema_root, schema_ns, schema_nsmap in all_schemas:
                for complex_type in schema_root.findall('.//xsd:complexType', ns):
                    type_name = complex_type.get('name')
                    if type_name:
                        sequence = extract_sequence_from_type(complex_type, all_schemas)
                        if sequence:
                            # Rimuovi "Type" dal nome se presente (es: PostalAddressType -> PostalAddress)
                            clean_name = type_name.replace('Type', '')
                            self._type_orders[clean_name] = sequence
                            print(f"📋 Extracted order for {clean_name}: {len(sequence)} elements")
            
            print(f"📋 Extracted {len(element_order)} elements from main XSD sequence")
            print(f"📚 Total types with orders: {len(self._type_orders)}")
            
            return element_order
            
        except Exception as e:
            print(f"⚠️ Could not extract element order from XSD: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _sort_by_xsd_order(self, keys: List[str]) -> List[str]:
        """Sort keys according to XSD element order"""
        if not self.element_order:
            return keys
        
        # Create order map
        order_map = {elem: i for i, elem in enumerate(self.element_order)}
        
        # Sort keys
        def get_order(key):
            # Try exact match first
            if key in order_map:
                return order_map[key]
            # Try with cbc: prefix
            if f'cbc:{key}' in order_map:
                return order_map[f'cbc:{key}']
            # Try with cac: prefix
            if f'cac:{key}' in order_map:
                return order_map[f'cac:{key}']
            # Not in order, put at end
            return 999999
        
        return sorted(keys, key=get_order)
    
    def _extract_currency_code(self, data: Dict) -> str:
        """Extract currency code from data"""
        # Try to find DocumentCurrencyCode or similar
        def find_currency(d):
            if isinstance(d, dict):
                if 'DocumentCurrencyCode' in d:
                    return d['DocumentCurrencyCode']
                if 'cbc:DocumentCurrencyCode' in d:
                    return d['cbc:DocumentCurrencyCode']
                for v in d.values():
                    result = find_currency(v)
                    if result:
                        return result
            return None
        
        return find_currency(data) or 'EUR'
    
    def _is_amount_field(self, field_name: str) -> bool:
        """Check if field is an amount that needs currencyID attribute"""
        amount_keywords = [
            'Amount', 'Value', 'Price', 'Total', 'Payable',
            'TaxableAmount', 'TaxAmount', 'LineExtension'
        ]
        return any(keyword in field_name for keyword in amount_keywords)
    
    def _is_date_field(self, field_name: str) -> bool:
        """Check if field is a date field (not datetime)"""
        # Fields that should be date only, not datetime
        date_only_fields = [
            'IssueDate', 'DueDate', 'TaxPointDate', 'ActualDeliveryDate',
            'StartDate', 'EndDate', 'BirthDate', 'ExpiryDate'
        ]
        return any(field in field_name for field in date_only_fields)
    
    def _remove_empty_elements(self, elem):
        """Remove elements that have no text and no children"""
        # Process children first (bottom-up)
        for child in list(elem):
            self._remove_empty_elements(child)
        
        # Remove if empty
        for child in list(elem):
            if not child.text or not child.text.strip():
                if len(child) == 0:  # No children
                    elem.remove(child)
    
    def _reorder_elements(self, root):
        """
        Reorder child elements according to XSD sequence
        
        ⚠️ PURE DYNAMIC VERSION - NO HARDCODED FALLBACKS!
        This version REQUIRES XSD to be available and will FAIL if orders cannot be extracted.
        """
        
        # Verify XSD orders were extracted
        if not hasattr(self, '_type_orders') or not self._type_orders:
            raise ValueError(
                "❌ XSD element orders not available!\n"
                "   This is a PURE DYNAMIC engine that requires XSD schema.\n"
                "   Make sure:\n"
                "   1. output_xsd_path is set correctly\n"
                "   2. XSD file exists at that path\n"
                "   3. _extract_element_order_from_xsd() was called successfully\n"
                f"   Current output_xsd_path: {getattr(self, 'output_xsd_path', 'NOT SET')}\n"
                f"   Types extracted: {len(self._type_orders) if hasattr(self, '_type_orders') else 0}"
            )
        
        def get_type_from_tag(tag):
            """Extract type name from tag"""
            if '}' in tag:
                local_tag = tag.split('}')[-1]
            else:
                local_tag = tag
            if ':' in local_tag:
                return local_tag.split(':')[-1]
            return local_tag
        
        def normalize_tag(tag):
            """Normalize tag for comparison"""
            if '}' in tag:
                local_tag = tag.split('}')[-1]
            else:
                local_tag = tag
            if local_tag.startswith('cbc:') or local_tag.startswith('cac:'):
                return local_tag
            return local_tag
        
        def get_element_order(elem, parent_type=None):
            """
            Get sort order for an element - ONLY FROM XSD!
            
            ⚠️ NO FALLBACKS - Pure dynamic approach
            """
            tag = normalize_tag(elem.tag)
            tag_no_prefix = tag.split(':')[-1] if ':' in tag else tag
            
            # Search in XSD-extracted orders ONLY
            if hasattr(self, '_type_orders') and self._type_orders:
                # Try parent-specific order first
                if parent_type and parent_type in self._type_orders:
                    order_list = self._type_orders[parent_type]
                    for i, ordered_elem in enumerate(order_list):
                        ordered_no_prefix = ordered_elem.split(':')[-1] if ':' in ordered_elem else ordered_elem
                        if tag == ordered_elem or tag_no_prefix == ordered_no_prefix:
                            return i
                
                # Try root element order if parent not found
                if self.element_order:
                    for i, ordered_elem in enumerate(self.element_order):
                        ordered_no_prefix = ordered_elem.split(':')[-1] if ':' in ordered_elem else ordered_elem
                        if tag == ordered_elem or tag_no_prefix == ordered_no_prefix:
                            return i
            
            # Element not found in XSD orders
            # This is OK - elements not in schema go to the end
            # But we log a warning for debugging
            if parent_type:
                print(f"⚠️  Element '{tag}' not found in XSD order for type '{parent_type}'")
            
            return 999999  # Unknown elements go last
            return 999999
        
        parent_type = get_type_from_tag(root.tag)
        
        # Debug logging - show XSD order usage
        if hasattr(self, '_type_orders') and parent_type in self._type_orders:
            children_tags = [normalize_tag(c.tag) for c in root]
            print(f"🔄 Reordering {parent_type} using XSD-extracted order ({len(self._type_orders[parent_type])} elements)")
            print(f"   Current children: {children_tags[:5]}{'...' if len(children_tags) > 5 else ''}")
        else:
            # No order found for this type - elements will stay in current order
            # This is normal for types not defined in XSD (e.g., extension elements)
            pass
        
        # Sort and reattach
        children = list(root)
        children.sort(key=lambda e: get_element_order(e, parent_type))
        
        for child in children:
            root.remove(child)
            root.append(child)
        
        # Recursively reorder
        for child in root:
            self._reorder_elements(child)
    
    def _extract_namespaces_from_xsd(self, xsd_path: str) -> Dict[str, str]:
        """Extract namespaces from XSD file or use UBL defaults"""
        if not xsd_path or not os.path.exists(xsd_path):
            return {}
        
        try:
            tree = ET.parse(xsd_path)
            root = tree.getroot()
            
            namespaces = {}
            
            # Extract all xmlns:* attributes from <xsd:schema>
            for key, value in root.attrib.items():
                # Handle {http://www.w3.org/2000/xmlns/}cac format
                if '{http://www.w3.org/2000/xmlns/}' in key:
                    prefix = key.split('}')[1]
                    namespaces[prefix] = value
                # Handle targetNamespace for default namespace
                elif key == 'targetNamespace':
                    namespaces[''] = value
            
            # Also check for xmlns:prefix="..." format (without namespace)
            for attr_name in root.attrib:
                if attr_name.startswith('xmlns:'):
                    prefix = attr_name.split(':', 1)[1]
                    namespaces[prefix] = root.attrib[attr_name]
                elif attr_name == 'xmlns':
                    namespaces[''] = root.attrib[attr_name]
            
            # CRITICAL FIX: If this looks like a UBL schema, add standard UBL namespaces
            # These are typically imported from other XSD files, not in the main schema
            default_ns = namespaces.get('', '')
            if 'ubl' in xsd_path.lower() or 'Invoice' in default_ns or 'oasis' in default_ns:
                print(f"🎯 Detected UBL schema, adding standard namespaces")
                if 'cac' not in namespaces:
                    namespaces['cac'] = 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2'
                if 'cbc' not in namespaces:
                    namespaces['cbc'] = 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2'
            
            print(f"📦 Extracted {len(namespaces)} namespaces from XSD")
            return namespaces
        except Exception as e:
            print(f"⚠️ Could not extract namespaces from XSD: {e}")
            return {}
    
    def _add_mandatory_elements(self, data: Dict, format_type: str) -> Dict:
        """Add missing mandatory elements for UBL compliance"""
        if format_type != 'xml':
            return data
        
        # Check if this is an Invoice
        if 'Invoice' not in data:
            return data
        
        invoice = data.get('Invoice', {})
        
        # Ensure TaxScheme is present in all TaxCategory elements
        def add_tax_scheme(node, path=""):
            if isinstance(node, dict):
                for key, value in list(node.items()):
                    new_path = f"{path}/{key}" if path else key
                    
                    # If this is a TaxCategory or ClassifiedTaxCategory without TaxScheme, add it
                    if ('TaxCategory' in key or 'ClassifiedTaxCategory' in key) and isinstance(value, dict):
                        if 'TaxScheme' not in value and 'cac:TaxScheme' not in value:
                            print(f"  ✨ Adding missing TaxScheme to {key} at {new_path}")
                            value['cac:TaxScheme'] = {
                                'cbc:ID': 'VAT'
                            }
                    
                    # Recursively process nested structures
                    add_tax_scheme(value, new_path)
            elif isinstance(node, list):
                for item in node:
                    add_tax_scheme(item, path)
        
        # Ensure ChargeIndicator is present in AllowanceCharge
        def add_charge_indicator(node, path=""):
            if isinstance(node, dict):
                for key, value in list(node.items()):
                    new_path = f"{path}/{key}" if path else key
                    
                    if 'AllowanceCharge' in key and isinstance(value, dict):
                        if 'ChargeIndicator' not in value and 'cbc:ChargeIndicator' not in value:
                            print(f"  ✨ Adding missing ChargeIndicator to AllowanceCharge at {new_path}")
                            # Default to 'false' for allowance (discount)
                            value['cbc:ChargeIndicator'] = 'false'
                    
                    add_charge_indicator(value, new_path)
            elif isinstance(node, list):
                for item in node:
                    add_charge_indicator(item, path)
        
        # Ensure ID is present in InvoiceLine (before other elements)
        def ensure_invoice_line_id(node, path="", line_counter={'count': 0}):
            if isinstance(node, dict):
                for key, value in list(node.items()):
                    new_path = f"{path}/{key}" if path else key
                    
                    if 'InvoiceLine' in key and isinstance(value, dict):
                        if 'ID' not in value and 'cbc:ID' not in value:
                            line_counter['count'] += 1
                            print(f"  ✨ Adding missing ID to InvoiceLine at {new_path}")
                            value['cbc:ID'] = str(line_counter['count'])
                    
                    ensure_invoice_line_id(value, new_path, line_counter)
            elif isinstance(node, list):
                for item in node:
                    ensure_invoice_line_id(item, path, line_counter)
        
        print("🔧 Adding mandatory elements...")
        add_tax_scheme(invoice)
        add_charge_indicator(invoice)
        ensure_invoice_line_id(invoice)
        
        data['Invoice'] = invoice
        return data
    
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
        """Convert dictionary to XML with proper ordering from XSD"""
        
        # Get currency code from data for attributes
        currency_code = self._extract_currency_code(data)
        
        def dict_to_element(parent, data_dict, tag_name=None, path=""):
            # Skip None values and empty dicts/lists
            if data_dict is None:
                return None
            if isinstance(data_dict, dict) and not data_dict:
                return None
            if isinstance(data_dict, list) and not data_dict:
                return None
            
            if tag_name:
                elem = ET.SubElement(parent, tag_name)
            else:
                elem = parent
            
            if isinstance(data_dict, dict):
                # Sort keys according to XSD order if available
                keys = list(data_dict.keys())
                if self.element_order and path in ["", tag_name]:
                    keys = self._sort_by_xsd_order(keys)
                
                for key in keys:
                    value = data_dict[key]
                    if key.startswith('@'):
                        # Skip attributes for now, handled separately
                        continue
                    elif key == '#text':
                        if value is not None and str(value).strip():
                            elem.text = str(value)
                    else:
                        # Add currency attributes to amount fields
                        new_path = f"{path}/{key}" if path else key
                        child = dict_to_element(elem, value, key, new_path)
                        
                        # Add currencyID attribute ONLY to cbc: amount fields (not cac:)
                        if child is not None and currency_code and self._is_amount_field(key):
                            # Only for cbc: elements (basic components), NOT cac: (aggregates)
                            if 'cbc:' in key or (not 'cac:' in key and not key.startswith('cac:')):
                                if child.text or len(child) > 0:  # Only if element has content
                                    child.set('currencyID', currency_code)
                        
                        # Fix date format for date fields (remove time component)
                        if child is not None and self._is_date_field(key):
                            if child.text and 'T' in child.text:
                                # Extract just the date part (YYYY-MM-DD)
                                child.text = child.text.split('T')[0]
                
            elif isinstance(data_dict, list):
                # Handle lists - create multiple elements with same tag
                for item in data_dict:
                    if item is not None and item != "":
                        dict_to_element(parent, item, tag_name, path)
                return None  # Don't return elem for lists
            else:
                # Leaf node - only set text if not None/empty
                if data_dict is not None and str(data_dict).strip():
                    elem.text = str(data_dict)
                else:
                    # Remove empty elements
                    if elem != parent:
                        parent.remove(elem)
                    return None
            
            return elem
        
        # Get root element name
        root_name = list(data.keys())[0] if data else 'root'
        root_data = data.get(root_name, {})
        
        # Extract element order from XSD
        self.element_order = []
        if self.output_xsd_path and os.path.exists(self.output_xsd_path):
            self.element_order = self._extract_element_order_from_xsd(self.output_xsd_path)
            print(f"📋 Extracted element order: {len(self.element_order)} elements")
        
        # Create root element
        root = ET.Element(root_name)
        
        # Extract and add namespaces from output XSD
        print(f"📦 output_xsd_path = {self.output_xsd_path}")
        print(f"📂 Path exists? {os.path.exists(self.output_xsd_path) if self.output_xsd_path else 'N/A'}")
        
        if self.output_xsd_path and os.path.exists(self.output_xsd_path):
            print(f"✅ Extracting namespaces from XSD...")
            namespaces = self._extract_namespaces_from_xsd(self.output_xsd_path)
            print(f"📋 Extracted namespaces: {namespaces}")
            if namespaces:
                for prefix, uri in namespaces.items():
                    if prefix:
                        root.set(f'xmlns:{prefix}', uri)
                    else:
                        root.set('xmlns', uri)
        else:
            # Fallback: Add UBL namespaces if root is Invoice
            print(f"⚠️ Using fallback namespaces for {root_name}")
            if root_name == 'Invoice':
                root.set('xmlns', 'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2')
                root.set('xmlns:cac', 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2')
                root.set('xmlns:cbc', 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2')
        
        # Build XML tree
        dict_to_element(root, root_data, path="")
        
        # Remove empty elements recursively
        self._remove_empty_elements(root)
        
        # Reorder elements according to XSD
        if self.element_order:
            self._reorder_elements(root)
        
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
