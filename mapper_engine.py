#!/usr/bin/env python3
"""
Mapping Engine
Executes mappings with transformations, conditions, cardinality handling

Supports:
- Direct mapping (1:1)
- Transformations (functions)
- Conditional mapping (if-then-else)
- Aggregation (N:1)
- Split (1:N)
- Lookup tables
"""

import json
import re
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from decimal import Decimal


class TransformationLibrary:
    """Built-in transformation functions"""
    
    @staticmethod
    def concat(*values, separator=''):
        """Concatenate values"""
        return separator.join(str(v) for v in values if v)
    
    @staticmethod
    def substring(value, start, length=None):
        """Extract substring"""
        if length:
            return str(value)[start:start+length]
        return str(value)[start:]
    
    @staticmethod
    def format_date(value, from_format='%Y-%m-%d', to_format='%Y-%m-%d'):
        """Convert date format"""
        try:
            if isinstance(value, str):
                dt = datetime.strptime(value, from_format)
            else:
                dt = value
            return dt.strftime(to_format)
        except:
            return value
    
    @staticmethod
    def lookup(value, lookup_table: Dict):
        """Lookup value in table"""
        return lookup_table.get(str(value), value)
    
    @staticmethod
    def default(value, default_value):
        """Return default if value is None/empty"""
        return value if value else default_value
    
    @staticmethod
    def upper(value):
        """Convert to uppercase"""
        return str(value).upper()
    
    @staticmethod
    def lower(value):
        """Convert to lowercase"""
        return str(value).lower()
    
    @staticmethod
    def trim(value):
        """Trim whitespace"""
        return str(value).strip()
    
    @staticmethod
    def replace(value, old, new):
        """Replace substring"""
        return str(value).replace(old, new)
    
    @staticmethod
    def split(value, delimiter):
        """Split string"""
        return str(value).split(delimiter)
    
    @staticmethod
    def regex_extract(value, pattern, group=0):
        """Extract using regex"""
        match = re.search(pattern, str(value))
        return match.group(group) if match else None
    
    @staticmethod
    def math_operation(value, operation, operand):
        """Math operations: add, subtract, multiply, divide"""
        ops = {
            'add': lambda x, y: x + y,
            'subtract': lambda x, y: x - y,
            'multiply': lambda x, y: x * y,
            'divide': lambda x, y: x / y if y != 0 else None
        }
        try:
            num_value = Decimal(str(value))
            num_operand = Decimal(str(operand))
            result = ops[operation](num_value, num_operand)
            return float(result) if result is not None else None
        except:
            return None
    
    @staticmethod
    def conditional(value, condition, true_value, false_value):
        """Conditional: if condition then true_value else false_value"""
        # Simple condition evaluation
        if condition == 'is_empty':
            return false_value if value else true_value
        elif condition == 'is_not_empty':
            return true_value if value else false_value
        elif condition.startswith('equals:'):
            target = condition.split(':', 1)[1]
            return true_value if str(value) == target else false_value
        elif condition.startswith('contains:'):
            target = condition.split(':', 1)[1]
            return true_value if target in str(value) else false_value
        else:
            return false_value


class MappingRule:
    """Single mapping rule"""
    
    def __init__(self, rule_data: Dict):
        self.id = rule_data.get('id')
        self.source = rule_data.get('source')  # Source path(s)
        self.target = rule_data.get('target')  # Target path(s)
        self.transformation = rule_data.get('transformation', {})
        self.condition = rule_data.get('condition')
        self.cardinality_handling = rule_data.get('cardinality_handling', 'direct')
        self.enabled = rule_data.get('enabled', True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'source': self.source,
            'target': self.target,
            'transformation': self.transformation,
            'condition': self.condition,
            'cardinality_handling': self.cardinality_handling,
            'enabled': self.enabled
        }


class MappingDefinition:
    """Complete mapping definition"""
    
    def __init__(self, name: str):
        self.name = name
        self.input_schema = None
        self.output_schema = None
        self.rules: List[MappingRule] = []
        self.lookup_tables: Dict[str, Dict] = {}
        self.metadata = {}
    
    def add_rule(self, rule: MappingRule):
        """Add mapping rule"""
        self.rules.append(rule)
    
    def remove_rule(self, rule_id: str):
        """Remove mapping rule"""
        self.rules = [r for r in self.rules if r.id != rule_id]
    
    def to_dict(self):
        return {
            'name': self.name,
            'input_schema': self.input_schema,
            'output_schema': self.output_schema,
            'rules': [r.to_dict() for r in self.rules],
            'lookup_tables': self.lookup_tables,
            'metadata': self.metadata
        }
    
    def save(self, filepath: str):
        """Save mapping to JSON"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
    
    @classmethod
    def load(cls, filepath: str) -> 'MappingDefinition':
        """Load mapping from JSON"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        mapping = cls(data['name'])
        mapping.input_schema = data.get('input_schema')
        mapping.output_schema = data.get('output_schema')
        mapping.lookup_tables = data.get('lookup_tables', {})
        mapping.metadata = data.get('metadata', {})
        
        for rule_data in data.get('rules', []):
            mapping.add_rule(MappingRule(rule_data))
        
        return mapping


class MappingEngine:
    """Execute mappings"""
    
    def __init__(self, mapping_def: MappingDefinition):
        self.mapping = mapping_def
        self.transforms = TransformationLibrary()
        self.errors = []
        self.warnings = []
    
    def execute(self, input_data: Dict) -> Dict:
        """Execute mapping on input data"""
        self.errors = []
        self.warnings = []
        
        output_data = {}
        
        for rule in self.mapping.rules:
            if not rule.enabled:
                continue
            
            try:
                # Get source value(s)
                source_values = self._get_source_values(input_data, rule.source)
                
                # Check condition
                if rule.condition and not self._evaluate_condition(rule.condition, source_values):
                    continue
                
                # Apply transformation
                transformed_values = self._apply_transformation(
                    source_values, 
                    rule.transformation
                )
                
                # Handle cardinality
                final_values = self._handle_cardinality(
                    transformed_values,
                    rule.cardinality_handling
                )
                
                # Set target value(s)
                self._set_target_values(output_data, rule.target, final_values)
            
            except Exception as e:
                self.errors.append({
                    'rule_id': rule.id,
                    'error': str(e)
                })
        
        return output_data
    
    def _get_source_values(self, data: Dict, source_spec) -> Any:
        """Get value(s) from source path(s)"""
        if isinstance(source_spec, str):
            return self._get_nested_value(data, source_spec)
        elif isinstance(source_spec, list):
            return [self._get_nested_value(data, path) for path in source_spec]
        else:
            return source_spec  # Constant value
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Get nested value by path (e.g., 'a.b.c')"""
        parts = path.split('.')
        value = data
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, list):
                # Handle array access
                if part.isdigit():
                    idx = int(part)
                    value = value[idx] if idx < len(value) else None
                else:
                    # Get field from all array items
                    value = [item.get(part) for item in value if isinstance(item, dict)]
            else:
                return None
            
            if value is None:
                return None
        
        return value
    
    def _evaluate_condition(self, condition: Dict, values: Any) -> bool:
        """Evaluate condition"""
        cond_type = condition.get('type')
        
        if cond_type == 'exists':
            return values is not None and values != ''
        
        elif cond_type == 'equals':
            return str(values) == str(condition.get('value'))
        
        elif cond_type == 'contains':
            return condition.get('value') in str(values)
        
        elif cond_type == 'greater_than':
            try:
                return float(values) > float(condition.get('value'))
            except:
                return False
        
        elif cond_type == 'regex':
            pattern = condition.get('pattern')
            return re.match(pattern, str(values)) is not None
        
        elif cond_type == 'custom':
            # Evaluate custom expression
            expr = condition.get('expression')
            try:
                # Safe eval with limited context
                return eval(expr, {'value': values, '__builtins__': {}})
            except:
                return False
        
        return True
    
    def _apply_transformation(self, values: Any, transformation: Dict) -> Any:
        """Apply transformation"""
        if not transformation:
            return values
        
        trans_type = transformation.get('type')
        
        if trans_type == 'direct':
            return values
        
        elif trans_type == 'function':
            func_name = transformation.get('function')
            params = transformation.get('params', {})
            
            if hasattr(self.transforms, func_name):
                func = getattr(self.transforms, func_name)
                
                # Handle lookup tables
                if func_name == 'lookup' and 'table' in params:
                    table_name = params['table']
                    lookup_table = self.mapping.lookup_tables.get(table_name, {})
                    return func(values, lookup_table)
                else:
                    return func(values, **params)
        
        elif trans_type == 'template':
            template = transformation.get('template')
            return template.format(value=values)
        
        elif trans_type == 'constant':
            return transformation.get('value')
        
        elif trans_type == 'script':
            # Execute custom Python script
            script = transformation.get('script')
            try:
                local_vars = {'value': values, 'transforms': self.transforms}
                exec(script, {'__builtins__': {}}, local_vars)
                return local_vars.get('result', values)
            except Exception as e:
                self.warnings.append(f"Script execution failed: {e}")
                return values
        
        return values
    
    def _handle_cardinality(self, values: Any, handling: str) -> Any:
        """Handle cardinality differences"""
        if handling == 'direct':
            return values
        
        elif handling == 'first':
            # Take first item from array
            return values[0] if isinstance(values, list) and values else values
        
        elif handling == 'last':
            # Take last item from array
            return values[-1] if isinstance(values, list) and values else values
        
        elif handling == 'join':
            # Join array into string
            separator = ','
            return separator.join(str(v) for v in values) if isinstance(values, list) else values
        
        elif handling == 'sum':
            # Sum numeric values
            if isinstance(values, list):
                try:
                    return sum(float(v) for v in values if v is not None)
                except:
                    return 0
            return values
        
        elif handling == 'count':
            # Count items
            return len(values) if isinstance(values, list) else 1
        
        return values
    
    def _set_target_values(self, data: Dict, target_spec, values: Any):
        """Set value(s) to target path(s)"""
        if isinstance(target_spec, str):
            self._set_nested_value(data, target_spec, values)
        elif isinstance(target_spec, list):
            # Multiple targets
            if isinstance(values, list) and len(values) == len(target_spec):
                for target, value in zip(target_spec, values):
                    self._set_nested_value(data, target, value)
            else:
                # Same value to all targets
                for target in target_spec:
                    self._set_nested_value(data, target, values)
    
    def _set_nested_value(self, data: Dict, path: str, value: Any):
        """Set nested value by path"""
        parts = path.split('.')
        current = data
        
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                # Check if next part is array index
                next_part = parts[i + 1]
                if next_part.isdigit():
                    current[part] = []
                else:
                    current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value


# Quick test
if __name__ == '__main__':
    # Example mapping
    mapping = MappingDefinition("TestMapping")
    
    # Add simple rule
    mapping.add_rule(MappingRule({
        'id': 'rule_001',
        'source': 'invoice.number',
        'target': 'Invoice.ID',
        'transformation': {'type': 'direct'}
    }))
    
    # Add transformation rule
    mapping.add_rule(MappingRule({
        'id': 'rule_002',
        'source': 'invoice.date',
        'target': 'Invoice.IssueDate',
        'transformation': {
            'type': 'function',
            'function': 'format_date',
            'params': {'from_format': '%d/%m/%Y', 'to_format': '%Y-%m-%d'}
        }
    }))
    
    print("Mapping Engine initialized with 2 rules")
