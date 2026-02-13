#!/usr/bin/env python3
"""
Enhanced XML/JSON Parser for Preview
Supports XPath and JSONPath extraction for hover popup
"""

import json
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List


class PreviewExtractor:
    """Extract preview values from XML/JSON examples"""
    
    @staticmethod
    def extract_xml_value(xml_content: str, xpath: str, field_name: str) -> Dict[str, Any]:
        """
        Extract value from XML using XPath
        Returns: {
            'value': extracted_value,
            'context': surrounding_xml,
            'highlight_start': int,
            'highlight_end': int
        }
        """
        try:
            # Parse XML
            root = ET.fromstring(xml_content)
            
            # Remove namespaces for easier XPath
            # Convert {namespace}tag to tag
            for elem in root.iter():
                if '}' in elem.tag:
                    elem.tag = elem.tag.split('}')[1]
            
            # Normalize XPath (remove leading slash if present)
            xpath_normalized = xpath.lstrip('/')
            
            # Find element
            # Convert path like "Invoice/InvoiceLines/Line/Price" to XPath
            parts = xpath_normalized.split('/')
            
            element = root
            for part in parts:
                # Handle array notation: Line[0] -> Line
                if '[' in part:
                    part = part.split('[')[0]
                
                found = element.find(part)
                if found is None:
                    # Try to find in children
                    found = element.find(f".//{part}")
                
                if found is None:
                    return {
                        'value': None,
                        'error': f'Path not found: {part}',
                        'context': None
                    }
                
                element = found
            
            # Get value
            value = element.text if element.text else ''
            
            # Get context (parent + siblings)
            parent = None
            for p in root.iter():
                if element in list(p):
                    parent = p
                    break
            
            if parent is not None:
                context_xml = ET.tostring(parent, encoding='unicode')
            else:
                context_xml = ET.tostring(element, encoding='unicode')
            
            # Find highlight position in context
            element_xml = ET.tostring(element, encoding='unicode')
            highlight_start = context_xml.find(element_xml)
            highlight_end = highlight_start + len(element_xml) if highlight_start >= 0 else -1
            
            return {
                'value': value,
                'context': context_xml,
                'highlight_start': highlight_start,
                'highlight_end': highlight_end,
                'element_xml': element_xml
            }
            
        except Exception as e:
            return {
                'value': None,
                'error': str(e),
                'context': None
            }
    
    @staticmethod
    def extract_json_value(json_content: str, json_path: str, field_name: str) -> Dict[str, Any]:
        """
        Extract value from JSON using JSONPath-like syntax
        Supports: $.Invoice.Lines[0].Price
        """
        try:
            data = json.loads(json_content)
            
            # Parse path: $.Invoice.Lines[0].Price
            path = json_path.lstrip('$').lstrip('.')
            parts = []
            
            # Split by . but handle array notation
            current = ""
            for char in path:
                if char == '.':
                    if current:
                        parts.append(current)
                        current = ""
                elif char == '[':
                    if current:
                        parts.append(current)
                        current = ""
                    current = "["
                elif char == ']':
                    current += "]"
                    parts.append(current)
                    current = ""
                else:
                    current += char
            
            if current:
                parts.append(current)
            
            # Navigate through data
            current_data = data
            for part in parts:
                if part.startswith('[') and part.endswith(']'):
                    # Array index
                    index = int(part[1:-1])
                    if isinstance(current_data, list):
                        current_data = current_data[index]
                    else:
                        return {
                            'value': None,
                            'error': f'Expected array but got {type(current_data).__name__}',
                            'context': None
                        }
                else:
                    # Object key
                    if isinstance(current_data, dict):
                        if part in current_data:
                            current_data = current_data[part]
                        else:
                            return {
                                'value': None,
                                'error': f'Key not found: {part}',
                                'context': None
                            }
                    else:
                        return {
                            'value': None,
                            'error': f'Expected object but got {type(current_data).__name__}',
                            'context': None
                        }
            
            # Get context (parent object)
            # Navigate to parent
            parent_data = data
            for part in parts[:-1]:
                if part.startswith('['):
                    index = int(part[1:-1])
                    parent_data = parent_data[index]
                else:
                    parent_data = parent_data[part]
            
            context_json = json.dumps(parent_data, indent=2)
            
            # Find highlight position
            value_str = json.dumps(current_data) if not isinstance(current_data, str) else current_data
            highlight_start = context_json.find(str(value_str))
            highlight_end = highlight_start + len(str(value_str)) if highlight_start >= 0 else -1
            
            return {
                'value': current_data,
                'context': context_json,
                'highlight_start': highlight_start,
                'highlight_end': highlight_end
            }
            
        except Exception as e:
            return {
                'value': None,
                'error': str(e),
                'context': None
            }
    
    @staticmethod
    def format_context_with_highlight(context: str, start: int, end: int, 
                                     max_lines: int = 11) -> tuple:
        """
        Format context with line highlighting
        Returns: (lines_array, highlight_line_index)
        """
        if not context:
            return [], -1
        
        lines = context.split('\n')
        
        # Find which line contains the highlight
        char_count = 0
        highlight_line = -1
        
        for i, line in enumerate(lines):
            line_start = char_count
            line_end = char_count + len(line)
            
            if line_start <= start < line_end:
                highlight_line = i
                break
            
            char_count += len(line) + 1  # +1 for newline
        
        # Get context lines around highlight
        if highlight_line >= 0:
            context_start = max(0, highlight_line - 5)
            context_end = min(len(lines), highlight_line + 6)
            context_lines = lines[context_start:context_end]
            relative_highlight = highlight_line - context_start
        else:
            # No highlight found, show first N lines
            context_lines = lines[:max_lines]
            relative_highlight = -1
        
        return context_lines, relative_highlight


# API endpoint for preview extraction
def extract_preview_value(example_content: str, 
                         field_path: str, 
                         field_name: str,
                         format_type: str = 'xml') -> Dict[str, Any]:
    """
    Main API function for preview extraction
    
    Args:
        example_content: XML/JSON content
        field_path: XPath for XML or JSONPath for JSON
        field_name: Field name for display
        format_type: 'xml' or 'json'
    
    Returns:
        {
            'value': extracted_value,
            'context_lines': [...],
            'highlight_line': int,
            'error': str or None
        }
    """
    extractor = PreviewExtractor()
    
    if format_type == 'xml':
        result = extractor.extract_xml_value(example_content, field_path, field_name)
    elif format_type == 'json':
        result = extractor.extract_json_value(example_content, field_path, field_name)
    else:
        return {'error': f'Unsupported format: {format_type}'}
    
    if result.get('error'):
        return result
    
    # Format context with highlighting
    context_lines, highlight_line = extractor.format_context_with_highlight(
        result.get('context', ''),
        result.get('highlight_start', -1),
        result.get('highlight_end', -1)
    )
    
    return {
        'value': result.get('value'),
        'context_lines': context_lines,
        'highlight_line': highlight_line,
        'element_xml': result.get('element_xml'),
        'error': None
    }


# Test
if __name__ == '__main__':
    # Test XML
    xml_test = """<?xml version="1.0"?>
<Invoice>
    <InvoiceNumber>INV-001</InvoiceNumber>
    <Date>2024-01-15</Date>
    <Lines>
        <Line>
            <Description>Product A</Description>
            <Quantity>5</Quantity>
            <Price>100.00</Price>
        </Line>
    </Lines>
    <Total>500.00</Total>
</Invoice>"""
    
    result = extract_preview_value(xml_test, '/Invoice/Lines/Line/Price', 'Price', 'xml')
    print("XML Test:")
    print(f"  Value: {result['value']}")
    print(f"  Highlight line: {result['highlight_line']}")
    print(f"  Context: {len(result['context_lines'])} lines")
    
    # Test JSON
    json_test = """{
    "Invoice": {
        "InvoiceNumber": "INV-001",
        "Date": "2024-01-15",
        "Lines": [
            {
                "Description": "Product A",
                "Quantity": 5,
                "Price": 100.00
            }
        ],
        "Total": 500.00
    }
}"""
    
    result = extract_preview_value(json_test, '$.Invoice.Lines[0].Price', 'Price', 'json')
    print("\nJSON Test:")
    print(f"  Value: {result['value']}")
    print(f"  Highlight line: {result['highlight_line']}")
    print(f"  Context: {len(result['context_lines'])} lines")
