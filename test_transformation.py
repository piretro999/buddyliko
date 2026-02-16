"""
Test script per debuggare la trasformazione
DA ESEGUIRE SULLA TUA MACCHINA dove hai il backend
Version: 20260216_113000
"""
import json
import sys
import os
from pathlib import Path

# Fix encoding per Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Carica il progetto
project_file = 'MappaFPAUBL2_1_project_complete.json'
if not os.path.exists(project_file):
    print(f"ERRORE: File {project_file} non trovato!")
    print(f"Posizione corrente: {os.getcwd()}")
    sys.exit(1)

with open(project_file, 'r', encoding='utf-8') as f:
    project = json.load(f)

print("="*80)
print("TEST TRANSFORMATION ENGINE")
print("="*80)

# Simula un file FatturaPA XML dal file reale
input_file = 'IT01234567890_FPA01.xml'
if not os.path.exists(input_file):
    print(f"ERRORE: File input {input_file} non trovato!")
    print("Uso XML di test minimale...")
    test_xml = """<?xml version="1.0" encoding="UTF-8"?>
<FatturaElettronica xmlns="http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2">
  <FatturaElettronicaHeader>
    <DatiTrasmissione>
      <CodiceDestinatario>AAAAAA</CodiceDestinatario>
    </DatiTrasmissione>
    <CedentePrestatore>
      <Sede>
        <Indirizzo>Via Roma</Indirizzo>
        <NumeroCivico>123</NumeroCivico>
        <CAP>00100</CAP>
        <Comune>Roma</Comune>
        <Nazione>IT</Nazione>
      </Sede>
    </CedentePrestatore>
  </FatturaElettronicaHeader>
  <FatturaElettronicaBody>
    <DatiGenerali>
      <DatiGeneraliDocumento>
        <Data>2017-01-18</Data>
        <Numero>123</Numero>
      </DatiGeneraliDocumento>
    </DatiGenerali>
    <DatiBeniServizi>
      <DatiRiepilogo>
        <ImponibileImporto>5.00</ImponibileImporto>
        <Imposta>1.10</Imposta>
      </DatiRiepilogo>
    </DatiBeniServizi>
  </FatturaElettronicaBody>
</FatturaElettronica>
"""
else:
    with open(input_file, 'r', encoding='utf-8') as f:
        test_xml = f.read()

print("\n[INPUT XML]")
print(f"Length: {len(test_xml)} chars")
print(f"First 300 chars: {test_xml[:300]}")

# Importa il transformation engine
sys.path.insert(0, '.')
try:
    from transformation_engine import TransformationEngine
    print("\n[IMPORT] transformation_engine importato con successo")
except Exception as e:
    print(f"\n[ERRORE] Impossibile importare transformation_engine: {e}")
    sys.exit(1)

# Crea engine
print("\n[ENGINE] Creazione TransformationEngine...")
engine = TransformationEngine()

# Prepara mapping rules
mapping_rules = {
    'connections': project['connections'],
    'inputSchema': project['inputSchema'],
    'outputSchema': project['outputSchema']
}

print(f"\n[MAPPING RULES]")
print(f"  Connections: {len(mapping_rules['connections'])}")
print(f"  Input schema fields: {len(mapping_rules['inputSchema']['fields'])}")
print(f"  Output schema fields: {len(mapping_rules['outputSchema']['fields'])}")

# Mostra prime 5 connections
print(f"\n[FIRST 5 CONNECTIONS]")
for i, conn in enumerate(mapping_rules['connections'][:5]):
    print(f"  {i+1}. {conn.get('source')} -> {conn.get('target')}")
    print(f"     SourcePath: {conn.get('sourcePath')}")
    print(f"     TargetPath: {conn.get('targetPath')}")

# Test trasformazione
print("\n" + "="*80)
print("ESECUZIONE TRASFORMAZIONE")
print("="*80)

try:
    result = engine.transform(
        input_content=test_xml,
        input_format='xml',
        output_format='xml',
        mapping_rules=mapping_rules,
        validate_input=False,
        validate_output=False
    )
    
    print(f"\n[RESULT] Success: {result.success}")
    
    if result.output_content:
        print(f"\n[OUTPUT XML] Length: {len(result.output_content)} chars")
        print("\n[OUTPUT XML PREVIEW - First 100 lines]:")
        lines = result.output_content.split('\n')
        for i, line in enumerate(lines[:100]):
            print(f"{i+1:3d}: {line}")
        
        if len(lines) > 100:
            print(f"\n... ({len(lines) - 100} more lines)")
        
        # Conta elementi nell'output
        import re
        elements = re.findall(r'<([^/\s>]+)', result.output_content)
        unique_elements = set(elements)
        print(f"\n[ELEMENTS] Found {len(elements)} total elements")
        print(f"[ELEMENTS] Unique: {sorted(unique_elements)}")
        
        # Verifica campi critici
        critical_fields = ['IssueDate', 'TaxAmount', 'PriceAmount', 'ID']
        print(f"\n[CRITICAL FIELDS CHECK]")
        for field in critical_fields:
            if f'cbc:{field}' in result.output_content or f'<{field}>' in result.output_content:
                print(f"  OK {field}: PRESENTE")
            else:
                print(f"  XX {field}: MANCANTE")
        
    else:
        print("\n[ERRORE] output_content e' None o vuoto!")
    
    if result.validation_errors:
        print(f"\n[VALIDATION ERRORS] {len(result.validation_errors)} errors:")
        for err in result.validation_errors[:10]:
            print(f"  - {err}")
    
    if result.transformation_errors:
        print(f"\n[TRANSFORMATION ERRORS] {len(result.transformation_errors)} errors:")
        for err in result.transformation_errors:
            print(f"  - {err}")
    
    if result.warnings:
        print(f"\n[WARNINGS] {len(result.warnings)} warnings:")
        for warn in result.warnings[:10]:
            print(f"  - {warn}")
        
except Exception as e:
    print(f"\n[EXCEPTION] {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("TEST COMPLETATO")
print("="*80)
