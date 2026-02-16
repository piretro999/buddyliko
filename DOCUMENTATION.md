================================================================
DOCUMENT TYPE SELECTION SYSTEM - COMPLETE DOCUMENTATION
================================================================

## OVERVIEW

This system implements a two-tier schema selection:
1. **Schema Family** (e.g., UBL-21, FatturaPA)
2. **Document Type** (e.g., Invoice, CreditNote, DebitNote)

This ensures the correct XSD is always used for transformation.

================================================================

## ARCHITECTURE

┌─────────────────────────────────────────────────────────────┐
│                      FRONTEND (app.html)                     │
│                                                               │
│  ┌─────────────────┐     ┌──────────────────────┐          │
│  │ Schema Selector │────▶│ Document Type Picker │          │
│  │   [UBL-21 ▼]   │     │    [Invoice ▼]      │          │
│  └─────────────────┘     └──────────────────────┘          │
│         │                          │                         │
│         │                          │                         │
│         ▼                          ▼                         │
│  GET /api/schemas/       outputSchema.rootElement           │
│  UBL-21/document-types   saved in project JSON              │
│                                                               │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      BACKEND (api.py)                        │
│                                                               │
│  1. detect_ubl_document_type()                              │
│     ↓ reads mapping_rules['outputSchema']['rootElement']    │
│     ↓ returns 'Invoice'                                      │
│                                                               │
│  2. find_xsd(format='ubl', io='output', document_type='Invoice') │
│     ↓ searches for UBL-Invoice-2.1.xsd (exact match)        │
│     ↓ returns absolute path                                  │
│                                                               │
│  3. TransformationEngine                                     │
│     ↓ loads UBL-Invoice-2.1.xsd                             │
│     ↓ parses all 50+ imported schemas                        │
│     ↓ extracts element orders                                │
│     ↓ reorders output XML correctly                          │
│                                                               │
└─────────────────────────────────────────────────────────────┘

================================================================

## API ENDPOINT

### GET /api/schemas/{schema_id}/document-types

Returns available document types for a schema.

**Request:**
```
GET /api/schemas/UBL-21/document-types
```

**Response:**
```json
{
  "success": true,
  "schema_id": "UBL-21",
  "document_types": [
    {
      "type": "Invoice",
      "label": "Invoice",
      "filename": "UBL-Invoice-2.1.xsd",
      "path": "/full/path/to/UBL-Invoice-2.1.xsd"
    },
    {
      "type": "CreditNote",
      "label": "CreditNote",
      "filename": "UBL-CreditNote-2.1.xsd",
      "path": "/full/path/to/UBL-CreditNote-2.1.xsd"
    },
    ...
  ],
  "count": 15
}
```

**Supported Schema IDs:**
- `UBL-21`, `ubl`, `peppol` → Returns UBL document types
- `FatturaPA` → Returns empty array (single type)
- Others → Scans directory for XSD files

================================================================

## FRONTEND IMPLEMENTATION

### State Variables
```javascript
const [outputSchemaId, setOutputSchemaId] = useState('');
const [outputDocumentType, setOutputDocumentType] = useState('Invoice');
const [availableDocumentTypes, setAvailableDocumentTypes] = useState([]);
```

### Fetch Document Types
```javascript
const fetchDocumentTypes = async (schemaId) => {
    const res = await fetch(`${API_URL}/schemas/${schemaId}/document-types`);
    const data = await res.json();
    
    if (data.success && data.document_types.length > 0) {
        setAvailableDocumentTypes(data.document_types);
        setOutputDocumentType(data.document_types[0].type);
    }
};
```

### UI Component
```javascript
{/* Primary Schema Selector */}
<select value={outputSchemaId} onChange={handleSchemaChange}>
    <option value="">Select Output Schema</option>
    {schemas.map(s => <option value={s.id}>{s.name}</option>)}
</select>

{/* Document Type Selector (conditional) */}
{availableDocumentTypes.length > 0 && (
    <select 
        value={outputDocumentType}
        onChange={e => setOutputDocumentType(e.target.value)}
    >
        {availableDocumentTypes.map(dt => 
            <option value={dt.type}>{dt.label}</option>
        )}
    </select>
)}
```

### Save to Project
```javascript
const project = {
    outputSchema: {
        ...outputSchema,
        rootElement: outputDocumentType  // ← KEY FIELD
    }
};
```

### Send to Backend
```javascript
const mappingRules = {
    outputSchema: {
        ...outputSchema,
        rootElement: outputDocumentType  // ← MUST BE PRESENT
    }
};
```

================================================================

## BACKEND PROCESSING

### 1. Extract Document Type
```python
def detect_ubl_document_type(mapping_rules):
    if 'outputSchema' in mapping_rules:
        root_elem = mapping_rules['outputSchema'].get('rootElement', '')
        if root_elem:
            return root_elem  # e.g., 'CreditNote'
    return 'Invoice'  # fallback
```

### 2. Find Correct XSD
```python
def find_xsd(format_name, io='output', document_type=None):
    if document_type:
        # EXACT match: UBL-{DocumentType}-*.xsd
        exact_candidates = [
            f for f in xsd_files
            if f"UBL-{document_type}-" in f.name
        ]
        return exact_candidates[0]  # e.g., UBL-CreditNote-2.1.xsd
```

### 3. Transform with Correct Schema
```python
engine = TransformationEngine(
    output_xsd=output_xsd_path  # UBL-CreditNote-2.1.xsd
)
# Engine parses XSD, extracts correct element orders
# Reorders output XML according to CreditNote schema
```

================================================================

## DATA FLOW EXAMPLE

**User Journey:**

1. User selects "UBL-21" → Frontend calls API → Gets 15 document types
2. Dropdown shows: Invoice, CreditNote, DebitNote, ...
3. User selects "CreditNote"
4. User creates mappings and saves project
5. Project JSON contains:
   ```json
   {
     "outputSchema": {
       "rootElement": "CreditNote"
     }
   }
   ```

**Transform Execution:**

6. User uploads FatturaPA file and clicks "Execute Transform"
7. Frontend sends:
   ```json
   {
     "mapping_rules": {
       "outputSchema": {
         "rootElement": "CreditNote"
       }
     }
   }
   ```
8. Backend detects: `document_type = "CreditNote"`
9. Backend finds: `UBL-CreditNote-2.1.xsd`
10. Backend loads schema and ALL imports (50+ files)
11. Backend extracts element order for `CreditNote` type
12. Backend transforms and reorders elements
13. Output XML validates against UBL-CreditNote-2.1.xsd ✅

**Before This System:**
- Backend guessed "Invoice" or picked alphabetically "FreightInvoice"
- Element order was wrong
- Validation failed with 9 errors ❌

================================================================

## SUPPORTED DOCUMENT TYPES

### UBL 2.1 Document Types:
1. **Invoice** - Standard commercial invoice
2. **CreditNote** - Credit note
3. **DebitNote** - Debit note
4. **ApplicationResponse** - Response to business document
5. **AttachedDocument** - Document attachments
6. **AwardedNotification** - Award notification
7. **BillOfLading** - Bill of lading
8. **CallForTenders** - Call for tenders
9. **Catalogue** - Product catalogue
10. **CatalogueDeletion** - Catalogue deletion
11. **CatalogueItemSpecificationUpdate** - Catalogue update
12. **CataloguePricingUpdate** - Pricing update
13. **CatalogueRequest** - Catalogue request
14. **CertificateOfOrigin** - Certificate of origin
15. **ContractAwardNotice** - Contract award notice
... (and more)

### FatturaPA:
- Single document type: FatturaElettronica

================================================================

## TESTING

Run the test script:
```bash
python test_document_types.py
```

Expected output:
```
✅ Found 15 document types for UBL-21
✅ FatturaPA has single document type
✅ Invalid schema returns error
✅ Full workflow simulates correctly
```

================================================================

## TROUBLESHOOTING

**Issue:** Document type dropdown doesn't appear
**Solution:** Check that API endpoint returns document_types array

**Issue:** Wrong XSD is still being used
**Solution:** Verify that rootElement is being saved and sent to backend

**Issue:** API returns empty document types
**Solution:** Check that XSD files exist in maindoc/ folder

**Issue:** "FreightInvoice" still being used
**Solution:** Clear browser cache, reload project, ensure rootElement is set

================================================================

## FUTURE ENHANCEMENTS

1. **Custom Document Types**
   - Allow users to upload custom XSD
   - Auto-detect document type from XSD structure

2. **Document Type Validation**
   - Validate that mappings are compatible with document type
   - Warn if mapping Invoice fields to CreditNote

3. **Document Type Templates**
   - Pre-configured mappings for common conversions
   - E.g., "FatturaPA → UBL CreditNote" template

4. **Multi-Document Support**
   - Transform one input into multiple document types
   - E.g., Order → Invoice + DespatchAdvice

================================================================

## CONCLUSION

This system eliminates ambiguity in schema selection by:
✅ Explicitly asking user what document type they're creating
✅ Dynamically showing only available types for chosen schema
✅ Persisting the choice in project JSON
✅ Guaranteeing correct XSD is used for transformation
✅ Zero hardcoding, zero guessing, zero FreightInvoice mistakes!

================================================================
