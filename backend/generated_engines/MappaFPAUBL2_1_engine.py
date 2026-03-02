#!/usr/bin/env python3
"""
MappaFPAUBL2.1 - Buddyliko Internal Engine Module
Generated: 2026-02-24 19:00:23 UTC
Connessioni: 60

Importato da Buddyliko come motore ad alta performance.
NON modificare manualmente: rigenerare dalla mappa.
"""

import json
import re
import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# Metadati mappatura
MAPPING_METADATA = {
    "name": 'MappaFPAUBL2.1',
    "generated": '2026-02-24 19:00:23 UTC',
    "connections": 60,
    "input_schema": 'tmpzxhbqn8h',
    "output_schema": 'tmppv8xko6g',
}


def _get(data: Any, path: str, default=None) -> Any:
    if data is None or not path:
        return default
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip() and not p.strip().startswith('@')]
    parts = [p.split(':')[-1] if ':' in p else p for p in parts]
    current = data
    for part in parts:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                found = False
                for k in current:
                    if (k.split(':')[-1] if ':' in k else k) == part:
                        current = current[k]; found = True; break
                if not found:
                    if len(current) == 1:
                        current = list(current.values())[0]
                        if isinstance(current, dict) and part in current:
                            current = current[part]
                        else:
                            return default
                    else:
                        return default
        elif isinstance(current, list):
            current = current[0] if current else default
            if isinstance(current, dict):
                current = current.get(part, default)
            else:
                return default
        else:
            return default
    return current


def _set(data: Dict, path: str, value: Any):
    if not path or value is None:
        return
    sep = '/' if '/' in path else '.'
    parts = [p.strip() for p in path.split(sep) if p.strip() and not p.strip().startswith('@')]
    parts = [p.split(':')[-1] if ':' in p else p for p in parts]
    current = data
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    if parts:
        current[parts[-1]] = value


def transform_record(input_data: Dict) -> Tuple[Dict, List[str]]:
    """
    Trasforma un singolo record. Ritorna (output, warnings).
    Ottimizzata: nessun loop, nessuna lookup della mappa.
    """
    output: Dict = {}
    warnings: List[str] = []


    # [1] FatturaElettronicaHeader/DatiTrasmissione/CodiceDestinatario → cac:AccountingCustomerParty/cac:Party/cbc:EndpointID/@schemeID
    try:
        _v0 = _get(input_data, 'FatturaElettronicaHeader/DatiTrasmissione/CodiceDestinatario')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cbc:EndpointID/@schemeID', _v0)
    except Exception as _e0:
        warnings.append(f'Connection 1 failed: {_e0}')

    # [2] FatturaElettronicaHeader/CedentePrestatore/Sede/Indirizzo → cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:StreetName
    try:
        _v1 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Sede/Indirizzo')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:StreetName', _v1)
    except Exception as _e1:
        warnings.append(f'Connection 2 failed: {_e1}')

    # [3] FatturaElettronicaHeader/CedentePrestatore/Sede/NumeroCivico → cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:AdditionalStreetName
    try:
        _v2 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Sede/NumeroCivico')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:AdditionalStreetName', _v2)
    except Exception as _e2:
        warnings.append(f'Connection 3 failed: {_e2}')

    # [4] FatturaElettronicaHeader/CedentePrestatore/Sede/CAP → cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:PostalZone
    try:
        _v3 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Sede/CAP')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:PostalZone', _v3)
    except Exception as _e3:
        warnings.append(f'Connection 4 failed: {_e3}')

    # [5] FatturaElettronicaHeader/CedentePrestatore/Sede/Comune → cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:CityName
    try:
        _v4 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Sede/Comune')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:CityName', _v4)
    except Exception as _e4:
        warnings.append(f'Connection 5 failed: {_e4}')

    # [6] FatturaElettronicaHeader/CedentePrestatore/Sede/Provincia → cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:CountrySubentity
    try:
        _v5 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Sede/Provincia')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cbc:CountrySubentity', _v5)
    except Exception as _e5:
        warnings.append(f'Connection 6 failed: {_e5}')

    # [7] FatturaElettronicaHeader/CedentePrestatore/Sede/Nazione → cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cac:Country/cbc:IdentificationCode
    try:
        _v6 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Sede/Nazione')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:PostalAddress/cac:Country/cbc:IdentificationCode', _v6)
    except Exception as _e6:
        warnings.append(f'Connection 7 failed: {_e6}')

    # [8] FatturaElettronicaHeader/CedentePrestatore/Contatti/Telefono → cac:AccountingSupplierParty/cac:Party/cac:Contact/cbc:Telephone
    try:
        _v7 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Contatti/Telefono')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:Contact/cbc:Telephone', _v7)
    except Exception as _e7:
        warnings.append(f'Connection 8 failed: {_e7}')

    # [9] FatturaElettronicaHeader/CedentePrestatore/Contatti/Email → cac:AccountingSupplierParty/cac:Party/cac:Contact/cbc:ElectronicMail
    try:
        _v8 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/Contatti/Email')
        _set(output, 'cac:AccountingSupplierParty/cac:Party/cac:Contact/cbc:ElectronicMail', _v8)
    except Exception as _e8:
        warnings.append(f'Connection 9 failed: {_e8}')

    # [10] FatturaElettronicaHeader/CedentePrestatore/RiferimentoAmministrazione → cbc:AccountingCost
    try:
        _v9 = _get(input_data, 'FatturaElettronicaHeader/CedentePrestatore/RiferimentoAmministrazione')
        _set(output, 'cbc:AccountingCost', _v9)
    except Exception as _e9:
        warnings.append(f'Connection 10 failed: {_e9}')

    # [11] FatturaElettronicaHeader/CessionarioCommittente/DatiAnagrafici/CodiceFiscale → cac:AccountingCustomerParty/cac:Party/cac:PartyLegalEntity/cbc:CompanyID/@schemeID
    try:
        _v10 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/DatiAnagrafici/CodiceFiscale')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PartyLegalEntity/cbc:CompanyID/@schemeID', _v10)
    except Exception as _e10:
        warnings.append(f'Connection 11 failed: {_e10}')

    # [12] FatturaElettronicaHeader/CessionarioCommittente/DatiAnagrafici/Anagrafica/CodEORI → cac:AccountingCustomerParty/cac:Party/cac:PartyIdentification/cbc:ID
    try:
        _v11 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/DatiAnagrafici/Anagrafica/CodEORI')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PartyIdentification/cbc:ID', _v11)
    except Exception as _e11:
        warnings.append(f'Connection 12 failed: {_e11}')

    # [13] FatturaElettronicaHeader/CessionarioCommittente/Sede/Indirizzo → cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:StreetName
    try:
        _v12 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/Sede/Indirizzo')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:StreetName', _v12)
    except Exception as _e12:
        warnings.append(f'Connection 13 failed: {_e12}')

    # [14] FatturaElettronicaHeader/CessionarioCommittente/Sede/NumeroCivico → cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:AdditionalStreetName
    try:
        _v13 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/Sede/NumeroCivico')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:AdditionalStreetName', _v13)
    except Exception as _e13:
        warnings.append(f'Connection 14 failed: {_e13}')

    # [15] FatturaElettronicaHeader/CessionarioCommittente/Sede/CAP → cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:PostalZone
    try:
        _v14 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/Sede/CAP')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:PostalZone', _v14)
    except Exception as _e14:
        warnings.append(f'Connection 15 failed: {_e14}')

    # [16] FatturaElettronicaHeader/CessionarioCommittente/Sede/Comune → cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:CityName
    try:
        _v15 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/Sede/Comune')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:CityName', _v15)
    except Exception as _e15:
        warnings.append(f'Connection 16 failed: {_e15}')

    # [17] FatturaElettronicaHeader/CessionarioCommittente/Sede/Provincia → cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:CountrySubentity
    try:
        _v16 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/Sede/Provincia')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cbc:CountrySubentity', _v16)
    except Exception as _e16:
        warnings.append(f'Connection 17 failed: {_e16}')

    # [18] FatturaElettronicaHeader/CessionarioCommittente/Sede/Nazione → cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cac:Country/cbc:IdentificationCode
    try:
        _v17 = _get(input_data, 'FatturaElettronicaHeader/CessionarioCommittente/Sede/Nazione')
        _set(output, 'cac:AccountingCustomerParty/cac:Party/cac:PostalAddress/cac:Country/cbc:IdentificationCode', _v17)
    except Exception as _e17:
        warnings.append(f'Connection 18 failed: {_e17}')

    # [19] FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Divisa → cbc:DocumentCurrencyCode
    try:
        _v18 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Divisa')
        _set(output, 'cbc:DocumentCurrencyCode', _v18)
    except Exception as _e18:
        warnings.append(f'Connection 19 failed: {_e18}')

    # [20] FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data → cbc:IssueDate
    try:
        _v19 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data')
        _set(output, 'cbc:IssueDate', _v19)
    except Exception as _e19:
        warnings.append(f'Connection 20 failed: {_e19}')

    # [21] FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero → cbc:ID
    try:
        _v20 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero')
        _set(output, 'cbc:ID', _v20)
    except Exception as _e20:
        warnings.append(f'Connection 21 failed: {_e20}')

    # [22] FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento → cac:LegalMonetaryTotal/cbc:TaxInclusiveAmount
    try:
        _v21 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento')
        _set(output, 'cac:LegalMonetaryTotal/cbc:TaxInclusiveAmount', _v21)
    except Exception as _e21:
        warnings.append(f'Connection 22 failed: {_e21}')

    # [23] FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Arrotondamento → cac:LegalMonetaryTotal/cbc:PayableRoundingAmount
    try:
        _v22 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Arrotondamento')
        _set(output, 'cac:LegalMonetaryTotal/cbc:PayableRoundingAmount', _v22)
    except Exception as _e22:
        warnings.append(f'Connection 23 failed: {_e22}')

    # [24] FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Causale → cbc:Note
    try:
        _v23 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Causale')
        _set(output, 'cbc:Note', _v23)
    except Exception as _e23:
        warnings.append(f'Connection 24 failed: {_e23}')

    # [25] FatturaElettronicaBody/DatiGenerali/DatiRicezione/IdDocumento → cac:ReceiptDocumentReference/cbc:ID
    try:
        _v24 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiRicezione/IdDocumento')
        _set(output, 'cac:ReceiptDocumentReference/cbc:ID', _v24)
    except Exception as _e24:
        warnings.append(f'Connection 25 failed: {_e24}')

    # [26] FatturaElettronicaBody/DatiGenerali/DatiFattureCollegate/IdDocumento → cac:BillingReference/cac:InvoiceDocumentReference/cbc:ID
    try:
        _v25 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiFattureCollegate/IdDocumento')
        _set(output, 'cac:BillingReference/cac:InvoiceDocumentReference/cbc:ID', _v25)
    except Exception as _e25:
        warnings.append(f'Connection 26 failed: {_e25}')

    # [27] FatturaElettronicaBody/DatiGenerali/DatiFattureCollegate/Data → cac:BillingReference/cac:InvoiceDocumentReference/cbc:IssueDate
    try:
        _v26 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiFattureCollegate/Data')
        _set(output, 'cac:BillingReference/cac:InvoiceDocumentReference/cbc:IssueDate', _v26)
    except Exception as _e26:
        warnings.append(f'Connection 27 failed: {_e26}')

    # [28] FatturaElettronicaBody/DatiGenerali/DatiSAL/RiferimentoFase → cac:AdditionalDocumentReference/cbc:DocumentTypeCode
    try:
        _v27 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiSAL/RiferimentoFase')
        _set(output, 'cac:AdditionalDocumentReference/cbc:DocumentTypeCode', _v27)
    except Exception as _e27:
        warnings.append(f'Connection 28 failed: {_e27}')

    # [29] FatturaElettronicaBody/DatiGenerali/DatiDDT/NumeroDDT → cac:DespatchDocumentReference/cbc:ID
    try:
        _v28 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiDDT/NumeroDDT')
        _set(output, 'cac:DespatchDocumentReference/cbc:ID', _v28)
    except Exception as _e28:
        warnings.append(f'Connection 29 failed: {_e28}')

    # [30] FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Indirizzo → cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:StreetName
    try:
        _v29 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Indirizzo')
        _set(output, 'cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:StreetName', _v29)
    except Exception as _e29:
        warnings.append(f'Connection 30 failed: {_e29}')

    # [31] FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/NumeroCivico → cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:AdditionalStreetName
    try:
        _v30 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/NumeroCivico')
        _set(output, 'cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:AdditionalStreetName', _v30)
    except Exception as _e30:
        warnings.append(f'Connection 31 failed: {_e30}')

    # [32] FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/CAP → cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:PostalZone
    try:
        _v31 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/CAP')
        _set(output, 'cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:PostalZone', _v31)
    except Exception as _e31:
        warnings.append(f'Connection 32 failed: {_e31}')

    # [33] FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Comune → cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:CityName
    try:
        _v32 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Comune')
        _set(output, 'cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:CityName', _v32)
    except Exception as _e32:
        warnings.append(f'Connection 33 failed: {_e32}')

    # [34] FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Provincia → cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:CountrySubentity
    try:
        _v33 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Provincia')
        _set(output, 'cac:Delivery/cac:DeliveryLocation/cac:Address/cbc:CountrySubentity', _v33)
    except Exception as _e33:
        warnings.append(f'Connection 34 failed: {_e33}')

    # [35] FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Nazione → cac:Delivery/cac:DeliveryLocation/cac:Address/cac:Country/cbc:IdentificationCode
    try:
        _v34 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiTrasporto/IndirizzoResa/Nazione')
        _set(output, 'cac:Delivery/cac:DeliveryLocation/cac:Address/cac:Country/cbc:IdentificationCode', _v34)
    except Exception as _e34:
        warnings.append(f'Connection 35 failed: {_e34}')

    # [36] FatturaElettronicaBody/DatiGenerali/DatiTrasporto/DataOraConsegna → cac:Delivery/cbc:ActualDeliveryDate
    try:
        _v35 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiTrasporto/DataOraConsegna')
        _set(output, 'cac:Delivery/cbc:ActualDeliveryDate', _v35)
    except Exception as _e35:
        warnings.append(f'Connection 36 failed: {_e35}')

    # [37] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Descrizione → cac:InvoiceLine/cac:Item/cbc:Name
    try:
        _v36 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Descrizione')
        _set(output, 'cac:InvoiceLine/cac:Item/cbc:Name', _v36)
    except Exception as _e36:
        warnings.append(f'Connection 37 failed: {_e36}')

    # [38] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Quantita → cac:InvoiceLine/cbc:InvoicedQuantity
    try:
        _v37 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Quantita')
        _set(output, 'cac:InvoiceLine/cbc:InvoicedQuantity', _v37)
    except Exception as _e37:
        warnings.append(f'Connection 38 failed: {_e37}')

    # [39] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/UnitaMisura → cac:InvoiceLine/cbc:InvoicedQuantity/@unitCode
    try:
        _v38 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/UnitaMisura')
        _set(output, 'cac:InvoiceLine/cbc:InvoicedQuantity/@unitCode', _v38)
    except Exception as _e38:
        warnings.append(f'Connection 39 failed: {_e38}')

    # [40] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/DataInizioPeriodo → cac:InvoiceLine/cac:InvoicePeriod/cbc:StartDate
    try:
        _v39 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/DataInizioPeriodo')
        _set(output, 'cac:InvoiceLine/cac:InvoicePeriod/cbc:StartDate', _v39)
    except Exception as _e39:
        warnings.append(f'Connection 40 failed: {_e39}')

    # [41] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/DataFinePeriodo → cac:InvoiceLine/cac:InvoicePeriod/cbc:EndDate
    try:
        _v40 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/DataFinePeriodo')
        _set(output, 'cac:InvoiceLine/cac:InvoicePeriod/cbc:EndDate', _v40)
    except Exception as _e40:
        warnings.append(f'Connection 41 failed: {_e40}')

    # [42] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/PrezzoUnitario → cac:InvoiceLine/cac:Price/cbc:PriceAmount
    try:
        _v41 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/PrezzoUnitario')
        _set(output, 'cac:InvoiceLine/cac:Price/cbc:PriceAmount', _v41)
    except Exception as _e41:
        warnings.append(f'Connection 42 failed: {_e41}')

    # [43] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/PrezzoTotale → cac:InvoiceLine/cbc:LineExtensionAmount
    try:
        _v42 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/PrezzoTotale')
        _set(output, 'cac:InvoiceLine/cbc:LineExtensionAmount', _v42)
    except Exception as _e42:
        warnings.append(f'Connection 43 failed: {_e42}')

    # [44] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AliquotaIVA → cac:InvoiceLine/cac:Item/cac:ClassifiedTaxCategory/cbc:Percent
    try:
        _v43 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AliquotaIVA')
        _set(output, 'cac:InvoiceLine/cac:Item/cac:ClassifiedTaxCategory/cbc:Percent', _v43)
    except Exception as _e43:
        warnings.append(f'Connection 44 failed: {_e43}')

    # [45] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/RiferimentoAmministrazione → cac:InvoiceLine/cbc:AccountingCost
    try:
        _v44 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/RiferimentoAmministrazione')
        _set(output, 'cac:InvoiceLine/cbc:AccountingCost', _v44)
    except Exception as _e44:
        warnings.append(f'Connection 45 failed: {_e44}')

    # [46] FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/AliquotaIVA → cac:TaxTotal/cac:TaxSubtotal/cac:TaxCategory/cbc:Percent
    try:
        _v45 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/AliquotaIVA')
        _set(output, 'cac:TaxTotal/cac:TaxSubtotal/cac:TaxCategory/cbc:Percent', _v45)
    except Exception as _e45:
        warnings.append(f'Connection 46 failed: {_e45}')

    # [47] FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/ImponibileImporto → cac:TaxTotal/cac:TaxSubtotal/cbc:TaxableAmount
    try:
        _v46 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/ImponibileImporto')
        _set(output, 'cac:TaxTotal/cac:TaxSubtotal/cbc:TaxableAmount', _v46)
    except Exception as _e46:
        warnings.append(f'Connection 47 failed: {_e46}')

    # [48] FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/Imposta → cac:TaxTotal/cac:TaxSubtotal/cbc:TaxAmount
    try:
        _v47 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/Imposta')
        _set(output, 'cac:TaxTotal/cac:TaxSubtotal/cbc:TaxAmount', _v47)
    except Exception as _e47:
        warnings.append(f'Connection 48 failed: {_e47}')

    # [49] FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/EsigibilitaIVA → cac:InvoicePeriod/cbc:DescriptionCode
    try:
        _v48 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/EsigibilitaIVA')
        _set(output, 'cac:InvoicePeriod/cbc:DescriptionCode', _v48)
    except Exception as _e48:
        warnings.append(f'Connection 49 failed: {_e48}')

    # [50] FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/Beneficiario → cac:PayeeParty/cac:PartyName/cbc:Name
    try:
        _v49 = _get(input_data, 'FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/Beneficiario')
        _set(output, 'cac:PayeeParty/cac:PartyName/cbc:Name', _v49)
    except Exception as _e49:
        warnings.append(f'Connection 50 failed: {_e49}')

    # [51] FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ModalitaPagamento → cac:PaymentMeans/cbc:PaymentMeansCode
    try:
        _v50 = _get(input_data, 'FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ModalitaPagamento')
        _set(output, 'cac:PaymentMeans/cbc:PaymentMeansCode', _v50)
    except Exception as _e50:
        warnings.append(f'Connection 51 failed: {_e50}')

    # [52] FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/DataScadenzaPagamento → cbc:DueDate
    try:
        _v51 = _get(input_data, 'FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/DataScadenzaPagamento')
        _set(output, 'cbc:DueDate', _v51)
    except Exception as _e51:
        warnings.append(f'Connection 52 failed: {_e51}')

    # [53] FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ImportoPagamento → cac:LegalMonetaryTotal/cbc:PayableAmount
    try:
        _v52 = _get(input_data, 'FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ImportoPagamento')
        _set(output, 'cac:LegalMonetaryTotal/cbc:PayableAmount', _v52)
    except Exception as _e52:
        warnings.append(f'Connection 53 failed: {_e52}')

    # [54] FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/IBAN → cac:PaymentMeans/cac:PayeeFinancialAccount/cbc:ID
    try:
        _v53 = _get(input_data, 'FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/IBAN')
        _set(output, 'cac:PaymentMeans/cac:PayeeFinancialAccount/cbc:ID', _v53)
    except Exception as _e53:
        warnings.append(f'Connection 54 failed: {_e53}')

    # [55] FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/BIC → cac:PaymentMeans/cac:PayeeFinancialAccount/cac:FinancialInstitutionBranch/cbc:ID
    try:
        _v54 = _get(input_data, 'FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/BIC')
        _set(output, 'cac:PaymentMeans/cac:PayeeFinancialAccount/cac:FinancialInstitutionBranch/cbc:ID', _v54)
    except Exception as _e54:
        warnings.append(f'Connection 55 failed: {_e54}')

    # [56] FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/CodicePagamento → cac:PaymentMeans/cbc:PaymentID
    try:
        _v55 = _get(input_data, 'FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/CodicePagamento')
        _set(output, 'cac:PaymentMeans/cbc:PaymentID', _v55)
    except Exception as _e55:
        warnings.append(f'Connection 56 failed: {_e55}')

    # [57] FatturaElettronicaBody/Allegati/DescrizioneAttachment → cac:AdditionalDocumentReference/cbc:DocumentDescription
    try:
        _v56 = _get(input_data, 'FatturaElettronicaBody/Allegati/DescrizioneAttachment')
        _set(output, 'cac:AdditionalDocumentReference/cbc:DocumentDescription', _v56)
    except Exception as _e56:
        warnings.append(f'Connection 57 failed: {_e56}')

    # [58] FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/Imposta → cac:TaxTotal/cbc:TaxAmount
    try:
        _v57 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/Imposta')
        _set(output, 'cac:TaxTotal/cbc:TaxAmount', _v57)
    except Exception as _e57:
        warnings.append(f'Connection 58 failed: {_e57}')

    # [59] FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/TipoDocumento → cbc:InvoiceTypeCode
    try:
        _v58 = _get(input_data, 'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/TipoDocumento')
        _set(output, 'cbc:InvoiceTypeCode', _v58)
    except Exception as _e58:
        warnings.append(f'Connection 59 failed: {_e58}')

    # [60] FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/NumeroLinea → cac:InvoiceLine/cbc:ID
    try:
        _v59 = _get(input_data, 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/NumeroLinea')
        _set(output, 'cac:InvoiceLine/cbc:ID', _v59)
    except Exception as _e59:
        warnings.append(f'Connection 60 failed: {_e59}')

    return output, warnings


def transform(input_data: Any, batch: bool = False) -> Tuple[Any, List[str]]:
    """
    Entry point principale.
    - batch=False: input_data è un singolo record, ritorna (dict, warnings)
    - batch=True:  input_data è una lista, ritorna (list[dict], all_warnings)
    """
    if batch or isinstance(input_data, list):
        records = input_data if isinstance(input_data, list) else [input_data]
        results, all_warnings = [], []
        for rec in records:
            out, warns = transform_record(rec)
            results.append(out)
            all_warnings.extend(warns)
        return results, all_warnings
    else:
        return transform_record(input_data)


def get_metadata() -> Dict:
    return MAPPING_METADATA
