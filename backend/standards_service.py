#!/usr/bin/env python3
"""
Buddyliko — Standards Library Service
Catalogo standard EDI, B2B, e-invoicing, Healthcare.
60+ standard seedati con metadati, link ufficiali, Helger refs.
"""

import json, uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, List, Any


DOMAINS = ('e-invoicing', 'edi', 'healthcare', 'payments', 'logistics', 'customs', 'general')
REGIONS = ('EU', 'US', 'INTL', 'IT', 'DE', 'FR', 'NO', 'SE', 'UK', 'AU', 'NZ', 'APAC')


class StandardsService:
    def __init__(self, conn, cursor_factory):
        self.conn = conn
        self.RDC = cursor_factory
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS standards_registry (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name            VARCHAR(255) NOT NULL,
                slug            VARCHAR(120) UNIQUE NOT NULL,
                short_name      VARCHAR(60),
                description     TEXT,
                long_description TEXT,
                domain          VARCHAR(40) NOT NULL DEFAULT 'general',
                region          VARCHAR(10) DEFAULT 'INTL',
                format_type     VARCHAR(20) DEFAULT 'xml',
                version         VARCHAR(30),
                org_body        VARCHAR(200),
                spec_url        TEXT,
                schema_url      TEXT,
                schematron_url  TEXT,
                sample_url      TEXT,
                github_url      TEXT,
                helger_lib      VARCHAR(120),
                helger_maven    VARCHAR(200),
                related_standards JSONB DEFAULT '[]',
                transaction_types JSONB DEFAULT '[]',
                tags            JSONB DEFAULT '[]',
                icon            VARCHAR(10) DEFAULT '📄',
                is_active       BOOLEAN DEFAULT TRUE,
                popularity      INTEGER DEFAULT 0,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_sr_domain ON standards_registry(domain, is_active);
            CREATE INDEX IF NOT EXISTS idx_sr_region ON standards_registry(region);
            CREATE INDEX IF NOT EXISTS idx_sr_slug ON standards_registry(slug);
            """)
            self.conn.commit()
            print("   ✅ Standards registry table ready")
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ⚠️  Standards table init: {e}")

    def _ser(self, v):
        if isinstance(v, Decimal): return str(v)
        if isinstance(v, datetime): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        return v

    def _ser_row(self, r):
        return {k: self._ser(v) for k, v in r.items()} if r else {}

    def _ser_rows(self, rows):
        return [self._ser_row(r) for r in rows]

    # ── BROWSE ──

    def browse(self, q=None, domain=None, region=None, format_type=None,
               sort_by='popularity', page=1, per_page=30):
        where = ["is_active=TRUE"]
        params = []
        if q:
            where.append("(name ILIKE %s OR short_name ILIKE %s OR description ILIKE %s OR org_body ILIKE %s)")
            p = f"%{q}%"; params.extend([p,p,p,p])
        if domain:
            where.append("domain=%s"); params.append(domain)
        if region:
            where.append("region=%s"); params.append(region)
        if format_type:
            where.append("format_type=%s"); params.append(format_type)

        sort_map = {'popularity':'popularity DESC','name':'name ASC','newest':'created_at DESC','region':'region ASC, name ASC'}
        order = sort_map.get(sort_by, 'popularity DESC')
        offset = (max(1,int(page))-1)*per_page

        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute(f"SELECT COUNT(*) as total FROM standards_registry WHERE {' AND '.join(where)}", params)
        total = cur.fetchone()['total']

        params.extend([per_page, offset])
        cur.execute(f"""
            SELECT * FROM standards_registry WHERE {' AND '.join(where)}
            ORDER BY {order} LIMIT %s OFFSET %s
        """, params)
        rows = [dict(r) for r in cur.fetchall()]
        return {
            'total': total, 'page': page, 'pages': (total+per_page-1)//per_page,
            'standards': self._ser_rows(rows)
        }

    def get_standard(self, slug_or_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM standards_registry WHERE slug=%s OR id::text=%s", (slug_or_id, slug_or_id))
        r = cur.fetchone()
        return self._ser_row(dict(r)) if r else None

    def get_domains(self):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT domain, COUNT(*) as count, array_agg(DISTINCT region) as regions
            FROM standards_registry WHERE is_active=TRUE GROUP BY domain ORDER BY count DESC
        """)
        return [dict(r) for r in cur.fetchall()]

    def get_by_domain(self, domain):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM standards_registry WHERE domain=%s AND is_active=TRUE ORDER BY popularity DESC", (domain,))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def get_regions(self):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT region, COUNT(*) as count FROM standards_registry WHERE is_active=TRUE GROUP BY region ORDER BY count DESC")
        return [dict(r) for r in cur.fetchall()]

    def get_related(self, slug_or_id):
        """Get related standards."""
        std = self.get_standard(slug_or_id)
        if not std: return []
        related = std.get('related_standards', [])
        if not related: return []
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM standards_registry WHERE slug = ANY(%s) AND is_active=TRUE", (related,))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def get_stats(self):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT domain) as domains,
                   COUNT(DISTINCT region) as regions,
                   COUNT(*) FILTER (WHERE helger_lib IS NOT NULL AND helger_lib != '') as with_helger,
                   COUNT(*) FILTER (WHERE github_url IS NOT NULL AND github_url != '') as with_github,
                   COUNT(*) FILTER (WHERE schema_url IS NOT NULL AND schema_url != '') as with_schema
            FROM standards_registry WHERE is_active=TRUE
        """)
        return dict(cur.fetchone())

    # ── SEED ──

    def seed_standards(self):
        """Seed 60+ standard if empty."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT COUNT(*) as cnt FROM standards_registry")
        if cur.fetchone()['cnt'] > 0:
            return 0

        standards = self._get_seed_data()
        count = 0
        cur2 = self.conn.cursor()
        for s in standards:
            try:
                cur2.execute("""
                    INSERT INTO standards_registry
                        (id, name, slug, short_name, description, long_description,
                         domain, region, format_type, version, org_body,
                         spec_url, schema_url, schematron_url, sample_url,
                         github_url, helger_lib, helger_maven,
                         related_standards, transaction_types, tags, icon, popularity)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    str(uuid.uuid4()), s['name'], s['slug'], s.get('short_name',''),
                    s.get('description',''), s.get('long_description',''),
                    s['domain'], s.get('region','INTL'), s.get('format_type','xml'),
                    s.get('version',''), s.get('org_body',''),
                    s.get('spec_url',''), s.get('schema_url',''),
                    s.get('schematron_url',''), s.get('sample_url',''),
                    s.get('github_url',''), s.get('helger_lib',''), s.get('helger_maven',''),
                    json.dumps(s.get('related_standards',[])),
                    json.dumps(s.get('transaction_types',[])),
                    json.dumps(s.get('tags',[])),
                    s.get('icon','📄'), s.get('popularity',50)
                ))
                count += 1
            except Exception as e:
                pass
        self.conn.commit()
        print(f"   ✅ Seeded {count} standards")
        return count

    def _get_seed_data(self):
        return [
            # ══════ EU E-INVOICING ══════
            {'name':'UBL 2.1 (Universal Business Language)','slug':'ubl-2-1','short_name':'UBL','icon':'🇪🇺',
             'description':'Standard OASIS per documenti business XML: fatture, ordini, DDT. Base per Peppol BIS.',
             'long_description':'UBL 2.1 è lo standard OASIS più diffuso in Europa per lo scambio di documenti B2B in formato XML. Supporta Invoice, CreditNote, Order, DespatchAdvice, e 60+ tipi documento. È la base per Peppol BIS 3.0 e molte implementazioni nazionali (CIUS).',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'2.1',
             'org_body':'OASIS','spec_url':'https://docs.oasis-open.org/ubl/UBL-2.1.html',
             'schema_url':'https://docs.oasis-open.org/ubl/os-UBL-2.1/xsd/',
             'github_url':'https://github.com/phax/ph-ubl',
             'helger_lib':'ph-ubl','helger_maven':'com.helger.ubl:ph-ubl21',
             'related_standards':['peppol-bis-3','cii-d16b','fatturapa-1-2'],
             'transaction_types':['Invoice','CreditNote','Order','OrderResponse','DespatchAdvice','Catalogue','Tender'],
             'tags':['peppol','oasis','xml','eu','invoice','order'],'popularity':100},

            {'name':'UBL 2.3','slug':'ubl-2-3','short_name':'UBL 2.3','icon':'🇪🇺',
             'description':'Versione aggiornata di UBL con supporto esteso per trasporti, waybill, certificati.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'2.3',
             'org_body':'OASIS','spec_url':'https://docs.oasis-open.org/ubl/UBL-2.3.html',
             'github_url':'https://github.com/phax/ph-ubl',
             'helger_lib':'ph-ubl','helger_maven':'com.helger.ubl:ph-ubl23',
             'related_standards':['ubl-2-1'],'tags':['oasis','xml','eu'],'popularity':70},

            {'name':'Peppol BIS Billing 3.0','slug':'peppol-bis-3','short_name':'Peppol BIS','icon':'🟢',
             'description':'Profilo Peppol per fatturazione elettronica B2G/B2B in Europa. Basato su UBL 2.1 e CII.',
             'long_description':'Peppol BIS Billing 3.0 definisce le regole business e le restrizioni su UBL 2.1 e UN/CEFACT CII per la fatturazione elettronica transfrontaliera in Europa. Include validazione Schematron con 100+ business rules.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'3.0.17',
             'org_body':'OpenPeppol','spec_url':'https://docs.peppol.eu/poacc/billing/3.0/',
             'schematron_url':'https://github.com/OpenPEPPOL/peppol-bis-invoice-3/tree/master/rules',
             'github_url':'https://github.com/phax/phive-rules','helger_lib':'phive-rules-peppol',
             'helger_maven':'com.helger.phive.rules:phive-rules-peppol',
             'related_standards':['ubl-2-1','cii-d16b','en-16931'],
             'transaction_types':['Invoice','CreditNote'],'tags':['peppol','eu','schematron','validation'],'popularity':98},

            {'name':'EN 16931 (European e-Invoice Norm)','slug':'en-16931','short_name':'EN 16931','icon':'🇪🇺',
             'description':'Norma europea semantica per la fattura elettronica. Base per tutte le CIUS nazionali.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'1.0.3',
             'org_body':'CEN TC 434','spec_url':'https://standards.cen.eu/dyn/www/f?p=204:110:0::::FSP_PROJECT:60602',
             'schematron_url':'https://github.com/ConnectingEurope/eInvoicing-EN16931',
             'github_url':'https://github.com/phax/en16931-cii-validation',
             'helger_lib':'en16931-cii-validation',
             'related_standards':['ubl-2-1','cii-d16b','peppol-bis-3','xrechnung','factur-x'],
             'tags':['cen','eu','semantic','norm'],'popularity':90},

            {'name':'FatturaPA 1.2.2','slug':'fatturapa-1-2','short_name':'FatturaPA','icon':'🇮🇹',
             'description':'Formato italiano per fatturazione elettronica verso PA e B2B tramite SdI.',
             'long_description':'FatturaPA è lo standard XML italiano gestito da AgID/Agenzia delle Entrate. Obbligatorio dal 2019 per tutte le fatture B2B e B2G in Italia. Trasmesso tramite Sistema di Interscambio (SdI).',
             'domain':'e-invoicing','region':'IT','format_type':'xml','version':'1.2.2',
             'org_body':'Agenzia delle Entrate','spec_url':'https://www.fatturapa.gov.it/it/norme-e-regole/documentazione-fatturapa/',
             'schema_url':'https://www.fatturapa.gov.it/export/documenti/fatturapa/v1.2.2/Schema_del_file_xml_FatturaPA_v1.2.2.xsd',
             'github_url':'https://github.com/phax/ph-fatturapa',
             'helger_lib':'ph-fatturapa','helger_maven':'com.helger:ph-fatturapa',
             'related_standards':['ubl-2-1','peppol-bis-3','en-16931','sdi-notification'],
             'transaction_types':['FatturaElettronica','NotaDiCredito'],'tags':['italy','sdi','xml','b2b','b2g'],'popularity':95},

            {'name':'XRechnung (Germany CIUS)','slug':'xrechnung','short_name':'XRechnung','icon':'🇩🇪',
             'description':'CIUS tedesca di EN 16931. Obbligatoria per fatture B2G in Germania.',
             'domain':'e-invoicing','region':'DE','format_type':'xml','version':'3.0',
             'org_body':'KoSIT','spec_url':'https://xeinkauf.de/xrechnung/',
             'schematron_url':'https://github.com/itplr-kosit/xrechnung-schematron',
             'github_url':'https://github.com/phax/phive-rules','helger_lib':'phive-rules-xrechnung',
             'related_standards':['en-16931','ubl-2-1','cii-d16b','zugferd'],
             'tags':['germany','cius','kosit'],'popularity':82},

            {'name':'ZUGFeRD 2.3 / Factur-X','slug':'zugferd','short_name':'ZUGFeRD','icon':'🇩🇪',
             'description':'Formato ibrido PDF/A-3 + XML CII. Standard tedesco-francese per fatture leggibili da umani e macchine.',
             'domain':'e-invoicing','region':'DE','format_type':'xml','version':'2.3',
             'org_body':'FeRD / FNFE','spec_url':'https://www.ferd-net.de/standards/zugferd-2.3/',
             'github_url':'https://github.com/ZUGFeRD/mustangproject',
             'related_standards':['cii-d16b','en-16931','factur-x','xrechnung'],
             'tags':['germany','france','pdf','hybrid','cii'],'popularity':80},

            {'name':'Factur-X (French CIUS)','slug':'factur-x','short_name':'Factur-X','icon':'🇫🇷',
             'description':'Standard francese per fattura elettronica ibrida, equivalente a ZUGFeRD 2.x.',
             'domain':'e-invoicing','region':'FR','format_type':'xml','version':'1.07',
             'org_body':'FNFE-MPE','spec_url':'https://fnfe-mpe.org/factur-x/',
             'related_standards':['zugferd','cii-d16b','en-16931'],
             'tags':['france','pdf','hybrid'],'popularity':75},

            {'name':'UN/CEFACT CII D16B','slug':'cii-d16b','short_name':'CII','icon':'🌐',
             'description':'Cross-Industry Invoice delle Nazioni Unite. Alternativa XML a UBL per Peppol.',
             'domain':'e-invoicing','region':'INTL','format_type':'xml','version':'D16B',
             'org_body':'UN/CEFACT','spec_url':'https://unece.org/trade/uncefact/xml-schemas',
             'github_url':'https://github.com/phax/ph-cii','helger_lib':'ph-cii',
             'helger_maven':'com.helger.cii:ph-cii-d16b',
             'related_standards':['ubl-2-1','en-16931','zugferd'],
             'tags':['uncefact','un','xml','invoice'],'popularity':78},

            {'name':'EHF (Norwegian)','slug':'ehf-norway','short_name':'EHF','icon':'🇳🇴',
             'description':'Formato norvegese per e-invoicing e e-ordering basato su UBL/Peppol.',
             'domain':'e-invoicing','region':'NO','format_type':'xml','version':'3.0',
             'org_body':'Difi/Digdir','spec_url':'https://anskaffelser.dev/',
             'related_standards':['ubl-2-1','peppol-bis-3','en-16931'],
             'tags':['norway','peppol','ubl'],'popularity':55},

            {'name':'Svefaktura (Swedish)','slug':'svefaktura','short_name':'SFTI','icon':'🇸🇪',
             'description':'Standard svedese per fatturazione elettronica, ora converge verso Peppol BIS.',
             'domain':'e-invoicing','region':'SE','format_type':'xml','version':'1.0',
             'org_body':'SFTI','related_standards':['ubl-2-1','peppol-bis-3'],
             'tags':['sweden','peppol'],'popularity':45},

            {'name':'PINT (Peppol International)','slug':'peppol-pint','short_name':'PINT','icon':'🌏',
             'description':'Peppol International Invoice Model per paesi extra-EU (Australia, NZ, Singapore, JP).',
             'domain':'e-invoicing','region':'INTL','format_type':'xml','version':'1.0',
             'org_body':'OpenPeppol','spec_url':'https://docs.peppol.eu/poac/pint/',
             'related_standards':['ubl-2-1','peppol-bis-3'],
             'tags':['peppol','international','apac'],'popularity':60},

            # ══════ EDI ══════
            {'name':'ANSI X12 810 Invoice','slug':'x12-810','short_name':'X12 810','icon':'🇺🇸',
             'description':'EDI invoice standard nordamericano. Utilizzato massivamente in US retail e manufacturing.',
             'domain':'edi','region':'US','format_type':'edi','version':'005010',
             'org_body':'ASC X12','spec_url':'https://x12.org/products/transaction-sets',
             'related_standards':['x12-850','x12-856','x12-820'],
             'transaction_types':['810 Invoice'],'tags':['x12','us','edi','invoice'],'popularity':92},

            {'name':'ANSI X12 850 Purchase Order','slug':'x12-850','short_name':'X12 850','icon':'🇺🇸',
             'description':'EDI Purchase Order standard per il mercato nordamericano.',
             'domain':'edi','region':'US','format_type':'edi','version':'005010',
             'org_body':'ASC X12',
             'related_standards':['x12-810','x12-856','x12-855'],
             'transaction_types':['850 PurchaseOrder'],'tags':['x12','us','edi','order'],'popularity':90},

            {'name':'ANSI X12 856 ASN','slug':'x12-856','short_name':'X12 856','icon':'🇺🇸',
             'description':'Advance Ship Notice — notifica di spedizione EDI con dettaglio packing.',
             'domain':'edi','region':'US','format_type':'edi','version':'005010',
             'org_body':'ASC X12','related_standards':['x12-850','x12-810'],
             'transaction_types':['856 ASN'],'tags':['x12','us','logistics'],'popularity':82},

            {'name':'ANSI X12 820 Payment Order','slug':'x12-820','short_name':'X12 820','icon':'🇺🇸',
             'description':'EDI Payment Order/Remittance Advice per pagamenti business.',
             'domain':'edi','region':'US','format_type':'edi','version':'005010',
             'org_body':'ASC X12','related_standards':['x12-810'],
             'transaction_types':['820 Payment'],'tags':['x12','us','payment'],'popularity':70},

            {'name':'ANSI X12 834 Enrollment','slug':'x12-834','short_name':'X12 834','icon':'🏥',
             'description':'Benefit enrollment e maintenance. Usato per assicurazioni sanitarie US.',
             'domain':'healthcare','region':'US','format_type':'edi','version':'005010',
             'org_body':'ASC X12','related_standards':['x12-835','x12-837'],
             'transaction_types':['834 Enrollment'],'tags':['x12','hipaa','insurance','enrollment'],'popularity':78},

            {'name':'ANSI X12 835 Claim Payment','slug':'x12-835','short_name':'X12 835','icon':'🏥',
             'description':'Health Care Claim Payment/Advice (ERA). Rimessa elettronica assicurativa US.',
             'domain':'healthcare','region':'US','format_type':'edi','version':'005010',
             'org_body':'ASC X12','related_standards':['x12-837','x12-834'],
             'transaction_types':['835 Remittance'],'tags':['x12','hipaa','insurance','payment'],'popularity':80},

            {'name':'ANSI X12 837 Health Claim','slug':'x12-837','short_name':'X12 837','icon':'🏥',
             'description':'Health Care Claim submission (Professional, Institutional, Dental). Standard HIPAA.',
             'domain':'healthcare','region':'US','format_type':'edi','version':'005010X222A2',
             'org_body':'ASC X12','related_standards':['x12-835','x12-834','x12-270'],
             'transaction_types':['837P Professional','837I Institutional','837D Dental'],
             'tags':['x12','hipaa','claim','healthcare'],'popularity':88},

            {'name':'ANSI X12 270/271 Eligibility','slug':'x12-270','short_name':'X12 270/271','icon':'🏥',
             'description':'Health Care Eligibility Inquiry/Response. Verifica copertura assicurativa.',
             'domain':'healthcare','region':'US','format_type':'edi','version':'005010X279A1',
             'org_body':'ASC X12','related_standards':['x12-837','x12-835'],
             'transaction_types':['270 Inquiry','271 Response'],'tags':['x12','hipaa','eligibility'],'popularity':75},

            {'name':'UN/EDIFACT INVOIC','slug':'edifact-invoic','short_name':'EDIFACT INVOIC','icon':'🌐',
             'description':'Messaggio fattura UN/EDIFACT, standard globale per EDI B2B.',
             'domain':'edi','region':'INTL','format_type':'edifact','version':'D96A',
             'org_body':'UN/CEFACT','spec_url':'https://unece.org/trade/uncefact/introducing-unedifact',
             'related_standards':['edifact-orders','edifact-desadv','cii-d16b'],
             'transaction_types':['INVOIC'],'tags':['un','edifact','global','invoice'],'popularity':85},

            {'name':'UN/EDIFACT ORDERS','slug':'edifact-orders','short_name':'EDIFACT ORDERS','icon':'🌐',
             'description':'Messaggio ordine UN/EDIFACT per supply chain globale.',
             'domain':'edi','region':'INTL','format_type':'edifact','version':'D96A',
             'org_body':'UN/CEFACT',
             'related_standards':['edifact-invoic','edifact-desadv','edifact-ordrsp'],
             'transaction_types':['ORDERS'],'tags':['un','edifact','order'],'popularity':80},

            {'name':'UN/EDIFACT DESADV','slug':'edifact-desadv','short_name':'EDIFACT DESADV','icon':'🚚',
             'description':'Despatch Advice — notifica di spedizione EDIFACT per logistica.',
             'domain':'logistics','region':'INTL','format_type':'edifact','version':'D96A',
             'org_body':'UN/CEFACT',
             'related_standards':['edifact-orders','edifact-invoic'],
             'transaction_types':['DESADV'],'tags':['un','edifact','logistics','dispatch'],'popularity':72},

            {'name':'UN/EDIFACT CUSCAR','slug':'edifact-cuscar','short_name':'EDIFACT CUSCAR','icon':'🛃',
             'description':'Customs Cargo Report — dichiarazione doganale cargo via EDIFACT.',
             'domain':'customs','region':'INTL','format_type':'edifact','version':'D96A',
             'org_body':'UN/CEFACT',
             'related_standards':['edifact-cusdec','edifact-desadv'],
             'transaction_types':['CUSCAR'],'tags':['customs','edifact','un'],'popularity':55},

            {'name':'UN/EDIFACT CUSDEC','slug':'edifact-cusdec','short_name':'EDIFACT CUSDEC','icon':'🛃',
             'description':'Customs Declaration — dichiarazione doganale elettronica.',
             'domain':'customs','region':'INTL','format_type':'edifact','version':'D96A',
             'org_body':'UN/CEFACT','related_standards':['edifact-cuscar'],
             'transaction_types':['CUSDEC'],'tags':['customs','edifact'],'popularity':50},

            {'name':'GS1 EANCOM','slug':'gs1-eancom','short_name':'EANCOM','icon':'🏪',
             'description':'Subset EDIFACT per retail e GDO. Usato da supermercati e distributori.',
             'domain':'edi','region':'INTL','format_type':'edifact','version':'D.01B',
             'org_body':'GS1','spec_url':'https://www.gs1.org/standards/eancom',
             'related_standards':['edifact-invoic','edifact-orders','gs1-xml'],
             'tags':['gs1','retail','eancom','edifact'],'popularity':70},

            {'name':'GS1 XML BMS','slug':'gs1-xml','short_name':'GS1 XML','icon':'🏪',
             'description':'GS1 Business Message Standard in XML per supply chain.',
             'domain':'edi','region':'INTL','format_type':'xml','version':'3.4',
             'org_body':'GS1','spec_url':'https://www.gs1.org/standards/gs1-xml',
             'related_standards':['gs1-eancom','ubl-2-1'],
             'tags':['gs1','xml','supply-chain'],'popularity':55},

            # ══════ HEALTHCARE ══════
            {'name':'HL7 FHIR R4','slug':'hl7-fhir-r4','short_name':'FHIR R4','icon':'🔥',
             'description':'Fast Healthcare Interoperability Resources. Standard REST/JSON per healthcare moderno.',
             'long_description':'HL7 FHIR R4 è lo standard più moderno per lo scambio di dati sanitari. Basato su risorse REST con rappresentazione JSON/XML. 150+ resource types: Patient, Observation, MedicationRequest, Claim, ExplanationOfBenefit, etc. Supportato da tutti i major EHR vendors.',
             'domain':'healthcare','region':'INTL','format_type':'json','version':'R4 (4.0.1)',
             'org_body':'HL7 International','spec_url':'https://hl7.org/fhir/R4/',
             'schema_url':'https://hl7.org/fhir/R4/fhir.schema.json',
             'github_url':'https://github.com/hapifhir/hapi-fhir',
             'related_standards':['hl7-v2','hl7-cda','x12-837'],
             'transaction_types':['Patient','Observation','MedicationRequest','Claim','Bundle','DiagnosticReport'],
             'tags':['hl7','fhir','json','rest','healthcare','ehr'],'popularity':95},

            {'name':'HL7 FHIR R5','slug':'hl7-fhir-r5','short_name':'FHIR R5','icon':'🔥',
             'description':'Versione più recente di FHIR con miglioramenti a subscriptions, workflow e types.',
             'domain':'healthcare','region':'INTL','format_type':'json','version':'R5 (5.0.0)',
             'org_body':'HL7 International','spec_url':'https://hl7.org/fhir/R5/',
             'related_standards':['hl7-fhir-r4','hl7-v2'],
             'tags':['hl7','fhir','json','latest'],'popularity':70},

            {'name':'HL7 v2.x Messages','slug':'hl7-v2','short_name':'HL7 v2','icon':'🏥',
             'description':'Standard HL7 v2 pipe-delimited per messaggistica clinica (ADT, ORM, ORU, SIU).',
             'long_description':'HL7 v2.x è lo standard di messaggistica più diffuso al mondo in ambito sanitario. Formato pipe-delimited (|). Message types: ADT (admit/discharge/transfer), ORM (order), ORU (results), SIU (scheduling), MDM (documents). Versioni da 2.1 a 2.9.',
             'domain':'healthcare','region':'INTL','format_type':'edi','version':'2.5.1',
             'org_body':'HL7 International','spec_url':'https://www.hl7.org/implement/standards/product_section.cfm?section=13',
             'related_standards':['hl7-fhir-r4','hl7-cda'],
             'transaction_types':['ADT','ORM','ORU','SIU','MDM','DFT','BAR','MFN','RDE'],
             'tags':['hl7','v2','pipe','hospital','lab','ehr'],'popularity':88},

            {'name':'HL7 CDA R2 (Clinical Document Architecture)','slug':'hl7-cda','short_name':'CDA','icon':'📋',
             'description':'Standard XML per documenti clinici strutturati (referti, dimissioni, prescrizioni).',
             'domain':'healthcare','region':'INTL','format_type':'xml','version':'R2',
             'org_body':'HL7 International','spec_url':'https://www.hl7.org/implement/standards/product_brief.cfm?product_id=7',
             'related_standards':['hl7-fhir-r4','hl7-v2','hl7-ccda'],
             'transaction_types':['ClinicalDocument','ContinuityOfCareDocument','DischargeSummary'],
             'tags':['hl7','cda','xml','clinical','document'],'popularity':72},

            {'name':'C-CDA (Consolidated CDA)','slug':'hl7-ccda','short_name':'C-CDA','icon':'📋',
             'description':'Templates CDA consolidati per US Meaningful Use. Referti, summary, care plans.',
             'domain':'healthcare','region':'US','format_type':'xml','version':'2.1',
             'org_body':'HL7 International',
             'related_standards':['hl7-cda','hl7-fhir-r4'],
             'tags':['hl7','ccda','meaningful-use','us'],'popularity':68},

            {'name':'IHE XDS.b (Cross-Enterprise Document Sharing)','slug':'ihe-xds','short_name':'IHE XDS','icon':'🏥',
             'description':'Profilo IHE per condivisione documenti clinici tra enti diversi via registry/repository.',
             'domain':'healthcare','region':'INTL','format_type':'xml','version':'3.0',
             'org_body':'IHE International','spec_url':'https://profiles.ihe.net/ITI/TF/Volume1/ch-10.html',
             'related_standards':['hl7-cda','hl7-fhir-r4'],
             'tags':['ihe','xds','document-sharing'],'popularity':58},

            {'name':'NCPDP SCRIPT (e-Prescribing)','slug':'ncpdp-script','short_name':'NCPDP','icon':'💊',
             'description':'Standard per prescrizioni elettroniche e comunicazione farmacia in US.',
             'domain':'healthcare','region':'US','format_type':'xml','version':'2017071',
             'org_body':'NCPDP','spec_url':'https://www.ncpdp.org/NCPDP/media/pdf/StandardsMatrix.pdf',
             'related_standards':['hl7-fhir-r4','x12-837'],
             'tags':['ncpdp','pharmacy','eprescribing','us'],'popularity':60},

            # ══════ PAYMENTS / BANKING ══════
            {'name':'ISO 20022 Financial Messaging','slug':'iso-20022','short_name':'ISO 20022','icon':'🏦',
             'description':'Standard universale XML per messaggistica finanziaria. Sostituisce SWIFT MT.',
             'domain':'payments','region':'INTL','format_type':'xml','version':'2024',
             'org_body':'ISO / SWIFT','spec_url':'https://www.iso20022.org/',
             'schema_url':'https://www.iso20022.org/catalogue-messages',
             'related_standards':['swift-mt','sepa-sct','sepa-sdd'],
             'transaction_types':['pain.001','pain.002','camt.053','camt.054','pacs.008','pacs.002'],
             'tags':['iso','banking','swift','xml','payment'],'popularity':90},

            {'name':'SWIFT MT Messages','slug':'swift-mt','short_name':'SWIFT MT','icon':'💳',
             'description':'Messaggi SWIFT legacy (MT103, MT202, MT940, MT950). In migrazione verso ISO 20022.',
             'domain':'payments','region':'INTL','format_type':'fixed','version':'2024',
             'org_body':'SWIFT','spec_url':'https://www.swift.com/standards/messaging-standards',
             'related_standards':['iso-20022','sepa-sct'],
             'transaction_types':['MT103','MT202','MT940','MT950','MT199'],
             'tags':['swift','banking','legacy','fixed-width'],'popularity':82},

            {'name':'SEPA SCT (Credit Transfer)','slug':'sepa-sct','short_name':'SEPA SCT','icon':'🇪🇺',
             'description':'Bonifico SEPA basato su ISO 20022 pain.001.',
             'domain':'payments','region':'EU','format_type':'xml','version':'pain.001.001.03',
             'org_body':'EPC','spec_url':'https://www.europeanpaymentscouncil.eu/what-we-do/sepa-credit-transfer',
             'related_standards':['iso-20022','sepa-sdd'],
             'tags':['sepa','eu','banking','credit-transfer'],'popularity':78},

            {'name':'SEPA SDD (Direct Debit)','slug':'sepa-sdd','short_name':'SEPA SDD','icon':'🇪🇺',
             'description':'Addebito diretto SEPA basato su ISO 20022 pain.008.',
             'domain':'payments','region':'EU','format_type':'xml','version':'pain.008.001.02',
             'org_body':'EPC','related_standards':['iso-20022','sepa-sct'],
             'tags':['sepa','eu','banking','direct-debit'],'popularity':72},

            {'name':'NACHA ACH (US Payments)','slug':'nacha-ach','short_name':'ACH','icon':'🇺🇸',
             'description':'Automated Clearing House — pagamenti elettronici batch negli Stati Uniti.',
             'domain':'payments','region':'US','format_type':'fixed','version':'2024',
             'org_body':'Nacha','spec_url':'https://www.nacha.org/',
             'related_standards':['swift-mt','iso-20022'],
             'tags':['nacha','ach','us','banking','batch'],'popularity':75},

            # ══════ LOGISTICS ══════
            {'name':'UN/CEFACT eCMR','slug':'ecmr','short_name':'eCMR','icon':'🚛',
             'description':'Lettera di vettura elettronica per trasporto stradale internazionale.',
             'domain':'logistics','region':'INTL','format_type':'xml','version':'1.0',
             'org_body':'UN/CEFACT','related_standards':['edifact-desadv'],
             'tags':['logistics','transport','cmr'],'popularity':45},

            {'name':'UN/CEFACT Multi-Modal Transport','slug':'uncefact-mmt','short_name':'MMT','icon':'🚢',
             'description':'Standard UN/CEFACT per trasporto multimodale (mare, aria, terra).',
             'domain':'logistics','region':'INTL','format_type':'xml',
             'org_body':'UN/CEFACT','related_standards':['edifact-desadv','ecmr'],
             'tags':['logistics','multimodal','un'],'popularity':40},

            # ══════ HELGER PH LIBRARIES ══════
            {'name':'ph-schematron','slug':'ph-schematron','short_name':'ph-schematron','icon':'✅',
             'description':'Libreria Java di Philip Helger per validazione Schematron (ISO, XSLT, Pure). Usata da Peppol e EN 16931.',
             'domain':'general','region':'INTL','format_type':'xml',
             'org_body':'Philip Helger / phax',
             'github_url':'https://github.com/phax/ph-schematron',
             'helger_lib':'ph-schematron','helger_maven':'com.helger.schematron:ph-schematron-api',
             'related_standards':['peppol-bis-3','en-16931','xrechnung'],
             'tags':['helger','schematron','validation','java'],'popularity':88},

            {'name':'ph-bdve / phive (Validation Engine)','slug':'phive','short_name':'phive','icon':'✅',
             'description':'Motore di validazione generico di Helger. Valida documenti contro regole Peppol, EN 16931, XRechnung.',
             'domain':'general','region':'INTL','format_type':'xml',
             'org_body':'Philip Helger / phax',
             'github_url':'https://github.com/phax/phive',
             'helger_lib':'phive','helger_maven':'com.helger.phive:phive-api',
             'related_standards':['ph-schematron','peppol-bis-3','en-16931'],
             'tags':['helger','validation','engine','java'],'popularity':85},

            {'name':'phive-rules (Validation Rules Sets)','slug':'phive-rules','short_name':'phive-rules','icon':'📏',
             'description':'Set completi di regole di validazione per Peppol, XRechnung, EN 16931, FatturaPA, OIOUBL, etc.',
             'domain':'general','region':'INTL','format_type':'xml',
             'org_body':'Philip Helger / phax',
             'github_url':'https://github.com/phax/phive-rules',
             'helger_lib':'phive-rules','helger_maven':'com.helger.phive.rules:phive-rules-peppol',
             'related_standards':['phive','peppol-bis-3','xrechnung','en-16931','fatturapa-1-2'],
             'tags':['helger','rules','peppol','xrechnung','en16931'],'popularity':82},

            {'name':'ph-ebinterface (Austrian)','slug':'ph-ebinterface','short_name':'ebInterface','icon':'🇦🇹',
             'description':'Supporto Java per ebInterface, standard austriaco per fatturazione elettronica B2G.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'7.0',
             'org_body':'Philip Helger / Austrian Federal Ministry',
             'github_url':'https://github.com/phax/ph-ebinterface',
             'helger_lib':'ph-ebinterface','helger_maven':'com.helger.ebinterface:ph-ebinterface',
             'related_standards':['en-16931','ubl-2-1'],
             'tags':['helger','austria','ebinterface'],'popularity':42},

            {'name':'peppol-commons (Peppol SMP/SML)','slug':'peppol-commons','short_name':'peppol-commons','icon':'🟢',
             'description':'Librerie Java per Peppol SMP, SML, Directory, SBDH, AS4 — infrastruttura di rete Peppol.',
             'domain':'general','region':'EU','format_type':'xml',
             'org_body':'Philip Helger / phax',
             'github_url':'https://github.com/phax/peppol-commons',
             'helger_lib':'peppol-commons','helger_maven':'com.helger.peppol:peppol-commons',
             'related_standards':['peppol-bis-3','as4'],
             'tags':['helger','peppol','smp','sml','infrastructure'],'popularity':72},

            {'name':'phase4 (AS4 Messaging)','slug':'phase4','short_name':'phase4','icon':'📨',
             'description':'Implementazione Java di AS4 (ebMS 3.0) di Helger. Per Peppol AS4 e eDelivery.',
             'domain':'general','region':'EU','format_type':'xml',
             'org_body':'Philip Helger / phax',
             'github_url':'https://github.com/phax/phase4',
             'helger_lib':'phase4','helger_maven':'com.helger.phase4:phase4-lib',
             'related_standards':['peppol-commons','peppol-bis-3'],
             'tags':['helger','as4','ebms','peppol','edelivery'],'popularity':68},

            # ══════ MISC/GENERAL ══════
            {'name':'OIOUBL (Danish)','slug':'oioubl','short_name':'OIOUBL','icon':'🇩🇰',
             'description':'Profilo danese di UBL per fatturazione elettronica B2G.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'2.1',
             'org_body':'Danish Agency for Digitisation',
             'github_url':'https://github.com/phax/phive-rules',
             'related_standards':['ubl-2-1','peppol-bis-3'],
             'tags':['denmark','ubl','oioubl'],'popularity':45},

            {'name':'FinVoice (Finnish)','slug':'finvoice','short_name':'FinVoice','icon':'🇫🇮',
             'description':'Standard finlandese per fatturazione elettronica tra banche.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'3.0',
             'org_body':'Finance Finland','related_standards':['peppol-bis-3'],
             'tags':['finland','banking','invoice'],'popularity':40},

            {'name':'SII TicketBAI (Spain)','slug':'ticketbai','short_name':'TicketBAI','icon':'🇪🇸',
             'description':'Sistema spagnolo di fatturazione elettronica per regioni basche (Bizkaia, Gipuzkoa, Araba).',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'1.2',
             'org_body':'Haciendas Forales',
             'related_standards':['facturae','peppol-bis-3'],
             'tags':['spain','tax','basque'],'popularity':48},

            {'name':'Facturae (Spain)','slug':'facturae','short_name':'Facturae','icon':'🇪🇸',
             'description':'Formato spagnolo per fatturazione elettronica B2G, gestito da FACe.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'3.2.2',
             'org_body':'Spanish Government','related_standards':['ticketbai','peppol-bis-3'],
             'tags':['spain','face','b2g'],'popularity':52},

            {'name':'SBDH (Standard Business Document Header)','slug':'sbdh','short_name':'SBDH','icon':'📨',
             'description':'Header standard GS1/UN per wrapping di documenti business in busta di trasporto.',
             'domain':'general','region':'INTL','format_type':'xml','version':'1.3',
             'org_body':'GS1 / UN/CEFACT',
             'github_url':'https://github.com/phax/peppol-commons',
             'related_standards':['peppol-commons','gs1-xml'],
             'tags':['sbdh','gs1','envelope','peppol'],'popularity':55},

            {'name':'SAF-T (Standard Audit File for Tax)','slug':'saf-t','short_name':'SAF-T','icon':'🧾',
             'description':'File di audit fiscale standard OECD. Obbligatorio in PT, NO, LT, PL, AT.',
             'domain':'e-invoicing','region':'EU','format_type':'xml','version':'2.0',
             'org_body':'OECD','related_standards':['fatturapa-1-2','xrechnung'],
             'tags':['oecd','tax','audit','saf-t','portugal','norway'],'popularity':58},
        ]
