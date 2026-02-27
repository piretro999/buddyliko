#!/usr/bin/env python3
"""
Buddyliko — Partner Service (Phase 4: Trading Partners)
CRUD trading partners, import CSV, link token, stats.
"""

import json, uuid, csv, io
from datetime import datetime, timezone
from typing import Optional, Dict, List

PARTNER_TYPES = ('supplier', 'customer', 'carrier', 'bank', 'government', 'clearinghouse', 'other')

class PartnerService:
    def __init__(self, conn, cursor_factory):
        self.conn = conn
        self.RDC = cursor_factory
        self._init_table()

    def _init_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trading_partners (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                name            VARCHAR(255) NOT NULL,
                code            VARCHAR(50),
                partner_type    VARCHAR(50) NOT NULL DEFAULT 'other',
                vat_number      VARCHAR(50),
                gln             VARCHAR(20),
                duns            VARCHAR(20),
                edi_id          VARCHAR(50),
                peppol_id       VARCHAR(100),
                sdi_code        VARCHAR(10),
                protocols       JSONB DEFAULT '[]',
                preferred_formats JSONB DEFAULT '{}',
                default_mappings  JSONB DEFAULT '{}',
                contact_name    VARCHAR(255),
                contact_email   VARCHAR(255),
                contact_phone   VARCHAR(50),
                status          VARCHAR(20) DEFAULT 'active',
                notes           TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(org_id, code)
            )
        """)
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_tp_org ON trading_partners(org_id)",
            "CREATE INDEX IF NOT EXISTS idx_tp_type ON trading_partners(org_id, partner_type)",
            "CREATE INDEX IF NOT EXISTS idx_tp_vat ON trading_partners(vat_number) WHERE vat_number IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_tp_peppol ON trading_partners(peppol_id) WHERE peppol_id IS NOT NULL",
        ]:
            cur.execute(idx)
        self.conn.commit()
        print("   ✅ trading_partners table + 4 indexes OK")

    # ── CRUD ──

    def create(self, org_id, data):
        pid = str(uuid.uuid4())
        d = {k: data.get(k) for k in [
            'name','code','partner_type','vat_number','gln','duns','edi_id','peppol_id','sdi_code',
            'contact_name','contact_email','contact_phone','status','notes'
        ]}
        d['partner_type'] = d.get('partner_type') or 'other'
        if d['partner_type'] not in PARTNER_TYPES:
            raise ValueError(f"partner_type deve essere uno di {PARTNER_TYPES}")
        if not d.get('name'):
            raise ValueError("name obbligatorio")
        protocols = data.get('protocols', [])
        preferred_formats = data.get('preferred_formats', {})
        default_mappings = data.get('default_mappings', {})
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO trading_partners (id, org_id, name, code, partner_type,
                vat_number, gln, duns, edi_id, peppol_id, sdi_code,
                protocols, preferred_formats, default_mappings,
                contact_name, contact_email, contact_phone, status, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (pid, org_id, d['name'], d.get('code'), d['partner_type'],
              d.get('vat_number'), d.get('gln'), d.get('duns'), d.get('edi_id'),
              d.get('peppol_id'), d.get('sdi_code'),
              json.dumps(protocols), json.dumps(preferred_formats), json.dumps(default_mappings),
              d.get('contact_name'), d.get('contact_email'), d.get('contact_phone'),
              d.get('status', 'active'), d.get('notes')))
        self.conn.commit()
        return self.get(org_id, pid)

    def get(self, org_id, partner_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM trading_partners WHERE id=%s AND org_id=%s", (partner_id, org_id))
        r = cur.fetchone()
        return dict(r) if r else None

    def list(self, org_id, partner_type=None, status=None, search=None, limit=200, offset=0):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        w = ["org_id=%s"]; p = [org_id]
        if partner_type: w.append("partner_type=%s"); p.append(partner_type)
        if status: w.append("status=%s"); p.append(status)
        if search: w.append("(name ILIKE %s OR code ILIKE %s OR vat_number ILIKE %s)"); s='%'+search+'%'; p+=[s,s,s]
        p += [min(limit,500), offset]
        cur.execute(f"SELECT * FROM trading_partners WHERE {' AND '.join(w)} ORDER BY name LIMIT %s OFFSET %s", tuple(p))
        return [dict(r) for r in cur.fetchall()]

    def count(self, org_id):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trading_partners WHERE org_id=%s", (org_id,))
        return cur.fetchone()[0]

    def update(self, org_id, partner_id, data):
        allowed = {'name','code','partner_type','vat_number','gln','duns','edi_id','peppol_id','sdi_code',
                    'contact_name','contact_email','contact_phone','status','notes',
                    'protocols','preferred_formats','default_mappings'}
        f = {}
        for k, v in data.items():
            if k in allowed:
                if k in ('protocols','preferred_formats','default_mappings'):
                    f[k] = json.dumps(v) if not isinstance(v, str) else v
                else:
                    f[k] = v
        if not f: return False
        f['updated_at'] = datetime.now(timezone.utc)
        s = ", ".join(f"{k}=%s" for k in f)
        cur = self.conn.cursor()
        cur.execute(f"UPDATE trading_partners SET {s} WHERE id=%s AND org_id=%s",
                    list(f.values()) + [partner_id, org_id])
        self.conn.commit()
        return cur.rowcount > 0

    def delete(self, org_id, partner_id):
        cur = self.conn.cursor()
        # Unlink tokens first
        try: cur.execute("UPDATE api_tokens SET partner_id=NULL WHERE partner_id=%s", (partner_id,))
        except: pass
        cur.execute("DELETE FROM trading_partners WHERE id=%s AND org_id=%s", (partner_id, org_id))
        self.conn.commit()
        return cur.rowcount > 0

    # ── CSV IMPORT ──

    def import_csv(self, org_id, csv_text):
        reader = csv.DictReader(io.StringIO(csv_text))
        created, errors = 0, []
        for i, row in enumerate(reader, 1):
            try:
                d = {k.strip().lower().replace(' ','_'): (v.strip() if v else None) for k,v in row.items() if v and v.strip()}
                if not d.get('name'):
                    errors.append(f"Riga {i}: nome mancante")
                    continue
                self.create(org_id, d)
                created += 1
            except Exception as e:
                errors.append(f"Riga {i}: {e}")
                try: self.conn.rollback()
                except: pass
        return created, errors

    # ── TOKEN LINK ──

    def link_token(self, org_id, partner_id, token_id):
        p = self.get(org_id, partner_id)
        if not p: raise ValueError("Partner non trovato")
        cur = self.conn.cursor()
        cur.execute("UPDATE api_tokens SET partner_id=%s WHERE id=%s AND org_id=%s", (partner_id, token_id, org_id))
        self.conn.commit()
        return cur.rowcount > 0

    def unlink_token(self, org_id, partner_id, token_id):
        cur = self.conn.cursor()
        cur.execute("UPDATE api_tokens SET partner_id=NULL WHERE id=%s AND org_id=%s AND partner_id=%s",
                    (token_id, org_id, partner_id))
        self.conn.commit()
        return cur.rowcount > 0

    def get_partner_tokens(self, org_id, partner_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""SELECT id, name, token_prefix, environment, scopes, status, last_used_at, use_count
            FROM api_tokens WHERE org_id=%s AND partner_id=%s ORDER BY name""", (org_id, partner_id))
        return [dict(r) for r in cur.fetchall()]

    # ── STATS ──

    def stats(self, org_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status='active') as active,
            COUNT(*) FILTER (WHERE status!='active') as inactive,
            COUNT(DISTINCT partner_type) as types_used
            FROM trading_partners WHERE org_id=%s""", (org_id,))
        s = dict(cur.fetchone())
        cur.execute("""SELECT partner_type, COUNT(*) as count
            FROM trading_partners WHERE org_id=%s AND status='active'
            GROUP BY partner_type ORDER BY count DESC""", (org_id,))
        s['by_type'] = [dict(r) for r in cur.fetchall()]
        return s
