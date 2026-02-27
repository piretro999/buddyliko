#!/usr/bin/env python3
"""
Buddyliko — Marketplace Service (Phase 7: Marketplace & Template)
Gestione mapping_templates, template_reviews, template_purchases.
CRUD, search/browse, purchase, review/rating, clone, stats, seed builtin.
"""

import json
import uuid
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, List, Any


CATEGORIES = (
    'invoice', 'order', 'dispatch', 'payment', 'healthcare',
    'customs', 'logistics', 'banking', 'other'
)

AVAILABILITY_TYPES = ('builtin', 'marketplace', 'private', 'community')

PRICE_TYPES = ('one_time', 'per_use', 'subscription')

FORMAT_TYPES = ('xml', 'json', 'csv', 'edi', 'x12', 'edifact', 'fixed', 'other')

TEMPLATE_STATUSES = ('draft', 'review', 'published', 'deprecated')


class MarketplaceService:
    """Marketplace per mapping templates."""

    def __init__(self, conn, cursor_factory, org_service=None, cost_service=None):
        self.conn = conn
        self.RDC = cursor_factory
        self.org_service = org_service
        self.cost_service = cost_service
        self._init_tables()

    # ──────────────────────────────────────────────────────────────
    # DDL: CREATE TABLES
    # ──────────────────────────────────────────────────────────────

    def _init_tables(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS mapping_templates (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                author_org_id   UUID REFERENCES organizations(id),
                author_user_id  INTEGER REFERENCES users(id),
                name            VARCHAR(255) NOT NULL,
                slug            VARCHAR(100) NOT NULL,
                description     TEXT,
                long_description TEXT,
                category        VARCHAR(50) NOT NULL DEFAULT 'other',
                input_standard  VARCHAR(80),
                output_standard VARCHAR(80),
                input_format    VARCHAR(20),
                output_format   VARCHAR(20),
                mapping_data    JSONB NOT NULL DEFAULT '{}',
                sample_input    TEXT,
                sample_output   TEXT,
                availability    VARCHAR(20) NOT NULL DEFAULT 'private',
                price_eur       NUMERIC(8,2) DEFAULT 0,
                price_type      VARCHAR(20) DEFAULT 'one_time',
                downloads_count INTEGER DEFAULT 0,
                rating_avg      NUMERIC(3,2) DEFAULT 0,
                rating_count    INTEGER DEFAULT 0,
                status          VARCHAR(20) DEFAULT 'draft',
                version         VARCHAR(20) DEFAULT '1.0.0',
                tags            JSONB DEFAULT '[]',
                icon            VARCHAR(10) DEFAULT '📄',
                featured        BOOLEAN DEFAULT FALSE,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(slug)
            );
            CREATE INDEX IF NOT EXISTS idx_mt_category ON mapping_templates(category, status);
            CREATE INDEX IF NOT EXISTS idx_mt_availability ON mapping_templates(availability, status);
            CREATE INDEX IF NOT EXISTS idx_mt_formats ON mapping_templates(input_format, output_format);
            CREATE INDEX IF NOT EXISTS idx_mt_author ON mapping_templates(author_org_id);
            CREATE INDEX IF NOT EXISTS idx_mt_slug ON mapping_templates(slug);

            CREATE TABLE IF NOT EXISTS template_reviews (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                template_id     UUID NOT NULL REFERENCES mapping_templates(id) ON DELETE CASCADE,
                reviewer_org_id UUID REFERENCES organizations(id),
                reviewer_user_id INTEGER REFERENCES users(id),
                rating          INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
                title           VARCHAR(200),
                body            TEXT,
                status          VARCHAR(20) DEFAULT 'published',
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(template_id, reviewer_org_id)
            );
            CREATE INDEX IF NOT EXISTS idx_tr_template ON template_reviews(template_id);

            CREATE TABLE IF NOT EXISTS template_purchases (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                template_id     UUID NOT NULL REFERENCES mapping_templates(id),
                buyer_org_id    UUID NOT NULL REFERENCES organizations(id),
                buyer_user_id   INTEGER REFERENCES users(id),
                price_paid_eur  NUMERIC(8,2) DEFAULT 0,
                price_type      VARCHAR(20),
                status          VARCHAR(20) DEFAULT 'active',
                installed_at    TIMESTAMPTZ DEFAULT NOW(),
                expires_at      TIMESTAMPTZ,
                UNIQUE(template_id, buyer_org_id)
            );
            CREATE INDEX IF NOT EXISTS idx_tp_buyer ON template_purchases(buyer_org_id);
            """)
            self.conn.commit()
            print("   ✅ Marketplace tables ready")
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ⚠️  Marketplace tables init: {e}")

    # ──────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────

    def _ser(self, v):
        if isinstance(v, Decimal): return str(v)
        if isinstance(v, datetime): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        return v

    def _ser_row(self, row):
        return {k: self._ser(v) for k, v in row.items()} if row else {}

    def _ser_rows(self, rows):
        return [self._ser_row(r) for r in rows]

    def _slugify(self, name):
        s = name.lower().strip()
        s = re.sub(r'[^a-z0-9]+', '-', s).strip('-')
        return s[:100] if s else 'tpl-' + str(uuid.uuid4())[:8]

    # ──────────────────────────────────────────────────────────────
    # 1. BROWSE / SEARCH
    # ──────────────────────────────────────────────────────────────

    def browse(self, org_id=None, org_plan='FREE', q=None, category=None,
               input_format=None, output_format=None, availability=None,
               price_max=None, sort_by='downloads', page=1, per_page=24):
        """Browse the marketplace with filters."""
        where = ["mt.status = 'published'"]
        params = []

        # Visibility: builtin + community + marketplace always visible
        # private only if author_org_id = org_id
        vis = "mt.availability IN ('builtin','community','marketplace')"
        if org_id:
            vis = f"({vis} OR (mt.availability='private' AND mt.author_org_id=%s))"
            params.append(org_id)
        where.append(vis)

        # Builtin: only visible to PRO+ plans
        # (actually builtin should be visible to all, but usable only by PRO+)

        if q:
            where.append("(mt.name ILIKE %s OR mt.description ILIKE %s OR mt.input_standard ILIKE %s OR mt.output_standard ILIKE %s)")
            p = f"%{q}%"
            params.extend([p, p, p, p])

        if category:
            where.append("mt.category = %s"); params.append(category)
        if input_format:
            where.append("mt.input_format = %s"); params.append(input_format)
        if output_format:
            where.append("mt.output_format = %s"); params.append(output_format)
        if availability:
            where.append("mt.availability = %s"); params.append(availability)
        if price_max is not None:
            where.append("mt.price_eur <= %s"); params.append(price_max)

        sort_map = {
            'downloads': 'mt.downloads_count DESC',
            'rating': 'mt.rating_avg DESC, mt.rating_count DESC',
            'newest': 'mt.created_at DESC',
            'price_asc': 'mt.price_eur ASC',
            'price_desc': 'mt.price_eur DESC',
            'name': 'mt.name ASC',
        }
        order = sort_map.get(sort_by, 'mt.downloads_count DESC')

        offset = (max(1, int(page)) - 1) * per_page

        cur = self.conn.cursor(cursor_factory=self.RDC)

        # Count
        cur.execute(f"SELECT COUNT(*) as total FROM mapping_templates mt WHERE {' AND '.join(where)}", params)
        total = cur.fetchone()['total']

        # Results
        params.extend([per_page, offset])
        cur.execute(f"""
            SELECT mt.id, mt.name, mt.slug, mt.description, mt.category, mt.icon,
                   mt.input_standard, mt.output_standard, mt.input_format, mt.output_format,
                   mt.availability, mt.price_eur, mt.price_type,
                   mt.downloads_count, mt.rating_avg, mt.rating_count,
                   mt.status, mt.version, mt.tags, mt.featured,
                   mt.author_org_id, mt.created_at,
                   o.name as author_name
            FROM mapping_templates mt
            LEFT JOIN organizations o ON mt.author_org_id = o.id
            WHERE {' AND '.join(where)}
            ORDER BY mt.featured DESC, {order}
            LIMIT %s OFFSET %s
        """, params)
        rows = [dict(r) for r in cur.fetchall()]

        # Mark which ones are owned/purchased by org
        if org_id:
            for r in rows:
                r['is_owned'] = str(r.get('author_org_id', '')) == str(org_id)
                r['is_builtin_free'] = r.get('availability') == 'builtin'

            # Check purchases
            tpl_ids = [r['id'] for r in rows]
            if tpl_ids:
                cur.execute("""
                    SELECT template_id FROM template_purchases
                    WHERE buyer_org_id = %s AND template_id = ANY(%s) AND status = 'active'
                """, (org_id, tpl_ids))
                purchased = {str(r['template_id']) for r in cur.fetchall()}
                for r in rows:
                    r['is_purchased'] = str(r['id']) in purchased

        return {
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': (total + per_page - 1) // per_page,
            'templates': self._ser_rows(rows),
        }

    # ──────────────────────────────────────────────────────────────
    # 2. GET TEMPLATE DETAIL
    # ──────────────────────────────────────────────────────────────

    def get_template(self, template_id, org_id=None, include_mapping=False):
        """Get full template detail."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        fields = """mt.id, mt.name, mt.slug, mt.description, mt.long_description,
                   mt.category, mt.icon, mt.input_standard, mt.output_standard,
                   mt.input_format, mt.output_format,
                   mt.availability, mt.price_eur, mt.price_type,
                   mt.downloads_count, mt.rating_avg, mt.rating_count,
                   mt.status, mt.version, mt.tags, mt.featured,
                   mt.sample_input, mt.sample_output,
                   mt.author_org_id, mt.author_user_id,
                   mt.created_at, mt.updated_at,
                   o.name as author_name, u.name as author_user_name"""
        if include_mapping:
            fields += ", mt.mapping_data"

        cur.execute(f"""
            SELECT {fields}
            FROM mapping_templates mt
            LEFT JOIN organizations o ON mt.author_org_id = o.id
            LEFT JOIN users u ON mt.author_user_id = u.id
            WHERE mt.id = %s
        """, (template_id,))
        row = cur.fetchone()
        if not row:
            return None

        result = self._ser_row(dict(row))

        # Check access
        if org_id:
            result['is_owned'] = str(row.get('author_org_id', '')) == str(org_id)
            cur.execute("SELECT id FROM template_purchases WHERE template_id=%s AND buyer_org_id=%s AND status='active'", (template_id, org_id))
            result['is_purchased'] = cur.fetchone() is not None
            result['can_use'] = result['is_owned'] or result['is_purchased'] or row.get('availability') in ('builtin', 'community')

        # Recent reviews
        cur.execute("""
            SELECT tr.rating, tr.title, tr.body, tr.created_at,
                   o.name as reviewer_name
            FROM template_reviews tr
            LEFT JOIN organizations o ON tr.reviewer_org_id = o.id
            WHERE tr.template_id = %s AND tr.status = 'published'
            ORDER BY tr.created_at DESC LIMIT 10
        """, (template_id,))
        result['reviews'] = self._ser_rows([dict(r) for r in cur.fetchall()])

        return result

    # ──────────────────────────────────────────────────────────────
    # 3. PUBLISH TEMPLATE
    # ──────────────────────────────────────────────────────────────

    def create_template(self, org_id, user_id, data):
        """Create a new mapping template."""
        name = data.get('name', '').strip()
        if not name:
            raise ValueError("Nome obbligatorio")

        slug = data.get('slug') or self._slugify(name)
        # Ensure unique slug
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT id FROM mapping_templates WHERE slug=%s", (slug,))
        if cur.fetchone():
            slug = slug + '-' + str(uuid.uuid4())[:6]

        category = data.get('category', 'other')
        if category not in CATEGORIES:
            category = 'other'

        availability = data.get('availability', 'private')
        if availability not in AVAILABILITY_TYPES:
            availability = 'private'

        # Only platform admin can create 'builtin'
        if availability == 'builtin':
            availability = 'private'  # downgrade, API layer checks admin

        price_eur = float(data.get('price_eur', 0))
        if availability != 'marketplace':
            price_eur = 0

        mapping_data = data.get('mapping_data', {})
        if isinstance(mapping_data, str):
            mapping_data = json.loads(mapping_data)

        tags = data.get('tags', [])
        if isinstance(tags, str):
            tags = json.loads(tags)

        tpl_id = str(uuid.uuid4())
        status = 'published' if availability in ('private', 'community') else 'review'

        cur2 = self.conn.cursor()
        cur2.execute("""
            INSERT INTO mapping_templates
                (id, author_org_id, author_user_id, name, slug, description, long_description,
                 category, input_standard, output_standard, input_format, output_format,
                 mapping_data, sample_input, sample_output,
                 availability, price_eur, price_type, status, version, tags, icon)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            tpl_id, org_id, user_id, name, slug,
            data.get('description', ''), data.get('long_description', ''),
            category, data.get('input_standard', ''), data.get('output_standard', ''),
            data.get('input_format', ''), data.get('output_format', ''),
            json.dumps(mapping_data), data.get('sample_input', ''), data.get('sample_output', ''),
            availability, price_eur, data.get('price_type', 'one_time'),
            status, data.get('version', '1.0.0'), json.dumps(tags),
            data.get('icon', '📄')
        ))
        self.conn.commit()
        return self.get_template(tpl_id, org_id)

    # ──────────────────────────────────────────────────────────────
    # 4. UPDATE TEMPLATE
    # ──────────────────────────────────────────────────────────────

    def update_template(self, template_id, org_id, user_id, data, is_admin=False):
        """Update a template. Only author or admin can update."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT author_org_id FROM mapping_templates WHERE id=%s", (template_id,))
        tpl = cur.fetchone()
        if not tpl:
            raise ValueError("Template non trovato")
        if not is_admin and str(tpl['author_org_id']) != str(org_id):
            raise ValueError("Non sei l'autore di questo template")

        allowed = ['name', 'description', 'long_description', 'category',
                   'input_standard', 'output_standard', 'input_format', 'output_format',
                   'mapping_data', 'sample_input', 'sample_output',
                   'availability', 'price_eur', 'price_type', 'status',
                   'version', 'tags', 'icon', 'featured']

        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                if k in ('mapping_data', 'tags') and isinstance(v, (dict, list)):
                    v = json.dumps(v)
                fields.append(f"{k} = %s")
                values.append(v)

        if not fields:
            return False

        fields.append("updated_at = NOW()")
        values.append(template_id)
        cur2 = self.conn.cursor()
        cur2.execute(f"UPDATE mapping_templates SET {', '.join(fields)} WHERE id = %s", values)
        self.conn.commit()
        return cur2.rowcount > 0

    # ──────────────────────────────────────────────────────────────
    # 5. PURCHASE / INSTALL
    # ──────────────────────────────────────────────────────────────

    def purchase_template(self, template_id, org_id, user_id):
        """Purchase or install a template."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM mapping_templates WHERE id=%s AND status='published'", (template_id,))
        tpl = cur.fetchone()
        if not tpl:
            raise ValueError("Template non trovato o non pubblicato")
        tpl = dict(tpl)

        # Already purchased?
        cur.execute("SELECT id FROM template_purchases WHERE template_id=%s AND buyer_org_id=%s AND status='active'", (template_id, org_id))
        if cur.fetchone():
            raise ValueError("Template già installato")

        # Own template
        if str(tpl.get('author_org_id', '')) == str(org_id):
            raise ValueError("Non puoi acquistare il tuo stesso template")

        # Builtin: check plan
        avail = tpl.get('availability', 'private')
        price = float(tpl.get('price_eur', 0))

        if avail == 'builtin':
            price = 0  # Free for PRO+ (plan check done at API level)
        elif avail == 'community':
            price = 0

        # Record purchase
        purchase_id = str(uuid.uuid4())
        cur2 = self.conn.cursor()
        cur2.execute("""
            INSERT INTO template_purchases (id, template_id, buyer_org_id, buyer_user_id, price_paid_eur, price_type, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'active')
        """, (purchase_id, template_id, org_id, user_id, price, tpl.get('price_type', 'one_time')))

        # Increment download count
        cur2.execute("UPDATE mapping_templates SET downloads_count = downloads_count + 1, updated_at = NOW() WHERE id = %s", (template_id,))
        self.conn.commit()

        return {
            'purchase_id': purchase_id,
            'template_id': str(template_id),
            'template_name': tpl['name'],
            'price_paid_eur': price,
        }

    # ──────────────────────────────────────────────────────────────
    # 6. REVIEWS
    # ──────────────────────────────────────────────────────────────

    def add_review(self, template_id, org_id, user_id, rating, title='', body=''):
        """Add a review for a template."""
        if not (1 <= int(rating) <= 5):
            raise ValueError("Rating deve essere tra 1 e 5")

        cur = self.conn.cursor(cursor_factory=self.RDC)
        # Verify template exists
        cur.execute("SELECT id, author_org_id FROM mapping_templates WHERE id=%s", (template_id,))
        tpl = cur.fetchone()
        if not tpl:
            raise ValueError("Template non trovato")
        if str(tpl['author_org_id']) == str(org_id):
            raise ValueError("Non puoi recensire il tuo template")

        # Upsert review
        review_id = str(uuid.uuid4())
        cur2 = self.conn.cursor()
        cur2.execute("""
            INSERT INTO template_reviews (id, template_id, reviewer_org_id, reviewer_user_id, rating, title, body)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (template_id, reviewer_org_id)
            DO UPDATE SET rating=%s, title=%s, body=%s, updated_at=NOW()
        """, (review_id, template_id, org_id, user_id, int(rating), title, body,
              int(rating), title, body))

        # Recalculate avg
        cur2.execute("""
            UPDATE mapping_templates SET
                rating_avg = (SELECT COALESCE(AVG(rating), 0) FROM template_reviews WHERE template_id=%s AND status='published'),
                rating_count = (SELECT COUNT(*) FROM template_reviews WHERE template_id=%s AND status='published'),
                updated_at = NOW()
            WHERE id = %s
        """, (template_id, template_id, template_id))
        self.conn.commit()
        return True

    def get_reviews(self, template_id, page=1, per_page=20):
        """Get reviews for a template."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        offset = (max(1, int(page)) - 1) * per_page
        cur.execute("""
            SELECT tr.*, o.name as reviewer_name, u.name as reviewer_user_name
            FROM template_reviews tr
            LEFT JOIN organizations o ON tr.reviewer_org_id = o.id
            LEFT JOIN users u ON tr.reviewer_user_id = u.id
            WHERE tr.template_id = %s AND tr.status = 'published'
            ORDER BY tr.created_at DESC
            LIMIT %s OFFSET %s
        """, (template_id, per_page, offset))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    # ──────────────────────────────────────────────────────────────
    # 7. MY TEMPLATES / MY PURCHASES
    # ──────────────────────────────────────────────────────────────

    def get_my_templates(self, org_id, status=None):
        """Templates published by this org."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        where = "mt.author_org_id = %s"
        params = [org_id]
        if status:
            where += " AND mt.status = %s"
            params.append(status)
        cur.execute(f"""
            SELECT mt.id, mt.name, mt.slug, mt.description, mt.category, mt.icon,
                   mt.input_format, mt.output_format, mt.availability, mt.price_eur,
                   mt.downloads_count, mt.rating_avg, mt.rating_count,
                   mt.status, mt.version, mt.created_at, mt.updated_at
            FROM mapping_templates mt
            WHERE {where}
            ORDER BY mt.updated_at DESC
        """, params)
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def get_my_purchases(self, org_id):
        """Templates purchased/installed by this org."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT tp.id as purchase_id, tp.price_paid_eur, tp.installed_at, tp.status as purchase_status,
                   mt.id as template_id, mt.name, mt.slug, mt.description, mt.category, mt.icon,
                   mt.input_format, mt.output_format, mt.version, mt.availability,
                   mt.rating_avg, mt.rating_count,
                   o.name as author_name
            FROM template_purchases tp
            JOIN mapping_templates mt ON tp.template_id = mt.id
            LEFT JOIN organizations o ON mt.author_org_id = o.id
            WHERE tp.buyer_org_id = %s
            ORDER BY tp.installed_at DESC
        """, (org_id,))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    # ──────────────────────────────────────────────────────────────
    # 8. CLONE
    # ──────────────────────────────────────────────────────────────

    def clone_template(self, template_id, org_id, user_id, new_name=None):
        """Clone a template as private copy."""
        tpl = self.get_template(template_id, org_id, include_mapping=True)
        if not tpl:
            raise ValueError("Template non trovato")

        new_data = {
            'name': new_name or (tpl['name'] + ' (copia)'),
            'description': tpl.get('description', ''),
            'long_description': tpl.get('long_description', ''),
            'category': tpl.get('category', 'other'),
            'input_standard': tpl.get('input_standard', ''),
            'output_standard': tpl.get('output_standard', ''),
            'input_format': tpl.get('input_format', ''),
            'output_format': tpl.get('output_format', ''),
            'mapping_data': tpl.get('mapping_data', {}),
            'sample_input': tpl.get('sample_input', ''),
            'sample_output': tpl.get('sample_output', ''),
            'availability': 'private',
            'tags': tpl.get('tags', []),
            'icon': tpl.get('icon', '📄'),
        }
        return self.create_template(org_id, user_id, new_data)

    # ──────────────────────────────────────────────────────────────
    # 9. CATEGORIES / STATS
    # ──────────────────────────────────────────────────────────────

    def get_categories(self):
        """Get category list with counts."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT category, COUNT(*) as count,
                   AVG(rating_avg) as avg_rating,
                   SUM(downloads_count) as total_downloads
            FROM mapping_templates
            WHERE status = 'published' AND availability != 'private'
            GROUP BY category ORDER BY count DESC
        """)
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def get_featured(self, limit=8):
        """Get featured / top templates."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT mt.id, mt.name, mt.slug, mt.description, mt.category, mt.icon,
                   mt.input_format, mt.output_format, mt.availability, mt.price_eur,
                   mt.downloads_count, mt.rating_avg, mt.rating_count, mt.featured,
                   o.name as author_name
            FROM mapping_templates mt
            LEFT JOIN organizations o ON mt.author_org_id = o.id
            WHERE mt.status = 'published' AND mt.availability != 'private'
            ORDER BY mt.featured DESC, mt.downloads_count DESC, mt.rating_avg DESC
            LIMIT %s
        """, (limit,))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def get_marketplace_stats(self):
        """Platform-level marketplace stats."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT
                COUNT(*) as total_templates,
                COUNT(*) FILTER (WHERE status='published') as published,
                COUNT(*) FILTER (WHERE availability='builtin') as builtin,
                COUNT(*) FILTER (WHERE availability='marketplace') as marketplace,
                COUNT(*) FILTER (WHERE availability='community') as community,
                COUNT(*) FILTER (WHERE availability='private') as private,
                SUM(downloads_count) as total_downloads,
                AVG(rating_avg) FILTER (WHERE rating_count > 0) as avg_rating,
                COUNT(DISTINCT author_org_id) as unique_authors
            FROM mapping_templates
        """)
        stats = self._ser_row(dict(cur.fetchone()))

        cur.execute("SELECT COUNT(*) as total FROM template_purchases")
        stats['total_purchases'] = cur.fetchone()['total']

        cur.execute("SELECT COUNT(*) as total FROM template_reviews")
        stats['total_reviews'] = cur.fetchone()['total']

        # Revenue from marketplace
        cur.execute("SELECT COALESCE(SUM(price_paid_eur), 0) as total FROM template_purchases WHERE status='active'")
        stats['total_revenue_eur'] = str(cur.fetchone()['total'])

        return stats

    # ──────────────────────────────────────────────────────────────
    # 10. SEED BUILTIN TEMPLATES
    # ──────────────────────────────────────────────────────────────

    def seed_builtin(self):
        """Seed built-in templates if none exist."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT COUNT(*) as cnt FROM mapping_templates WHERE availability='builtin'")
        if cur.fetchone()['cnt'] > 0:
            return 0

        builtins = [
            {'name': 'FatturaPA → UBL 2.1', 'slug': 'fatturapa-to-ubl',
             'description': 'Converti fatture elettroniche italiane FatturaPA nel formato europeo UBL 2.1 (Peppol BIS)',
             'category': 'invoice', 'input_standard': 'FatturaPA', 'output_standard': 'UBL-2.1',
             'input_format': 'xml', 'output_format': 'xml', 'icon': '🇮🇹'},

            {'name': 'UBL 2.1 → FatturaPA', 'slug': 'ubl-to-fatturapa',
             'description': 'Converti fatture europee UBL 2.1 nel formato italiano FatturaPA per invio SdI',
             'category': 'invoice', 'input_standard': 'UBL-2.1', 'output_standard': 'FatturaPA',
             'input_format': 'xml', 'output_format': 'xml', 'icon': '🇪🇺'},

            {'name': 'X12 810 → UBL Invoice', 'slug': 'x12-810-to-ubl',
             'description': 'ANSI X12 EDI 810 Invoice to UBL 2.1 Invoice format conversion',
             'category': 'invoice', 'input_standard': 'X12-810', 'output_standard': 'UBL-2.1',
             'input_format': 'edi', 'output_format': 'xml', 'icon': '🇺🇸'},

            {'name': 'EDIFACT INVOIC → JSON', 'slug': 'edifact-invoic-json',
             'description': 'UN/EDIFACT INVOIC D96A to structured JSON for API integration',
             'category': 'invoice', 'input_standard': 'EDIFACT-INVOIC', 'output_standard': 'JSON-Invoice',
             'input_format': 'edifact', 'output_format': 'json', 'icon': '🔄'},

            {'name': 'X12 850 Purchase Order → UBL', 'slug': 'x12-850-to-ubl',
             'description': 'ANSI X12 850 Purchase Order to UBL 2.1 Order format',
             'category': 'order', 'input_standard': 'X12-850', 'output_standard': 'UBL-2.1',
             'input_format': 'edi', 'output_format': 'xml', 'icon': '📦'},

            {'name': 'EDIFACT DESADV → JSON Dispatch', 'slug': 'edifact-desadv-json',
             'description': 'UN/EDIFACT DESADV dispatch advice to JSON for warehouse systems',
             'category': 'dispatch', 'input_standard': 'EDIFACT-DESADV', 'output_standard': 'JSON-Dispatch',
             'input_format': 'edifact', 'output_format': 'json', 'icon': '🚚'},

            {'name': 'CSV Fatture → FatturaPA XML', 'slug': 'csv-to-fatturapa',
             'description': 'Converti un CSV con dati fattura nel formato FatturaPA XML pronto per SdI',
             'category': 'invoice', 'input_standard': 'CSV-Custom', 'output_standard': 'FatturaPA',
             'input_format': 'csv', 'output_format': 'xml', 'icon': '📊'},

            {'name': 'SWIFT MT103 → JSON Payment', 'slug': 'swift-mt103-json',
             'description': 'SWIFT MT103 single customer credit transfer to JSON payment object',
             'category': 'payment', 'input_standard': 'SWIFT-MT103', 'output_standard': 'JSON-Payment',
             'input_format': 'fixed', 'output_format': 'json', 'icon': '💳'},
        ]

        count = 0
        cur2 = self.conn.cursor()
        for b in builtins:
            try:
                cur2.execute("""
                    INSERT INTO mapping_templates
                        (id, name, slug, description, category, input_standard, output_standard,
                         input_format, output_format, availability, status, icon, mapping_data,
                         featured, version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'builtin', 'published', %s, '{}', TRUE, '1.0.0')
                """, (str(uuid.uuid4()), b['name'], b['slug'], b['description'],
                      b['category'], b['input_standard'], b['output_standard'],
                      b['input_format'], b['output_format'], b.get('icon', '📄')))
                count += 1
            except Exception as e:
                pass  # slug conflict, skip

        self.conn.commit()
        print(f"   ✅ Seeded {count} builtin templates")
        return count
