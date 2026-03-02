#!/usr/bin/env python3
"""
Buddyliko — Approval Service (Phase 5: Approval Workflow)
═════════════════════════════════════════════════════════
Gestisce richieste di approvazione per operazioni protette.

Flusso:
1. Un utente (operator, tech_lead) tenta un'operazione protetta
2. Se non ha il permesso `*.approve`, viene creata una approval_request
3. Un admin/owner con `*.approve` vede la richiesta e la approva/rigetta
4. Se approvata, l'operazione viene eseguita (callback)
5. Le richieste scadono dopo 48h

Tabella: approval_requests
Operazioni protette: batch_execute, mapping_delete_production, dbconn_write,
                     token_create, token_revoke, member_remove, org_settings_change
"""

import json
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Callable


# Operazioni che richiedono approvazione se l'utente non ha il permesso diretto
APPROVAL_OPERATIONS = {
    'batch_execute': {
        'label': 'Esecuzione batch',
        'description': 'Esegui un batch di trasformazioni',
        'required_permission': 'transforms:approve',
        'approver_permission': 'transforms:approve',
    },
    'mapping_delete_production': {
        'label': 'Eliminazione mapping di produzione',
        'description': 'Elimina un mapping utilizzato in produzione',
        'required_permission': 'mappings:delete',
        'approver_permission': 'mappings:delete',
    },
    'dbconn_write': {
        'label': 'Scrittura su DB esterno',
        'description': 'Esegui write SQL su un database collegato',
        'required_permission': 'dbconn:execute',
        'approver_permission': 'dbconn:execute',
    },
    'token_create': {
        'label': 'Creazione API key',
        'description': 'Crea una nuova API key per l\'organizzazione',
        'required_permission': 'tokens:manage',
        'approver_permission': 'tokens:manage',
    },
    'token_revoke': {
        'label': 'Revoca API key',
        'description': 'Revoca una API key esistente',
        'required_permission': 'tokens:manage',
        'approver_permission': 'tokens:manage',
    },
    'member_remove': {
        'label': 'Rimozione membro',
        'description': 'Rimuovi un membro dall\'organizzazione',
        'required_permission': 'members:manage',
        'approver_permission': 'members:manage',
    },
    'org_settings_change': {
        'label': 'Modifica impostazioni org',
        'description': 'Modifica le impostazioni dell\'organizzazione',
        'required_permission': 'settings:manage',
        'approver_permission': 'settings:manage',
    },
}


class ApprovalService:
    """Gestione richieste di approvazione."""

    EXPIRY_HOURS = 48

    def __init__(self, conn, cursor_factory, permission_service=None):
        self.conn = conn
        self.cursor_factory = cursor_factory
        self.permission_service = permission_service

    # ══════════════════════════════════════════════════════════════════
    # RICHIESTE
    # ══════════════════════════════════════════════════════════════════

    def create_request(self, org_id: str, requested_by: int,
                       operation: str, payload: dict) -> dict:
        """
        Crea una richiesta di approvazione.
        Returns: la request creata con ID.
        """
        if operation not in APPROVAL_OPERATIONS:
            raise ValueError(f"Operazione non valida: {operation}")

        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            request_id = str(uuid.uuid4())
            expires_at = datetime.utcnow() + timedelta(hours=self.EXPIRY_HOURS)

            cur.execute("""
                INSERT INTO approval_requests (id, org_id, requested_by, operation, payload, status, expires_at)
                VALUES (%s, %s, %s, %s, %s, 'pending', %s)
                RETURNING *
            """, (request_id, org_id, requested_by, operation, json.dumps(payload), expires_at))
            row = cur.fetchone()
            self.conn.commit()
            cur.close()

            result = dict(row)
            result['id'] = str(result['id'])
            result['org_id'] = str(result['org_id'])
            if isinstance(result.get('payload'), str):
                result['payload'] = json.loads(result['payload'])
            result['operation_label'] = APPROVAL_OPERATIONS[operation]['label']
            result['operation_description'] = APPROVAL_OPERATIONS[operation]['description']
            return result
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore creazione richiesta: {e}")

    def list_pending(self, org_id: str, approver_user_id: int = None) -> List[dict]:
        """
        Lista richieste pending per un'org.
        Se approver_user_id fornito, filtra solo quelle che l'utente può approvare.
        """
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            # Prima: scadere le vecchie
            cur.execute("""
                UPDATE approval_requests SET status = 'expired'
                WHERE org_id = %s AND status = 'pending' AND expires_at < NOW()
            """, (org_id,))

            cur.execute("""
                SELECT ar.*, u.email as requester_email, u.name as requester_name
                FROM approval_requests ar
                JOIN users u ON u.id = ar.requested_by
                WHERE ar.org_id = %s AND ar.status = 'pending'
                ORDER BY ar.created_at DESC
            """, (org_id,))
            rows = cur.fetchall()
            self.conn.commit()
            cur.close()

            result = []
            for r in rows:
                d = dict(r)
                d['id'] = str(d['id'])
                d['org_id'] = str(d['org_id'])
                if isinstance(d.get('payload'), str):
                    d['payload'] = json.loads(d['payload'])
                op_info = APPROVAL_OPERATIONS.get(d['operation'], {})
                d['operation_label'] = op_info.get('label', d['operation'])
                d['operation_description'] = op_info.get('description', '')

                # Filtra per permessi dell'approver se richiesto
                if approver_user_id and self.permission_service:
                    approver_perm = op_info.get('approver_permission', '')
                    if approver_perm:
                        scope, action = approver_perm.split(':')
                        if not self.permission_service.has_permission(
                            approver_user_id, str(d['org_id']), scope, action
                        ):
                            continue
                result.append(d)
            return result
        except Exception as e:
            print(f"⚠️  ApprovalService.list_pending error: {e}")
            return []

    def list_all(self, org_id: str, limit: int = 50) -> List[dict]:
        """Lista tutte le richieste (pending, approved, rejected, expired)."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            # Scadere le vecchie
            cur.execute("""
                UPDATE approval_requests SET status = 'expired'
                WHERE org_id = %s AND status = 'pending' AND expires_at < NOW()
            """, (org_id,))

            cur.execute("""
                SELECT ar.*,
                       u1.email as requester_email, u1.name as requester_name,
                       u2.email as approver_email, u2.name as approver_name
                FROM approval_requests ar
                JOIN users u1 ON u1.id = ar.requested_by
                LEFT JOIN users u2 ON u2.id = ar.approved_by
                WHERE ar.org_id = %s
                ORDER BY ar.created_at DESC
                LIMIT %s
            """, (org_id, limit))
            rows = cur.fetchall()
            self.conn.commit()
            cur.close()

            result = []
            for r in rows:
                d = dict(r)
                d['id'] = str(d['id'])
                d['org_id'] = str(d['org_id'])
                if isinstance(d.get('payload'), str):
                    d['payload'] = json.loads(d['payload'])
                op_info = APPROVAL_OPERATIONS.get(d['operation'], {})
                d['operation_label'] = op_info.get('label', d['operation'])
                result.append(d)
            return result
        except Exception as e:
            print(f"⚠️  ApprovalService.list_all error: {e}")
            return []

    def get_request(self, request_id: str) -> Optional[dict]:
        """Dettaglio singola richiesta."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                SELECT ar.*,
                       u1.email as requester_email, u1.name as requester_name,
                       u2.email as approver_email, u2.name as approver_name
                FROM approval_requests ar
                JOIN users u1 ON u1.id = ar.requested_by
                LEFT JOIN users u2 ON u2.id = ar.approved_by
                WHERE ar.id = %s
            """, (request_id,))
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            d = dict(row)
            d['id'] = str(d['id'])
            d['org_id'] = str(d['org_id'])
            if isinstance(d.get('payload'), str):
                d['payload'] = json.loads(d['payload'])
            op_info = APPROVAL_OPERATIONS.get(d['operation'], {})
            d['operation_label'] = op_info.get('label', d['operation'])
            d['operation_description'] = op_info.get('description', '')
            return d
        except Exception:
            return None

    # ══════════════════════════════════════════════════════════════════
    # APPROVE / REJECT
    # ══════════════════════════════════════════════════════════════════

    def approve(self, request_id: str, approved_by: int, org_id: str) -> dict:
        """
        Approva una richiesta pending.
        Verifica che l'approver abbia il permesso necessario.
        """
        req = self.get_request(request_id)
        if not req:
            raise ValueError("Richiesta non trovata")
        if str(req['org_id']) != str(org_id):
            raise ValueError("Richiesta non appartiene a questa org")
        if req['status'] != 'pending':
            raise ValueError(f"Richiesta non è più pending (status: {req['status']})")
        if req['expires_at'] and req['expires_at'] < datetime.utcnow():
            # Scaduta
            self._mark_expired(request_id)
            raise ValueError("Richiesta scaduta")
        if req['requested_by'] == approved_by:
            raise ValueError("Non puoi approvare la tua stessa richiesta")

        # Verifica permesso approver
        if self.permission_service:
            op_info = APPROVAL_OPERATIONS.get(req['operation'], {})
            approver_perm = op_info.get('approver_permission', '')
            if approver_perm:
                scope, action = approver_perm.split(':')
                if not self.permission_service.has_permission(approved_by, org_id, scope, action):
                    raise ValueError("Non hai il permesso per approvare questa operazione")

        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                UPDATE approval_requests
                SET status = 'approved', approved_by = %s, approved_at = NOW()
                WHERE id = %s AND status = 'pending'
                RETURNING *
            """, (approved_by, request_id))
            row = cur.fetchone()
            self.conn.commit()
            cur.close()

            if not row:
                raise ValueError("Richiesta non aggiornata (potrebbe essere stata già processata)")

            d = dict(row)
            d['id'] = str(d['id'])
            d['org_id'] = str(d['org_id'])
            if isinstance(d.get('payload'), str):
                d['payload'] = json.loads(d['payload'])
            return d
        except ValueError:
            raise
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore approvazione: {e}")

    def reject(self, request_id: str, rejected_by: int, org_id: str,
               note: str = None) -> dict:
        """Rigetta una richiesta pending."""
        req = self.get_request(request_id)
        if not req:
            raise ValueError("Richiesta non trovata")
        if str(req['org_id']) != str(org_id):
            raise ValueError("Richiesta non appartiene a questa org")
        if req['status'] != 'pending':
            raise ValueError(f"Richiesta non è più pending (status: {req['status']})")

        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                UPDATE approval_requests
                SET status = 'rejected', approved_by = %s, approved_at = NOW(),
                    rejection_note = %s
                WHERE id = %s AND status = 'pending'
                RETURNING *
            """, (rejected_by, note, request_id))
            row = cur.fetchone()
            self.conn.commit()
            cur.close()

            if not row:
                raise ValueError("Richiesta non aggiornata")
            d = dict(row)
            d['id'] = str(d['id'])
            d['org_id'] = str(d['org_id'])
            if isinstance(d.get('payload'), str):
                d['payload'] = json.loads(d['payload'])
            return d
        except ValueError:
            raise
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore rigetto: {e}")

    def _mark_expired(self, request_id: str):
        """Segna una richiesta come scaduta."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                UPDATE approval_requests SET status = 'expired'
                WHERE id = %s AND status = 'pending'
            """, (request_id,))
            self.conn.commit()
            cur.close()
        except Exception:
            self.conn.rollback()

    # ══════════════════════════════════════════════════════════════════
    # HELPER: check-or-request
    # ══════════════════════════════════════════════════════════════════

    def check_or_request(self, user_id: int, org_id: str,
                         operation: str, payload: dict) -> dict:
        """
        Check se l'utente ha il permesso diretto per l'operazione.
        Se sì: ritorna {"allowed": True}
        Se no: crea una approval_request e ritorna {"allowed": False, "request": ...}
        
        Usato dai controller per decidere se eseguire subito o mettere in coda.
        """
        op_info = APPROVAL_OPERATIONS.get(operation)
        if not op_info:
            return {"allowed": True}  # operazione non protetta

        if not self.permission_service:
            return {"allowed": True}  # no permission service → tutto permesso

        required = op_info['required_permission']
        scope, action = required.split(':')

        if self.permission_service.has_permission(user_id, org_id, scope, action):
            return {"allowed": True}

        # Non ha il permesso → crea richiesta
        request = self.create_request(org_id, user_id, operation, payload)
        return {
            "allowed": False,
            "request": request,
            "message": f"Richiesta di approvazione inviata per: {op_info['label']}"
        }

    # ══════════════════════════════════════════════════════════════════
    # STATS
    # ══════════════════════════════════════════════════════════════════

    def get_stats(self, org_id: str) -> dict:
        """Statistiche delle richieste per l'org."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            # Scadere le vecchie
            cur.execute("""
                UPDATE approval_requests SET status = 'expired'
                WHERE org_id = %s AND status = 'pending' AND expires_at < NOW()
            """, (org_id,))

            cur.execute("""
                SELECT status, COUNT(*) as count
                FROM approval_requests
                WHERE org_id = %s
                GROUP BY status
            """, (org_id,))
            rows = cur.fetchall()
            self.conn.commit()
            cur.close()

            stats = {'pending': 0, 'approved': 0, 'rejected': 0, 'expired': 0, 'total': 0}
            for r in rows:
                stats[r['status']] = r['count']
                stats['total'] += r['count']
            return stats
        except Exception:
            return {'pending': 0, 'approved': 0, 'rejected': 0, 'expired': 0, 'total': 0}

    def get_pending_count(self, org_id: str) -> int:
        """Conteggio rapido delle richieste pending."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                SELECT COUNT(*) as count FROM approval_requests
                WHERE org_id = %s AND status = 'pending' AND expires_at > NOW()
            """, (org_id,))
            row = cur.fetchone()
            cur.close()
            return row['count'] if row else 0
        except Exception:
            return 0
