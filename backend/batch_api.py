#!/usr/bin/env python3
"""
Buddyliko — Batch API (Phase 8 Part 2A) — 8 endpoint REST
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict

class BatchCreate(BaseModel):
    name: Optional[str] = None
    items: List[Dict] = Field(default_factory=list)
    config: Optional[Dict] = None
    template_id: Optional[str] = None
    operation_type: str = 'transform'

class BatchItemInput(BaseModel):
    name: str
    data_base64: Optional[str] = None


def register_batch_endpoints(app, get_auth_context, require_org_role, batch_service):
    from fastapi import Depends, HTTPException, Query, UploadFile, File, Form
    from typing import List as TList
    import tempfile, os, base64, json

    # 1. Create batch from JSON
    @app.post("/api/batch")
    def create_batch(data: BatchCreate, ctx=Depends(require_org_role('operator'))):
        if not data.items:
            raise HTTPException(400, "Almeno un item richiesto")

        items_data = []
        for i, item in enumerate(data.items):
            # If base64 data provided, save to temp file
            path = ''
            size = 0
            if item.get('data_base64'):
                try:
                    raw = base64.b64decode(item['data_base64'])
                    tf = tempfile.NamedTemporaryFile(delete=False, suffix='.dat')
                    tf.write(raw); tf.close()
                    path = tf.name; size = len(raw)
                except Exception as e:
                    raise HTTPException(400, f"Item {i}: {e}")
            items_data.append({
                'name': item.get('name', f'item_{i+1}'),
                'path': path, 'size': size
            })

        try:
            batch = batch_service.create_batch(
                ctx.org_id, ctx.user_id, data.name, items_data,
                data.config, data.template_id, data.operation_type)
            return {"success": True, "batch": batch}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 2. Upload batch (multipart)
    @app.post("/api/batch/upload")
    async def upload_batch(
        files: TList[UploadFile] = File(...),
        name: str = Form(None),
        config: str = Form('{}'),
        template_id: str = Form(None),
        ctx=Depends(require_org_role('operator'))
    ):
        if not files:
            raise HTTPException(400, "Nessun file")

        items_data = []
        for f in files:
            content = await f.read()
            tf = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(f.filename or '.dat')[1])
            tf.write(content); tf.close()
            items_data.append({
                'name': f.filename or 'file',
                'path': tf.name,
                'size': len(content)
            })

        try:
            cfg = json.loads(config) if config else {}
        except: cfg = {}

        try:
            batch = batch_service.create_batch(
                ctx.org_id, ctx.user_id,
                name or f"Upload batch ({len(files)} file)",
                items_data, cfg, template_id)
            return {"success": True, "batch": batch}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 3. Start processing
    @app.post("/api/batch/{batch_id}/start")
    def start_batch(batch_id: str, ctx=Depends(require_org_role('operator'))):
        try:
            batch_service.start_batch(batch_id)
            return {"success": True, "message": "Batch avviato"}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 4. Get batch status
    @app.get("/api/batch/{batch_id}")
    def get_batch(batch_id: str, ctx=Depends(require_org_role('viewer'))):
        batch = batch_service.get_batch(batch_id)
        if not batch:
            raise HTTPException(404, "Batch non trovato")
        return batch

    # 5. List batches
    @app.get("/api/batch")
    def list_batches(status: str = Query(None), limit: int = Query(50),
                     ctx=Depends(require_org_role('viewer'))):
        return {"batches": batch_service.list_batches(ctx.org_id, status, limit)}

    # 6. Cancel batch
    @app.delete("/api/batch/{batch_id}")
    def cancel_batch(batch_id: str, ctx=Depends(require_org_role('operator'))):
        ok = batch_service.cancel_batch(batch_id, ctx.org_id)
        if not ok:
            raise HTTPException(400, "Batch non cancellabile")
        return {"success": True}

    # 7. Download results ZIP
    @app.get("/api/batch/{batch_id}/download")
    def download_batch(batch_id: str, ctx=Depends(require_org_role('viewer'))):
        batch = batch_service.get_batch(batch_id, include_items=False)
        if not batch:
            raise HTTPException(404, "Batch non trovato")
        zip_path = batch.get('output_zip_path')
        if not zip_path or not os.path.exists(zip_path):
            raise HTTPException(404, "Output non disponibile")
        from fastapi.responses import FileResponse
        return FileResponse(zip_path, filename=f"batch_{batch_id[:8]}.zip",
                           media_type='application/zip')

    # 8. Batch stats
    @app.get("/api/batch/stats/overview")
    def batch_stats(ctx=Depends(require_org_role('viewer'))):
        return batch_service.get_batch_stats(ctx.org_id)

    print("   ✅ Batch API: 8 endpoints registered")
