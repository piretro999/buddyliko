#!/usr/bin/env python3
"""
SFTP Monitor & Transformation API
Monitors SFTP folders and transforms files automatically

Features:
- Watch SFTP folders for new files
- Auto-transform based on rules
- Upload results to destination
- Error handling and logging
- API endpoints for transformation
"""

import os
import time
import json
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# ===========================================================================
# SFTP MONITOR
# ===========================================================================

class SFTPMonitor:
    """Monitor SFTP folder and auto-transform files"""
    
    def __init__(self, 
                 watch_dir: str,
                 output_dir: str,
                 transformation_engine,
                 mapping_rules: Dict,
                 input_format: str = 'xml',
                 output_format: str = 'xml',
                 file_pattern: str = '*.xml'):
        
        self.watch_dir = watch_dir
        self.output_dir = output_dir
        self.transformation_engine = transformation_engine
        self.mapping_rules = mapping_rules
        self.input_format = input_format
        self.output_format = output_format
        self.file_pattern = file_pattern
        
        # Create directories
        os.makedirs(watch_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(f"{output_dir}/success", exist_ok=True)
        os.makedirs(f"{output_dir}/error", exist_ok=True)
        
        # Statistics
        self.stats = {
            'processed': 0,
            'success': 0,
            'errors': 0,
            'last_processed': None
        }
    
    def start(self):
        """Start monitoring folder"""
        event_handler = SFTPFileHandler(self)
        observer = Observer()
        observer.schedule(event_handler, self.watch_dir, recursive=False)
        observer.start()
        
        print(f"🔍 Monitoring: {self.watch_dir}")
        print(f"📤 Output to: {self.output_dir}")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
    
    def process_file(self, file_path: str):
        """Process single file"""
        try:
            print(f"📥 Processing: {file_path}")
            
            # Read input
            with open(file_path, 'r', encoding='utf-8') as f:
                input_content = f.read()
            
            # Transform
            result = self.transformation_engine.transform(
                input_content=input_content,
                input_format=self.input_format,
                output_format=self.output_format,
                mapping_rules=self.mapping_rules,
                validate_input=True,
                validate_output=True
            )
            
            # Handle result
            filename = os.path.basename(file_path)
            base_name = os.path.splitext(filename)[0]
            
            if result.success:
                # Write output
                output_file = f"{self.output_dir}/success/{base_name}_transformed.{self.output_format}"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(result.output_content)
                
                print(f"✅ Success: {output_file}")
                self.stats['success'] += 1
            
            else:
                # Write error report
                error_file = f"{self.output_dir}/error/{base_name}_error.json"
                error_report = {
                    'input_file': file_path,
                    'timestamp': datetime.now().isoformat(),
                    'validation_errors': result.validation_errors,
                    'transformation_errors': result.transformation_errors,
                    'warnings': result.warnings
                }
                
                with open(error_file, 'w', encoding='utf-8') as f:
                    json.dump(error_report, f, indent=2)
                
                print(f"❌ Error: {error_file}")
                self.stats['errors'] += 1
            
            self.stats['processed'] += 1
            self.stats['last_processed'] = datetime.now().isoformat()
            
            # Archive processed file
            archive_dir = f"{self.watch_dir}/processed"
            os.makedirs(archive_dir, exist_ok=True)
            archive_path = f"{archive_dir}/{filename}"
            os.rename(file_path, archive_path)
        
        except Exception as e:
            print(f"❌ Processing error: {e}")
            self.stats['errors'] += 1


class SFTPFileHandler(FileSystemEventHandler):
    """Handle file system events"""
    
    def __init__(self, monitor: SFTPMonitor):
        self.monitor = monitor
    
    def on_created(self, event):
        """File created in watch folder"""
        if event.is_directory:
            return
        
        file_path = event.src_path
        
        # Check if matches pattern
        if self.monitor.file_pattern != '*':
            pattern = self.monitor.file_pattern.replace('*', '.*')
            import re
            if not re.match(pattern, os.path.basename(file_path)):
                return
        
        # Wait for file to be fully written
        time.sleep(1)
        
        # Process file
        self.monitor.process_file(file_path)


# ===========================================================================
# TRANSFORMATION API ENDPOINTS (for api.py)
# ===========================================================================

class TransformationAPI:
    """API endpoints for transformation service"""
    
    def __init__(self, app, transformation_engine):
        self.app = app
        self.engine = transformation_engine
        self.active_monitors = {}
    
    def register_endpoints(self):
        """Register all transformation endpoints"""
        
        from fastapi import File, UploadFile, HTTPException, BackgroundTasks
        from pydantic import BaseModel
        
        # ===========================================================
        # POST /api/transform
        # ===========================================================
        
        class TransformRequest(BaseModel):
            input_format: str  # xml, json, csv
            output_format: str  # xml, json, csv
            mapping_id: str  # ID of saved mapping
            validate_input: bool = True
            validate_output: bool = True
        
        @self.app.post("/api/transform")
        async def transform_file(
            file: UploadFile = File(...),
            input_format: str = 'xml',
            output_format: str = 'xml',
            mapping_id: str = None,
            validate_input: bool = True,
            validate_output: bool = True
        ):
            """
            Transform uploaded file using saved mapping
            
            Example:
                curl -X POST http://localhost:8080/api/transform \
                  -F "file=@input.xml" \
                  -F "input_format=xml" \
                  -F "output_format=xml" \
                  -F "mapping_id=mapping_123"
            """
            try:
                # Read file
                content = await file.read()
                input_content = content.decode('utf-8')
                
                # Load mapping rules
                # TODO: Get from storage by mapping_id
                mapping_rules = {}  # Placeholder
                
                # Transform
                result = self.engine.transform(
                    input_content=input_content,
                    input_format=input_format,
                    output_format=output_format,
                    mapping_rules=mapping_rules,
                    validate_input=validate_input,
                    validate_output=validate_output
                )
                
                if result.success:
                    return {
                        "success": True,
                        "output": result.output_content,
                        "format": result.output_format,
                        "metadata": result.metadata
                    }
                else:
                    return {
                        "success": False,
                        "validation_errors": result.validation_errors,
                        "transformation_errors": result.transformation_errors,
                        "warnings": result.warnings
                    }
            
            except Exception as e:
                raise HTTPException(500, f"Transformation error: {str(e)}")
        
        # ===========================================================
        # POST /api/transform/batch
        # ===========================================================
        
        @self.app.post("/api/transform/batch")
        async def transform_batch(
            files: List[UploadFile] = File(...),
            input_format: str = 'xml',
            output_format: str = 'xml',
            mapping_id: str = None
        ):
            """Transform multiple files at once"""
            results = []
            
            for file in files:
                try:
                    content = await file.read()
                    input_content = content.decode('utf-8')
                    
                    # TODO: Load mapping
                    mapping_rules = {}
                    
                    result = self.engine.transform(
                        input_content=input_content,
                        input_format=input_format,
                        output_format=output_format,
                        mapping_rules=mapping_rules
                    )
                    
                    results.append({
                        "filename": file.filename,
                        "success": result.success,
                        "output": result.output_content if result.success else None,
                        "errors": result.validation_errors + result.transformation_errors if not result.success else []
                    })
                
                except Exception as e:
                    results.append({
                        "filename": file.filename,
                        "success": False,
                        "errors": [str(e)]
                    })
            
            return {
                "total": len(files),
                "successful": sum(1 for r in results if r['success']),
                "failed": sum(1 for r in results if not r['success']),
                "results": results
            }
        
        # ===========================================================
        # POST /api/monitor/start
        # ===========================================================
        
        class MonitorConfig(BaseModel):
            name: str
            watch_dir: str
            output_dir: str
            mapping_id: str
            input_format: str = 'xml'
            output_format: str = 'xml'
            file_pattern: str = '*.xml'
        
        @self.app.post("/api/monitor/start")
        async def start_monitor(config: MonitorConfig, background_tasks: BackgroundTasks):
            """Start SFTP folder monitoring"""
            
            if config.name in self.active_monitors:
                raise HTTPException(400, f"Monitor '{config.name}' already running")
            
            # TODO: Load mapping
            mapping_rules = {}
            
            # Create monitor
            monitor = SFTPMonitor(
                watch_dir=config.watch_dir,
                output_dir=config.output_dir,
                transformation_engine=self.engine,
                mapping_rules=mapping_rules,
                input_format=config.input_format,
                output_format=config.output_format,
                file_pattern=config.file_pattern
            )
            
            # Start in background
            thread = threading.Thread(target=monitor.start, daemon=True)
            thread.start()
            
            self.active_monitors[config.name] = {
                'monitor': monitor,
                'thread': thread,
                'config': config,
                'started_at': datetime.now().isoformat()
            }
            
            return {
                "success": True,
                "message": f"Monitor '{config.name}' started",
                "watch_dir": config.watch_dir,
                "output_dir": config.output_dir
            }
        
        # ===========================================================
        # GET /api/monitor/status
        # ===========================================================
        
        @self.app.get("/api/monitor/status")
        async def monitor_status():
            """Get status of all active monitors"""
            status = {}
            
            for name, monitor_info in self.active_monitors.items():
                monitor = monitor_info['monitor']
                status[name] = {
                    'config': monitor_info['config'].dict(),
                    'stats': monitor.stats,
                    'started_at': monitor_info['started_at'],
                    'is_alive': monitor_info['thread'].is_alive()
                }
            
            return {
                "active_monitors": len(self.active_monitors),
                "monitors": status
            }
        
        # ===========================================================
        # POST /api/monitor/stop/{name}
        # ===========================================================
        
        @self.app.post("/api/monitor/stop/{name}")
        async def stop_monitor(name: str):
            """Stop SFTP folder monitoring"""
            
            if name not in self.active_monitors:
                raise HTTPException(404, f"Monitor '{name}' not found")
            
            # TODO: Graceful shutdown
            del self.active_monitors[name]
            
            return {
                "success": True,
                "message": f"Monitor '{name}' stopped"
            }
        
        # ===========================================================
        # POST /api/validate
        # ===========================================================
        
        @self.app.post("/api/validate")
        async def validate_file(
            file: UploadFile = File(...),
            format_type: str = 'xml',
            xsd_path: Optional[str] = None,
            schematron_path: Optional[str] = None
        ):
            """Validate file against XSD and Schematron"""
            try:
                content = await file.read()
                file_content = content.decode('utf-8')
                
                errors = []
                
                # XSD validation
                if xsd_path and format_type == 'xml':
                    from transformation_engine import XSDValidator
                    validator = XSDValidator(xsd_path)
                    valid, xsd_errors = validator.validate(file_content)
                    if not valid:
                        errors.extend([f"XSD: {e}" for e in xsd_errors])
                
                # Schematron validation
                if schematron_path and format_type == 'xml':
                    from transformation_engine import SchematronValidator
                    validator = SchematronValidator(schematron_path)
                    valid, sch_errors = validator.validate(file_content)
                    if not valid:
                        errors.extend([f"Schematron: {e}" for e in sch_errors])
                
                return {
                    "valid": len(errors) == 0,
                    "errors": errors,
                    "filename": file.filename
                }
            
            except Exception as e:
                raise HTTPException(500, f"Validation error: {str(e)}")


# ===========================================================================
# USAGE EXAMPLE
# ===========================================================================

if __name__ == '__main__':
    from transformation_engine import TransformationEngine
    
    # Create engine
    engine = TransformationEngine(
        input_xsd='schemas/FatturaPA.xsd',
        output_xsd='schemas/UBL-Invoice.xsd'
    )
    
    # Start SFTP monitor
    monitor = SFTPMonitor(
        watch_dir='sftp/incoming',
        output_dir='sftp/outgoing',
        transformation_engine=engine,
        mapping_rules={},  # Load from config
        input_format='xml',
        output_format='xml',
        file_pattern='*.xml'
    )
    
    monitor.start()
