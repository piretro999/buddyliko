import React, { useState } from 'react';
import './SchemaUpload.css';

export function SchemaUpload({ onUploadSuccess }) {
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [dragOver, setDragOver] = useState(false);

  const handleUpload = async (file) => {
    if (!file) return;

    // Validate file type
    if (!file.name.endsWith('.zip')) {
      setError('Please upload a .zip file');
      return;
    }

    setUploading(true);
    setError(null);
    setSuccess(null);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const res = await fetch('/api/schemas/upload', {
        method: 'POST',
        body: formData
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.detail || 'Upload failed');
      }

      const data = await res.json();
      setSuccess(`✅ Schema "${data.schemaName}" uploaded successfully!`);
      
      // Notify parent component
      if (onUploadSuccess) {
        onUploadSuccess(data.schemaName);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setUploading(false);
    }
  };

  const handleFileSelect = (e) => {
    const file = e.target.files[0];
    if (file) {
      handleUpload(file);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    
    const file = e.dataTransfer.files[0];
    if (file) {
      handleUpload(file);
    }
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    setDragOver(true);
  };

  const handleDragLeave = () => {
    setDragOver(false);
  };

  return (
    <div className="schema-upload">
      <h3>📤 Upload New Schema</h3>
      
      <div className="upload-info">
        <p className="info-title">Required ZIP structure:</p>
        <pre className="zip-structure">{`schema_name/
├── schema.xsd   (required)
└── rules.sch    (optional)`}</pre>
        <p className="info-note">
          The schema will be installed in both <code>schemas/input/</code> and <code>schemas/output/</code>
        </p>
      </div>

      <div
        className={`upload-zone ${dragOver ? 'drag-over' : ''}`}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
      >
        <input
          type="file"
          id="schema-file-input"
          accept=".zip"
          onChange={handleFileSelect}
          disabled={uploading}
          className="file-input"
        />
        
        <label htmlFor="schema-file-input" className="upload-label">
          {uploading ? (
            <>
              <span className="upload-icon">⏳</span>
              <span>Uploading...</span>
            </>
          ) : (
            <>
              <span className="upload-icon">📁</span>
              <span>Click or drag ZIP file here</span>
            </>
          )}
        </label>
      </div>

      {error && (
        <div className="upload-error">
          <span>❌ {error}</span>
        </div>
      )}

      {success && (
        <div className="upload-success">
          <span>{success}</span>
        </div>
      )}
    </div>
  );
}
