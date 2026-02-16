import React, { useState, useEffect } from 'react';
import { SchemaUpload } from '../components/SchemaUpload';
import './SchemaManagement.css';

export function SchemaManagement() {
  const [schemas, setSchemas] = useState({ input: [], output: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadSchemas();
  }, []);

  const loadSchemas = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/schemas/list');
      
      if (!res.ok) {
        throw new Error('Failed to load schemas');
      }
      
      const data = await res.json();
      setSchemas(data);
      setError(null);
    } catch (err) {
      console.error('Failed to load schemas:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const deleteSchema = async (schemaName) => {
    if (!confirm(`Are you sure you want to delete schema "${schemaName}"?\n\nThis will remove it from both input and output directories.`)) {
      return;
    }

    try {
      const res = await fetch(`/api/schemas/${schemaName}`, {
        method: 'DELETE'
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.detail || 'Delete failed');
      }

      // Reload schemas list
      loadSchemas();
    } catch (err) {
      alert(`Failed to delete schema: ${err.message}`);
    }
  };

  const handleUploadSuccess = (schemaName) => {
    // Reload schemas list after successful upload
    loadSchemas();
  };

  return (
    <div className="schema-management">
      <div className="page-header">
        <h1>📋 Schema Management</h1>
        <p className="page-description">
          Manage XSD and Schematron validation schemas for input and output formats
        </p>
      </div>

      <SchemaUpload onUploadSuccess={handleUploadSuccess} />

      {loading && (
        <div className="loading-container">
          <p>⏳ Loading schemas...</p>
        </div>
      )}

      {error && (
        <div className="error-container">
          <p>❌ {error}</p>
          <button onClick={loadSchemas} className="retry-btn">
            🔄 Retry
          </button>
        </div>
      )}

      {!loading && !error && (
        <div className="schemas-grid">
          <div className="schema-column">
            <h2 className="column-title">
              <span className="icon">📥</span>
              Input Schemas ({schemas.input.length})
            </h2>
            
            {schemas.input.length === 0 ? (
              <div className="empty-state">
                <p>No input schemas available</p>
                <p className="hint">Upload a schema to get started</p>
              </div>
            ) : (
              <div className="schema-list">
                {schemas.input.map(schema => (
                  <SchemaCard
                    key={schema.name}
                    schema={schema}
                    onDelete={deleteSchema}
                  />
                ))}
              </div>
            )}
          </div>

          <div className="schema-column">
            <h2 className="column-title">
              <span className="icon">📤</span>
              Output Schemas ({schemas.output.length})
            </h2>
            
            {schemas.output.length === 0 ? (
              <div className="empty-state">
                <p>No output schemas available</p>
                <p className="hint">Upload a schema to get started</p>
              </div>
            ) : (
              <div className="schema-list">
                {schemas.output.map(schema => (
                  <SchemaCard
                    key={schema.name}
                    schema={schema}
                    onDelete={deleteSchema}
                  />
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function SchemaCard({ schema, onDelete }) {
  return (
    <div className="schema-card">
      <div className="card-header">
        <h3 className="schema-name">{schema.name}</h3>
        <button
          className="delete-btn"
          onClick={() => onDelete(schema.name)}
          title="Delete schema"
        >
          🗑️
        </button>
      </div>
      
      <div className="card-body">
        <div className="schema-files">
          <div className={`file-badge ${schema.hasXsd ? 'has-file' : 'missing-file'}`}>
            {schema.hasXsd ? '✅' : '❌'} schema.xsd
          </div>
          <div className={`file-badge ${schema.hasSchematron ? 'has-file' : 'missing-file'}`}>
            {schema.hasSchematron ? '✅' : '⚪'} rules.sch
          </div>
        </div>
        
        {!schema.hasXsd && (
          <div className="warning-message">
            ⚠️ Missing required schema.xsd file
          </div>
        )}
      </div>
    </div>
  );
}
