import React, { useState, useEffect } from 'react';
import './SchemaSelector.css';

export function SchemaSelector({ type, value, onChange, label }) {
  const [schemas, setSchemas] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchSchemas();
  }, []);

  const fetchSchemas = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/schemas/list');
      
      if (!res.ok) {
        throw new Error('Failed to load schemas');
      }
      
      const data = await res.json();
      setSchemas(type === 'input' ? data.input : data.output);
      setError(null);
    } catch (err) {
      console.error('Failed to load schemas:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (e) => {
    onChange(e.target.value);
  };

  return (
    <div className="schema-selector">
      <label className="schema-label">
        {label || (type === 'input' ? 'Input Schema Type' : 'Output Schema Type')}
      </label>
      
      {loading && (
        <div className="schema-loading">
          <span>⏳ Loading schemas...</span>
        </div>
      )}
      
      {error && (
        <div className="schema-error">
          <span>❌ {error}</span>
          <button onClick={fetchSchemas} className="retry-btn">
            🔄 Retry
          </button>
        </div>
      )}
      
      {!loading && !error && (
        <select
          value={value || ''}
          onChange={handleChange}
          className="schema-select"
        >
          <option value="">-- Select schema type --</option>
          {schemas.map(schema => (
            <option key={schema.name} value={schema.name}>
              {schema.name}
              {!schema.hasXsd && ' ⚠️ (no XSD)'}
              {schema.hasXsd && schema.hasSchematron && ' ✅'}
            </option>
          ))}
        </select>
      )}
      
      {!loading && schemas.length === 0 && !error && (
        <div className="schema-empty">
          <p>📭 No schemas available</p>
          <p className="hint">Upload a schema ZIP to get started</p>
        </div>
      )}
    </div>
  );
}
