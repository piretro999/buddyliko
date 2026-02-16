import React, { useState, useEffect } from 'react';
import { SchemaSelector } from '../components/SchemaSelector';
import './ProjectSettings.css';

export function ProjectSettings() {
  const [project, setProject] = useState({
    projectName: '',
    inputSchema: {
      name: '',
      format: 'xml',
      schemaType: '',
      fields: []
    },
    outputSchema: {
      name: '',
      format: 'xml',
      schemaType: '',
      fields: []
    },
    connections: []
  });

  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const updateProject = (updates) => {
    setProject(prev => ({ ...prev, ...updates }));
    setSaved(false);
  };

  const updateInputSchema = (updates) => {
    setProject(prev => ({
      ...prev,
      inputSchema: { ...prev.inputSchema, ...updates }
    }));
    setSaved(false);
  };

  const updateOutputSchema = (updates) => {
    setProject(prev => ({
      ...prev,
      outputSchema: { ...prev.outputSchema, ...updates }
    }));
    setSaved(false);
  };

  const saveProject = async () => {
    setSaving(true);
    try {
      // Here you would save to backend
      // For now, just save to localStorage
      localStorage.setItem('current_project', JSON.stringify(project));
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (error) {
      console.error('Failed to save project:', error);
      alert('Failed to save project');
    } finally {
      setSaving(false);
    }
  };

  // Load project from localStorage on mount
  useEffect(() => {
    const savedProject = localStorage.getItem('current_project');
    if (savedProject) {
      try {
        setProject(JSON.parse(savedProject));
      } catch (error) {
        console.error('Failed to load project:', error);
      }
    }
  }, []);

  return (
    <div className="project-settings">
      <div className="settings-header">
        <h1>⚙️ Project Settings</h1>
        <button
          onClick={saveProject}
          disabled={saving}
          className={`save-btn ${saved ? 'saved' : ''}`}
        >
          {saving ? '⏳ Saving...' : saved ? '✅ Saved!' : '💾 Save Project'}
        </button>
      </div>

      <div className="settings-content">
        {/* Project Info */}
        <section className="settings-section">
          <h2>📄 Project Information</h2>
          <div className="form-group">
            <label>Project Name</label>
            <input
              type="text"
              value={project.projectName}
              onChange={(e) => updateProject({ projectName: e.target.value })}
              placeholder="e.g., FatturaPA_to_UBL"
              className="text-input"
            />
          </div>
        </section>

        {/* Input Schema */}
        <section className="settings-section">
          <h2>📥 Input Schema</h2>
          
          <div className="form-group">
            <label>Schema Name</label>
            <input
              type="text"
              value={project.inputSchema.name}
              onChange={(e) => updateInputSchema({ name: e.target.value })}
              placeholder="e.g., FatturaPA"
              className="text-input"
            />
          </div>

          <div className="form-group">
            <label>Format</label>
            <select
              value={project.inputSchema.format}
              onChange={(e) => updateInputSchema({ format: e.target.value })}
              className="select-input"
            >
              <option value="xml">XML</option>
              <option value="json">JSON</option>
              <option value="csv">CSV</option>
            </select>
          </div>

          <SchemaSelector
            type="input"
            value={project.inputSchema.schemaType}
            onChange={(schemaType) => updateInputSchema({ schemaType })}
            label="Schema Type (XSD/Schematron)"
          />
        </section>

        {/* Output Schema */}
        <section className="settings-section">
          <h2>📤 Output Schema</h2>
          
          <div className="form-group">
            <label>Schema Name</label>
            <input
              type="text"
              value={project.outputSchema.name}
              onChange={(e) => updateOutputSchema({ name: e.target.value })}
              placeholder="e.g., UBL Invoice"
              className="text-input"
            />
          </div>

          <div className="form-group">
            <label>Format</label>
            <select
              value={project.outputSchema.format}
              onChange={(e) => updateOutputSchema({ format: e.target.value })}
              className="select-input"
            >
              <option value="xml">XML</option>
              <option value="json">JSON</option>
              <option value="csv">CSV</option>
            </select>
          </div>

          <SchemaSelector
            type="output"
            value={project.outputSchema.schemaType}
            onChange={(schemaType) => updateOutputSchema({ schemaType })}
            label="Schema Type (XSD/Schematron)"
          />
        </section>

        {/* Project Summary */}
        <section className="settings-section summary-section">
          <h2>📊 Project Summary</h2>
          <div className="summary-grid">
            <div className="summary-item">
              <span className="summary-label">Input Schema Type:</span>
              <span className="summary-value">
                {project.inputSchema.schemaType || 'Not selected'}
              </span>
            </div>
            <div className="summary-item">
              <span className="summary-label">Output Schema Type:</span>
              <span className="summary-value">
                {project.outputSchema.schemaType || 'Not selected'}
              </span>
            </div>
            <div className="summary-item">
              <span className="summary-label">Input Format:</span>
              <span className="summary-value">{project.inputSchema.format.toUpperCase()}</span>
            </div>
            <div className="summary-item">
              <span className="summary-label">Output Format:</span>
              <span className="summary-value">{project.outputSchema.format.toUpperCase()}</span>
            </div>
          </div>
        </section>

        {/* Quick Links */}
        <section className="settings-section">
          <h2>🔗 Quick Links</h2>
          <div className="quick-links">
            <a href="/schemas" className="quick-link-btn">
              📋 Manage Schemas
            </a>
            <a href="/mapping" className="quick-link-btn">
              🗺️ Go to Mapping Canvas
            </a>
          </div>
        </section>
      </div>
    </div>
  );
}
