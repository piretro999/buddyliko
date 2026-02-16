import React from 'react';
import './MappingCanvas.css';

export function MappingCanvas() {
  return (
    <div className="mapping-canvas">
      <div className="canvas-header">
        <h1>🗺️ Mapping Canvas</h1>
        <p>Visual field mapping interface</p>
      </div>

      <div className="canvas-placeholder">
        <div className="placeholder-content">
          <span className="placeholder-icon">🚧</span>
          <h2>Coming Soon</h2>
          <p>The visual mapping canvas will be available here</p>
          <p className="hint">
            This is where you'll create field mappings by dragging and dropping
          </p>
        </div>
      </div>
    </div>
  );
}
