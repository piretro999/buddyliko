import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { ProjectSettings } from './pages/ProjectSettings';
import { SchemaManagement } from './pages/SchemaManagement';
import { MappingCanvas } from './pages/MappingCanvas';
import './App.css';

function App() {
  return (
    <Router>
      <div className="app">
        <Navigation />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<ProjectSettings />} />
            <Route path="/mapping" element={<MappingCanvas />} />
            <Route path="/schemas" element={<SchemaManagement />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

function Navigation() {
  return (
    <nav className="main-nav">
      <div className="nav-brand">
        <h1>🔄 Visual Mapping System</h1>
      </div>
      <div className="nav-links">
        <Link to="/" className="nav-link">
          <span className="nav-icon">⚙️</span>
          Project Settings
        </Link>
        <Link to="/mapping" className="nav-link">
          <span className="nav-icon">🗺️</span>
          Mapping Canvas
        </Link>
        <Link to="/schemas" className="nav-link">
          <span className="nav-icon">📋</span>
          Schema Management
        </Link>
      </div>
    </nav>
  );
}

export default App;
