/**
 * Buddyliko Theme Toggle System v2
 * Covers ALL pages: app (light default), account (light), admin/finance/workspace (dark), index/login/404/privacy/terms (dark)
 */
(function () {
    'use strict';
    const KEY = 'buddyliko_theme';
    function saved() { try { return localStorage.getItem(KEY); } catch(e) { return null; } }
    function preferred() {
        const s = saved();
        if (s === 'light' || s === 'dark') return s;
        return (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) ? 'light' : 'dark';
    }
    function apply(mode) {
        document.documentElement.setAttribute('data-theme', mode);
        try { localStorage.setItem(KEY, mode); } catch(e) {}
        const btn = document.getElementById('theme-toggle-btn');
        if (btn) { btn.textContent = mode === 'dark' ? '☀️' : '🌙'; btn.title = mode === 'dark' ? 'Light Mode' : 'Dark Mode'; }
    }
    function toggle() {
        const cur = document.documentElement.getAttribute('data-theme') || preferred();
        apply(cur === 'dark' ? 'light' : 'dark');
    }
    function createBtn() {
        if (document.getElementById('theme-toggle-btn')) return;
        const btn = document.createElement('button');
        btn.id = 'theme-toggle-btn';
        btn.onclick = toggle;
        const mode = document.documentElement.getAttribute('data-theme') || preferred();
        btn.textContent = mode === 'dark' ? '☀️' : '🌙';
        btn.title = mode === 'dark' ? 'Light Mode' : 'Dark Mode';
        Object.assign(btn.style, {
            background:'transparent', border:'1px solid rgba(128,128,128,.4)',
            borderRadius:'8px', padding:'4px 10px', cursor:'pointer',
            fontSize:'16px', lineHeight:'1', minWidth:'36px', height:'32px',
            display:'inline-flex', alignItems:'center', justifyContent:'center',
            transition:'all .2s', zIndex:'10000', position:'relative'
        });
        // Priority: use #theme-slot if available
        const slot = document.getElementById('theme-slot');
        if (slot) { slot.appendChild(btn); return; }
        const targets = [
            document.getElementById('lang-switcher')?.parentElement,
            document.getElementById('app-lang-switcher')?.parentElement,
            document.querySelector('.topbar-right'),
            document.querySelector('.topbar-actions'),
            document.querySelector('.topbar-links'),
            document.querySelector('.header-right'),
            document.querySelector('.nav-links'),
            document.querySelector('nav'),
        ];
        let placed = false;
        for (const c of targets) {
            if (c) {
                const sw = c.querySelector('#lang-switcher, #app-lang-switcher');
                if (sw) c.insertBefore(btn, sw); else c.insertBefore(btn, c.firstChild);
                placed = true; break;
            }
        }
        if (!placed) {
            const sf = document.querySelector('.sidebar-footer');
            if (sf) { sf.prepend(btn); placed = true; }
        }
        if (!placed) {
            Object.assign(btn.style, { position:'fixed', top:'12px', right:'12px', background:'rgba(0,0,0,.6)', boxShadow:'0 2px 8px rgba(0,0,0,.3)' });
            document.body.appendChild(btn);
        }
    }
    function injectCSS() {
        if (document.getElementById('buddy-theme-css')) return;
        const s = document.createElement('style');
        s.id = 'buddy-theme-css';
        s.textContent = `
/* ══ DARK MODE (for light-default pages: app, account, account-settings) ══ */
[data-theme="dark"] body { background:#0a0e1a !important; color:#e2e8f0 !important; }

/* app.html */
[data-theme="dark"] .sidebar { background:#111827 !important; border-color:#1e293b !important; }
[data-theme="dark"] .sidebar-header { background:#0a1628 !important; }
[data-theme="dark"] .sidebar-content { background:#111827 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .sidebar-footer { background:#0f172a !important; border-color:#1e293b !important; }
[data-theme="dark"] .sidebar-footer .user-name { color:#e2e8f0 !important; }
[data-theme="dark"] .sidebar-footer .user-email { color:#94a3b8 !important; }
[data-theme="dark"] .sidebar-footer button[style*="background: rgb(34, 38, 58)"],
[data-theme="dark"] .sidebar-footer button[style*="background: #22263a"],
[data-theme="dark"] .sidebar-footer button[style*="#22263a"] {
    background:#1e293b !important; border-color:#334155 !important; color:#94a3b8 !important;
}
[data-theme="dark"] .sidebar-footer button[style*="background: rgb(30, 58, 95)"],
[data-theme="dark"] .sidebar-footer button[style*="background: #1e3a5f"],
[data-theme="dark"] .sidebar-footer button[style*="#1e3a5f"] {
    background:#1e3a5f !important; border-color:#2563eb44 !important; color:#93c5fd !important;
}
[data-theme="light"] .sidebar-footer button[style*="background: rgb(34, 38, 58)"],
[data-theme="light"] .sidebar-footer button[style*="background: #22263a"],
[data-theme="light"] .sidebar-footer button[style*="#22263a"] {
    background:#f1f5f9 !important; border-color:#cbd5e1 !important; color:#475569 !important;
}
[data-theme="light"] .sidebar-footer button[style*="background: rgb(30, 58, 95)"],
[data-theme="light"] .sidebar-footer button[style*="background: #1e3a5f"],
[data-theme="light"] .sidebar-footer button[style*="#1e3a5f"] {
    background:#dbeafe !important; border-color:#93c5fd !important; color:#2563eb !important;
}
[data-theme="dark"] .btn-logout { background:#1e293b !important; border-color:#334155 !important; color:#94a3b8 !important; }
[data-theme="dark"] .btn-logout:hover { border-color:#00BCD4 !important; color:#00BCD4 !important; }
[data-theme="dark"] .toolbar { background:#111827 !important; border-color:#1e293b !important; }
[data-theme="dark"] .canvas-area { background:#0a0e1a !important; }
[data-theme="dark"] .main-canvas { background:#0a0e1a !important; }
[data-theme="dark"] .properties-panel { background:#111827 !important; border-color:#1e293b !important; color:#e2e8f0 !important; }
[data-theme="dark"] .field-box { background:#1e293b !important; border-color:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .field-name { color:#e2e8f0 !important; }
[data-theme="dark"] .field-meta { color:#94a3b8 !important; }
[data-theme="dark"] .field-business { color:#67e8f9 !important; }
[data-theme="dark"] .field-badge { background:#334155 !important; color:#94a3b8 !important; }
[data-theme="dark"] .field-box:hover { border-color:#00BCD4 !important; }
[data-theme="dark"] .field-box[style*="background: rgb(224, 247, 250)"],
[data-theme="dark"] .field-box[style*="background: #e0f7fa"],
[data-theme="dark"] .field-box[style*="background:#e0f7fa"] {
    background: #0e3a4a !important;
    border-color: #00BCD4 !important;
}
[data-theme="dark"] .field-box[style*="border: 3px dashed"],
[data-theme="dark"] .field-box[style*="border:3px dashed"] {
    border-color: #00BCD4 !important;
    background: #0a2a3a !important;
}
/* Override all light backgrounds in dark mode for app */
[data-theme="dark"] .field-box[style*="background: rgb(232, 245, 233)"],
[data-theme="dark"] .field-box[style*="#e8f5e9"] { background:#0a3020 !important; }
[data-theme="dark"] .field-box[style*="background: rgb(255, 243, 205)"],
[data-theme="dark"] .field-box[style*="#fff3cd"] { background:#3a2f0a !important; }
[data-theme="dark"] div[style*="background: rgb(240, 240, 240)"],
[data-theme="dark"] div[style*="background: #f0f0f0"],
[data-theme="dark"] div[style*="background:#f0f0f0"] { background:#1e293b !important; color:#e2e8f0 !important; }
[data-theme="dark"] div[style*="background: rgb(245, 245, 245)"],
[data-theme="dark"] div[style*="background: #f5f5f5"],
[data-theme="dark"] div[style*="background:#f5f5f5"] { background:#0f172a !important; color:#e2e8f0 !important; }
[data-theme="dark"] div[style*="background: rgb(250, 250, 250)"],
[data-theme="dark"] div[style*="background: #fafafa"],
[data-theme="dark"] div[style*="background:#fafafa"] { background:#111827 !important; color:#e2e8f0 !important; }
[data-theme="dark"] div[style*="background: white"],
[data-theme="dark"] div[style*="background: rgb(255, 255, 255)"] { background:#1e293b !important; color:#e2e8f0 !important; }
[data-theme="dark"] code[style*="background: #fff"],
[data-theme="dark"] code[style*="background: rgb(255, 255, 255)"] { background:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] div[style*="border: 1px solid #ddd"],
[data-theme="dark"] div[style*="border: 1px solid rgb(221, 221, 221)"] { border-color:#334155 !important; }
[data-theme="dark"] div[style*="border: 2px solid #ddd"],
[data-theme="dark"] div[style*="border-bottom: 2px solid #ddd"] { border-color:#334155 !important; }
[data-theme="dark"] span[style*="color: #333"],
[data-theme="dark"] div[style*="color: #333"],
[data-theme="dark"] td[style*="color: #333"] { color:#e2e8f0 !important; }
[data-theme="dark"] span[style*="color: #666"],
[data-theme="dark"] div[style*="color: #666"],
[data-theme="dark"] td[style*="color: #666"] { color:#94a3b8 !important; }
[data-theme="dark"] span[style*="color: #999"],
[data-theme="dark"] div[style*="color: #999"] { color:#64748b !important; }
[data-theme="dark"] .canvas-column { color:#e2e8f0 !important; }
[data-theme="dark"] .modal-overlay { background:rgba(0,0,0,.6) !important; }
[data-theme="dark"] .modal { background:#1e293b !important; border:1px solid #334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .form-group label { color:#94a3b8 !important; }
[data-theme="dark"] .form-group input,[data-theme="dark"] .form-group select,[data-theme="dark"] .form-group textarea,
[data-theme="dark"] input[type="text"],[data-theme="dark"] input[type="email"],[data-theme="dark"] input[type="password"],[data-theme="dark"] input[type="number"],
[data-theme="dark"] select,[data-theme="dark"] textarea {
    background:#0f172a !important; border-color:#334155 !important; color:#e2e8f0 !important;
}
[data-theme="dark"] .search-input { background:#0f172a !important; border-color:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .formula-editor,[data-theme="dark"] .formula-editor-body,[data-theme="dark"] .formula-editor-header,
[data-theme="dark"] .formula-editor-footer,[data-theme="dark"] .formula-editor-modal {
    background:#1e293b !important; color:#e2e8f0 !important;
}
[data-theme="dark"] .formula-textarea { background:#0f172a !important; color:#e2e8f0 !important; border-color:#334155 !important; }
[data-theme="dark"] .node-edit-modal,[data-theme="dark"] .node-import-modal,[data-theme="dark"] .node-bundle {
    background:#1e293b !important; border-color:#334155 !important; color:#e2e8f0 !important;
}
[data-theme="dark"] .node-bundle-title { color:#e2e8f0 !important; }
[data-theme="dark"] .empty-state { color:#94a3b8 !important; }
[data-theme="dark"] .connection-list-item { background:#1e293b !important; border-color:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .connection-list-item:hover { background:#334155 !important; }
[data-theme="dark"] .hover-popup { background:#1e293b !important; border-color:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .field-context-menu { background:#1e293b !important; border-color:#334155 !important; }
[data-theme="dark"] .field-context-menu-item { color:#e2e8f0 !important; }
[data-theme="dark"] .field-context-menu-item:hover { background:#334155 !important; }
[data-theme="dark"] .operator-btn { background:#0f172a !important; border-color:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .operator-btn:hover { background:#334155 !important; border-color:#00BCD4 !important; }
[data-theme="dark"] .btn-secondary { background:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .col-import-btn { background:#1e293b !important; border-color:#334155 !important; color:#94a3b8 !important; }
[data-theme="dark"] .col-import-btn:hover { border-color:#00BCD4 !important; color:#00BCD4 !important; }
[data-theme="dark"] .resize-handle-input,[data-theme="dark"] .resize-handle-output { background:#334155 !important; }
[data-theme="dark"] .status-message { background:#111827 !important; border-color:#1e293b !important; color:#94a3b8 !important; }
[data-theme="dark"] .tabs { border-color:#1e293b !important; }

/* account / account-settings */
[data-theme="dark"] .topbar { background:#111827 !important; border-color:#1e293b !important; }
[data-theme="dark"] .topbar-links a { color:#94a3b8 !important; }
[data-theme="dark"] .topbar-links .active { color:#60a5fa !important; }
[data-theme="dark"] .container { color:#e2e8f0 !important; }
[data-theme="dark"] .container h1 { color:#e2e8f0 !important; }
[data-theme="dark"] .page-sub { color:#94a3b8 !important; }
[data-theme="dark"] .card { background:#1e293b !important; border-color:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .card h2 { color:#e2e8f0 !important; }
[data-theme="dark"] .provider-card { border-color:#334155 !important; background:#111827 !important; }
[data-theme="dark"] .provider-card.linked { border-color:#22c55e !important; background:rgba(34,197,94,.1) !important; }
[data-theme="dark"] .provider-name { color:#e2e8f0 !important; }
[data-theme="dark"] .provider-email { color:#94a3b8 !important; }
[data-theme="dark"] .provider-icon { background:#334155 !important; }
[data-theme="dark"] .user-info-box,[data-theme="dark"] div.user-info { background:#0f172a !important; color:#e2e8f0 !important; }
[data-theme="dark"] div.user-info h3 { color:#e2e8f0 !important; }
[data-theme="dark"] div.user-info p { color:#94a3b8 !important; }
[data-theme="dark"] .btn-outline { border-color:#334155 !important; color:#94a3b8 !important; background:transparent !important; }
[data-theme="dark"] .btn-outline:hover { border-color:#60a5fa !important; color:#60a5fa !important; }
[data-theme="dark"] .btn-danger { border-color:#ef4444 !important; color:#ef4444 !important; background:transparent !important; }
[data-theme="dark"] .alert-success { background:rgba(34,197,94,.15) !important; color:#4ade80 !important; border-color:#22c55e !important; }
[data-theme="dark"] .alert-error { background:rgba(239,68,68,.15) !important; color:#f87171 !important; border-color:#ef4444 !important; }
[data-theme="dark"] .alert-info { background:rgba(59,130,246,.15) !important; color:#60a5fa !important; border-color:#3b82f6 !important; }

/* ══ LIGHT MODE (for dark-default pages: index, login, 404, privacy, terms, admin, finance, workspace) ══ */
[data-theme="light"] body { background:#f8fafc !important; color:#1e293b !important; }

/* nav */
[data-theme="light"] nav { background:rgba(255,255,255,.95) !important; border-color:#e2e8f0 !important; }
[data-theme="light"] nav a,[data-theme="light"] .nav-links a { color:#475569 !important; }
[data-theme="light"] nav a:hover { color:#1e293b !important; }
[data-theme="light"] nav .btn-outline { border-color:#cbd5e1 !important; color:#475569 !important; }
[data-theme="light"] nav img { filter:brightness(0.3); }

/* hero / index */
[data-theme="light"] .hero h1 { background:linear-gradient(135deg,#1e293b,#2563eb,#0891b2) !important; -webkit-background-clip:text !important; }
[data-theme="light"] .hero .subtitle { color:#64748b !important; }
[data-theme="light"] .warning-box { background:rgba(245,158,11,.06) !important; border-color:rgba(245,158,11,.2) !important; }
[data-theme="light"] .section { color:#1e293b !important; }
[data-theme="light"] .section-title { color:#1e293b !important; }
[data-theme="light"] .section-sub { color:#64748b !important; }
[data-theme="light"] .feature-card { background:#fff !important; border-color:#e2e8f0 !important; box-shadow:0 1px 3px rgba(0,0,0,.06) !important; }
[data-theme="light"] .feature-card h3 { color:#1e293b !important; }
[data-theme="light"] .feature-card p { color:#64748b !important; }
[data-theme="light"] .pricing-section { background:#f1f5f9 !important; }
[data-theme="light"] .pricing-card { background:#fff !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .pricing-card h3,.pricing-card .price { color:#1e293b !important; }
[data-theme="light"] .pricing-features li { color:#475569 !important; }
[data-theme="light"] .usecase { background:#fff !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .usecase h4 { color:#0891b2 !important; }
[data-theme="light"] .usecase p { color:#64748b !important; }
[data-theme="light"] .cta-section { background:linear-gradient(180deg,#f8fafc,#f1f5f9) !important; }
[data-theme="light"] .cta-section h2 { color:#1e293b !important; }
[data-theme="light"] .cta-section p { color:#64748b !important; }

/* footer */
[data-theme="light"] footer { background:#f8fafc !important; border-color:#e2e8f0 !important; color:#64748b !important; }
[data-theme="light"] footer a { color:#475569 !important; }

/* privacy / terms */
[data-theme="light"] .content { color:#334155 !important; }
[data-theme="light"] .content h2 { color:#1e293b !important; }
[data-theme="light"] .content p { color:#475569 !important; }
[data-theme="light"] .content h1, [data-theme="light"] .hero h1 { background:linear-gradient(135deg,#1e293b,#0891b2) !important; -webkit-background-clip:text !important; -webkit-text-fill-color:transparent !important; }
[data-theme="light"] .updated { color:#64748b !important; }

/* 404 */
[data-theme="light"] .code { background:linear-gradient(135deg,#2563eb,#7c3aed) !important; -webkit-background-clip:text !important; }

/* admin (light by default - needs DARK overrides) */
[data-theme="dark"] .header { background:linear-gradient(135deg,#0f172a,#1e293b) !important; }
[data-theme="dark"] .header-left h1 { color:#e2e8f0 !important; -webkit-text-fill-color:#e2e8f0 !important; }
[data-theme="dark"] .header-left p { color:#94a3b8 !important; }
[data-theme="dark"] .container { background:#0a0e1a !important; color:#e2e8f0 !important; }
[data-theme="dark"] .stat-card { background:#1e293b !important; border:1px solid #334155 !important; box-shadow:none !important; }
[data-theme="dark"] .stat-value { color:#60a5fa !important; }
[data-theme="dark"] .stat-label { color:#94a3b8 !important; }
[data-theme="dark"] .section { background:#1e293b !important; box-shadow:none !important; border:1px solid #334155 !important; }
[data-theme="dark"] .section-title { color:#e2e8f0 !important; }
[data-theme="dark"] .section-title .count { background:#1e3a5f !important; color:#60a5fa !important; }
[data-theme="dark"] .tab { background:#334155 !important; color:#94a3b8 !important; border:none !important; }
[data-theme="dark"] .tab.active { background:#2563eb !important; color:white !important; }
[data-theme="dark"] .role-select,[data-theme="dark"] .status-select { background:#1e293b !important; border-color:#334155 !important; color:#e2e8f0 !important; }
[data-theme="dark"] .empty { color:#64748b !important; }
[data-theme="dark"] .btn-success { background:#059669 !important; }
[data-theme="dark"] .btn-sm { background:#334155 !important; border-color:#475569 !important; color:#e2e8f0 !important; }
/* admin light - header stays dark/elegant */
[data-theme="light"] .header { background:linear-gradient(135deg,#0f172a,#1e293b) !important; }
[data-theme="light"] .header-left h1 { color:white !important; -webkit-text-fill-color:white !important; background:none !important; }
[data-theme="light"] .header-left p { color:rgba(255,255,255,.85) !important; }
[data-theme="light"] .header #theme-toggle-btn { color:white !important; border-color:rgba(255,255,255,.4) !important; }
[data-theme="light"] .header #lang-btn { color:white !important; border-color:rgba(255,255,255,.4) !important; }
[data-theme="light"] .header #lang-dropdown { background:#fff !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .header #lang-dropdown div { color:#1e293b !important; }
[data-theme="light"] .stat-card { background:#fff !important; border:1px solid #e2e8f0 !important; box-shadow:0 1px 3px rgba(0,0,0,.06) !important; }
[data-theme="light"] .stat-value { color:#2563eb !important; }
[data-theme="light"] .stat-label { color:#64748b !important; }
[data-theme="light"] .section { background:#fff !important; box-shadow:0 1px 3px rgba(0,0,0,.06) !important; }
[data-theme="light"] .section-title { color:#1e293b !important; }
[data-theme="light"] .section-title .count { background:#dbeafe !important; color:#2563eb !important; }
[data-theme="light"] .tab { background:#f1f5f9 !important; color:#475569 !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .tab.active { background:#2563eb !important; color:white !important; }
[data-theme="light"] .empty { color:#94a3b8 !important; }
[data-theme="light"] .container { color:#1e293b !important; }
[data-theme="light"] .role-select,[data-theme="light"] .status-select { background:#fff !important; border-color:#ddd !important; color:#1e293b !important; }
/* admin header always keeps white buttons */
.header #theme-toggle-btn { color:white !important; border-color:rgba(255,255,255,.4) !important; }
.header #lang-btn { color:white !important; border-color:rgba(255,255,255,.4) !important; }

/* finance */
[data-theme="light"] .kpi-card { background:#fff !important; border:1px solid #e2e8f0 !important; }
[data-theme="light"] .kpi-label { color:#64748b !important; }
[data-theme="light"] .kpi-value { color:#1e293b !important; }
[data-theme="light"] .chart-card { background:#fff !important; border:1px solid #e2e8f0 !important; }
[data-theme="light"] .tab-bar .tab { color:#64748b !important; }
[data-theme="light"] .tab-bar .tab.active { color:#2563eb !important; border-color:#2563eb !important; }
[data-theme="light"] { --bg:#f8fafc; --surface:#fff; --surface2:#f1f5f9; --border:#e2e8f0; --border2:#cbd5e1; --text:#1e293b; --muted:#64748b; --muted2:#475569; }
[data-theme="light"] .topbar-brand { color:#1e293b !important; }
[data-theme="light"] .topbar-nav a::after { background:#2563eb !important; }
[data-theme="light"] .period-sel button { background:#f1f5f9 !important; color:#475569 !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .period-sel button.active { background:#2563eb !important; color:white !important; }
[data-theme="light"] .badge-free { background:#f1f5f9 !important; color:#64748b !important; }
[data-theme="light"] .badge-pro { background:#dbeafe !important; color:#2563eb !important; }
[data-theme="light"] .badge-enterprise { background:#ede9fe !important; color:#7c3aed !important; }
[data-theme="light"] .badge-custom { background:#d1fae5 !important; color:#059669 !important; }
[data-theme="light"] .badge-ok { background:#d1fae5 !important; color:#059669 !important; }
[data-theme="light"] table { background:#fff !important; }
[data-theme="light"] table th { background:#f8fafc !important; color:#475569 !important; border-color:#e2e8f0 !important; }
[data-theme="light"] table td { color:#1e293b !important; border-color:#f1f5f9 !important; }
[data-theme="light"] table tr:hover td { background:#f8fafc !important; }
[data-theme="light"] .detail-row,.detail-row td { background:#f8fafc !important; }
[data-theme="light"] .alert-card { background:#fff !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .section-header { color:#1e293b !important; }
[data-theme="light"] .empty { color:#94a3b8 !important; }
[data-theme="light"] .pricing-row { background:#fff !important; border-color:#e2e8f0 !important; }
[data-theme="light"] select { background:#fff !important; border-color:#cbd5e1 !important; color:#1e293b !important; }
[data-theme="light"] .tab-bar .tab { color:#64748b !important; }
[data-theme="light"] .tab-bar .tab.active { color:#2563eb !important; border-color:#2563eb !important; }

/* workspace */
[data-theme="light"] .topbar { background:#fff !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .topbar h1 { color:#1e293b !important; }
[data-theme="light"] .topbar-nav a { color:#64748b !important; }
[data-theme="light"] .topbar-nav a.active { color:#2563eb !important; }
[data-theme="light"] .topbar-nav a:hover { color:#1e293b !important; }
[data-theme="light"] .topbar-right .user-name { color:#1e293b !important; }
[data-theme="light"] .topbar-right .user-email { color:#64748b !important; }
[data-theme="light"] .topbar-actions button { background:#f1f5f9 !important; color:#475569 !important; border-color:#e2e8f0 !important; }
/* workspace sidebar + file area */
[data-theme="light"] .sidebar { background:#f8fafc !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .sidebar-section { border-color:#e2e8f0 !important; }
[data-theme="light"] .sidebar-title { color:#64748b !important; }
[data-theme="light"] .sidebar-item { color:#475569 !important; }
[data-theme="light"] .sidebar-item:hover { background:#f1f5f9 !important; color:#1e293b !important; }
[data-theme="light"] .sidebar-item.active { background:#dbeafe !important; color:#2563eb !important; }
[data-theme="light"] .sidebar-item .count { background:#e2e8f0 !important; color:#475569 !important; }
[data-theme="light"] .main-content { background:#fff !important; }
[data-theme="light"] .card { background:#fff !important; border-color:#e2e8f0 !important; color:#1e293b !important; }
[data-theme="light"] .card-header { color:#1e293b !important; }
[data-theme="light"] .card-title { color:#1e293b !important; }
[data-theme="light"] .file-card { background:#fff !important; border-color:#e2e8f0 !important; color:#1e293b !important; }
[data-theme="light"] .file-card:hover { border-color:#3b82f6 !important; }
[data-theme="light"] .file-name { color:#1e293b !important; }
[data-theme="light"] .file-meta { color:#64748b !important; }
[data-theme="light"] .file-icon { background:#f1f5f9 !important; }
[data-theme="light"] .group-header { color:#1e293b !important; }
[data-theme="light"] .member-item { color:#475569 !important; border-color:#e2e8f0 !important; }
[data-theme="light"] .stat-card { background:#fff !important; border-color:#e2e8f0 !important; color:#1e293b !important; }
[data-theme="light"] .search-box { background:#fff !important; border-color:#cbd5e1 !important; color:#1e293b !important; }
[data-theme="light"] .form-input { background:#fff !important; border-color:#cbd5e1 !important; color:#1e293b !important; }
[data-theme="light"] .empty { color:#94a3b8 !important; }
[data-theme="light"] .loading { color:#94a3b8 !important; }
[data-theme="light"] .btn-ghost { color:#475569 !important; }
[data-theme="light"] .btn-ghost:hover { background:#f1f5f9 !important; }
[data-theme="light"] .file-actions button { background:#f1f5f9 !important; border-color:#e2e8f0 !important; color:#475569 !important; }
[data-theme="light"] .toast { background:#fff !important; border-color:#e2e8f0 !important; color:#1e293b !important; }
/* finance topbar specific */
[data-theme="light"] .topbar-brand span { color:#1e293b !important; }
[data-theme="light"] .topbar-right #theme-toggle-btn { color:#475569 !important; border-color:#cbd5e1 !important; }
[data-theme="light"] .topbar-right #lang-btn { color:#475569 !important; border-color:#cbd5e1 !important; }

/* common inputs in light mode */
[data-theme="light"] input,[data-theme="light"] select,[data-theme="light"] textarea {
    background:#fff !important; border-color:#cbd5e1 !important; color:#1e293b !important;
}
[data-theme="light"] .badge,[data-theme="light"] .count { background:#dbeafe !important; color:#2563eb !important; }
[data-theme="light"] ::-webkit-scrollbar-thumb { background:#cbd5e1 !important; }
[data-theme="light"] ::-webkit-scrollbar-track { background:#f1f5f9 !important; }

/* ══ TOGGLE BUTTON ══ */
#theme-toggle-btn { opacity:.8; }
#theme-toggle-btn:hover { opacity:1; transform:scale(1.15); }
[data-theme="dark"] #theme-toggle-btn { color:#e2e8f0 !important; border-color:#334155 !important; }
[data-theme="light"] #theme-toggle-btn { color:#1e293b !important; border-color:#cbd5e1 !important; }

/* ══ LANG SWITCHER FIX ══ */
.sidebar-footer #app-lang-switcher #lang-dropdown,
.sidebar #app-lang-switcher #lang-dropdown {
    right: auto !important;
    left: 0 !important;
}
[data-theme="light"] #lang-dropdown {
    background: #fff !important;
    border-color: #e2e8f0 !important;
    box-shadow: 0 8px 24px rgba(0,0,0,.1) !important;
}
[data-theme="light"] #lang-dropdown div {
    color: #1e293b !important;
}
[data-theme="light"] #lang-dropdown div:hover {
    background: #f1f5f9 !important;
}
[data-theme="light"] #lang-btn {
    border-color: #cbd5e1 !important;
    color: #1e293b !important;
}
[data-theme="dark"] #lang-btn {
    border-color: #334155 !important;
    color: #e2e8f0 !important;
}
[data-theme="dark"] #lang-dropdown {
    background: #1e293b !important;
    border-color: #334155 !important;
    box-shadow: 0 8px 24px rgba(0,0,0,.4) !important;
}
[data-theme="dark"] #lang-dropdown div {
    color: #e2e8f0 !important;
}
[data-theme="dark"] #lang-dropdown div:hover {
    background: #334155 !important;
}
/* Account modal dark mode */
[data-theme="dark"] .modal div[style*="background: white"],
[data-theme="dark"] .modal div[style*="background: rgb(255, 255, 255)"],
[data-theme="dark"] .modal-overlay div[style*="background: white"],
[data-theme="dark"] .modal-overlay div[style*="background: rgb(255, 255, 255)"] {
    background: #1e293b !important; color: #e2e8f0 !important;
}
[data-theme="dark"] .modal div[style*="background: #f8f9ff"],
[data-theme="dark"] .modal div[style*="background: rgb(248, 249, 255)"] {
    background: #111827 !important;
}
[data-theme="dark"] .modal div[style*="border-bottom: 2px"],
[data-theme="dark"] .modal div[style*="borderBottom: 2px"] {
    border-color: #334155 !important;
}
[data-theme="dark"] .modal h2, [data-theme="dark"] .modal h3 {
    color: #e2e8f0 !important;
}
[data-theme="dark"] .modal label { color: #94a3b8 !important; }
[data-theme="dark"] .modal p { color: #94a3b8 !important; }
[data-theme="dark"] .modal small { color: #64748b !important; }

/* ══ RESPONSIVE ══ */
@media(max-width:1024px){
    .sidebar:not(.collapsed){width:100% !important;max-height:280px !important;overflow-y:auto !important;border-right:none !important;border-bottom:1px solid rgba(128,128,128,.2) !important;}
    .properties-panel{width:100% !important;border-left:none !important;border-top:1px solid rgba(128,128,128,.2) !important;max-height:300px !important;}
    .stats-grid,.kpi-grid{grid-template-columns:repeat(2,1fr) !important;}
    .chart-grid{grid-template-columns:1fr !important;}
}
@media(max-width:768px){
    body{font-size:14px !important;}
    .topbar{padding:8px 12px !important;flex-wrap:wrap !important;gap:8px !important;}
    .topbar h1{font-size:16px !important;}
    .topbar-nav{order:3 !important;width:100% !important;overflow-x:auto !important;white-space:nowrap !important;}
    .topbar-nav a{font-size:12px !important;padding:4px 10px !important;}
    .topbar-right .user-info{display:none !important;}
    .header{padding:12px !important;flex-direction:column !important;gap:8px !important;text-align:center !important;}
    .header h1{font-size:18px !important;}
    .header-right{justify-content:center !important;flex-wrap:wrap !important;}
    .card,.section{padding:14px !important;}
    .stats-grid,.kpi-grid{grid-template-columns:1fr !important;gap:8px !important;}
    table{font-size:12px !important;}
    th,td{padding:6px 8px !important;}
    .modal{width:95vw !important;max-width:95vw !important;padding:16px !important;}
    nav{padding:0 12px !important;height:52px !important;}
    .content{padding:60px 12px 40px !important;}
    footer{flex-direction:column !important;text-align:center !important;padding:16px !important;}
    .toolbar{padding:8px !important;gap:4px !important;}
    .toolbar .btn{min-width:auto !important;padding:6px 10px !important;font-size:11px !important;height:34px !important;}
    .sidebar-footer{flex-wrap:wrap !important;gap:4px !important;}
    .sidebar-footer button,.sidebar-footer .btn-logout{font-size:11px !important;padding:6px 10px !important;}
}
@media(max-width:480px){
    .topbar-nav{display:none !important;}
    table{display:block !important;overflow-x:auto !important;}
    .features-grid,.pricing-grid,.usecases{grid-template-columns:1fr !important;}
}

/* transition */
body,.sidebar,.toolbar,.canvas-area,.properties-panel,.modal,.card,.topbar,nav,footer,
.field-box,input,select,textarea,.sidebar-footer,.sidebar-content,.btn-logout,
.feature-card,.pricing-card,.stat-card,.kpi-card,.connection-list-item,.provider-card,
#theme-toggle-btn{transition:background-color .25s,border-color .25s,color .2s !important;}
`;
        document.head.appendChild(s);
    }

    window.BuddyTheme = {
        init() { injectCSS(); apply(preferred()); if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',()=>setTimeout(createBtn,150)); else setTimeout(createBtn,150); },
        toggle, set: apply, get:()=>document.documentElement.getAttribute('data-theme')||preferred()
    };
    window.BuddyTheme.init();
})();
