/**
 * Buddyliko Unified Topbar v1.0
 * ─────────────────────────────
 * Drop-in topbar for ALL pages.
 * Usage: <div id="buddy-topbar"></div> + <script src="/assets/topbar.js"></script>
 *
 * Detects current page, user role, shows avatar + name + email.
 * Provides: theme-slot, lang-switcher, logout.
 */
(function () {
    'use strict';

    // ── Detect current page ──
    const path = window.location.pathname.split('/').pop() || 'index.html';
    const PAGE_MAP = {
        'app.html':              'mapper',
        'workspace.html':        'workspace',
        'account-settings.html': 'account',
        'account.html':          'subscription',
        'admin.html':            'admin',
        'finance.html':          'finance',
        'org-dashboard.html':    'organization',
        'org-users.html':        'organization',
        'org-billing.html':      'organization',
        'org-partners.html':     'organization',
        'help-ai.html':          'help-ai',
        'standards-library.html': 'standards',
        'onboarding.html':       'onboarding',
        'marketplace.html':      'marketplace',
        'partner-dashboard.html': 'partnership',
        'partner-orgs.html':      'partnership',
        'org-reports.html':     'organization',
        'org-settings.html':     'organization'
    };
    const activePage = PAGE_MAP[path] || '';

    // ── User info ──
    let user = { name: '', email: '', role: '' };
    try { user = JSON.parse(localStorage.getItem('buddyliko_user') || '{}'); } catch (e) {}
    const initial = (user.name || user.email || 'U').charAt(0).toUpperCase();
    const isAdmin = user.role === 'ADMIN' || user.role === 'MASTER';

    // ── i18n helper (fallback if i18n not loaded yet) ──
    const T = (key, fallback) => {
        try { if (typeof i18n !== 'undefined' && i18n.isLoaded()) return i18n.t(key) || fallback; } catch(e) {}
        return fallback;
    };

    // ── CSS ──
    const CSS = `
.buddy-topbar{background:linear-gradient(135deg,#0d1220,#141b2d);border-bottom:1px solid #2e3250;padding:10px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:9999;font-family:system-ui,-apple-system,sans-serif}
.buddy-topbar *{box-sizing:border-box;margin:0;padding:0}
.bt-left{display:flex;align-items:center;gap:14px}
.bt-left .bt-logo{width:28px;height:28px}
.bt-nav{display:flex;gap:4px;flex-wrap:wrap}
.bt-nav a{padding:6px 14px;border-radius:6px;text-decoration:none;font-size:13px;color:#94a3b8;transition:all .15s;white-space:nowrap}
.bt-nav a:hover{background:#1e2235;color:#e2e8f0}
.bt-nav a.bt-active{background:#1e3a5f;color:#93c5fd}
.bt-right{display:flex;align-items:center;gap:10px}
.bt-avatar{width:32px;height:32px;border-radius:50%;background:linear-gradient(135deg,#2196f3,#3f51b5);color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;flex-shrink:0}
.bt-user{display:flex;align-items:center;gap:8px;cursor:default}
.bt-user-name{font-size:12px;font-weight:600;color:#e2e8f0;line-height:1.3}
.bt-user-email{font-size:11px;color:#64748b;line-height:1.3}
.bt-logout{background:transparent;border:1px solid #2e3250;color:#94a3b8;padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px;font-family:inherit;transition:all .15s}
.bt-logout:hover{border-color:#ef4444;color:#f87171}
.bt-ctx{position:relative}
.bt-ctx-btn{display:flex;align-items:center;gap:6px;padding:5px 12px;border:1px solid #2e3250;border-radius:6px;background:transparent;color:#e2e8f0;cursor:pointer;font-size:12px;font-family:inherit;transition:all .15s;white-space:nowrap}
.bt-ctx-btn:hover{border-color:#3b82f6;background:#1e2235}
.bt-ctx-btn .bt-ctx-icon{font-size:14px}
.bt-ctx-dd{display:none;position:absolute;top:100%;right:0;margin-top:4px;background:#1a1f2e;border:1px solid #2e3250;border-radius:8px;min-width:200px;box-shadow:0 8px 24px rgba(0,0,0,.4);z-index:10001;overflow:hidden}
.bt-ctx-dd.open{display:block}
.bt-ctx-item{display:flex;align-items:center;gap:8px;padding:10px 14px;cursor:pointer;font-size:13px;color:#94a3b8;transition:background .1s}
.bt-ctx-item:hover{background:#0f172a}
.bt-ctx-item.active{color:#60a5fa;background:rgba(37,99,235,.1)}
.bt-ctx-item .ctx-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.bt-ctx-item .ctx-dot.personal{background:#22c55e}
.bt-ctx-item .ctx-dot.org{background:#3b82f6}
/* Light mode */
[data-theme="light"] .buddy-topbar{background:linear-gradient(135deg,#f8fafc,#f1f5f9);border-bottom-color:#e2e8f0}
[data-theme="light"] .bt-nav a{color:#64748b}
[data-theme="light"] .bt-nav a:hover{background:#e2e8f0;color:#1e293b}
[data-theme="light"] .bt-nav a.bt-active{background:#dbeafe;color:#2563eb}
[data-theme="light"] .bt-user-name{color:#1e293b}
[data-theme="light"] .bt-user-email{color:#94a3b8}
[data-theme="light"] .bt-logout{border-color:#e2e8f0;color:#64748b}
[data-theme="light"] .bt-logout:hover{border-color:#ef4444;color:#ef4444}
[data-theme="light"] .bt-ctx-btn{border-color:#e2e8f0;color:#1e293b}
[data-theme="light"] .bt-ctx-dd{background:#fff;border-color:#e2e8f0}
[data-theme="light"] .bt-ctx-item{color:#64748b}
[data-theme="light"] .bt-ctx-item:hover{background:#f1f5f9}
[data-theme="light"] .bt-ctx-item.active{color:#2563eb;background:#eff6ff}
@media(max-width:768px){.buddy-topbar{flex-wrap:wrap;gap:8px;padding:8px 12px}.bt-nav{order:3;width:100%;justify-content:center}.bt-user-details{display:none}}
`;

    // ── Build HTML ──
    function buildTopbar() {
        const slot = document.getElementById('buddy-topbar');
        if (!slot) return;

        // Inject CSS once
        if (!document.getElementById('buddy-topbar-css')) {
            const s = document.createElement('style');
            s.id = 'buddy-topbar-css';
            s.textContent = CSS;
            document.head.appendChild(s);
        }

        const navLinks = [
            { href: 'app.html',              icon: '🗺️', label: T('nav.mapper', 'Mapper'),           id: 'mapper' },
            { href: 'workspace.html',        icon: '🗂️', label: T('nav.workspace', 'Workspace'),     id: 'workspace' },
            { href: 'account-settings.html', icon: '👤', label: T('nav.account', 'Account'),         id: 'account' },
            { href: 'org-dashboard.html', icon: '🏢', label: T('nav.organization', 'Organizzazione'), id: 'organization' },
            { href: 'account.html',          icon: '💳', label: T('nav.subscription', 'Abbonamento'), id: 'subscription' },
        ];
        navLinks.push({ href: 'marketplace.html', icon: '🏪', label: T('nav.marketplace', 'Marketplace'), id: 'marketplace' });
        navLinks.push({ href: 'standards-library.html', icon: '📚', label: T('nav.standards', 'Standards'), id: 'standards' });
        navLinks.push({ href: 'help-ai.html', icon: '🤖', label: T('nav.helpai', 'Help AI'), id: 'help-ai' });
        if (isAdmin) {
            navLinks.push({ href: 'admin.html',   icon: '🔐', label: T('nav.admin', 'Admin'),     id: 'admin' });
            navLinks.push({ href: 'finance.html',  icon: '💰', label: T('nav.finance', 'Finance'), id: 'finance' });
        }

        const linksHtml = navLinks.map(l =>
            `<a href="${l.href}" class="${l.id === activePage ? 'bt-active' : ''}">${l.icon} ${l.label}</a>`
        ).join('');

        slot.innerHTML = `
<div class="buddy-topbar">
  <div class="bt-left">
    <img src="/assets/logo-icon.svg" class="bt-logo" alt="Buddyliko">
  </div>
  <nav class="bt-nav">${linksHtml}</nav>
  <div class="bt-right">
    <span id="theme-slot"></span>
    <span id="lang-switcher"></span>
    <div class="bt-ctx" id="bt-ctx-wrapper">
      <button class="bt-ctx-btn" id="bt-ctx-btn" onclick="window._btToggleCtx()">
        <span class="bt-ctx-icon" id="bt-ctx-icon">👤</span>
        <span id="bt-ctx-label">Personale</span>
        <span style="font-size:10px;opacity:.6">▼</span>
      </button>
      <div class="bt-ctx-dd" id="bt-ctx-dd"></div>
    </div>
    <div class="bt-user">
      <div class="bt-avatar">${initial}</div>
      <div class="bt-user-details">
        <div class="bt-user-name">${user.name || 'User'}</div>
        <div class="bt-user-email">${user.email || ''}</div>
      </div>
    </div>
    <button class="bt-logout" onclick="if(confirm('${T('nav.confirm_logout','Vuoi uscire?')}')){localStorage.removeItem('buddyliko_token');localStorage.removeItem('buddyliko_user');window.location.href='login.html';}">🚪 ${T('nav.logout','Logout')}</button>
  </div>
</div>`;
    }

    // ── Init: run after DOM ready ──
    function init() {
        buildTopbar();
        // Re-build after i18n loads (labels may change)
        if (typeof i18n !== 'undefined' && !i18n.isLoaded()) {
            const check = setInterval(() => {
                if (typeof i18n !== 'undefined' && i18n.isLoaded()) {
                    clearInterval(check);
                    buildTopbar();
                    // Create lang switcher inside our topbar
                    try { i18n.createSwitcher('lang-switcher'); } catch(e) {}
                }
            }, 100);
            // Safety timeout
            setTimeout(() => clearInterval(check), 5000);
        } else {
            try { i18n.createSwitcher('lang-switcher'); } catch(e) {}
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Listen for language changes to rebuild labels
    if (typeof window !== 'undefined') {
        const origSwitchTo = (typeof i18n !== 'undefined' && i18n.switchTo) ? i18n.switchTo : null;
        // Poll for i18n to patch switchTo
        const patchInterval = setInterval(() => {
            if (typeof i18n !== 'undefined' && i18n.switchTo && !i18n._topbarPatched) {
                const orig = i18n.switchTo;
                i18n.switchTo = async function(lang) {
                    const result = await orig.call(this, lang);
                    buildTopbar();
                    try { i18n.createSwitcher('lang-switcher'); } catch(e) {}
                    return result;
                };
                i18n._topbarPatched = true;
                clearInterval(patchInterval);
            }
        }, 200);
        setTimeout(() => clearInterval(patchInterval), 10000);
    }

    // ── Context Switcher Logic ─────────────────────────────────────────
    const API_BASE = (window.APP_CONFIG && window.APP_CONFIG.api_base) || '/api';

    function _getToken() { return localStorage.getItem('buddyliko_token'); }

    // Toggle dropdown
    window._btToggleCtx = function() {
        const dd = document.getElementById('bt-ctx-dd');
        if (!dd) return;
        const isOpen = dd.classList.contains('open');
        dd.classList.toggle('open');
        if (!isOpen) _loadContexts();
    };

    // Close on click outside
    document.addEventListener('click', function(e) {
        const wrapper = document.getElementById('bt-ctx-wrapper');
        const dd = document.getElementById('bt-ctx-dd');
        if (wrapper && dd && !wrapper.contains(e.target)) {
            dd.classList.remove('open');
        }
    });

    async function _loadContexts() {
        const dd = document.getElementById('bt-ctx-dd');
        if (!dd) return;
        const token = _getToken();
        if (!token) { dd.innerHTML = '<div class="bt-ctx-item" style="color:#666">Non autenticato</div>'; return; }

        try {
            const r = await fetch(API_BASE + '/auth/contexts', {
                headers: { 'Authorization': 'Bearer ' + token }
            });
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const data = await r.json();
            const contexts = data.contexts || [];

            dd.innerHTML = contexts.map(c => {
                const isActive = c.active;
                const icon = c.type === 'personal' ? '👤' : '🏢';
                const dot = c.type === 'personal' ? 'personal' : 'org';
                const label = c.type === 'personal' ? 'Spazio personale' : c.name;
                const sub = c.type === 'org' ? `<div style="font-size:10px;opacity:.6">${c.role} · ${c.plan||'FREE'}</div>` : '';
                return `<div class="bt-ctx-item ${isActive?'active':''}" onclick="window._btSwitchCtx('${c.type}','${c.org_id||''}')">
                    <span class="ctx-dot ${dot}"></span>
                    <div>${icon} ${label}${sub}</div>
                    ${isActive ? '<span style="margin-left:auto;font-size:11px">✓</span>' : ''}
                </div>`;
            }).join('');
        } catch(e) {
            dd.innerHTML = '<div class="bt-ctx-item" style="color:#f87171">Errore caricamento</div>';
        }
    }

    window._btSwitchCtx = async function(type, orgId) {
        const token = _getToken();
        if (!token) return;
        const dd = document.getElementById('bt-ctx-dd');
        if (dd) dd.classList.remove('open');

        try {
            const body = { context: type };
            if (type === 'org' && orgId) body.org_id = orgId;

            const r = await fetch(API_BASE + '/auth/switch-context', {
                method: 'POST',
                headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const data = await r.json();

            if (data.token) {
                localStorage.setItem('buddyliko_token', data.token);
                // Aggiorna user info con contesto
                const udata = JSON.parse(localStorage.getItem('buddyliko_user') || '{}');
                udata.context = data.context;
                if (data.org) {
                    udata.org_id = data.org.org_id;
                    udata.org_name = data.org.org_name;
                    udata.org_role = data.org.org_role;
                } else {
                    delete udata.org_id;
                    delete udata.org_name;
                    delete udata.org_role;
                }
                localStorage.setItem('buddyliko_user', JSON.stringify(udata));
                _updateCtxButton(data.context, data.org);
                // Dispatch event per app.html e altre pagine
                window.dispatchEvent(new CustomEvent('buddyliko:context-changed', { detail: data }));
            }
        } catch(e) {
            console.error('Context switch error:', e);
            alert('Errore switch contesto: ' + e.message);
        }
    };

    function _updateCtxButton(context, org) {
        const icon = document.getElementById('bt-ctx-icon');
        const label = document.getElementById('bt-ctx-label');
        if (!icon || !label) return;
        if (context === 'org' && org) {
            icon.textContent = '🏢';
            label.textContent = org.org_name || 'Organizzazione';
        } else {
            icon.textContent = '👤';
            label.textContent = 'Personale';
        }
    }

    // Init context from localStorage
    function _initCtx() {
        try {
            const udata = JSON.parse(localStorage.getItem('buddyliko_user') || '{}');
            if (udata.context === 'org' && udata.org_name) {
                _updateCtxButton('org', { org_name: udata.org_name });
            }
        } catch(e) {}
    }

    // Run after DOM
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _initCtx);
    } else {
        setTimeout(_initCtx, 50);
    }
})();
