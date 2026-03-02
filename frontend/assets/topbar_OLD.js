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
        'finance.html':          'finance'
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
/* Light mode */
[data-theme="light"] .buddy-topbar{background:linear-gradient(135deg,#f8fafc,#f1f5f9);border-bottom-color:#e2e8f0}
[data-theme="light"] .bt-nav a{color:#64748b}
[data-theme="light"] .bt-nav a:hover{background:#e2e8f0;color:#1e293b}
[data-theme="light"] .bt-nav a.bt-active{background:#dbeafe;color:#2563eb}
[data-theme="light"] .bt-user-name{color:#1e293b}
[data-theme="light"] .bt-user-email{color:#94a3b8}
[data-theme="light"] .bt-logout{border-color:#e2e8f0;color:#64748b}
[data-theme="light"] .bt-logout:hover{border-color:#ef4444;color:#ef4444}
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
            { href: 'account.html',          icon: '💳', label: T('nav.subscription', 'Abbonamento'), id: 'subscription' },
        ];
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
})();
