/**
 * Buddyliko i18n — Internationalization System
 * Usage: Include this script, then call i18n.init()
 * Elements with data-i18n="path.to.key" get auto-translated
 * Elements with data-i18n-html="path.to.key" get innerHTML set
 * Elements with data-i18n-placeholder="path.to.key" get placeholder set
 */
const i18n = (() => {
    const SUPPORTED = ['it', 'en'];
    const DEFAULT = 'it';
    let lang = localStorage.getItem('buddyliko_lang') || navigator.language?.slice(0, 2) || DEFAULT;
    if (!SUPPORTED.includes(lang)) lang = DEFAULT;
    let strings = {};
    let loaded = false;

    function get(path, fallback) {
        const keys = path.split('.');
        let val = strings;
        for (const k of keys) {
            if (val && typeof val === 'object' && k in val) val = val[k];
            else return fallback || path;
        }
        return val || fallback || path;
    }

    function t(path, replacements) {
        let s = get(path);
        if (replacements) {
            for (const [k, v] of Object.entries(replacements)) {
                s = s.replace(`{${k}}`, v);
            }
        }
        return s;
    }

    function applyAll() {
        document.querySelectorAll('[data-i18n]').forEach(el => {
            el.textContent = get(el.dataset.i18n);
        });
        document.querySelectorAll('[data-i18n-html]').forEach(el => {
            el.innerHTML = get(el.dataset.i18nHtml);
        });
        document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
            el.placeholder = get(el.dataset.i18nPlaceholder);
        });
        document.documentElement.lang = lang;
        // Update lang switcher if present
        const sw = document.getElementById('lang-current');
        if (sw) sw.textContent = strings.meta?.flag || lang.toUpperCase();
    }

    async function load(code) {
        try {
            const r = await fetch(`/lang/${code}.json`);
            if (!r.ok) throw new Error(`Lang ${code} not found`);
            strings = await r.json();
            lang = code;
            localStorage.setItem('buddyliko_lang', code);
            loaded = true;
            applyAll();
            // Fire event for dynamic content
            window.dispatchEvent(new CustomEvent('langchange', { detail: { lang: code, strings } }));
        } catch (e) {
            console.warn(`[i18n] Failed to load ${code}, falling back to ${DEFAULT}`);
            if (code !== DEFAULT) await load(DEFAULT);
        }
    }

    async function init() {
        await load(lang);
        return { lang, strings };
    }

    function switchTo(code) {
        if (SUPPORTED.includes(code) && code !== lang) load(code);
    }

    function getLang() { return lang; }
    function isLoaded() { return loaded; }

    // Language switcher component — inject into any element
    function createSwitcher(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;
        container.innerHTML = `
            <div style="position:relative;display:inline-block">
                <button id="lang-btn" style="background:transparent;border:1px solid var(--border,#2e3250);border-radius:6px;padding:4px 10px;cursor:pointer;font-size:14px;color:var(--text,#e2e8f0);display:flex;align-items:center;gap:4px" onclick="document.getElementById('lang-dropdown').classList.toggle('show')">
                    <span id="lang-current">${strings.meta?.flag || '🌐'}</span>
                    <span style="font-size:10px">▼</span>
                </button>
                <div id="lang-dropdown" style="display:none;position:absolute;right:0;top:110%;background:var(--bg2,#0a1128);border:1px solid var(--border,#2e3250);border-radius:8px;overflow:hidden;min-width:130px;z-index:999;box-shadow:0 8px 24px rgba(0,0,0,.4)">
                    ${SUPPORTED.map(code => {
                        const flags = { it: '🇮🇹', en: '🇬🇧' };
                        const labels = { it: 'Italiano', en: 'English' };
                        return `<div onclick="i18n.switchTo('${code}')" style="padding:8px 14px;cursor:pointer;font-size:13px;color:var(--text,#e2e8f0);display:flex;align-items:center;gap:8px;${code === lang ? 'background:var(--bg3,#111a35)' : ''}" onmouseover="this.style.background='var(--bg3,#111a35)'" onmouseout="this.style.background='${code === lang ? 'var(--bg3,#111a35)' : 'transparent'}'">
                            <span>${flags[code] || code}</span> ${labels[code] || code}
                        </div>`;
                    }).join('')}
                </div>
            </div>
            <style>#lang-dropdown.show{display:block!important}</style>`;
        // Close on click outside
        document.addEventListener('click', e => {
            if (!container.contains(e.target)) {
                const dd = document.getElementById('lang-dropdown');
                if (dd) dd.classList.remove('show');
            }
        });
    }

    return { init, t, get, switchTo, getLang, isLoaded, applyAll, createSwitcher, load };
})();
