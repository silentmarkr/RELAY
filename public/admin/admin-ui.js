/* RELAY ADMIN — shared navigation: floating Menu button + quick-search palette (Ctrl/Cmd+K, or "/").
   Loaded (deferred) by every admin page. No dependencies; it only reads the page and never changes its data.
   On the main dashboard it switches tabs in place (via the page's own switchTab); everywhere else it navigates. */
(function () {
  'use strict';
  if (window.__relayAdminUi) return;
  window.__relayAdminUi = true;

  var NAV = [
    { group: 'Dashboard', items: [
      { t: 'Home', d: 'Pending approvals, device counts, alerts', ico: '🏠', href: 'index.html#home', tab: 'home' },
      { t: 'Clients', d: 'Allowed devices, labels, seen devices', ico: '👥', href: 'index.html#clients', tab: 'clients' },
      { t: 'Revenue', d: 'Subscriptions, sales, expiring licences', ico: '💰', href: 'index.html#revenue', tab: 'revenue' },
      { t: 'Plans & Pricing', d: 'Plan catalogue and prices', ico: '🏷️', href: 'index.html#plans', tab: 'plans' },
      { t: 'Releases', d: 'Publish, download codes, build history', ico: '🚀', href: 'index.html#releases', tab: 'releases' },
      { t: 'System', d: 'Health, settings, tools, danger zone', ico: '⚙️', href: 'index.html#system', tab: 'system' }
    ] },
    { group: 'Clients & licences', items: [
      { t: 'Installation Data', d: 'Cloud backups per installation', ico: '🗄️', href: 'installations.html', k: 'cloud backup neon storage' },
      { t: 'Client Activation & Licensing', d: 'Issue and manage activation keys', ico: '🔑', href: 'client-activations.html', k: 'license key customer' },
      { t: 'Support Tickets', d: 'Tickets sent from the Help chatbot', ico: '🎫', href: 'tickets.html', k: 'help support' },
      { t: 'Schools (Attendance)', d: 'School Gate / Parent Watch accounts', ico: '🏫', href: 'schools.html', k: 'school attendance parent qr' },
      { t: 'Client Cost Allocation', d: 'Neon cost per client, safety net', ico: '📊', href: 'client-cost-allocation.html', k: 'neon cost profit' }
    ] },
    { group: 'Pricing', items: [
      { t: 'Feature Pricing', d: 'Feature prices, Omni AI credits and providers', ico: '🧩', href: 'feature-pricing.html', k: 'omni ai credits tokens' },
      { t: 'Cloud Backup Pricing', d: 'Backup plans and price calculator', ico: '☁️', href: 'pricing.html', k: 'backup plans' }
    ] },
    { group: 'Tools & security', items: [
      { t: 'Database Migration', d: 'Move a Neon database to a new URL', ico: '🛰️', href: 'database-migration.html', k: 'neon postgres move migrate' },
      { t: 'Public URL Checker', d: 'What still points to an old RELAY URL', ico: '🔗', href: 'url-check.html', k: 'domain webhook' },
      { t: 'BIR Reset Pass Keys', d: 'Issue one-time pass keys', ico: '🧾', href: 'bir-passkeys.html', k: 'bir reset' },
      { t: 'Kiosk Developer PIN', d: "One PIN for all clients' kiosk setup", ico: '🔐', href: 'kiosk-dev-pin.html', k: 'kiosk pin' },
      { t: 'BIR Test Wipe', d: 'Remote wipe for testing only', ico: '🧪', href: 'bir-test-wipe.html', k: 'bir wipe test' }
    ] }
  ];

  var page = (location.pathname.split('/').pop() || 'index.html').toLowerCase();
  if (page === '') page = 'index.html';
  var onIndex = page === 'index.html';

  function currentTab() {
    var b = document.querySelector('.tab-btn.active[data-tab]');
    return b ? b.getAttribute('data-tab') : null;
  }
  function isCurrent(it) {
    var target = it.href.split('#')[0].toLowerCase();
    if (target !== page) return false;
    if (!it.tab) return true;
    return onIndex && currentTab() === it.tab;
  }

  var fab, overlay, input, list, flat = [], active = 0, lastFocus = null;

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function build() {
    fab = el('button', null);
    fab.id = 'relayNavFab';
    fab.type = 'button';
    fab.setAttribute('aria-haspopup', 'dialog');
    fab.setAttribute('aria-label', 'Open the RELAY admin menu');
    fab.appendChild(el('span', null, '☰'));
    fab.appendChild(el('span', null, 'Menu'));
    fab.appendChild(el('kbd', null, 'Ctrl K'));
    fab.addEventListener('click', open);
    document.body.appendChild(fab);

    overlay = el('div');
    overlay.id = 'relayPalette';
    overlay.setAttribute('role', 'dialog');
    overlay.setAttribute('aria-modal', 'true');
    overlay.setAttribute('aria-label', 'RELAY admin menu and quick search');
    var box = el('div', 'rp-box');
    input = el('input', 'rp-input');
    input.type = 'text';
    input.placeholder = 'Go to… (type a page, tool or tab)';
    input.setAttribute('autocomplete', 'off');
    input.setAttribute('spellcheck', 'false');
    input.setAttribute('aria-label', 'Search the admin menu');
    list = el('div', 'rp-list');
    list.setAttribute('role', 'listbox');
    var foot = el('div', 'rp-foot');
    [['↑↓', 'move'], ['Enter', 'open'], ['Esc', 'close']].forEach(function (p) {
      var s = el('span');
      s.appendChild(el('kbd', null, p[0]));
      s.appendChild(document.createTextNode(' ' + p[1]));
      foot.appendChild(s);
    });
    box.appendChild(input); box.appendChild(list); box.appendChild(foot);
    overlay.appendChild(box);
    overlay.addEventListener('mousedown', function (e) { if (e.target === overlay) close(); });
    input.addEventListener('input', function () { render(input.value); });
    input.addEventListener('keydown', onKey);
    document.body.appendChild(overlay);
  }

  function matches(it, group, q) {
    if (!q) return true;
    var hay = (it.t + ' ' + it.d + ' ' + group + ' ' + (it.k || '')).toLowerCase();
    return q.toLowerCase().split(/\s+/).filter(Boolean).every(function (w) { return hay.indexOf(w) !== -1; });
  }

  function render(q) {
    list.textContent = '';
    flat = [];
    // When searching, title matches rank above description/keyword matches (groups and items both).
    function score(it) {
      if (!q) return 0;
      var t = it.t.toLowerCase(), ws = q.toLowerCase().split(/\s+/).filter(Boolean);
      if (ws.every(function (w) { return t.indexOf(w) === 0 || t.indexOf(' ' + w) !== -1; })) return 0;
      if (ws.every(function (w) { return t.indexOf(w) !== -1; })) return 1;
      return 2;
    }
    var groups = NAV.map(function (g, gi) {
      var items = g.items.filter(function (it) { return matches(it, g.group, q); });
      if (q) items = items.map(function (it, i) { return { it: it, s: score(it), i: i }; })
        .sort(function (a, b) { return a.s - b.s || a.i - b.i; }).map(function (x) { return x.it; });
      return { group: g.group, items: items, best: items.length ? score(items[0]) : 9, gi: gi };
    }).filter(function (g) { return g.items.length; });
    if (q) groups.sort(function (a, b) { return a.best - b.best || a.gi - b.gi; });
    groups.forEach(function (g) {
      var items = g.items;
      list.appendChild(el('div', 'rp-group', g.group));
      items.forEach(function (it) {
        var b = el('button', 'rp-item');
        b.type = 'button';
        b.setAttribute('role', 'option');
        b.id = 'rp-opt-' + flat.length;
        b.appendChild(el('span', 'rp-ico', it.ico));
        var txt = el('span');
        txt.appendChild(el('div', 'rp-t', it.t));
        txt.appendChild(el('div', 'rp-d', it.d));
        b.appendChild(txt);
        if (isCurrent(it)) b.appendChild(el('span', 'rp-cur', 'YOU ARE HERE'));
        var idx = flat.length;
        b.addEventListener('click', function () { go(it); });
        b.addEventListener('mousemove', function () { setActive(idx, false); });
        list.appendChild(b);
        flat.push({ it: it, node: b });
      });
    });
    if (!flat.length) list.appendChild(el('div', 'rp-empty', 'Nothing matches “' + q + '”.'));
    // Start on the current page's entry when the list is unfiltered, otherwise on the first match.
    var start = 0;
    if (!q) flat.forEach(function (f, i) { if (isCurrent(f.it)) start = i; });
    setActive(start, true);
  }

  function setActive(i, scroll) {
    if (!flat.length) { active = 0; input.removeAttribute('aria-activedescendant'); return; }
    active = (i + flat.length) % flat.length;
    flat.forEach(function (f, n) { f.node.setAttribute('aria-selected', n === active ? 'true' : 'false'); });
    input.setAttribute('aria-activedescendant', flat[active].node.id);
    if (scroll) flat[active].node.scrollIntoView({ block: 'nearest' });
  }

  function onKey(e) {
    if (e.key === 'ArrowDown') { e.preventDefault(); setActive(active + 1, true); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActive(active - 1, true); }
    else if (e.key === 'Enter') { e.preventDefault(); if (flat[active]) go(flat[active].it); }
    else if (e.key === 'Escape') { e.preventDefault(); close(); }
    else if (e.key === 'Tab') { e.preventDefault(); setActive(active + (e.shiftKey ? -1 : 1), true); } // keep focus inside the dialog
  }

  function go(it) {
    close();
    if (it.tab && onIndex && typeof window.switchTab === 'function') {
      try { window.switchTab(it.tab); window.scrollTo(0, 0); return; } catch (_e) { /* fall through to navigation */ }
    }
    var target = it.href;
    if (it.href.split('#')[0].toLowerCase() === page && !it.tab) return; // already on this page
    location.href = target;
  }

  function open() {
    if (!overlay) build();
    lastFocus = document.activeElement;
    overlay.classList.add('open');
    input.value = '';
    render('');
    input.focus();
  }
  function close() {
    if (!overlay) return;
    overlay.classList.remove('open');
    if (lastFocus && typeof lastFocus.focus === 'function') { try { lastFocus.focus(); } catch (_e) { /* ignore */ } }
  }

  function typing(t) {
    if (!t) return false;
    var tag = (t.tagName || '').toLowerCase();
    return tag === 'input' || tag === 'textarea' || tag === 'select' || t.isContentEditable;
  }
  document.addEventListener('keydown', function (e) {
    var isOpen = overlay && overlay.classList.contains('open');
    if ((e.ctrlKey || e.metaKey) && !e.altKey && (e.key === 'k' || e.key === 'K')) {
      e.preventDefault();
      if (isOpen) close(); else open();
    } else if (e.key === '/' && !e.ctrlKey && !e.metaKey && !e.altKey && !typing(e.target) && !isOpen) {
      e.preventDefault();
      open();
    }
  });

  function init() { if (!fab) build(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
