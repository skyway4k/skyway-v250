/* Skyway v2 client — one call to /api/v2/board, keyed per-cell DOM patching (no flicker). */
(function () {
  'use strict';
  var ROLE = (location.pathname.match(/^\/v2\/(dispatch|line|lobby|gm)\b/) || [])[1] || 'dispatch';
  var ROLE_TABS = [['dispatch', 'Dispatch'], ['line', 'Line'], ['lobby', 'Lobby'], ['gm', 'GM']];
  var TZ = 'America/Los_Angeles';
  var MIN = 60000, HOUR = 3600000;
  var S = { data: null, lastOk: 0, err: '', fetching: false, allLanes: false, openId: null, openSec: null, adsb: [], prevStatus: {} };
  var KEY_LS = 'skyway_ramp_edit_key';
  var $ = function (s, r) { return (r || document).querySelector(s); };

  // ---------- formatting ----------
  var fmtHM = new Intl.DateTimeFormat('en-US', { timeZone: TZ, hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
  var fmtHMS = new Intl.DateTimeFormat('en-US', { timeZone: TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
  var fmtDay = new Intl.DateTimeFormat('en-US', { timeZone: TZ, weekday: 'short', month: 'short', day: 'numeric' });
  function t(iso) { var m = Date.parse(iso || ''); return isNaN(m) ? 0 : m; }
  function clk(iso) { var m = t(iso); return m ? fmtHM.format(m) : '—'; }
  function dayTag(iso) { var m = t(iso); if (!m) return ''; var a = fmtDay.format(m), b = fmtDay.format(Date.now()); return a === b ? '' : a.split(',')[0]; }
  function dur(mins) {
    mins = Math.max(0, Math.round(mins));
    if (mins < 60) return mins + 'm';
    var h = Math.floor(mins / 60), m = mins % 60;
    if (h >= 24) { var d = Math.floor(h / 24); return d + 'd ' + (h % 24) + 'h'; }
    return h + 'h' + (m < 10 ? '0' : '') + m;
  }
  function hmOver(mins) { mins = Math.round(mins); return '+' + Math.floor(mins / 60) + ':' + ('0' + (mins % 60)).slice(-2); }
  function ago(sec) { if (sec == null) return '—'; if (sec < 60) return sec + 's'; if (sec < 3600) return Math.round(sec / 60) + 'm'; if (sec < 86400) return Math.round(sec / 3600) + 'h'; return Math.round(sec / 86400) + 'd'; }
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var MAKERS = /^(Gulfstream Aerospace|Gulfstream|Cessna|Textron Aviation|Textron|Bombardier|Embraer|Dassault Falcon|Dassault|Piper|Beechcraft|Beech|Raytheon|Cirrus|Pilatus|Hawker Beechcraft|Hawker|Honda|HondaJet|Daher|Socata|Mooney|Diamond|Israel Aircraft Industries|IAI|Airbus Helicopters|Eurocopter|Sikorsky|Robinson|Bell|Learjet)\s+/i;
  function shortModel(m) { m = String(m || '').trim(); var s = m.replace(MAKERS, ''); return s || m; }
  function money(n) { return n == null ? '—' : '$' + Math.round(n).toLocaleString('en-US'); }
  function num(n) { return n == null ? '—' : Math.round(n).toLocaleString('en-US'); }
  function spotShort(name) {
    if (!name) return '';
    var d = spotByName(name); return d ? d.short : String(name).slice(0, 6);
  }
  function spotByName(name) {
    var sp = (S.data && S.data.spots) || []; var u = String(name || '').trim().toUpperCase();
    for (var i = 0; i < sp.length; i++) if (sp[i].name.toUpperCase() === u) return sp[i];
    return null;
  }
  function findRow(id, sec) {
    var d = S.data; if (!d) return null;
    var order = sec ? [sec] : ['arrivals', 'onGround', 'departures', 'departed'];
    for (var i = 0; i < order.length; i++) { var l = d.sections[order[i]] || []; for (var j = 0; j < l.length; j++) if (l[j].id === id) return { r: l[j], sec: order[i] }; }
    if (sec) return findRow(id);
    return null;
  }

  // ---------- data ----------
  function load() {
    if (S.fetching) return; S.fetching = true;
    fetch('/api/v2/board?view=' + ROLE, { cache: 'no-store' }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status); return r.json();
    }).then(function (d) {
      S.data = d; S.lastOk = Date.now(); S.err = ''; render();
      var deep = new URLSearchParams(location.search).get('open');
      if (deep && !S.deepDone) { S.deepDone = true; openSheet(deep.toUpperCase().replace(/[^A-Z0-9]/g, ''), null, new URLSearchParams(location.search).get('focus')); }
    }).catch(function (e) { S.err = e.message || String(e); renderStale(); })
      .then(function () { S.fetching = false; });
  }
  var loadSoonT = 0;
  function loadSoon() { clearTimeout(loadSoonT); loadSoonT = setTimeout(load, 1200); }
  function ws() {
    try {
      var sock = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
      sock.onmessage = function (ev) { try { var m = JSON.parse(ev.data); if (m.type === 'board' || m.type === 'ramp_update') loadSoon(); } catch (e) { } };
      sock.onclose = function () { setTimeout(ws, 8000); };
    } catch (e) { }
  }

  // ---------- shell ----------
  function shell() {
    var app = $('#app');
    if (ROLE === 'lobby') { document.body.classList.add('lobby'); app.innerHTML = lobbyShell(); return; }
    var tabs = ROLE_TABS.map(function (x) { return '<a href="/v2/' + x[0] + '" class="' + (x[0] === ROLE ? 'on' : '') + '">' + x[1] + '</a>'; }).join('');
    app.innerHTML =
      '<header class="hdr" id="hdr"><div class="hdr-r1"><div class="brand"><b>Sky<i>way</i><span class="v2tag">v2 preview</span></b><span>KSFO San Francisco Intl</span></div>' +
      '<div class="clock tn" id="clock">--:--</div><button class="icon-btn" id="theme" title="Light / dark" aria-label="Toggle light or dark theme">◐</button></div>' +
      '<div class="hdr-r2"><nav class="roles" aria-label="Role">' + tabs + '</nav><div class="feeds" id="feeds"></div></div></header>' +
      '<div class="stalebar" id="stale" hidden></div>' +
      '<main class="' + (ROLE === 'gm' ? 'v-gm' : 'v-std') + '">' +
      (ROLE === 'gm' ? '<section class="gmsum" id="gmsum"></section>' : '') +
      '<section class="now" id="now"></section>' +
      '<section class="top" id="top"><div class="panel"><div class="panel-h">Live traffic · ADS-B<span class="sp"></span><span class="legend" id="maplg"></span></div><div id="map"></div></div>' +
      '<div class="panel"><div class="panel-h">Movements per hour (PT)<span class="sp"></span><span class="legend"><span><i style="background:var(--green)"></i>Arrivals</span><span><i style="background:var(--blue)"></i>Departures</span></span></div><div class="chart" id="chart"></div></div>' +
      (ROLE !== 'gm' ? '<div class="panel gwrap"><div class="panel-h">Ramp timeline · spot occupancy<span class="sp"></span><span class="legend"><span><i style="background:var(--teal)"></i>On ground</span><span><i style="background:transparent;border:1px solid var(--acc)"></i>Inbound (assigned)</span><span><i style="border:1px dashed var(--violet)"></i>Suggested</span><span><i style="background:var(--red)"></i>Conflict</span></span><button id="lanes">All spots</button></div><div class="gantt" id="gantt"></div></div>' : '') +
      '</section>' +
      '<nav class="jump" id="jump"></nav>' +
      '<div class="boards"><div class="col">' + secShell('arrivals', 'arr', 'Arrivals') + secShell('onGround', 'gnd', 'Arrived · on ground') + '</div>' +
      '<div class="col">' + secShell('departures', 'dep', 'Departures') + secShell('departed', 'off', 'Departed · last 6 h') + '</div></div>' +
      '<div class="foot" id="foot"></div></main>';
    var cols = $('.boards').children; for (var i = 0; i < cols.length; i++) { cols[i].style.display = 'grid'; cols[i].style.gap = '10px'; cols[i].style.alignContent = 'start'; }
    $('#theme').onclick = function () {
      var light = document.documentElement.getAttribute('data-theme') !== 'light';
      if (light) document.documentElement.setAttribute('data-theme', 'light'); else document.documentElement.removeAttribute('data-theme');
      try { localStorage.setItem('skyway_v2_theme', light ? 'light' : 'dark'); } catch (e) { }
      if (S.map) setTiles();
    };
    var lb = $('#lanes'); if (lb) lb.onclick = function () { S.allLanes = !S.allLanes; lb.textContent = S.allLanes ? 'Used + front row' : 'All spots'; renderGantt(); };
    app.addEventListener('click', onClick);
    var setH = function () { var h = $('#hdr'); if (h) document.documentElement.style.setProperty('--hdrH', h.offsetHeight + 'px'); };
    setH(); window.addEventListener('resize', setH);
    $('#foot').innerHTML = footHTML();
  }
  function secShell(k, cls, title) {
    return '<section class="sec ' + cls + '" id="sec-' + k + '"><div class="sec-h"><h2>' + title + '</h2><span class="n" id="n-' + k + '">–</span><span class="hint" id="h-' + k + '"></span></div>' +
      colHead(k) + '<div class="rows" id="rows-' + k + '"></div>' +
      (k === 'onGround' ? '<details class="older" id="older"><summary id="older-s"></summary><div class="rows" id="rows-older"></div></details>' : '') + '</section>';
  }
  function colHead(k) {
    var tm = k === 'arrivals' ? 'ETA' : k === 'onGround' ? 'Landed' : k === 'departures' ? 'ETD' : 'Off';
    var ap = (k === 'arrivals' || k === 'onGround') ? 'From' : 'To';
    if (ROLE === 'gm') return '<div class="colh"><span>On base</span><span>Ident</span><span class="c-ty">Type</span><span class="c-rt">' + (k === 'arrivals' || k === 'onGround' ? 'From · next' : 'To') + '</span><span style="text-align:right">' + tm + '</span><span class="c-px">Pax</span><span style="text-align:right">Fuel est</span><span class="c-vp">VIP</span></div>';
    return '<div class="colh"><span>On base</span><span>Ident</span><span>Type</span><span>' + ap + '</span><span style="text-align:right">' + tm + '</span><span class="c-st">Status · data</span><span style="text-align:center">Spot</span></div>';
  }
  function footHTML() {
    return '<span class="lg"><i style="background:var(--c3)"></i>FAA plan + live ADS-B</span><span class="lg"><i style="background:var(--c2)"></i>FAA filed plan</span><span class="lg"><i style="background:var(--c1)"></i>ADS-B estimate / no time yet</span><span class="lg"><i style="background:var(--c0)"></i>Stale</span><br>' +
      'Left edge = data confidence. Tap any row for sources, freshness and details. <b>ON BASE</b>: countdown to ETD for aircraft on the ground (red +h:mm = past ETD, no wheels-off seen); planned ground time (ETD − ETA) for inbound. ' +
      'VIP with a dashed outline and spot suggestions are <b>estimates</b> (operator/type/stay rules) — never written until someone taps them. Times PT. Sources: FAA SWIM TFMS first, then ADS-B (adsb.lol), FAA registry types. GA &amp; business aviation only.';
  }

  // ---------- render ----------
  function render() {
    if (!S.data) return;
    if (ROLE === 'lobby') return renderLobby();
    renderFeeds(); renderStale();
    if (ROLE === 'gm') renderGM();
    renderNow();
    ['arrivals', 'onGround', 'departures', 'departed'].forEach(renderSec);
    renderJump();
    if (isDesk()) { renderChart(); renderGantt(); ensureMap(); }
    if (S.openId) refreshSheet();
  }
  function isDesk() { return window.matchMedia('(min-width:1100px)').matches; }
  function renderFeeds() {
    var f = S.data.feed || {}; var sw = f.swim || {}, ad = f.adsb || {};
    var age = Math.round((Date.now() - S.lastOk) / 1000);
    var h = '<span class="feed" title="FAA SWIM TFMS filed plans' + (sw.reason ? ' — ' + esc(sw.reason) : '') + '"><i class="dot ' + (sw.connected ? 'ok' : 'bad') + '"></i>FAA</span>' +
      '<span class="feed" title="adsb.lol live positions' + (ad.error ? ' — ' + esc(ad.error) : '') + '"><i class="dot ' + (ad.ok ? 'ok' : ad.backoffSec ? 'warn' : 'bad') + '"></i>ADS-B' + (!ad.ok && ad.backoffSec ? '<small class="dk"> rate-limited</small>' : '') + '</span>' +
      '<span class="feed tn" title="Board last refreshed"><span class="dk">updated </span><span id="upd">' + ago(age) + '</span></span>';
    $('#feeds').innerHTML = h;
  }
  function renderStale() {
    var el = $('#stale'); if (!el) return;
    var age = S.lastOk ? (Date.now() - S.lastOk) / 1000 : 0;
    if ((S.err && (!S.lastOk || age > 60)) || age > 120) { el.hidden = false; el.textContent = S.lastOk ? ('Board data is ' + dur(age / 60) + ' old — reconnecting… ' + (S.err ? '(' + S.err + ')' : '')) : ('Loading board… ' + (S.err ? '(' + S.err + ')' : '')); }
    else el.hidden = true;
  }
  function setHTML(el, h) { if (el && el.__h !== h) { el.innerHTML = h; el.__h = h; } }

  function renderNow() {
    var n = S.data.now; if (!n) return;
    var now = Date.now();
    var na = n.nextArrivals.length ? n.nextArrivals.map(function (a) {
      var m = (t(a.etaISO) - now) / MIN;
      return '<b data-open="' + esc(a.id) + '" data-sec="arrivals">' + esc(a.ident) + '</b><span class="t">' + esc(a.type || '') + '</span><span class="f' + (a.intl ? ' intl' : '') + '">' + esc(a.from || '—') + (a.intl ? ' 🛃' : '') + (a.vip ? ' <span class="chip vip est">VIP</span>' : '') + '</span><span class="cd tn">' + (m > 0 ? dur(m) : 'due') + '</span>';
    }).join('') : '<span class="empty">No inbound GA in the window</span>';
    var h = '<div class="card next"><h4>Next arrivals</h4><div class="na">' + na + '</div></div>' +
      '<div class="card"><h4>On ground</h4><div class="big tn">' + n.onGroundRecent + (n.onGround > n.onGroundRecent ? ' <small>+' + (n.onGround - n.onGroundRecent) + ' older</small>' : '') + '</div><div class="sm">' + n.onGroundNoSpot + ' without a spot</div></div>' +
      '<div class="card"><h4>Open spots</h4><div class="big tn">' + n.spotsOpen + '<small>/' + n.spotsTotal + '</small></div><div class="sm">' + n.closeSpotsOpen + ' front-row free</div></div>' +
      '<div class="card vip"><h4>VIP due · 60 min</h4><div class="big tn">' + n.vipDue60 + '</div><div class="sm">' + (n.vipDue60Idents.length ? esc(n.vipDue60Idents.slice(0, 3).join(' · ')) : 'none due') + '</div></div>' +
      '<div class="card"><h4>Departing · 60 min</h4><div class="big tn">' + n.departuresNext60 + '</div><div class="sm">' + n.intlDue + ' intl arrival' + (n.intlDue === 1 ? '' : 's') + ' pending</div></div>' +
      (n.conflicts ? '<div class="card alert"><h4>Spot conflicts</h4><div class="big tn">' + n.conflicts + '</div><div class="sm">overlapping stays</div></div>' : '');
    setHTML($('#now'), h);
  }

  function renderJump() {
    var d = S.data.sections; var og = d.onGround.filter(function (r) { return !r.stale; }).length;
    var h = [['arrivals', 'Arrivals', d.arrivals.length], ['onGround', 'On ground', og], ['departures', 'Departures', d.departures.length], ['departed', 'Departed', d.departed.length]]
      .map(function (x) { return '<a href="#sec-' + x[0] + '">' + x[1] + '<b class="tn">' + x[2] + '</b></a>'; }).join('');
    setHTML($('#jump'), h);
  }

  // ---- rows ----
  function obCell(r, sec, now) {
    var o = r.ob || { mode: 'none' }, v = '—', sub = '', c = 'ob-none';
    if (sec === 'departed') { if (r.onBaseSinceISO && r.offISO) { v = dur((t(r.offISO) - t(r.onBaseSinceISO)) / MIN); sub = 'was on'; c = 'ob-plan'; } else sub = ''; }
    else if (o.mode === 'countdown') { var m = (t(o.toISO) - now) / MIN; v = dur(m); sub = 'to ETD'; c = m <= 30 ? 'ob-soon' : 'ob-cd'; }
    else if (o.mode === 'overdue') { v = hmOver((now - t(o.toISO)) / MIN); sub = 'past ETD'; c = 'ob-late'; }
    else if (o.mode === 'planned') { v = dur((t(o.toISO) - t(o.fromISO)) / MIN); sub = 'plan'; c = 'ob-plan'; }
    else if (sec === 'onGround') { sub = 'no ETD'; }
    return cell('c-ob ' + c, '<b class="tn">' + v + '</b>' + (sub ? '<small>' + sub + '</small>' : ''));
  }
  function cell(cls, inner) { return { cls: cls, h: inner }; }
  function idCell(r) {
    var sub = r.ladd ? 'LADD' : (r.callsign || (r.reg && r.reg !== r.ident ? r.reg : ''));
    return cell('c-id', '<b>' + esc(r.ident || '—') + '</b>' + (sub ? '<small>' + esc(sub) + '</small>' : ''));
  }
  function tyCell(r) {
    var m = shortModel(r.model);
    return cell('c-ty', '<b>' + esc(r.type || (m ? m : '—')) + (r.heli ? ' 🚁' : '') + '</b>' + (r.type && m ? '<small title="' + esc(r.model) + '">' + esc(m) + '</small>' : ''));
  }
  function apCell(r, sec) {
    var code = (sec === 'arrivals' || sec === 'onGround') ? r.from : r.to;
    var sub = r.intl ? ((sec === 'arrivals' || sec === 'onGround') ? '🛃 ' : '') + (r.country || r.city || 'International') : (r.city || (!code && r.fromNote ? r.fromNote : ''));
    return cell('c-ap', '<b>' + esc(code || '—') + '</b>' + (sub ? '<small>' + esc(sub) + '</small>' : ''));
  }
  function stLabel(r) { return (r.status && r.status.label) || ''; }
  function tmCell(r, sec, now) {
    var main = '—', sub = '', sc = 's-' + (r.status ? r.status.code : '');
    var est = r.conf && r.conf.level <= 1 && r.conf.src.indexOf('ADS-B') === 0;
    if (sec === 'arrivals') {
      main = (est ? '~' : '') + clk(r.etaISO);
      var m = (t(r.etaISO) - now) / MIN, code = r.status.code;
      if (code === 'diverted') sub = 'DIV ' + r.divert.to;
      else if (code === 'final') sub = 'FINAL';
      else if (code === 'due') sub = 'DUE';
      else if (r.etaISO && m > 0) sub = 'in ' + dur(m);
      else sub = 'no ETA';
      var dt = dayTag(r.etaISO); if (dt && r.etaISO) main = '<span style="font-size:10px;color:var(--tx3)">' + dt + ' </span>' + main;
    } else if (sec === 'onGround') {
      main = clk(r.landedISO); sub = r.landedAssumed ? 'LANDED?' : dur((now - t(r.landedISO)) / MIN) + ' ago';
      var dt2 = dayTag(r.landedISO); if (dt2) main = '<span style="font-size:10px;color:var(--tx3)">' + dt2 + ' </span>' + main;
    } else if (sec === 'departures') {
      main = clk(r.etdISO); var m2 = (t(r.etdISO) - now) / MIN;
      sub = !r.etdISO ? 'no ETD' : r.status.code === 'taxi' ? 'TAXI' : m2 >= 0 ? 'in ' + dur(m2) : 'late ' + dur(-m2);
      var dt3 = dayTag(r.etdISO); if (dt3 && r.etdISO) main = '<span style="font-size:10px;color:var(--tx3)">' + dt3 + ' </span>' + main;
    } else { main = clk(r.offISO); sub = dur((now - t(r.offISO)) / MIN) + ' ago'; }
    return cell('c-tm', '<b class="tn">' + main + '</b><small class="' + sc + '">' + esc(sub) + '</small>');
  }
  function stCell(r) {
    var c = r.conf || {};
    var fresh = c.liveAgeSec != null && c.liveAgeSec < 600 ? 'live ' + ago(c.liveAgeSec) : c.updatedAgoSec != null ? ago(c.updatedAgoSec) + ' old' : '';
    return cell('c-st', '<b><span class="chip st s-' + esc(r.status.code) + '">' + esc(stLabel(r)) + (r.status.est ? '?' : '') + '</span></b><small title="' + esc(c.note || '') + '">' + esc(c.src || '') + (fresh ? ' · ' + fresh : '') + '</small>');
  }
  function spCell(r, sec) {
    var canEdit = S.data.role.edit.indexOf('spot') >= 0 && sec !== 'departed' && !r.ladd;
    var s = r.spot ? spotShort(r.spot) : '';
    var pax = r.pax != null ? r.pax + ' pax' : (ROLE === 'dispatch' && sec !== 'departed' ? '– pax' : '');
    var sug = !s && r.suggest && ROLE !== 'gm' ? '<small class="sug" title="Suggested (not assigned): ' + esc(r.suggest.spot + ' — ' + r.suggest.why) + '">' + esc(r.suggest.short) + '?</small>' : '';
    return cell('c-sp' + (canEdit ? ' edit' : ''), '<b class="' + (s ? '' : 'none') + '"' + (canEdit ? ' data-spot="' + esc(r.id) + '"' : '') + '>' + (s ? esc(s) : (canEdit ? '+' : '—')) + '</b>' + (r.pax != null ? '<small>' + pax + '</small>' : sug || (pax ? '<small>' + pax + '</small>' : '')));
  }
  function badges(r, sec, now) {
    var b = [];
    if (r.status.code === 'diverted' && r.divert) { var left = (t(r.divert.holdUntilISO) - now) / MIN; b.push('<span class="chip hold">Divert ' + esc(r.divert.to) + ' · hold ' + (left > 0 ? dur(left) : '0m') + '</span>'); }
    if (r.status.code === 'landed_unconfirmed') b.push('<span class="chip warn">No landing report</span>');
    if (r.status.code === 'late') b.push('<span class="chip warn">No wheels-off seen</span>');
    if (r.vip && r.vip.vip) b.push('<span class="chip vip' + (r.vip.est ? ' est' : '') + '" title="' + esc(r.vip.reasons.join(' · ')) + '">VIP' + (r.vip.est ? ' est' : '') + '</span>');
    if (r.qt && sec !== 'departed') b.push('<span class="chip qt">QT</span>');
    if (r.intl && sec === 'arrivals') b.push('<span class="chip cust">Customs</span>');
    (r.tasks || []).forEach(function (x) { if (x.code === 'CUSTOMS' && x.auto) return; b.push('<span class="chip task' + (x.done ? ' done' : '') + '">' + esc(x.code) + (x.done ? ' ✓' : '') + '</span>'); });
    (r.flags || []).forEach(function (f) { var u = String(f).toUpperCase().replace(/\s*✓$/, ''); if (/^(VIP|TOW|FUEL|GPU|LAV|CATERING|CUSTOMS)$/.test(u)) return; b.push('<span class="chip flag">' + esc(f) + '</span>'); });
    if (r.towNotes) b.push('<span class="chip flag" title="' + esc(r.towNotes) + '">📝 note</span>');
    if (sec === 'arrivals' && r.status.code !== 'diverted' && r.nextDepartTo && ROLE !== 'gm') b.push('<span class="chip">Next → ' + esc(r.nextDepartTo) + ' ' + clk(r.nextDepartISO) + '</span>');
    return b.length ? cell('bd', b.join('')) : null;
  }
  function gmCells(r, sec, now) {
    var arrish = sec === 'arrivals' || sec === 'onGround';
    var rt = arrish ? cell('c-rt', '<b>' + esc(r.from || '—') + (r.intl ? ' 🛃' : '') + '</b><small>' + (r.nextDepartTo ? 'next → ' + esc(r.nextDepartTo) : esc(r.city || '')) + '</small>')
      : cell('c-rt', '<b>' + esc(r.to || '—') + '</b><small>' + esc(r.city || '') + '</small>');
    var f = r.fuel;
    var fu = f ? cell('c-fu', '<b class="tn" title="Estimate: ' + esc(f.family) + ' · ' + f.nm + ' nm great-circle · ' + f.gph + ' gph @ ' + f.ktas + ' KTAS × 1.15">' + num(f.gal) + ' gal</b><small class="tn">' + money(f.usd) + ' est</small>')
      : cell('c-fu', '<b style="color:var(--tx3)">—</b><small>' + (arrish && !r.nextDepartTo ? 'no outbound' : 'n/a') + '</small>');
    var vp = r.vip && r.vip.vip ? cell('c-vp', '<b><span class="chip vip' + (r.vip.est ? ' est' : '') + '" title="' + esc(r.vip.reasons.join(' · ')) + '">VIP</span></b><small>' + (r.suggest ? '→ ' + esc(r.suggest.short) : (r.spot ? esc(spotShort(r.spot)) : '')) + '</small>') : cell('c-vp', '<b style="color:var(--tx3)">—</b>');
    var px = cell('c-px', '<b class="tn">' + (r.pax != null ? r.pax : '—') + '</b>');
    return [obCell(r, sec, now), idCell(r), tyCell(r), rt, tmCell(r, sec, now), px, fu, vp];
  }
  function rowCells(r, sec, now) {
    var cells = ROLE === 'gm' ? gmCells(r, sec, now) : [obCell(r, sec, now), idCell(r), tyCell(r), apCell(r, sec), tmCell(r, sec, now), stCell(r), spCell(r, sec)];
    var bd = ROLE === 'gm' ? badgesGM(r) : badges(r, sec, now);
    if (bd) cells.push(bd);
    return cells;
  }
  function badgesGM(r) {
    var b = [];
    if (r.vip && r.vip.vip) b.push('<span class="chip vip' + (r.vip.est ? ' est' : '') + ' gm-ph">VIP' + (r.vip.est ? ' est' : '') + '</span>');
    if (r.status.code === 'diverted') b.push('<span class="chip hold">Divert ' + esc(r.divert.to) + '</span>');
    if (r.qt) b.push('<span class="chip qt">QT</span>');
    if (r.op) b.push('<span class="chip">' + esc(r.op.name) + '</span>');
    var onlyVip = b.length === 1 && r.vip && r.vip.vip;
    return b.length ? cell('bd' + (onlyVip ? ' ph-only' : ''), b.join('')) : null;
  }
  function rowClass(r) {
    return 'row c' + (r.conf ? r.conf.level : 0) + (r.intl ? ' intl' : '') + (r.stale ? ' stale' : '');
  }

  // keyed patcher: container keeps id → element; cells patched individually
  function patchList(container, rows, sec, now) {
    if (!container) return;
    var map = container.__m || (container.__m = {});
    var seen = {};
    var prev = null;
    if (!rows.length) {
      Object.keys(map).forEach(function (k) { map[k].remove(); delete map[k]; });
      if (!container.__empty) { container.innerHTML = '<div class="empty">' + emptyText(sec) + '</div>'; container.__empty = true; }
      return;
    }
    if (container.__empty) { container.innerHTML = ''; container.__empty = false; }
    rows.forEach(function (r) {
      var k = r.id + '|' + r.kind; seen[k] = 1;
      var el = map[k], fresh = false;
      if (!el) { el = document.createElement('div'); el.dataset.id = r.id; el.dataset.sec = sec; map[k] = el; fresh = true; }
      var cls = rowClass(r);
      var stc = r.status ? r.status.code : '';
      if (!fresh && el.__st && el.__st !== stc) cls += ' flash';
      el.__st = stc;
      if (el.className.replace(' flash', '') !== cls.replace(' flash', '') || cls.indexOf('flash') >= 0) el.className = cls;
      el.dataset.sec = sec;
      var cells = rowCells(r, sec, now);
      var kids = el.children;
      for (var i = 0; i < cells.length; i++) {
        var c = kids[i];
        if (!c) { c = document.createElement('span'); el.appendChild(c); }
        if (c.className !== cells[i].cls) c.className = cells[i].cls;
        if (c.__h !== cells[i].h) { c.innerHTML = cells[i].h; c.__h = cells[i].h; }
      }
      while (el.children.length > cells.length) el.removeChild(el.lastChild);
      var want = prev ? prev.nextSibling : container.firstChild;
      if (want !== el) container.insertBefore(el, want);
      prev = el;
    });
    Object.keys(map).forEach(function (k) { if (!seen[k]) { map[k].remove(); delete map[k]; } });
  }
  function emptyText(sec) {
    return { arrivals: 'No inbound GA ' + ((S.data.window && S.data.window.label) || ''), onGround: 'Nothing on the ground right now', departures: 'No outbound GA ' + ((S.data.window && S.data.window.label) || ''), departed: 'None departed in the last 6 hours', older: '' }[sec] || '';
  }
  function renderSec(k) {
    var d = S.data, now = Date.now();
    var rows = d.sections[k] || [];
    var main = rows, older = [];
    if (k === 'onGround') { main = rows.filter(function (r) { return !r.stale; }); older = rows.filter(function (r) { return r.stale; }); }
    if (k === 'departed') main = rows.slice(0, 40);
    patchList($('#rows-' + k), main, k, now);
    $('#n-' + k).textContent = main.length;
    var hint = { arrivals: d.window ? 'showing ' + d.window.label : '', onGround: 'kept until it departs', departures: d.window ? 'showing ' + d.window.label : '', departed: rows.length > 40 ? 'latest 40 of ' + rows.length : 'wheels-off, last 6 h' }[k];
    setHTML($('#h-' + k), esc(hint));
    if (k === 'onGround') {
      var det = $('#older'); det.style.display = older.length ? '' : 'none';
      setHTML($('#older-s'), older.length + ' older than 12 h with no departure filed — likely already gone or long-term parked (unverified)');
      if (det.open || !$('#rows-older').__m) patchList($('#rows-older'), older, 'onGround', now);
    }
  }

  // ---------- GM summary ----------
  function renderGM() {
    var g = S.data.gm; if (!g) return;
    var pk = g.peakHour ? (('0' + g.peakHour.hourPT).slice(-2) + ':00') : '—';
    var h = '<div class="card"><h4>Movements left</h4><div class="big tn">' + g.movementsRemaining + '</div><div class="sm">' + g.arrivalsRemaining + ' arr · ' + g.departuresRemaining + ' dep</div></div>' +
      '<div class="card"><h4>Fuel uplift <span class="est">EST</span></h4><div class="big tn">' + num(g.fuelGalEst) + '<small> gal</small></div><div class="sm">' + g.fuelLegsEstimated + ' legs · ' + g.fuelLegsUnknown + ' unknown</div></div>' +
      '<div class="card"><h4>Fuel revenue <span class="est">EST</span></h4><div class="big tn">' + money(g.fuelUsdEst) + '</div><div class="sm">@ $' + Number(g.fuelPricePerGal).toFixed(2) + '/gal</div></div>' +
      '<div class="card vip"><h4>VIPs <span class="est">EST</span></h4><div class="big tn">' + g.vipCount + '</div><div class="sm">in / on base / out</div></div>' +
      '<div class="card"><h4>Peak hour</h4><div class="big tn">' + pk + '</div><div class="sm">' + (g.peakHour ? g.peakHour.count + ' movements (' + g.peakHour.arr + '↓ ' + g.peakHour.dep + '↑)' : 'quiet') + '</div></div>' +
      '<div class="card"><h4>Last 6 h</h4><div class="big tn">' + (g.landedLast6h + g.departedLast6h) + '</div><div class="sm">' + g.landedLast6h + ' landed · ' + g.departedLast6h + ' off</div></div>' +
      '<div class="note">Fuel = estimated burn for each remaining departure’s outbound leg (great-circle nm ÷ cruise speed × type-family gph × 1.15), not flight-plan fuel or actual uplift. Arrivals show the estimate for their filed next leg. VIP = operator/type/stay heuristic unless flagged by dispatch.</div>';
    setHTML($('#gmsum'), h);
  }

  // ---------- chart ----------
  function renderChart() {
    var hs = S.data.hourly || []; if (!hs.length) return;
    var W = 640, H = 170, padL = 22, padB = 22, padT = 14;
    var max = 1; hs.forEach(function (h) { max = Math.max(max, h.arr, h.dep); });
    var bw = (W - padL - 4) / hs.length;
    var y = function (v) { return H - padB - (v / max) * (H - padB - padT); };
    var nowMs = Date.now(), out = '';
    for (var g = 0; g <= max; g += Math.max(1, Math.ceil(max / 4))) out += '<line x1="' + padL + '" x2="' + W + '" y1="' + y(g) + '" y2="' + y(g) + '" stroke="var(--line)" stroke-width="1"/><text x="' + (padL - 4) + '" y="' + (y(g) + 3) + '" fill="var(--tx3)" font-size="9" text-anchor="end">' + g + '</text>';
    var pk = S.data.gm && S.data.gm.peakHour ? S.data.gm.peakHour.startISO : (function () { var b = null; hs.forEach(function (h) { if (!h.past && (!b || h.arr + h.dep > b.arr + b.dep)) b = h; }); return b && (b.arr + b.dep) ? b.startISO : ''; })();
    hs.forEach(function (h, i) {
      var x = padL + i * bw, w = Math.max(2, bw / 2 - 2), op = h.past ? .45 : 1;
      if (h.arr) out += '<rect x="' + (x + 1) + '" y="' + y(h.arr) + '" width="' + w + '" height="' + (H - padB - y(h.arr)) + '" fill="var(--green)" opacity="' + op + '" rx="1.5"><title>' + h.hourPT + ':00 PT · ' + h.arr + ' arrivals</title></rect>';
      if (h.dep) out += '<rect x="' + (x + 1 + w + 1) + '" y="' + y(h.dep) + '" width="' + w + '" height="' + (H - padB - y(h.dep)) + '" fill="var(--blue)" opacity="' + op + '" rx="1.5"><title>' + h.hourPT + ':00 PT · ' + h.dep + ' departures</title></rect>';
      if (i % (hs.length > 18 ? 2 : 1) === 0) out += '<text x="' + (x + bw / 2) + '" y="' + (H - 7) + '" fill="var(--tx3)" font-size="9.5" text-anchor="middle">' + ('0' + h.hourPT).slice(-2) + '</text>';
      if (pk && h.startISO === pk) out += '<text x="' + (x + bw / 2) + '" y="' + (Math.min(y(h.arr), y(h.dep)) - 3) + '" fill="var(--amber)" font-size="9" font-weight="700" text-anchor="middle">peak</text>';
    });
    var t0 = t(hs[0].startISO), xNow = padL + ((nowMs - t0) / HOUR) * bw;
    out += '<line x1="' + xNow + '" x2="' + xNow + '" y1="' + padT + '" y2="' + (H - padB) + '" stroke="var(--red)" stroke-width="1.5"/><text x="' + (xNow + 3) + '" y="' + (padT - 3) + '" fill="var(--red)" font-size="9" font-weight="700">now</text>';
    setHTML($('#chart'), '<svg viewBox="0 0 ' + W + ' ' + H + '" role="img" aria-label="Movements per hour">' + out + '</svg>');
  }

  // ---------- gantt ----------
  function renderGantt() {
    var el = $('#gantt'); if (!el || !S.data.timeline) return;
    var tl = S.data.timeline, s = t(tl.startISO), e = t(tl.endISO), span = e - s, now = Date.now();
    var pct = function (m) { return Math.max(0, Math.min(100, ((m - s) / span) * 100)); };
    var lanes = tl.lanes.filter(function (l) { return S.allLanes || l.items.length || l.pri <= 2; });
    var hrs = '', lbl = '';
    for (var h = Math.ceil(s / HOUR) * HOUR; h < e; h += HOUR) { hrs += '<div class="g-hr" style="left:' + pct(h) + '%"></div>'; lbl += '<span class="g-ax-lbl" style="left:' + pct(h) + '%">' + fmtHM.format(h).slice(0, 2) + '</span>'; }
    var nowL = '<div class="g-now" style="left:' + pct(now) + '%"></div>';
    var out = '<div class="g-axis"><div class="lb" style="padding:3px 8px">Spot</div><div class="g-track">' + lbl + nowL + '</div></div>';
    lanes.forEach(function (l) {
      var busyNow = l.items.some(function (it) { return it.kind !== 'suggested' && t(it.startISO) <= now && t(it.endISO) > now; });
      var items = l.items.map(function (it) {
        var a = pct(t(it.startISO)), b = pct(t(it.endISO));
        var cls = 'g-it ' + it.kind + (it.openEnd ? ' open' : '') + (it.conflict ? ' conflict' : '') + (it.vip ? ' vip' : '') + (it.fit === false ? ' nofit' : '');
        var tip = it.ident + ' ' + (it.type || '') + ' · ' + clk(it.startISO) + '–' + (it.openEnd ? 'no ETD' : clk(it.endISO)) + (it.kind === 'suggested' ? ' · suggested (not assigned)' : '') + (it.conflict ? ' · CONFLICT' : '') + (it.fit === false ? ' · size may not fit' : '');
        return '<div class="' + cls + '" style="left:' + a + '%;width:' + Math.max(0.6, b - a) + '%" title="' + esc(tip) + '" data-open="' + esc(it.id) + '">' + esc(it.ident) + (it.kind === 'suggested' ? ' ?' : '') + '</div>';
      }).join('');
      out += '<div class="g-lane' + (l.pri <= 2 ? ' close' : '') + (!busyNow && !l.restricted ? ' free' : '') + '"><div class="lb" title="' + esc(l.spot + ' · ' + l.area + (l.restricted ? ' · requires United approval' : '')) + '">' + esc(l.short) + ' <small>' + esc(l.area === 'Front row' ? '' : l.area.split(' ')[0]) + '</small></div><div class="g-track">' + hrs + nowL + items + '</div></div>';
    });
    if (!tl.lanes.some(function (l) { return l.items.length; })) out += '<div class="g-empty">No spots assigned or suggested yet. Assign a spot from any row (tap → Spot) and it appears here with its planned stay until ETD.</div>';
    setHTML(el, out);
  }

  // ---------- map (desktop) ----------
  function ensureMap() {
    if (S.mapInit) return; S.mapInit = true;
    var css = document.createElement('link'); css.rel = 'stylesheet'; css.href = 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css'; document.head.appendChild(css);
    var sc = document.createElement('script'); sc.src = 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js';
    sc.onload = function () {
      S.map = L.map('map', { zoomControl: true, attributionControl: true }).setView([37.62, -122.38], 9);
      setTiles();
      L.circleMarker([37.6188, -122.3754], { radius: 5, color: '#38bdf8', weight: 2, fillOpacity: .6 }).addTo(S.map).bindTooltip('KSFO', { permanent: false });
      S.mapLayer = L.layerGroup().addTo(S.map);
      pollAdsb(); setInterval(pollAdsb, 20000);
    };
    sc.onerror = function () { setHTML($('#map'), '<div class="g-empty">Map library could not load.</div>'); };
    document.head.appendChild(sc);
  }
  function setTiles() {
    if (S.tiles) S.map.removeLayer(S.tiles);
    var light = document.documentElement.getAttribute('data-theme') === 'light';
    // Esri canvas basemaps: no API key (CARTO now watermarks keyless tiles).
    S.tiles = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_' + (light ? 'Light' : 'Dark') + '_Gray_Base/MapServer/tile/{z}/{y}/{x}', { maxZoom: 16, maxNativeZoom: 16, attribution: 'Tiles © Esri' }).addTo(S.map);
  }
  function pollAdsb() {
    if (document.hidden || !S.map) return;
    fetch('/adsb/states?lat=37.6188&lon=-122.3754&dist=80', { cache: 'no-store' }).then(function (r) { return r.ok ? r.json() : null; }).then(function (d) {
      if (!d || !d.states) { setHTML($('#maplg'), '<span>ADS-B unavailable right now</span>'); return; }
      var meta = {}; (d.ac || []).forEach(function (m) { if (m && m.hex) meta[String(m.hex).toLowerCase()] = m; });
      var idx = {}; var secs = S.data ? S.data.sections : {};
      ['arrivals', 'departures', 'onGround', 'departed'].forEach(function (k) { (secs[k] || []).forEach(function (r) { [r.id, r.callsign, r.reg].forEach(function (x) { x = String(x || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); if (x && !idx[x]) idx[x] = { r: r, sec: k }; }); }); });
      S.mapLayer.clearLayers(); var shown = 0;
      d.states.forEach(function (s) {
        var cs = String(s[1] || '').trim().toUpperCase(), m = meta[String(s[0] || '').toLowerCase()] || {};
        var hit = idx[cs.replace(/[^A-Z0-9]/g, '')] || idx[String(m.reg || '').toUpperCase().replace(/[^A-Z0-9]/g, '')];
        if (!hit || s[6] == null || s[5] == null) return;
        shown++;
        var col = hit.sec === 'arrivals' ? '#4ade80' : hit.sec === 'departures' || hit.sec === 'departed' ? '#60a5fa' : '#2dd4bf';
        var rot = s[10] || 0, ft = s[7] != null ? Math.round(s[7] * 3.28084 / 100) * 100 : null;
        var icon = L.divIcon({ className: '', iconSize: [120, 22], iconAnchor: [9, 11], html: '<div style="display:flex;align-items:center;gap:3px;white-space:nowrap"><svg width="18" height="18" viewBox="0 0 24 24" style="transform:rotate(' + rot + 'deg)"><path fill="' + col + '" stroke="#000" stroke-width=".8" d="M12 2l1.6 7.2L21 13v2l-7.4-2.2-.6 5.2 2.5 1.8V21L12 20l-3.5 1v-1.2L11 18l-.6-5.2L3 15v-2l7.4-3.8z"/></svg><span style="font:700 11px/1 -apple-system,Segoe UI,sans-serif;color:' + col + ';text-shadow:0 0 3px #000,0 0 3px #000">' + esc(hit.r.ident) + '</span></div>' });
        L.marker([s[6], s[5]], { icon: icon }).addTo(S.mapLayer).bindTooltip(esc(hit.r.ident) + ' ' + esc(hit.r.type || '') + (ft != null ? ' · ' + ft + ' ft' : '') + (s[9] != null ? ' · ' + Math.round(s[9] * 1.94384) + ' kt' : ''));
      });
      setHTML($('#maplg'), '<span>' + shown + ' board aircraft with a live position · ' + esc(d.source || 'adsb') + '</span>');
    }).catch(function () { });
  }

  // ---------- sheet (details + edit) ----------
  function onClick(ev) {
    var sp = ev.target.closest('[data-spot]');
    if (sp) { ev.stopPropagation(); openSheet(sp.getAttribute('data-spot'), sp.closest('.row') && sp.closest('.row').dataset.sec, 'spot'); return; }
    var o = ev.target.closest('[data-open]');
    if (o) { openSheet(o.getAttribute('data-open'), o.getAttribute('data-sec')); return; }
    var row = ev.target.closest('.row');
    if (row) openSheet(row.dataset.id, row.dataset.sec);
  }
  function openSheet(id, sec, focus) {
    var f = findRow(id, sec); if (!f) return;
    S.openId = id; S.openSec = f.sec; S.focus = focus || '';
    $('#sheet-wrap').hidden = false; refreshSheet(true);
  }
  function closeSheet() { S.openId = null; $('#sheet-wrap').hidden = true; }
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeSheet(); });
  function refreshSheet(first) {
    var f = findRow(S.openId, S.openSec); if (!f) { closeSheet(); return; }
    if (!first && document.activeElement && $('#sheet').contains(document.activeElement) && /INPUT|TEXTAREA/.test(document.activeElement.tagName)) return;
    var r = f.r, sec = f.sec, now = Date.now(), role = S.data.role, d = S.data;
    var canSpot = role.edit.indexOf('spot') >= 0 && sec !== 'departed' && !r.ladd;
    var canAll = role.edit.indexOf('flags') >= 0 && !r.ladd;
    var c = r.conf || {};
    var h = '<div class="grab"></div><div class="sh-h"><div class="t"><h3>' + esc(r.ident) + (r.callsign ? ' <span style="font-size:15px;color:var(--tx2)">' + esc(r.callsign) + '</span>' : '') + '</h3>' +
      '<div class="sub">' + esc(r.type || '—') + (r.model ? ' · ' + esc(r.model) : '') + (r.op ? ' · ' + esc(r.op.name) + ' (' + esc(r.op.kind) + ', from callsign)' : '') + '</div></div>' +
      '<span class="chip st s-' + esc(r.status.code) + '" style="font-size:12px">' + esc(r.status.label) + '</span><button class="icon-btn" data-close aria-label="Close">✕</button></div>';
    h += '<div class="sh-sec"><div class="conf"><span class="bars l' + (c.level || 0) + '"><i></i><i></i><i></i></span><div><b>' + esc(c.src || '') + '</b> — ' + esc(c.note || '') +
      (c.updatedAgoSec != null ? ' Last update ' + ago(c.updatedAgoSec) + ' ago.' : '') + (r.status.note ? ' ' + esc(r.status.note) : '') + (r.stale ? ' Landed more than 12 h ago with no departure filed: treat as unverified.' : '') + '</div></div></div>';
    var kv = [];
    if (r.kind === 'arr') {
      kv.push(['From', esc(r.from || '—') + (r.city ? ' · ' + esc(r.city) : '') + (r.intl ? ' <span class="chip cust">International · customs</span>' : '')]);
      if (r.originDepISO) kv.push(['Departed origin (plan)', clk(r.originDepISO) + ' PT ' + dayTag(r.originDepISO)]);
      if (r.etaISO) kv.push([r.landedISO ? 'ETA was' : 'ETA', clk(r.etaISO) + ' PT ' + dayTag(r.etaISO) + (r.landedISO ? '' : ' · in ' + dur((t(r.etaISO) - now) / MIN))]);
      if (r.landedISO) kv.push(['Landed', clk(r.landedISO) + ' PT ' + dayTag(r.landedISO) + (r.landedAssumed ? ' (assumed from ETA)' : '') + ' · ' + dur((now - t(r.landedISO)) / MIN) + ' ago']);
      if (r.divert) kv.push(['Diverted', 'to ' + esc(r.divert.to) + ' · board hold until ' + clk(r.divert.holdUntilISO)]);
      kv.push(['Next departure', r.nextDepartISO ? clk(r.nextDepartISO) + ' PT ' + dayTag(r.nextDepartISO) + ' → ' + esc(r.nextDepartTo || '—') : 'none filed']);
      if (r.legNm) kv.push(['Inbound leg', num(r.legNm) + ' nm great-circle']);
    } else {
      kv.push(['To', esc(r.to || '—') + (r.city ? ' · ' + esc(r.city) : '') + (r.intl ? ' · international' : '')]);
      kv.push([r.offISO ? 'Wheels-off' : 'ETD', clk(r.etdISO || r.offISO) + ' PT ' + dayTag(r.etdISO || r.offISO) + (r.offISO ? ' · ' + dur((now - t(r.offISO)) / MIN) + ' ago' : (r.etdISO ? ((t(r.etdISO) - now) >= 0 ? ' · in ' + dur((t(r.etdISO) - now) / MIN) : ' · ' + dur((now - t(r.etdISO)) / MIN) + ' past') : ''))]);
      if (r.onBaseSinceISO) kv.push(['On base since', clk(r.onBaseSinceISO) + ' PT ' + dayTag(r.onBaseSinceISO)]);
      if (r.legNm) kv.push(['Leg', num(r.legNm) + ' nm great-circle']);
    }
    if (r.stayHrs != null) kv.push(['Ground time', dur(r.stayHrs * 60) + (r.qt ? ' · quick turn' : '')]);
    if (r.live) kv.push(['ADS-B', 'seen ' + ago(r.live.ageSec) + ' ago' + (r.live.alt != null ? (r.live.alt <= 0 ? ' · on ground' : ' · ' + num(r.live.alt) + ' ft') : '') + (r.live.gs != null ? ' · ' + r.live.gs + ' kt' : '') + (r.live.distNm != null ? ' · ' + r.live.distNm + ' nm out' : '') + (r.live.evidence ? ' · ' + esc(r.live.evidence) : '')]);
    if (r.vip && r.vip.vip) kv.push(['VIP', (r.vip.est ? '<span class="est">EST</span> ' : '') + esc(r.vip.reasons.join(' · ')) + (r.vip.est ? ' → prefers close parking' : '')]);
    if (r.fuel) kv.push(['Fuel est', num(r.fuel.gal) + ' gal ≈ ' + money(r.fuel.usd) + ' <span class="est">EST</span> <small style="color:var(--tx3)">' + esc(r.fuel.family) + ', ' + r.fuel.nm + ' nm' + (r.kind === 'arr' ? ' next leg' : '') + '</small>']);
    kv.push(['Spot', r.spot ? esc(r.spot) : '<span style="color:var(--tx3)">unassigned</span>' + (r.suggest ? ' · suggestion <b style="color:var(--violet)">' + esc(r.suggest.spot) + '</b> <small style="color:var(--tx3)">(' + esc(r.suggest.why) + ')</small>' : '')]);
    kv.push(['Pax', r.pax != null ? String(r.pax) : '—']);
    h += '<div class="sh-sec"><dl class="kv">' + kv.map(function (x) { return '<dt>' + x[0] + '</dt><dd>' + x[1] + '</dd>'; }).join('') + '</dl></div>';

    if ((canSpot || canAll) && !d.feed.rampEditEnabled) h += '<div class="sh-sec"><div class="notice">Ramp edits are switched off on the server (RAMP_EDIT_KEY not set), so changes can’t be saved yet. Everything below is view-only until that key is configured.</div></div>';
    if (canSpot) h += spotPicker(r, sec);
    if (canAll) h += dispatchEditor(r);
    $('#sheet').innerHTML = h;
    wireSheet(r, sec);
    if (first && S.focus === 'spot') { var p = $('#sh-spots'); if (p) p.scrollIntoView({ block: 'start' }); }
  }
  function interval(r) {
    var now = Date.now();
    var s = r.landedISO ? now : t(r.etaISO) || now;
    var e = t(r.nextDepartISO || r.etdISO); if (!(e > s)) e = s + 24 * HOUR;
    if (r.kind === 'dep') { s = now; e = Math.max(now + MIN, t(r.etdISO) || now + HOUR); }
    return [s, e];
  }
  function spotPicker(r, sec) {
    var iv = interval(r), lanes = {};
    (S.data.timeline ? S.data.timeline.lanes : []).forEach(function (l) { lanes[l.spot.toUpperCase()] = l; });
    var byArea = {}, order = [];
    S.data.spots.forEach(function (s) { if (!byArea[s.area]) { byArea[s.area] = []; order.push(s.area); } byArea[s.area].push(s); });
    var dis = !S.data.feed.rampEditEnabled ? ' disabled' : '';
    var h = '<div class="sh-sec" id="sh-spots"><h5>Spot · ' + esc(r.ident) + ' needs a ' + catName(r.cat) + ' spot ' + clk(isoOf(iv[0])) + '–' + (r.nextDepartISO || r.etdISO ? clk(isoOf(iv[1])) : 'open') + '</h5>';
    if (r.suggest && !r.spot) h += '<div class="btnrow" style="margin:0 0 8px"><button class="btn sug" data-setspot="' + esc(r.suggest.spot) + '"' + dis + '>Use suggestion: ' + esc(r.suggest.spot) + '</button><span style="font-size:12px;color:var(--tx3);align-self:center">' + esc(r.suggest.why) + ' <span class="est">EST</span></span></div>';
    order.forEach(function (a) {
      h += '<div class="area">' + esc(a) + '</div><div class="spots">';
      byArea[a].forEach(function (s) {
        var l = lanes[s.name.toUpperCase()]; var occ = null;
        if (l) l.items.forEach(function (it) { if (it.id !== r.id && it.kind !== 'suggested' && t(it.startISO) < iv[1] && iv[0] < t(it.endISO)) occ = occ || it; });
        var fit = s.cats.indexOf(r.cat) >= 0, cur = r.spot && r.spot.toUpperCase() === s.name.toUpperCase(), sug = r.suggest && r.suggest.spot === s.name;
        var cls = 'spotb' + (cur ? ' cur' : occ ? ' busy' : fit ? ' free' : ' nofit') + (sug ? ' sug' : '') + (s.restricted ? ' restr' : '');
        var sub = cur ? 'current' : occ ? esc(occ.ident) + ' ' + clk(occ.startISO) : !fit ? 'size' : s.restricted ? 'United appr.' : sug ? 'suggested' : 'free';
        h += '<button class="' + cls + '" data-setspot="' + esc(s.name) + '" title="' + esc(s.name + (s.restricted ? ' — requires United approval' : '') + (s.towOnly ? ' — tow only' : '')) + '"' + dis + '><b>' + esc(s.short) + '</b><small>' + sub + '</small></button>';
      });
      h += '</div>';
    });
    if (r.spot) h += '<div class="btnrow"><button class="btn" data-setspot=""' + dis + '>Clear spot</button></div>';
    return h + '</div>';
  }
  function catName(c) { return { 1: 'light', 2: 'light-jet', 3: 'midsize', 4: 'large', 5: 'heavy', 6: 'heavy+' }[c] || 'standard'; }
  function isoOf(m) { return new Date(m).toISOString(); }
  var FLAG_OPTS = ['FUEL', 'GPU', 'TOW', 'LAV', 'CATERING', 'CUSTOMS', 'VIP', 'MED', 'CREW HOLD', 'NO TOW', 'FUEL HOLD'];
  var TASKS = ['FUEL', 'GPU', 'TOW', 'LAV', 'CATERING', 'CUSTOMS'];
  function dispatchEditor(r) {
    var dis = !S.data.feed.rampEditEnabled ? ' disabled' : '';
    var have = {}; (r.flags || []).forEach(function (f) { var s = String(f).toUpperCase().trim(); have[s.replace(/\s*✓$/, '')] = /✓$/.test(s) ? 'done' : 'on'; });
    var h = '<div class="sh-sec"><h5>Pax</h5><div class="pax"><button class="btn" data-pax="-1"' + dis + '>−</button><input class="inp tn" id="pax" type="number" inputmode="numeric" min="0" max="99" value="' + (r.pax != null ? r.pax : '') + '" placeholder="—"' + dis + '><button class="btn" data-pax="1"' + dis + '>+</button></div></div>';
    h += '<div class="sh-sec"><h5>Flags &amp; task cues <small style="text-transform:none;letter-spacing:0;font-weight:600">(tap: off → needed → done ✓)</small></h5><div class="flags">';
    FLAG_OPTS.forEach(function (f) { var st = have[f]; h += '<button class="flagb ' + (st || '') + '" data-flag="' + esc(f) + '"' + dis + '>' + esc(f) + (st === 'done' ? ' ✓' : '') + '</button>'; });
    Object.keys(have).forEach(function (f) { if (FLAG_OPTS.indexOf(f) < 0) h += '<button class="flagb on" data-flag="' + esc(f) + '"' + dis + '>' + esc(f) + '</button>'; });
    h += '</div></div><div class="sh-sec"><h5>Tow / ramp notes</h5><textarea class="inp" id="tow" rows="2" maxlength="300" placeholder="e.g. tow to 3rd line after pax off"' + dis + '>' + esc(r.towNotes || '') + '</textarea>' +
      '<div class="btnrow"><button class="btn pri" id="save"' + dis + '>Save pax, flags &amp; notes</button></div></div>';
    return h;
  }
  function wireSheet(r, sec) {
    var sh = $('#sheet');
    sh.querySelectorAll('[data-close]').forEach(function (b) { b.onclick = closeSheet; });
    sh.querySelectorAll('[data-setspot]').forEach(function (b) {
      b.onclick = function () {
        var name = b.getAttribute('data-setspot'), prev = r.spot || '';
        saveRamp(r.id, { spot: name }, function () { toast(name ? r.ident + ' → ' + name : r.ident + ' spot cleared', function () { saveRamp(r.id, { spot: prev }); }); });
      };
    });
    var pax = $('#pax');
    sh.querySelectorAll('[data-pax]').forEach(function (b) { b.onclick = function () { var v = parseInt(pax.value, 10); v = isNaN(v) ? 0 : v; pax.value = Math.max(0, Math.min(99, v + parseInt(b.getAttribute('data-pax'), 10))); }; });
    var fl = {}; (r.flags || []).forEach(function (f) { var s = String(f).toUpperCase().trim(); fl[s.replace(/\s*✓$/, '')] = /✓$/.test(s) ? 'done' : 'on'; });
    sh.querySelectorAll('[data-flag]').forEach(function (b) {
      b.onclick = function () {
        var f = b.getAttribute('data-flag'), st = fl[f];
        var next = !st ? 'on' : (st === 'on' && TASKS.indexOf(f) >= 0) ? 'done' : '';
        if (next) fl[f] = next; else delete fl[f];
        b.className = 'flagb ' + next; b.textContent = f + (next === 'done' ? ' ✓' : '');
      };
    });
    var sv = $('#save');
    if (sv) sv.onclick = function () {
      var flags = Object.keys(fl).map(function (k) { return fl[k] === 'done' ? k + ' ✓' : k; });
      var pv = pax.value === '' ? null : Math.max(0, Math.min(99, parseInt(pax.value, 10) || 0));
      saveRamp(r.id, { pax: pv, flags: flags, towNotes: $('#tow').value }, function () { toast('Saved ' + r.ident); });
    };
  }
  function rampKey() { try { return localStorage.getItem(KEY_LS) || ''; } catch (e) { return S.keyMem || ''; } }
  function saveRamp(id, fields, ok, retried) {
    var role = S.data.role; if (!role.writeView) return;
    var k = rampKey();
    if (!k) { k = window.prompt('Ramp edit key (from your manager). Stored on this device only.'); if (!k) return; try { localStorage.setItem(KEY_LS, k); } catch (e) { S.keyMem = k; } }
    fetch('/api/' + role.writeView + '/ramp/' + encodeURIComponent(id), { method: 'PATCH', headers: { 'Content-Type': 'application/json', 'X-Skyway-Key': k }, body: JSON.stringify(fields) })
      .then(function (r) {
        if (r.status === 401 && !retried) { try { localStorage.removeItem(KEY_LS); } catch (e) { } toast('Edit key rejected — enter it again'); return saveRamp(id, fields, ok, true); }
        if (r.status === 503) { toast('Ramp edits are disabled on the server'); return; }
        if (!r.ok) return r.json().then(function (j) { toast('Not saved: ' + (j.error || r.status)); });
        return r.json().then(function (saved) {
          ['arrivals', 'onGround', 'departures', 'departed'].forEach(function (s) { (S.data.sections[s] || []).forEach(function (row) { if (row.id === id) { Object.keys(fields).forEach(function (f) { row[f] = saved[f] !== undefined ? saved[f] : fields[f]; }); } }); });
          render(); if (ok) ok(); loadSoon();
        });
      }).catch(function (e) { toast('Not saved: ' + e.message); });
  }
  var toastT = 0;
  function toast(msg, undo) {
    var el = $('#toast'); el.hidden = false; el.innerHTML = '<span>' + esc(msg) + '</span>' + (undo ? '<button id="undo">Undo</button>' : '');
    if (undo) $('#undo').onclick = function () { el.hidden = true; undo(); };
    clearTimeout(toastT); toastT = setTimeout(function () { el.hidden = true; }, 5000);
  }
  $('#sheet-wrap').addEventListener('click', function (e) { if (e.target.hasAttribute && e.target.hasAttribute('data-close') && e.target.classList.contains('sheet-bg')) closeSheet(); });

  // ---------- lobby (TV) ----------
  function lobbyShell() {
    return '<div class="lob"><div class="lob-h"><div class="b">Sky<i>way</i></div><div class="s">KSFO San Francisco Intl · General Aviation</div><div class="clk tn" id="lclk">--:--</div></div>' +
      '<div class="lob-b"><div class="lp arr"><div class="lp-h"><h2>Arrivals</h2><span class="pg" id="pg-a"></span></div><div class="lp-ch"><span>Aircraft</span><span>Type</span><span style="text-align:right">Time</span><span style="text-align:right">Status</span></div><div class="lp-rows" id="lr-a"></div></div>' +
      '<div class="lp dep"><div class="lp-h"><h2>Departures</h2><span class="pg" id="pg-d"></span></div><div class="lp-ch"><span>Aircraft</span><span>Type</span><span style="text-align:right">Time</span><span style="text-align:right">Status</span></div><div class="lp-rows" id="lr-d"></div></div></div>' +
      '<div class="lob-f"><span>Times shown in Pacific Time · estimated times may change</span><span id="lupd"></span></div></div>';
  }
  var LOB = { a: 0, d: 0 };
  function lobbyRemark(r, now) {
    var c = r.status.code;
    if (r.kind === 'arr') {
      if (c === 'landed' || c === 'onground' || c === 'landed_unconfirmed') return ['Landed', 'var(--green)'];
      if (c === 'final') return ['Arriving', 'var(--amber)'];
      if (c === 'due') return ['Due', 'var(--amber)'];
      if (c === 'enroute' || c === 'departed_origin') return ['En route', 'var(--acc)'];
      return ['Scheduled', 'var(--tx2)'];
    }
    if (c === 'off') return ['Departed', 'var(--tx3)'];
    if (c === 'taxi') return ['Taxiing', 'var(--amber)'];
    if (c === 'due') return ['Departing', 'var(--amber)'];
    if (c === 'late') return ['Pending', 'var(--tx2)'];
    return ['Scheduled', 'var(--tx2)'];
  }
  function lobbyRows(list, kind, now) {
    return list.map(function (r) {
      var iso = kind === 'a' ? (r.landedISO || r.etaISO) : (r.offISO || r.etdISO);
      var rm = lobbyRemark(r, now), m = shortModel(r.model);
      return '<div class="lr"><span class="t">' + esc(r.ident) + '</span><span class="ty"><b>' + esc(r.type || '—') + '</b><small>' + esc(m || '') + '</small></span><span class="tm tn">' + (iso ? clk(iso) : '—') + '</span><span class="rm" style="color:' + rm[1] + '">' + rm[0] + '</span></div>';
    });
  }
  function renderLobby() {
    var d = S.data, now = Date.now();
    var tk = function (iso) { return t(iso) || Infinity; };
    var arr = d.sections.arrivals.slice().sort(function (a, b) { return tk(a.landedISO || a.etaISO) - tk(b.landedISO || b.etaISO); });
    var dep = d.sections.departures.slice().sort(function (a, b) { return tk(a.offISO || a.etdISO) - tk(b.offISO || b.etdISO); });
    [['a', arr], ['d', dep]].forEach(function (x) {
      var box = $('#lr-' + x[0]); var rows = lobbyRows(x[1], x[0], now);
      var per = Math.max(1, Math.floor(box.clientHeight / (window.innerHeight * 0.086)));
      var pages = Math.max(1, Math.ceil(rows.length / per)); if (LOB[x[0]] >= pages) LOB[x[0]] = 0;
      var slice = rows.slice(LOB[x[0]] * per, LOB[x[0]] * per + per);
      setHTML(box, rows.length ? slice.join('') : '<div class="none">' + (x[0] === 'a' ? 'No arrivals scheduled' : 'No departures scheduled') + '</div>');
      $('#pg-' + x[0]).textContent = pages > 1 ? 'Page ' + (LOB[x[0]] + 1) + ' of ' + pages : '';
      LOB['n' + x[0]] = pages;
    });
    $('#lupd').textContent = 'Updated ' + fmtHM.format(S.lastOk) + ' PT';
  }
  setInterval(function () { if (ROLE !== 'lobby' || !S.data) return; ['a', 'd'].forEach(function (k) { if (LOB['n' + k] > 1) LOB[k] = (LOB[k] + 1) % LOB['n' + k]; }); renderLobby(); }, 12000);

  // ---------- clocks / ticks ----------
  function tickClock() {
    var el = $('#clock') || $('#lclk'); if (!el) return;
    var s = (ROLE === 'lobby' ? fmtHM : fmtHMS).format(Date.now());
    el.innerHTML = s + '<small>PT</small>';
    var u = $('#upd'); if (u && S.lastOk) u.textContent = ago(Math.round((Date.now() - S.lastOk) / 1000));
  }
  shell(); tickClock(); load(); ws();
  setInterval(tickClock, 1000);
  setInterval(function () { if (S.data && ROLE !== 'lobby') { render(); } }, 15000);
  setInterval(load, 30000);
  document.addEventListener('visibilitychange', function () { if (!document.hidden) load(); });
  window.addEventListener('resize', function () { if (S.data && ROLE !== 'lobby' && isDesk()) { renderChart(); renderGantt(); ensureMap(); } if (ROLE === 'lobby' && S.data) renderLobby(); });
  window.__skyway = S;
})();
