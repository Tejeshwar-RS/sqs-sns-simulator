/* =====================================================================
 * flow.js  -  Live message-flow diagram (the "enhancement" visual)
 * ---------------------------------------------------------------------
 * Draws  Producer -> SNS topics -> SQS queues -> Consumers  (+ dead-letter
 * queues) as an SVG, and animates a dot along the matching path whenever
 * the engine emits an event (published, delivered, filtered, received,
 * dead-lettered ...). Counters on every queue update live.
 * ===================================================================== */
(function (global) {
  'use strict';

  const NS = 'http://www.w3.org/2000/svg';
  const COLORS = { topic: '#a58bff', msg: '#ffb74d', ok: '#4fd18b', bad: '#ff7a66', blue: '#6aa3ff' };
  const X = { p: 24, t: 200, q: 470, c: 790 };
  const W = { p: 124, t: 176, q: 230, c: 132 };
  const H = { p: 64, t: 62, q: 80, c: 58 };
  const ROW = 108, PAD = 26, VBW = 960;

  const reduced = () => global.matchMedia && global.matchMedia('(prefers-reduced-motion: reduce)').matches;

  let svg, edgeLayer, nodeLayer, dotLayer, onSelect;
  let nodes = {}, edges = {}, signature = '', activeDots = 0;

  function el(name, attrs, parent) {
    const e = document.createElementNS(NS, name);
    Object.keys(attrs || {}).forEach(k => e.setAttribute(k, attrs[k]));
    if (parent) parent.appendChild(e);
    return e;
  }
  function text(parent, str, x, y, cls, anchor) {
    const t = el('text', { x, y, class: cls || '', 'text-anchor': anchor || 'start' }, parent);
    t.textContent = str;
    return t;
  }
  const curve = (x1, y1, x2, y2) => { const mx = (x1 + x2) / 2; return `M${x1} ${y1} C${mx} ${y1} ${mx} ${y2} ${x2} ${y2}`; };
  const trunc = (s, n) => s.length > n ? s.slice(0, n - 1) + '…' : s;

  /* A short string that changes whenever the diagram's shape must change. */
  function topologySignature() {
    const S = Sim.getState();
    return JSON.stringify([
      Object.values(S.topics).map(t => [t.name, t.subscriptions.map(s => [s.protocol, s.endpoint, s.filterPolicy ? 1 : 0])]),
      Object.values(S.queues).map(q => [q.name, q.type, q.redrive && q.redrive.dlqName])
    ]);
  }

  function build() {
    const S = Sim.getState();
    const topics = Object.values(S.topics);
    const queues = Object.values(S.queues);
    const dlqNames = new Set(queues.filter(q => q.redrive).map(q => q.redrive.dlqName));
    const ordered = [...queues.filter(q => !dlqNames.has(q.name)), ...queues.filter(q => dlqNames.has(q.name))];

    const rows = Math.max(topics.length, ordered.length, 1);
    const height = PAD * 2 + rows * ROW;
    svg.setAttribute('viewBox', `0 0 ${VBW} ${height}`);
    svg.innerHTML = '';
    nodes = { t: {}, q: {}, c: {}, p: null };
    edges = {};

    edgeLayer = el('g', {}, svg);
    nodeLayer = el('g', {}, svg);
    dotLayer = el('g', {}, svg);

    const centerY = i => (height - ordered.length * ROW) / 2 + i * ROW + ROW / 2;
    const topicY = i => (height - topics.length * ROW) / 2 + i * ROW + ROW / 2;
    const py = height / 2;

    /* column headings */
    [['Producer', X.p + W.p / 2], ['SNS topics', X.t + W.t / 2], ['SQS queues', X.q + W.q / 2], ['Consumers', X.c + W.c / 2]]
      .forEach(([label, x]) => text(svg, label, x, 16, 'col-title', 'middle'));

    /* ---- nodes ---- */
    const pg = el('g', { class: 'node n-producer' }, nodeLayer);
    el('rect', { x: X.p, y: py - H.p / 2, width: W.p, height: H.p, rx: 10 }, pg);
    text(pg, 'Your app', X.p + W.p / 2, py - 3, 'n-title', 'middle');
    text(pg, 'sends or publishes', X.p + W.p / 2, py + 14, 'n-sub', 'middle');
    nodes.p = pg;

    topics.forEach((t, i) => {
      const y = topicY(i);
      const g = el('g', { class: 'node n-topic clickable', tabindex: 0, role: 'link', 'aria-label': `Topic ${t.name}` }, nodeLayer);
      el('rect', { x: X.t, y: y - H.t / 2, width: W.t, height: H.t, rx: 10 }, g);
      text(g, trunc(t.name, 20), X.t + W.t / 2, y - 4, 'n-title', 'middle');
      const other = t.subscriptions.filter(s => s.protocol !== 'sqs').length;
      text(g, `${t.subscriptions.length} subscription${t.subscriptions.length === 1 ? '' : 's'}` + (other ? ` (${other} email/HTTP)` : ''), X.t + W.t / 2, y + 14, 'n-sub', 'middle');
      g.addEventListener('click', () => onSelect && onSelect('topic', t.name));
      g.addEventListener('keydown', e => { if (e.key === 'Enter') onSelect && onSelect('topic', t.name); });
      nodes.t[t.name] = { g, y, x: X.t };
    });

    ordered.forEach((q, i) => {
      const y = centerY(i);
      const isDlq = dlqNames.has(q.name);
      const g = el('g', { class: 'node n-queue clickable' + (isDlq ? ' is-dlq' : ''), tabindex: 0, role: 'link', 'aria-label': `Queue ${q.name}` }, nodeLayer);
      el('rect', { x: X.q, y: y - H.q / 2, width: W.q, height: H.q, rx: 10 }, g);
      text(g, trunc(q.name, 26), X.q + 14, y - 17, 'n-title', 'start');
      text(g, (q.type === 'fifo' ? 'FIFO' : 'Standard') + (isDlq ? ', dead-letter queue' : ''), X.q + 14, y - 2, 'n-sub', 'start');
      const line = text(g, '', X.q + 14, y + 22, 'n-count', 'start');
      const a = el('tspan', { class: 'c-av' }, line);
      const b = el('tspan', { class: 'c-in', dx: 12 }, line);
      const c = el('tspan', { class: 'c-de', dx: 12 }, line);
      g.addEventListener('click', () => onSelect && onSelect('queue', q.name));
      g.addEventListener('keydown', e => { if (e.key === 'Enter') onSelect && onSelect('queue', q.name); });
      nodes.q[q.name] = { g, y, a, b, c, q };

      const cg = el('g', { class: 'node n-consumer' }, nodeLayer);
      el('rect', { x: X.c, y: y - H.c / 2, width: W.c, height: H.c, rx: 10 }, cg);
      text(cg, 'Consumer', X.c + W.c / 2, y - 3, 'n-title', 'middle');
      const st = text(cg, 'idle', X.c + W.c / 2, y + 14, 'n-sub', 'middle');
      nodes.c[q.name] = { g: cg, y, status: st };
    });

    /* ---- edges ---- */
    topics.forEach(t => {
      edges['pt:' + t.name] = el('path', { d: curve(X.p + W.p, py, X.t, nodes.t[t.name].y), class: 'edge edge-topic' }, edgeLayer);
      t.subscriptions.filter(s => s.protocol === 'sqs' && nodes.q[s.endpoint]).forEach(s => {
        const y2 = nodes.q[s.endpoint].y, y1 = nodes.t[t.name].y;
        edges[`tq:${t.name}:${s.endpoint}`] = el('path', { d: curve(X.t + W.t, y1, X.q, y2), class: 'edge edge-sub' }, edgeLayer);
        if (s.filterPolicy) {
          const mx = (X.t + W.t + X.q) / 2, my = (y1 + y2) / 2;
          const pill = el('g', { class: 'filter-pill' }, edgeLayer);
          el('rect', { x: mx - 22, y: my - 9, width: 44, height: 18, rx: 9 }, pill);
          text(pill, 'filter', mx, my + 4, '', 'middle');
        }
      });
    });
    ordered.forEach(q => {
      const y = nodes.q[q.name].y;
      edges['pq:' + q.name] = el('path', { d: curve(X.p + W.p, py, X.q, y), class: 'edge edge-direct' }, edgeLayer);
      edges['qc:' + q.name] = el('path', { d: `M${X.q + W.q} ${y} L${X.c} ${y}`, class: 'edge edge-consume' }, edgeLayer);
      if (q.redrive && nodes.q[q.redrive.dlqName]) {
        const y2 = nodes.q[q.redrive.dlqName].y, xr = X.q + W.q;
        const d = `M${xr} ${y + 18} C${xr + 46} ${y + 18} ${xr + 46} ${y2 - 18} ${xr} ${y2 - 18}`;
        edges['qd:' + q.name] = el('path', { d, class: 'edge edge-dlq' }, edgeLayer);
        const mid = (y + y2) / 2, cx = xr + 36;
        const pill = el('g', { class: 'dlq-pill' }, edgeLayer);
        el('rect', { x: cx - 46, y: mid - 9, width: 92, height: 18, rx: 9 }, pill);
        text(pill, `after ${q.redrive.maxReceiveCount} receives`, cx, mid + 4, '', 'middle');
      }
    });

    if (!topics.length && !ordered.length) {
      text(svg, 'Create a queue or a topic, or load the sample setup, and messages will flow here.', VBW / 2, height / 2 + 80, 'empty-hint', 'middle');
    }
    signature = topologySignature();
    refresh();
  }

  /* Update the counters and consumer status without rebuilding. */
  function refresh() {
    if (!svg) return;
    if (topologySignature() !== signature) build();
    const S = Sim.getState();
    Object.keys(nodes.q).forEach(name => {
      const q = S.queues[name]; if (!q) return;
      const c = Sim.counts(q);
      const n = nodes.q[name];
      n.a.textContent = `${c.available} ready`;
      n.b.textContent = `${c.inflight} in flight`;
      n.c.textContent = `${c.delayed} delayed`;
      const cons = S.consumers[name];
      const label = !cons || !cons.enabled ? 'off' : cons.working.length ? 'working…' : 'polling';
      nodes.c[name].status.textContent = label;
      nodes.c[name].g.classList.toggle('on', !!(cons && cons.enabled));
    });
  }

  /* ---------- animation helpers ---------- */
  function flash(node, kind) {
    if (!node) return;
    const g = node.g || node;
    const cls = kind === 'bad' ? 'flash-bad' : 'flash-ok';
    g.classList.remove('flash-ok', 'flash-bad');
    void g.getBoundingClientRect();
    g.classList.add(cls);
    setTimeout(() => g.classList.remove(cls), 800);
  }

  function dot(path, o) {
    o = o || {};
    if (!path || reduced() || !path.getTotalLength || activeDots > 30) return;
    const len = path.getTotalLength();
    const to = o.to === undefined ? 1 : o.to;
    const dur = o.dur || 800;
    const c = el('circle', { r: 6.5, fill: o.color || COLORS.msg, class: 'dot' }, dotLayer);
    c.style.opacity = 0;
    activeDots++;
    const start = performance.now() + (o.delay || 0);
    (function frame(t) {
      if (t < start) return requestAnimationFrame(frame);
      const k = Math.min(1, (t - start) / dur);
      const e = k < .5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;        // ease in-out
      const pos = (o.reverse ? 1 - to * e : to * e) * len;
      const p = path.getPointAtLength(pos);
      c.setAttribute('cx', p.x); c.setAttribute('cy', p.y);
      c.style.opacity = 1;
      if (k < 1) return requestAnimationFrame(frame);
      if (o.fail) {                                                           // "blocked" marker
        c.setAttribute('fill', COLORS.bad);
        const x = el('text', { x: p.x, y: p.y + 5, class: 'dot-x', 'text-anchor': 'middle' }, dotLayer);
        x.textContent = '✕';
        setTimeout(() => x.remove(), 900);
      }
      setTimeout(() => { c.remove(); activeDots--; if (o.done) o.done(); }, o.fail ? 250 : 0);
    })(performance.now());
  }

  /* React to engine events. */
  function handle(e) {
    if (!svg) return;
    if (e.type === 'queue-created' || e.type === 'queue-deleted' || e.type === 'topic-created' || e.type === 'topic-deleted' ||
        e.type === 'subscribed' || e.type === 'unsubscribed' || e.type === 'queue-updated') { refresh(); return; }
    const q = e.queue && nodes.q[e.queue];
    switch (e.type) {
      case 'published':
        dot(edges['pt:' + e.topic], { color: COLORS.topic });
        flash(nodes.t[e.topic]);
        break;
      case 'delivered':
        if (e.protocol === 'sqs') {
          dot(edges[`tq:${e.topic}:${e.queue}`], { color: COLORS.msg, delay: 700 });
          setTimeout(() => flash(nodes.q[e.queue]), 1500);
        }
        break;
      case 'filtered':
        if (e.protocol === 'sqs') dot(edges[`tq:${e.topic}:${e.endpoint}`], { color: COLORS.bad, delay: 700, to: .5, fail: true });
        break;
      case 'sent':
        if (!e.via) { dot(edges['pq:' + e.queue], { color: COLORS.msg }); setTimeout(() => flash(q), 800); }
        break;
      case 'received':
        dot(edges['qc:' + e.queue], { color: COLORS.msg, dur: 500 });
        break;
      case 'deleted':
        flash(nodes.c[e.queue], 'ok');
        break;
      case 'consumer-failed':
        flash(nodes.c[e.queue], 'bad');
        break;
      case 'visibility-expired':
        dot(edges['qc:' + e.queue], { color: COLORS.blue, reverse: true, dur: 500 });
        break;
      case 'dead-lettered':
        dot(edges['qd:' + e.queue], { color: COLORS.bad, dur: 900 });
        setTimeout(() => flash(nodes.q[e.dlq], 'bad'), 900);
        break;
      case 'redriven':
        dot(edges['qd:' + e.queue], { color: COLORS.ok, reverse: true, dur: 900 });
        break;
      default:
    }
  }

  global.Flow = {
    mount(svgEl, selectCb) {
      svg = svgEl; onSelect = selectCb; signature = '';
      build();
    },
    unmount() { svg = null; },
    refresh, handle,
    rebuild() { if (svg) build(); }
  };
})(window);
