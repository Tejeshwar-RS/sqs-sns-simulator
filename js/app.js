/* =====================================================================
 * app.js  -  The user interface
 * ---------------------------------------------------------------------
 * Hash-routed single page app. Each "view" returns { html, after, refresh }:
 *   html     the markup for the page
 *   after    runs once after the markup is inserted (mount diagrams etc.)
 *   refresh  runs every 250 ms to update live numbers without touching forms
 * All AWS behaviour comes from engine.js (the global `Sim`).
 * ===================================================================== */
(function () {
  'use strict';

  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
  const esc = s => String(s === undefined || s === null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const STORAGE_KEY = 'sqs-sns-simulator-v1';

  const ui = { speed: 1, paused: false, received: {}, lastPublish: {}, logFilter: 'all', view: null, demo: -1 };

  /* ---------- formatting ---------- */
  const pad = n => String(n).padStart(2, '0');
  const fmtTime = ms => { const d = new Date(ms); return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`; };
  const fmtClock = ms => { const d = new Date(ms); return `${d.getDate()} ${d.toLocaleString('en-GB', { month: 'short' })} ${fmtTime(ms)}`; };
  function fmtDur(sec) {
    sec = Math.max(0, Math.ceil(sec));
    if (sec < 60) return sec + 's';
    if (sec < 3600) return Math.floor(sec / 60) + 'm ' + (sec % 60) + 's';
    if (sec < 86400) return Math.floor(sec / 3600) + 'h ' + Math.floor(sec % 3600 / 60) + 'm';
    return Math.floor(sec / 86400) + 'd ' + Math.floor(sec % 86400 / 3600) + 'h';
  }
  const chip = (t, cls) => `<span class="chip ${cls || ''}">${esc(t)}</span>`;
  const typeChip = t => t === 'fifo' ? chip('FIFO', 'chip-fifo') : chip('Standard', 'chip-std');
  const stateChip = s => chip(s === 'inflight' ? 'In flight' : s[0].toUpperCase() + s.slice(1), 'st-' + s);
  const protoChip = p => chip(p === 'sqs' ? 'SQS' : p === 'http' ? 'HTTP' : 'Email', 'chip-' + p);
  const link = (kind, name) => `<a href="#/${kind}/${encodeURIComponent(name)}">${esc(name)}</a>`;
  const snippet = (s, n) => esc(s.length > n ? s.slice(0, n) + '…' : s).replace(/\n/g, ' ');

  function setHTML(el, html) {
    if (el && el.__html !== html) { el.innerHTML = html; el.__html = html; }
  }

  /* ---------- toasts + modal ---------- */
  function toast(msg, kind) {
    const t = document.createElement('div');
    t.className = 'toast ' + (kind || 'info');
    t.textContent = msg;
    $('#toasts').appendChild(t);
    setTimeout(() => t.remove(), kind === 'err' ? 7000 : 4000);
  }
  function openModal(title, body) {
    $('#modal-root').innerHTML = `<div class="modal-backdrop" data-act="close-modal"><div class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
      <div class="modal-head"><h2 id="modal-title">${esc(title)}</h2><button class="btn btn-ghost btn-sm" data-act="close-modal" aria-label="Close">Close</button></div>${body}</div></div>`;
    const first = $('#modal-root input:not([type=radio]), #modal-root select, #modal-root textarea');
    if (first) first.focus();
  }
  const closeModal = () => { $('#modal-root').innerHTML = ''; };

  function guard(fn) {
    try { fn(); } catch (err) {
      if (err && err.name === 'AwsError') toast(`${err.code}: ${err.message}`, 'err');
      else { console.error(err); toast('Something went wrong: ' + err.message, 'err'); }
    }
  }

  /* ---------- navigation ---------- */
  function go(hash) {
    if (location.hash === hash) render(); else location.hash = hash;
  }
  function parseRoute() {
    const parts = location.hash.replace(/^#\/?/, '').split('/');
    return { view: parts[0] || 'flow', id: decodeURIComponent(parts.slice(1).join('/') || '') };
  }

  /* =====================================================================
   *  Shared pieces
   * ===================================================================== */
  const emptyState = (title, text, buttons) => `<div class="empty"><strong>${esc(title)}</strong><p>${text}</p><div class="row">${buttons || ''}</div></div>`;
  const notFound = (kind, name, back) => ({ html: `<div class="page-head"><h1>${kind} not found</h1></div>${emptyState(`"${esc(name)}" doesn't exist`, 'It may have been deleted.', `<a class="btn" href="${back}">Back to the list</a>`)}` });

  function attrFields(hintExtra) {
    return `<label class="field"><span>Message attributes (optional)</span>
      <textarea name="attributes" rows="2" placeholder="type=physical&#10;amount=499"></textarea>
      <small class="hint">One per line as name=value. Numbers are detected automatically. ${hintExtra || ''}</small></label>`;
  }

  /** Fields for "send a message to a queue". `q` is the queue when known; null shows every option. */
  function sendFields(q) {
    const fifo = !q || q.type === 'fifo', std = !q || q.type === 'standard';
    const delay = std ? `<label class="field"><span>Delivery delay in seconds (optional)</span><input name="delaySeconds" type="number" min="0" max="900" placeholder="${q ? q.delaySeconds : 0}"></label>` : '';
    const fifoBox = fifo ? `<div class="two">
        <label class="field"><span>Message group ID</span><input name="groupId" value="${q ? 'group-1' : ''}" placeholder="group-1"></label>
        <label class="field"><span>Deduplication ID</span><input name="dedupId" placeholder="${q && q.contentBasedDedup ? 'optional (content-based is on)' : 'order-1001'}"></label></div>` : '';
    const inner = delay + fifoBox;
    return `<label class="field"><span>Message body</span><textarea name="body" rows="3" required placeholder='{"orderId": 1001, "item": "book"}'></textarea></label>
      ${attrFields()}
      ${q ? inner : `<details class="more"><summary>Delay and FIFO options</summary>${inner}</details>`}`;
  }

  function chartSVG(hist) {
    if (hist.length < 2) return '<p class="muted small">The chart fills in as simulated time passes. Try the speed buttons at the top.</p>';
    const W = 640, H = 190, L = 34, B = 26, T = 10, R = 10;
    const maxY = Math.max(1, ...hist.map(h => Math.max(h.available, h.inflight, h.delayed)));
    const t0 = hist[0].t, span = Math.max(1, hist[hist.length - 1].t - t0);
    const x = t => L + (t - t0) / span * (W - L - R);
    const y = v => T + (1 - v / maxY) * (H - T - B);
    const line = (k, c) => `<polyline class="ln ${c}" points="${hist.map(h => x(h.t).toFixed(1) + ',' + y(h[k]).toFixed(1)).join(' ')}"/>`;
    return `<svg viewBox="0 0 ${W} ${H}" class="chart" role="img" aria-label="Messages in the queue over simulated time">
      <line class="grid" x1="${L}" x2="${W - R}" y1="${y(0)}" y2="${y(0)}"/><line class="grid" x1="${L}" x2="${W - R}" y1="${y(maxY)}" y2="${y(maxY)}"/>
      <text class="axis" x="${L - 6}" y="${y(0) + 4}" text-anchor="end">0</text><text class="axis" x="${L - 6}" y="${y(maxY) + 4}" text-anchor="end">${maxY}</text>
      <text class="axis" x="${L}" y="${H - 6}">${fmtTime(t0)}</text><text class="axis" x="${W - R}" y="${H - 6}" text-anchor="end">${fmtTime(hist[hist.length - 1].t)}</text>
      ${line('available', 'ln-av')}${line('inflight', 'ln-in')}${line('delayed', 'ln-de')}</svg>
      <div class="legend-row"><span><i class="sw sw-av"></i>Available</span><span><i class="sw sw-in"></i>In flight</span><span><i class="sw sw-de"></i>Delayed</span></div>`;
  }

  /* =====================================================================
   *  View: message flow (home)
   * ===================================================================== */
  function flowView() {
    const S = Sim.getState();
    const queues = Object.keys(S.queues), topics = Object.keys(S.topics);
    const empty = !queues.length && !topics.length;
    const topicOpts = topics.map(t => `<option>${esc(t)}</option>`).join('');
    const queueOpts = queues.map(q => `<option>${esc(q)}</option>`).join('');
    return {
      html: `
      <div class="page-head"><div><h1>Message flow</h1>
        <p class="lede">Messages move from a producer through SNS topics into SQS queues, and out to consumers. Click a topic or queue to open it.</p></div>
        <div class="head-actions"><button class="btn btn-primary" data-act="demo-start">Start guided demo</button><button class="btn" data-act="load-sample">Load sample setup</button></div></div>
      <section class="flow-panel" aria-label="Live message flow">
        <svg id="flow-svg" role="img" aria-label="Diagram of producer, topics, queues and consumers"></svg>
        <div class="legend-row on-dark">
          <span><i class="sw" style="background:#a58bff"></i>Published to topic</span><span><i class="sw" style="background:#ffb74d"></i>Message moving</span>
          <span><i class="sw" style="background:#ff7a66"></i>Filtered out or dead-lettered</span><span><i class="sw" style="background:#6aa3ff"></i>Back in queue after timeout</span><span><i class="sw" style="background:#4fd18b"></i>Redriven</span>
        </div>
      </section>
      ${empty ? emptyState('Nothing to simulate yet', 'Create a queue and a topic, or load the sample setup (an <em>orders</em> topic feeding billing and shipping queues).', '<button class="btn btn-primary" data-act="load-sample">Load sample setup</button><button class="btn" data-act="new-queue">Create a queue</button><button class="btn" data-act="new-topic">Create a topic</button>') : ''}
      <div class="grid-2">
        <section class="card"><h2>Publish to an SNS topic</h2>
          <p class="muted small">Every subscriber whose filter policy matches gets its own copy (fan-out).</p>
          <form data-form="publish" class="stack">
            <label class="field"><span>Topic</span><select name="topic" ${topics.length ? '' : 'disabled'}>${topicOpts || '<option value="">No topics yet</option>'}</select></label>
            <label class="field"><span>Subject (optional)</span><input name="subject" placeholder="New order"></label>
            <label class="field"><span>Message</span><textarea name="message" rows="2" required placeholder="Order #1001: 2 books"></textarea></label>
            ${attrFields('Filter policies match on these.')}
            <button class="btn btn-primary" ${topics.length ? '' : 'disabled'}>Publish message</button>
          </form></section>
        <section class="card"><h2>Send straight to an SQS queue</h2>
          <p class="muted small">Skips SNS. Only that one queue receives the message.</p>
          <form data-form="send-message" class="stack">
            <label class="field"><span>Queue</span><select name="queue" ${queues.length ? '' : 'disabled'}>${queueOpts || '<option value="">No queues yet</option>'}</select></label>
            ${sendFields(null)}
            <button class="btn btn-primary" ${queues.length ? '' : 'disabled'}>Send message</button>
          </form></section>
      </div>`,
      after() { Flow.mount($('#flow-svg'), (kind, name) => go(`#/${kind === 'queue' ? 'queues' : 'topics'}/${encodeURIComponent(name)}`)); },
      refresh() {}
    };
  }

  /* =====================================================================
   *  View: queues list
   * ===================================================================== */
  function queuesRows() {
    const S = Sim.getState();
    const list = Object.values(S.queues);
    if (!list.length) return `<tr><td colspan="8">${emptyState('No queues yet', 'Create a queue to start sending messages.', '<button class="btn btn-primary" data-act="new-queue">Create queue</button><button class="btn" data-act="load-sample">Load sample setup</button>')}</td></tr>`;
    return list.map(q => {
      const c = Sim.counts(q), cons = S.consumers[q.name];
      return `<tr><td class="strong">${link('queues', q.name)}</td><td>${typeChip(q.type)}</td><td class="num">${c.available}</td><td class="num">${c.inflight}</td><td class="num">${c.delayed}</td>
        <td>${q.redrive ? `${link('queues', q.redrive.dlqName)} <span class="muted small">after ${q.redrive.maxReceiveCount} receives</span>` : '<span class="muted">None</span>'}</td>
        <td>${cons && cons.enabled ? chip('Running', 'chip-ok') : '<span class="muted">Off</span>'}</td>
        <td class="right"><button class="btn btn-sm btn-ghost" data-act="delete-queue" data-queue="${esc(q.name)}">Delete</button></td></tr>`;
    }).join('');
  }
  function queuesView() {
    return {
      html: `<div class="page-head"><div><h1>Queues</h1><p class="lede">SQS queues hold messages until a consumer processes and deletes them.</p></div>
        <div class="head-actions"><button class="btn btn-primary" data-act="new-queue">Create queue</button></div></div>
        <section class="card flush"><div class="table-wrap"><table class="table"><thead><tr><th>Name</th><th>Type</th><th class="num">Available</th><th class="num">In flight</th><th class="num">Delayed</th><th>Dead-letter queue</th><th>Consumer</th><th></th></tr></thead>
        <tbody id="queues-body"></tbody></table></div></section>`,
      refresh() { setHTML($('#queues-body'), queuesRows()); }
    };
  }

  /* =====================================================================
   *  View: one queue
   * ===================================================================== */
  function queueView(name) {
    const q0 = Sim.getState().queues[name];
    if (!q0) return notFound('Queue', name, '#/queues');
    const url = Sim.queueUrl(name);
    const qa = esc(name);
    const sources = Sim.dlqSources(name);
    const cons0 = Sim.getConsumer(name);
    const days = q0.retention / 86400;

    return {
      html: `
      <nav class="crumbs"><a href="#/queues">Queues</a> / <span>${qa}</span></nav>
      <div class="page-head"><div><h1>${qa} ${typeChip(q0.type)} ${sources.length ? chip('Dead-letter queue', 'chip-dlq') : ''}</h1>
        <p class="mono small muted">${esc(url)} <button class="link" data-act="copy" data-text="${esc(url)}">Copy URL</button></p></div>
        <div class="head-actions"><button class="btn" data-act="edit-queue" data-queue="${qa}">Edit settings</button><button class="btn" data-act="purge-queue" data-queue="${qa}">Purge</button><button class="btn btn-danger" data-act="delete-queue" data-queue="${qa}" data-back="1">Delete</button></div></div>
      <div id="q-tiles" class="tiles"></div>

      <div class="grid-2">
        <section class="card"><h2>Send message</h2>
          <form data-form="send-message" class="stack"><input type="hidden" name="queue" value="${qa}">${sendFields(q0)}<button class="btn btn-primary">Send message</button></form></section>
        <section class="card"><h2>Receive messages</h2>
          <p class="muted small">Polling hides each message from other consumers for the visibility timeout. Delete it once processed, or it comes back.</p>
          <form data-form="poll" class="stack"><input type="hidden" name="queue" value="${qa}">
            <div class="two"><label class="field"><span>Maximum messages</span><input name="max" type="number" min="1" max="10" value="1"></label>
            <label class="field"><span>Visibility timeout (s)</span><input name="visibilityTimeout" type="number" min="0" placeholder="${q0.visibilityTimeout}"></label></div>
            <button class="btn btn-primary">Poll for messages</button></form>
          <div id="q-received" class="received"></div></section>
      </div>

      <section class="card"><h2>Messages in this queue</h2>
        <p class="muted small">Simulator-only inspector. Real SQS cannot list messages without receiving them.</p>
        <div id="q-messages" class="table-wrap"></div></section>

      <div class="grid-2">
        <section class="card"><h2>Auto-consumer <span id="q-cons-status"></span></h2>
          <p class="muted small">A fake worker app that polls this queue. Make it fail to watch retries and the dead-letter queue in action.</p>
          <form data-form="consumer" class="stack"><input type="hidden" name="queue" value="${qa}">
            <div class="three">
              <label class="field"><span>Poll every (s)</span><input name="pollEvery" type="number" min="1" max="60" value="${cons0.pollEvery}"></label>
              <label class="field"><span>Processing (s)</span><input name="processTime" type="number" min="0" max="600" value="${cons0.processTime}"></label>
              <label class="field"><span>Random failure (%)</span><input name="failRate" type="number" min="0" max="100" value="${cons0.failRate}"></label></div>
            <label class="check"><input type="checkbox" name="poisonFails" ${cons0.poisonFails ? 'checked' : ''}> Messages containing the word “poison” always fail</label>
            <div class="row"><button class="btn btn-primary" name="op" value="start">Start consumer</button><button class="btn" name="op" value="stop">Stop consumer</button></div>
          </form></section>
        <section class="card"><h2>Settings</h2>
          <dl class="kv">
            <dt>Visibility timeout</dt><dd>${fmtDur(q0.visibilityTimeout)}</dd>
            <dt>Delivery delay</dt><dd>${fmtDur(q0.delaySeconds)}</dd>
            <dt>Message retention</dt><dd>${fmtDur(q0.retention)} ${days >= 1 ? `<span class="muted small">(${+days.toFixed(1)} days)</span>` : ''}</dd>
            ${q0.type === 'fifo' ? `<dt>Content-based deduplication</dt><dd>${q0.contentBasedDedup ? 'On' : 'Off'}</dd>` : ''}
            <dt>Dead-letter queue</dt><dd>${q0.redrive ? `${link('queues', q0.redrive.dlqName)}, after ${q0.redrive.maxReceiveCount} receives` : 'None'}</dd>
            <dt>ARN</dt><dd class="mono small">${esc(Sim.queueArn(name))}</dd>
          </dl>
          <div id="q-dlq"></div></section>
      </div>

      <section class="card"><h2>Monitoring</h2><div id="q-chart"></div></section>`,

      refresh() {
        const S = Sim.getState();
        const q = S.queues[name];
        if (!q) { go('#/queues'); return; }
        const c = Sim.counts(q);

        setHTML($('#q-tiles'), `
          <div class="tile t-av"><span class="n">${c.available}</span><span class="l">Available</span></div>
          <div class="tile t-in"><span class="n">${c.inflight}</span><span class="l">In flight</span></div>
          <div class="tile t-de"><span class="n">${c.delayed}</span><span class="l">Delayed</span></div>
          <div class="tile t-dl"><span class="n">${q.stats.deadLettered}</span><span class="l">Moved to dead-letter queue</span></div>
          <p class="totals">Totals: ${q.stats.sent} sent, ${q.stats.received} received, ${q.stats.deleted} deleted</p>`);

        /* messages you received and are holding */
        const held = (ui.received[name] || []).filter(r => q.messages.some(m => m.id === r.id && m.state === 'inflight' && m.receiptHandle === r.handle));
        ui.received[name] = held;
        setHTML($('#q-received'), held.length ? held.map(r => {
          const m = q.messages.find(x => x.id === r.id);
          return `<div class="rcv"><div class="rcv-top"><span class="mono small">${esc(m.id.slice(0, 8))}</span>${chip('Receive #' + m.receiveCount)}<span class="small muted">returns in <span class="cd" data-until="${m.availableAt}"></span></span></div>
            <pre class="body">${esc(m.body)}</pre>
            <div class="row"><button class="btn btn-sm btn-primary" data-act="delete-received" data-queue="${esc(name)}" data-handle="${esc(m.receiptHandle)}">Delete (processed)</button>
            <button class="btn btn-sm" data-act="release" data-queue="${esc(name)}" data-handle="${esc(m.receiptHandle)}">Return to queue now</button></div></div>`;
        }).join('') : '<p class="muted small">Messages you receive appear here.</p>');

        /* message table */
        const msgs = q.messages.slice().sort((a, b) => a.seq - b.seq);
        setHTML($('#q-messages'), msgs.length ? `<table class="table"><thead><tr><th>State</th><th>Body</th><th class="num">Receives</th>${q.type === 'fifo' ? '<th>Group</th>' : ''}<th>Timing</th><th>Origin</th></tr></thead><tbody>${msgs.map(m => {
          const timing = m.state === 'delayed' ? `visible in <span class="cd" data-until="${m.availableAt}"></span>` : m.state === 'inflight' ? `returns in <span class="cd" data-until="${m.availableAt}"></span>` : '<span class="muted">ready to receive</span>';
          const origin = m.deadLetterSource ? `dead-lettered from ${esc(m.deadLetterSource)}` : m.source === 'direct' ? 'sent directly' : 'via SNS topic ' + esc(m.source.replace('sns:', ''));
          return `<tr><td>${stateChip(m.state)}</td><td class="mono small"><details><summary>${snippet(m.body, 60)}</summary><pre class="body">${esc(m.body)}</pre>${Object.keys(m.attributes).length ? `<div class="small muted">Attributes: ${esc(Object.entries(m.attributes).map(([k, v]) => `${k}=${v.value}`).join(', '))}</div>` : ''}</details></td><td class="num">${m.receiveCount}</td>${q.type === 'fifo' ? `<td class="mono small">${esc(m.groupId)}</td>` : ''}<td class="small">${timing}</td><td class="small muted">${origin}</td></tr>`;
        }).join('')}</tbody></table>` : emptyState('This queue is empty', 'Send a message with the form above, or publish to a topic that feeds this queue.'));

        /* consumer status */
        const cons = S.consumers[name];
        setHTML($('#q-cons-status'), cons && cons.enabled ? chip(cons.working.length ? 'Running, processing 1' : 'Running, polling', 'chip-ok') : chip('Stopped', ''));

        /* dead-letter queue block */
        const srcs = Sim.dlqSources(name);
        const waiting = q.messages.filter(m => m.deadLetterSource && m.state === 'available').length;
        setHTML($('#q-dlq'), srcs.length ? `<div class="callout"><strong>This is a dead-letter queue</strong><p class="small">It collects messages that failed in ${srcs.map(s => link('queues', s.name)).join(', ')}. ${waiting} waiting to be redriven.</p>
          <button class="btn btn-primary btn-sm" data-act="redrive" data-queue="${esc(name)}" ${waiting ? '' : 'disabled'}>Redrive to source queues</button></div>` : '');

        setHTML($('#q-chart'), chartSVG(q.history));
      }
    };
  }

  /* =====================================================================
   *  View: topics list + one topic
   * ===================================================================== */
  function topicsRows() {
    const list = Object.values(Sim.getState().topics);
    if (!list.length) return `<tr><td colspan="6">${emptyState('No topics yet', 'Create a topic, then subscribe queues to it.', '<button class="btn btn-primary" data-act="new-topic">Create topic</button><button class="btn" data-act="load-sample">Load sample setup</button>')}</td></tr>`;
    return list.map(t => `<tr><td class="strong">${link('topics', t.name)}</td><td class="num">${t.subscriptions.length}</td><td class="num">${t.stats.published}</td><td class="num">${t.stats.delivered}</td><td class="num">${t.stats.filtered}</td>
      <td class="right"><button class="btn btn-sm btn-ghost" data-act="delete-topic" data-topic="${esc(t.name)}">Delete</button></td></tr>`).join('');
  }
  function topicsView() {
    return {
      html: `<div class="page-head"><div><h1>Topics</h1><p class="lede">An SNS topic pushes each published message to all of its subscribers.</p></div>
        <div class="head-actions"><button class="btn btn-primary" data-act="new-topic">Create topic</button></div></div>
        <section class="card flush"><div class="table-wrap"><table class="table"><thead><tr><th>Name</th><th class="num">Subscriptions</th><th class="num">Published</th><th class="num">Delivered</th><th class="num">Filtered out</th><th></th></tr></thead><tbody id="topics-body"></tbody></table></div></section>`,
      refresh() { setHTML($('#topics-body'), topicsRows()); }
    };
  }

  function publishResultHTML(topic) {
    const r = ui.lastPublish[topic];
    if (!r) return '<p class="muted small">Publish a message to see which subscribers received it, and why the others did not.</p>';
    return `<p class="small muted">Message <span class="mono">${esc(r.messageId.slice(0, 8))}</span> published at ${fmtTime(r.at)}</p>
      <table class="table"><thead><tr><th>Subscriber</th><th>Result</th><th>Why</th></tr></thead><tbody>${r.results.map(x => `<tr><td>${protoChip(x.protocol)} <span class="mono small">${esc(x.endpoint)}</span></td><td>${x.delivered ? chip('Delivered', 'chip-ok') : chip('Not delivered', 'chip-bad')}</td><td class="small">${esc(x.reason)}</td></tr>`).join('') || '<tr><td colspan="3" class="muted">No subscriptions on this topic yet.</td></tr>'}</tbody></table>`;
  }

  function topicView(name) {
    const t0 = Sim.getState().topics[name];
    if (!t0) return notFound('Topic', name, '#/topics');
    const na = esc(name);
    const stdQueues = Object.values(Sim.getState().queues).filter(q => q.type === 'standard');
    const examples = [['type is physical', '{"type": ["physical"]}'], ['amount of 1000 or more', '{"amount": [{"numeric": [">=", 1000]}]}'], ['region starts with ap-', '{"region": [{"prefix": "ap-"}]}'], ['not a test', '{"env": [{"anything-but": ["test"]}]}']];
    return {
      html: `
      <nav class="crumbs"><a href="#/topics">Topics</a> / <span>${na}</span></nav>
      <div class="page-head"><div><h1>${na}</h1><p class="mono small muted">${esc(Sim.topicArn(name))}</p></div>
        <div class="head-actions"><button class="btn btn-danger" data-act="delete-topic" data-topic="${na}" data-back="1">Delete topic</button></div></div>
      <div id="t-tiles" class="tiles"></div>
      <section class="card flush"><div class="card-pad"><h2>Subscriptions</h2></div><div id="t-subs" class="table-wrap"></div></section>
      <div class="grid-2">
        <section class="card"><h2>Add a subscription</h2>
          <form data-form="subscribe" class="stack"><input type="hidden" name="topic" value="${na}">
            <label class="field"><span>Protocol</span><select name="protocol" data-protocol><option value="sqs">Amazon SQS queue</option><option value="email">Email (simulated)</option><option value="http">HTTP endpoint (simulated)</option></select></label>
            <label class="field" data-ep="sqs"><span>Queue</span><select name="endpointSqs">${stdQueues.map(q => `<option>${esc(q.name)}</option>`).join('') || '<option value="">No Standard queues yet</option>'}</select></label>
            <label class="field" data-ep="email" hidden><span>Email address</span><input name="endpointEmail" placeholder="ops@example.com"></label>
            <label class="field" data-ep="http" hidden><span>Endpoint URL</span><input name="endpointHttp" placeholder="https://example.com/webhook"></label>
            <label class="field"><span>Filter policy (optional)</span><textarea name="filterPolicy" rows="3" class="mono" placeholder='{"type": ["physical"]}'></textarea>
              <small class="hint">Only messages whose attributes match are delivered. Try an example:</small></label>
            <div class="row wrap">${examples.map(([l, j]) => `<button type="button" class="btn btn-sm" data-act="filter-example" data-json="${esc(j)}">${esc(l)}</button>`).join('')}</div>
            <label class="check" data-ep="sqs"><input type="checkbox" name="raw"> Raw message delivery (send only the message, without the SNS JSON wrapper)</label>
            <button class="btn btn-primary">Subscribe</button></form></section>
        <section class="card"><h2>Publish a message</h2>
          <form data-form="publish" class="stack"><input type="hidden" name="topic" value="${na}">
            <label class="field"><span>Subject (optional)</span><input name="subject" placeholder="New order"></label>
            <label class="field"><span>Message</span><textarea name="message" rows="3" required placeholder="Order #1001: 2 books"></textarea></label>
            ${attrFields('Filter policies match on these.')}
            <button class="btn btn-primary">Publish message</button></form></section>
      </div>
      <section class="card"><h2>Delivery result</h2><div id="t-result"></div></section>
      <section class="card"><h2>Simulated email and HTTP inbox</h2><p class="muted small">What the non-SQS subscribers received.</p><div id="t-inbox"></div></section>`,
      after() { syncProtocol(); },
      refresh() {
        const S = Sim.getState();
        const t = S.topics[name];
        if (!t) { go('#/topics'); return; }
        setHTML($('#t-tiles'), `<div class="tile t-sns"><span class="n">${t.stats.published}</span><span class="l">Published</span></div><div class="tile t-av"><span class="n">${t.stats.delivered}</span><span class="l">Delivered</span></div><div class="tile t-dl"><span class="n">${t.stats.filtered}</span><span class="l">Filtered out</span></div>`);
        setHTML($('#t-subs'), t.subscriptions.length ? `<table class="table"><thead><tr><th>Protocol</th><th>Endpoint</th><th>Filter policy</th><th>Delivery</th><th class="num">Delivered</th><th class="num">Filtered out</th><th></th></tr></thead><tbody>${t.subscriptions.map(s => `<tr><td>${protoChip(s.protocol)}</td><td class="mono small">${s.protocol === 'sqs' ? link('queues', s.endpoint) : esc(s.endpoint)}</td>
          <td>${s.filterPolicy ? `<code class="policy">${esc(JSON.stringify(s.filterPolicy))}</code>` : '<span class="muted">None, receives everything</span>'}</td><td class="small">${s.protocol === 'sqs' ? (s.rawMessageDelivery ? 'Raw message' : 'SNS JSON wrapper') : 'Notification'}</td>
          <td class="num">${s.stats.delivered}</td><td class="num">${s.stats.filtered}</td><td class="right"><button class="btn btn-sm btn-ghost" data-act="unsubscribe" data-topic="${na}" data-sub="${esc(s.id)}">Remove</button></td></tr>`).join('')}</tbody></table>`
          : `<div class="card-pad">${emptyState('No subscriptions', 'Add a queue below. Without subscribers, a published message goes nowhere.')}</div>`);
        setHTML($('#t-result'), publishResultHTML(name));
        const inbox = S.endpointLog.filter(e => e.topic === name).slice(0, 8);
        setHTML($('#t-inbox'), inbox.length ? inbox.map(e => `<details class="mail"><summary>${protoChip(e.kind)} <span class="mono small">${esc(e.to)}</span> <span class="muted small">${fmtTime(e.t)}${e.subject ? ', ' + esc(e.subject) : ''}</span></summary><pre class="body">${esc(e.body)}</pre></details>`).join('') : '<p class="muted small">Nothing yet. Subscribe an email or HTTP endpoint and publish.</p>');
      }
    };
  }

  function syncProtocol() {
    const sel = $('[data-protocol]');
    if (!sel) return;
    $$('[data-ep]').forEach(el => { el.hidden = el.dataset.ep !== sel.value; });
  }

  /* =====================================================================
   *  View: activity log
   * ===================================================================== */
  function activityView() {
    const filters = [['all', 'All'], ['sqs', 'SQS'], ['sns', 'SNS'], ['dlq', 'Dead-letter'], ['consumer', 'Consumer']];
    return {
      html: `<div class="page-head"><div><h1>Activity log</h1><p class="lede">Every event the simulated services produce, with the simulated time it happened.</p></div>
        <div class="head-actions"><button class="btn" data-act="log-export">Export JSON</button><button class="btn" data-act="log-clear">Clear</button></div></div>
        <div class="seg" role="group" aria-label="Filter log">${filters.map(([k, l]) => `<button data-act="log-filter" data-v="${k}" aria-pressed="${ui.logFilter === k}">${l}</button>`).join('')}</div>
        <section class="card flush"><div class="table-wrap"><table class="table"><thead><tr><th>Simulated time</th><th>Service</th><th>Event</th></tr></thead><tbody id="log-body"></tbody></table></div></section>`,
      refresh() {
        $$('[data-act=log-filter]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.v === ui.logFilter)));
        const log = Sim.getState().log.filter(e => ui.logFilter === 'all' || e.category === ui.logFilter).slice(0, 150);
        setHTML($('#log-body'), log.length ? log.map(e => `<tr><td class="mono small nowrap">${fmtClock(e.t)}</td><td>${chip(e.category === 'dlq' ? 'DLQ' : e.category === 'sns' ? 'SNS' : e.category === 'consumer' ? 'Consumer' : 'SQS', 'cat-' + e.category)}</td><td class="small">${esc(e.text)}</td></tr>`).join('') : `<tr><td colspan="3">${emptyState('No events yet', 'Create something or send a message and it shows up here.')}</td></tr>`);
      }
    };
  }

  /* =====================================================================
   *  View: learn (explains the services, for the "understanding" criterion)
   * ===================================================================== */
  function learnView() {
    return {
      html: `<div class="page-head"><div><h1>How SQS and SNS work</h1><p class="lede">The ideas this simulator is built on, in the order they matter.</p></div></div>
      <article class="prose card">
        <h2>The problem: services that depend on each other directly</h2>
        <p>Imagine a shop. When an order is placed, billing, shipping and an analytics service all need to know. If the order service calls each of them directly, one slow or crashed service breaks the whole order, and adding a fourth service means changing the order code.</p>
        <p>AWS solves this by putting messaging between the services. The sender hands the message to AWS and moves on. Receivers handle it when they are ready.</p>

        <h2>Amazon SQS: a queue you pull from</h2>
        <p><strong>Simple Queue Service</strong> stores messages until a consumer processes them. Producers <em>send</em>, consumers <em>poll</em>. This is a <strong>pull</strong> model: the queue is a durable buffer, so a consumer that crashes or is slow does not lose anything.</p>
        <p><strong>Amazon SNS</strong> (Simple Notification Service) is a <strong>push</strong> model. A publisher sends one message to a <strong>topic</strong>, and SNS immediately pushes a copy to every subscriber: SQS queues, email addresses, HTTP endpoints, Lambda functions and more.</p>

        <h2>Fan-out: using them together</h2>
        <p>Publish once to an SNS topic and subscribe one SQS queue per service. Each service gets its own copy, its own buffer, and its own pace. A subscriber that is down just builds up a backlog in its queue. Try it on the <a href="#/flow">Message flow</a> page: one publish lands in several queues.</p>

        <h2>The life of an SQS message</h2>
        <svg viewBox="0 0 900 250" class="lifecycle" role="img" aria-label="Message states: sent, delayed, available, in flight, deleted, with timeout and dead-letter paths">
          <defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" class="arrow-head"/></marker></defs>
          <g class="lc-box"><rect x="10" y="95" width="110" height="46" rx="8" class="lc-n"/><text x="65" y="123" text-anchor="middle">Sent</text></g>
          <g class="lc-box"><rect x="190" y="95" width="120" height="46" rx="8" class="lc-de"/><text x="250" y="123" text-anchor="middle">Delayed</text></g>
          <g class="lc-box"><rect x="380" y="95" width="130" height="46" rx="8" class="lc-av"/><text x="445" y="123" text-anchor="middle">Available</text></g>
          <g class="lc-box"><rect x="580" y="95" width="130" height="46" rx="8" class="lc-in"/><text x="645" y="123" text-anchor="middle">In flight</text></g>
          <g class="lc-box"><rect x="780" y="95" width="110" height="46" rx="8" class="lc-n"/><text x="835" y="123" text-anchor="middle">Deleted</text></g>
          <g class="lc-box"><rect x="560" y="196" width="170" height="46" rx="8" class="lc-dl"/><text x="645" y="224" text-anchor="middle">Dead-letter queue</text></g>
          <path d="M120 118H188" class="lc-arrow"/><text x="154" y="86" text-anchor="middle" class="lc-note">delay &gt; 0</text>
          <path d="M310 118H378" class="lc-arrow"/><text x="344" y="86" text-anchor="middle" class="lc-note">delay ends</text>
          <path d="M120 100 C 200 40, 330 40, 400 93" class="lc-arrow dashed"/><text x="262" y="38" text-anchor="middle" class="lc-note">no delay</text>
          <path d="M510 118H578" class="lc-arrow"/><text x="544" y="86" text-anchor="middle" class="lc-note">ReceiveMessage</text>
          <path d="M710 118H778" class="lc-arrow"/><text x="744" y="86" text-anchor="middle" class="lc-note">DeleteMessage</text>
          <path d="M600 140 C 560 190, 480 190, 460 143" class="lc-arrow"/><text x="470" y="206" text-anchor="middle" class="lc-note">visibility timeout ends</text>
          <path d="M645 141V194" class="lc-arrow bad"/><text x="655" y="172" class="lc-note">receives reach the limit</text>
        </svg>
        <p>While a message is <strong>in flight</strong> nobody else can receive it. If the consumer finishes, it deletes the message. If it crashes, the <strong>visibility timeout</strong> runs out and the message becomes available again, so SQS delivers <em>at least once</em>. A message that keeps failing is a <strong>poison message</strong>; after <em>maxReceiveCount</em> attempts it moves to a <strong>dead-letter queue</strong> so it stops blocking the work, and engineers can inspect it and <strong>redrive</strong> it after a fix.</p>

        <h2>Standard and FIFO queues</h2>
        <div class="table-wrap"><table class="table"><thead><tr><th></th><th>Standard</th><th>FIFO</th></tr></thead><tbody>
          <tr><td>Ordering</td><td>Best effort, can arrive out of order</td><td>Strict order within a message group</td></tr>
          <tr><td>Duplicates</td><td>Possible (at least once)</td><td>Removed within a 5 minute window</td></tr>
          <tr><td>Name</td><td>Any valid name</td><td>Must end with <code>.fifo</code></td></tr>
          <tr><td>Extra fields</td><td>Per-message delay</td><td>Message group ID, deduplication ID</td></tr>
          <tr><td>Use when</td><td>Throughput matters, order does not</td><td>Order or exactly-once handling matters</td></tr></tbody></table></div>

        <h2>Filter policies</h2>
        <p>Without a filter, every subscriber gets every message. A <strong>filter policy</strong> is a JSON document on a subscription. SNS compares it with the message attributes and only delivers on a match. All keys must match (AND); inside one key, any listed value may match (OR). Supported here: exact values, <code>prefix</code>, <code>suffix</code>, <code>anything-but</code>, <code>numeric</code> and <code>exists</code>.</p>

        <h2>Words you will meet</h2>
        <dl class="kv wide">
          <dt>Visibility timeout</dt><dd>How long a received message stays hidden from other consumers.</dd>
          <dt>Delivery delay</dt><dd>How long a new message stays invisible before anyone can receive it.</dd>
          <dt>Retention period</dt><dd>How long SQS keeps an unprocessed message before deleting it (4 days by default).</dd>
          <dt>Receipt handle</dt><dd>A token returned on every receive. You need the latest one to delete the message.</dd>
          <dt>Receive count</dt><dd>How many times a message has been received. Compared with maxReceiveCount.</dd>
          <dt>Redrive</dt><dd>Moving messages from a dead-letter queue back to their source queue.</dd>
          <dt>Raw message delivery</dt><dd>By default SNS wraps the message in a JSON envelope before putting it in SQS. Raw delivery removes the wrapper.</dd>
        </dl>

        <h2>What is simulated, and what is simplified</h2>
        <div class="table-wrap"><table class="table"><thead><tr><th>Real AWS</th><th>This simulator</th></tr></thead><tbody>
          <tr><td>Messages are stored redundantly across many servers</td><td>One in-browser state saved to localStorage</td></tr>
          <tr><td>Real time passes</td><td>A simulated clock you can pause, speed up (10x, 60x) or jump forward</td></tr>
          <tr><td>A message moves to the DLQ on the receive after the limit</td><td>It moves as soon as the visibility timeout ends on the last allowed receive, which is easier to see</td></tr>
          <tr><td>Standard queue ordering varies across servers</td><td>Random pick among the first three available messages</td></tr>
          <tr><td>Email subscriptions need confirmation, queues need an access policy</td><td>Confirmed and permitted automatically</td></tr>
          <tr><td>You cannot list messages in a queue</td><td>An inspector table, to make the demo understandable</td></tr></tbody></table></div>

        <h2>Enhancements added beyond basic SQS and SNS</h2>
        <p><strong>1. Live message-flow animation</strong> on the Message flow page. <strong>2. Dead-letter queue workflow</strong> with an auto-consumer that can fail, plus one-click redrive. <strong>3. Filter-policy engine</strong> that explains in plain words why a subscriber did or did not receive a message. The simulated clock, FIFO deduplication and queue monitoring chart support these.</p>
      </article>`,
      refresh() {}
    };
  }

  /* =====================================================================
   *  Modals: create / edit queue, create topic
   * ===================================================================== */
  function queueFormHTML(q) {
    const others = Object.values(Sim.getState().queues).filter(x => !q || x.name !== q.name);
    const v = (k, d) => q ? q[k] : d;
    return `<form data-form="${q ? 'update-queue' : 'create-queue'}" class="stack">
      ${q ? `<input type="hidden" name="name" value="${esc(q.name)}"><p class="muted small">${typeChip(q.type)} The name and type of a queue cannot change.</p>` : `
      <fieldset class="field"><legend>Type</legend><div class="row">
        <label class="check"><input type="radio" name="type" value="standard" checked> Standard</label>
        <label class="check"><input type="radio" name="type" value="fifo"> FIFO</label></div>
        <small class="hint">Standard: high throughput, best-effort order. FIFO: strict order and deduplication (name ends in .fifo, added for you).</small></fieldset>
      <label class="field"><span>Name</span><input name="name" required maxlength="80" placeholder="billing-queue" autocomplete="off"></label>`}
      <div class="three">
        <label class="field"><span>Visibility timeout (s)</span><input name="visibilityTimeout" type="number" min="0" max="43200" value="${v('visibilityTimeout', 30)}"></label>
        <label class="field"><span>Delivery delay (s)</span><input name="delaySeconds" type="number" min="0" max="900" value="${v('delaySeconds', 0)}"></label>
        <label class="field"><span>Retention (s)</span><input name="retention" type="number" min="60" max="1209600" value="${v('retention', 345600)}"></label></div>
      <div class="two">
        <label class="field"><span>Dead-letter queue</span><select name="dlqName"><option value="">None</option>${others.map(o => `<option ${q && q.redrive && q.redrive.dlqName === o.name ? 'selected' : ''}>${esc(o.name)}</option>`).join('')}</select></label>
        <label class="field"><span>Receives before dead-lettering</span><input name="maxReceiveCount" type="number" min="1" max="1000" value="${q && q.redrive ? q.redrive.maxReceiveCount : 3}"></label></div>
      ${!q || q.type === 'fifo' ? `<label class="check"><input type="checkbox" name="contentBasedDedup" ${q && q.contentBasedDedup ? 'checked' : ''}> FIFO only: content-based deduplication (the message body is the deduplication ID)</label>` : ''}
      <div class="row end"><button type="button" class="btn" data-act="close-modal">Cancel</button><button class="btn btn-primary">${q ? 'Save settings' : 'Create queue'}</button></div></form>`;
  }

  /* =====================================================================
   *  Actions (buttons) and form handlers
   * ===================================================================== */
  const num = v => (v === null || v === '' ? undefined : v);

  const forms = {
    'create-queue'(f, fd) {
      const type = fd.get('type') || 'standard';
      let name = (fd.get('name') || '').trim();
      if (type === 'fifo' && name && !name.endsWith('.fifo')) name += '.fifo';
      const opts = { type, visibilityTimeout: num(fd.get('visibilityTimeout')), delaySeconds: num(fd.get('delaySeconds')), retention: num(fd.get('retention')), contentBasedDedup: fd.get('contentBasedDedup') === 'on' };
      if (fd.get('dlqName')) { opts.dlqName = fd.get('dlqName'); opts.maxReceiveCount = num(fd.get('maxReceiveCount')); }
      Sim.createQueue(name, opts);
      closeModal(); toast(`Queue "${name}" created`, 'ok');
      go('#/queues/' + encodeURIComponent(name));
    },
    'update-queue'(f, fd) {
      const name = fd.get('name');
      Sim.updateQueue(name, { visibilityTimeout: num(fd.get('visibilityTimeout')), delaySeconds: num(fd.get('delaySeconds')), retention: num(fd.get('retention')), contentBasedDedup: fd.get('contentBasedDedup') === 'on', dlqName: fd.get('dlqName') || '', maxReceiveCount: num(fd.get('maxReceiveCount')) });
      closeModal(); toast('Settings saved', 'ok'); render();
    },
    'create-topic'(f, fd) {
      const name = (fd.get('name') || '').trim();
      Sim.createTopic(name);
      closeModal(); toast(`Topic "${name}" created`, 'ok');
      go('#/topics/' + encodeURIComponent(name));
    },
    'send-message'(f, fd) {
      const q = fd.get('queue');
      if (!q) throw new Sim.AwsError('MissingParameter', 'Create a queue first.');
      const r = Sim.sendMessage(q, { body: fd.get('body'), attributes: Sim.parseAttributes(fd.get('attributes')), delaySeconds: fd.get('delaySeconds'), groupId: fd.get('groupId'), dedupId: fd.get('dedupId') });
      if (r.duplicate) toast('FIFO deduplication dropped this duplicate', 'warn'); else toast(`Message sent to "${q}"`, 'ok');
      tickUI();
    },
    publish(f, fd) {
      const topic = fd.get('topic');
      if (!topic) throw new Sim.AwsError('MissingParameter', 'Create a topic first.');
      const r = Sim.publish(topic, { subject: fd.get('subject'), message: fd.get('message'), attributes: Sim.parseAttributes(fd.get('attributes')) });
      r.at = Sim.now();
      ui.lastPublish[topic] = r;
      const got = r.results.filter(x => x.delivered).length;
      toast(`Published to "${topic}": delivered to ${got} of ${r.results.length} subscriber${r.results.length === 1 ? '' : 's'}`, got ? 'ok' : 'warn');
      tickUI();
    },
    subscribe(f, fd) {
      const protocol = fd.get('protocol');
      const endpoint = protocol === 'sqs' ? fd.get('endpointSqs') : protocol === 'email' ? fd.get('endpointEmail') : fd.get('endpointHttp');
      Sim.subscribe(fd.get('topic'), { protocol, endpoint, filterPolicy: fd.get('filterPolicy'), rawMessageDelivery: fd.get('raw') === 'on' });
      toast('Subscribed', 'ok'); render();
    },
    poll(f, fd) {
      const q = fd.get('queue');
      const got = Sim.receiveMessages(q, { max: fd.get('max'), visibilityTimeout: num(fd.get('visibilityTimeout')) });
      ui.received[q] = (ui.received[q] || []).concat(got.map(m => ({ id: m.id, handle: m.receiptHandle })));
      if (!got.length) toast('No messages available. In-flight and delayed messages are not returned.', 'warn');
      tickUI();
    },
    consumer(f, fd, submitter) {
      const q = fd.get('queue');
      const start = submitter && submitter.value === 'start';
      Sim.setConsumer(q, { pollEvery: fd.get('pollEvery'), processTime: fd.get('processTime'), failRate: fd.get('failRate'), poisonFails: fd.get('poisonFails') === 'on', enabled: start });
      toast(start ? 'Consumer started' : 'Consumer stopped', start ? 'ok' : 'info');
      tickUI();
    }
  };

  const acts = {
    'new-queue'() { openModal('Create queue', queueFormHTML(null)); },
    'edit-queue'(b) { openModal('Edit queue settings', queueFormHTML(Sim.getState().queues[b.dataset.queue])); },
    'new-topic'() { openModal('Create topic', `<form data-form="create-topic" class="stack"><label class="field"><span>Name</span><input name="name" required maxlength="256" placeholder="orders" autocomplete="off"></label><div class="row end"><button type="button" class="btn" data-act="close-modal">Cancel</button><button class="btn btn-primary">Create topic</button></div></form>`); },
    'close-modal'(b, e) { if (b.classList.contains('modal-backdrop') && e.target !== b) return; closeModal(); },
    'delete-queue'(b) {
      if (!confirm(`Delete queue "${b.dataset.queue}" and all of its messages?`)) return;
      Sim.deleteQueue(b.dataset.queue); toast('Queue deleted', 'info');
      if (b.dataset.back) go('#/queues'); else render();
    },
    'purge-queue'(b) { if (confirm(`Remove every message from "${b.dataset.queue}"?`)) { Sim.purgeQueue(b.dataset.queue); tickUI(); } },
    'delete-topic'(b) {
      if (!confirm(`Delete topic "${b.dataset.topic}"?`)) return;
      Sim.deleteTopic(b.dataset.topic); toast('Topic deleted', 'info');
      if (b.dataset.back) go('#/topics'); else render();
    },
    unsubscribe(b) { Sim.unsubscribe(b.dataset.topic, b.dataset.sub); render(); },
    'delete-received'(b) {
      Sim.deleteMessage(b.dataset.queue, b.dataset.handle);
      toast('Message deleted. Processing is complete.', 'ok'); tickUI();
    },
    release(b) { Sim.changeVisibility(b.dataset.queue, b.dataset.handle, 0); toast('Message returned to the queue', 'info'); tickUI(); },
    redrive(b) { const n = Sim.redriveDlq(b.dataset.queue); toast(`${n} message${n === 1 ? '' : 's'} redriven to the source queue`, 'ok'); tickUI(); },
    'filter-example'(b) { const ta = $('[name=filterPolicy]'); if (ta) ta.value = b.dataset.json; },
    copy(b) { if (navigator.clipboard) navigator.clipboard.writeText(b.dataset.text); toast('Copied', 'info'); },
    speed(b) { ui.speed = Number(b.dataset.v); syncControls(); },
    pause() { ui.paused = !ui.paused; syncControls(); },
    skip(b) { Sim.advance(Number(b.dataset.ms)); toast(`Clock moved forward ${fmtDur(Number(b.dataset.ms) / 1000)}`, 'info'); tickUI(); },
    reset() {
      if (!confirm('Delete every queue, topic and message and start again?')) return;
      hardReset(); toast('Everything cleared', 'info');
    },
    'log-filter'(b) { ui.logFilter = b.dataset.v; tickUI(); },
    'log-clear'() { Sim.getState().log.length = 0; tickUI(); },
    'log-export'() {
      const blob = new Blob([JSON.stringify(Sim.getState().log, null, 2)], { type: 'application/json' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'activity-log.json'; a.click(); URL.revokeObjectURL(a.href);
    },
    'load-sample'() {
      const S = Sim.getState();
      if ((Object.keys(S.queues).length || Object.keys(S.topics).length) && !confirm('This replaces your current queues and topics with the sample setup. Continue?')) return;
      loadSample(); go('#/flow'); toast('Sample setup loaded', 'ok');
    },
    'demo-start'() {
      const S = Sim.getState();
      if ((Object.keys(S.queues).length || Object.keys(S.topics).length) && !confirm('The guided demo replaces your current queues and topics. Continue?')) return;
      ui.demo = -1; demoNext();
    },
    'demo-next'() { demoNext(); },
    'demo-exit'() { ui.demo = -1; ui.speed = 1; syncControls(); renderDemo(); }
  };

  document.addEventListener('submit', e => {
    const f = e.target.closest('form[data-form]');
    if (!f) return;
    e.preventDefault();
    guard(() => forms[f.dataset.form](f, new FormData(f), e.submitter));
  });
  document.addEventListener('click', e => {
    const b = e.target.closest('[data-act]');
    if (!b || !acts[b.dataset.act]) return;
    if (b.tagName === 'A') e.preventDefault();
    guard(() => acts[b.dataset.act](b, e));
  });
  document.addEventListener('change', e => { if (e.target.matches('[data-protocol]')) syncProtocol(); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape' && $('.modal-backdrop')) closeModal(); });

  /* =====================================================================
   *  Sample data + guided demo
   * ===================================================================== */
  function hardReset() {
    try { localStorage.removeItem(STORAGE_KEY); } catch (e) { /* ignore */ }
    Sim.reset(); ui.received = {}; ui.lastPublish = {}; ui.demo = -1; ui.speed = 1; ui.paused = false;
    syncControls(); renderDemo(); go('#/flow'); render();
  }

  function loadSample() {
    Sim.reset(); ui.received = {}; ui.lastPublish = {};
    Sim.createQueue('orders-dlq');
    Sim.createQueue('billing-queue', { visibilityTimeout: 30, dlqName: 'orders-dlq', maxReceiveCount: 3 });
    Sim.createQueue('shipping-queue', { visibilityTimeout: 30 });
    Sim.createTopic('orders');
    Sim.subscribe('orders', { protocol: 'sqs', endpoint: 'billing-queue' });
    Sim.subscribe('orders', { protocol: 'sqs', endpoint: 'shipping-queue', filterPolicy: '{"type": ["physical"]}' });
    Sim.subscribe('orders', { protocol: 'email', endpoint: 'finance@example.com', filterPolicy: '{"amount": [{"numeric": [">=", 1000]}]}' });
  }

  const DEMO = [
    { title: 'Create the building blocks', run() { loadSample(); go('#/flow'); },
      text: 'We created an SNS topic called "orders", two SQS queues (billing and shipping) subscribed to it, a dead-letter queue, and an email subscriber. Shipping only wants physical orders and finance only wants orders of 1000 or more; those are filter policies.' },
    { title: 'Publish a physical order (fan-out)', run() {
        pub('Order #1001: 2 books', 'type=physical\namount=499'); },
      text: 'One publish, two copies: billing (no filter) and shipping (type is physical) each got the message in their own queue. The email was skipped because 499 is below 1000. Watch the dots on the diagram.' },
    { title: 'Publish a digital order (filtering)', run() {
        pub('Order #1002: online course', 'type=digital\namount=2500'); },
      text: 'Now shipping is skipped (red ✕ on its path) because the order is not physical, but billing and the email subscriber both match. Open the "orders" topic to read the exact reason for each decision.' },
    { title: 'Receive a message (visibility timeout)', run() {
        const got = Sim.receiveMessages('billing-queue', { max: 1 });
        ui.received['billing-queue'] = got.map(m => ({ id: m.id, handle: m.receiptHandle }));
        go('#/queues/billing-queue'); },
      text: 'We polled billing-queue. The message is now in flight: hidden from every other consumer for 30 simulated seconds. If we do nothing, it reappears, which is how SQS survives a crashed consumer.' },
    { title: 'Delete it (processing finished)', run() {
        (ui.received['billing-queue'] || []).forEach(r => { try { Sim.deleteMessage('billing-queue', r.handle); } catch (e) { /* already expired */ } });
        ui.received['billing-queue'] = []; },
      text: 'Deleting with the receipt handle tells SQS the work is done, and the message is gone for good. Receive, process, delete is the core SQS loop.' },
    { title: 'Send a poison message and start a failing consumer', run() {
        Sim.sendMessage('billing-queue', { body: 'poison: corrupted order #666' });
        Sim.setConsumer('billing-queue', { enabled: true, pollEvery: 2, processTime: 2, failRate: 0, poisonFails: true });
        ui.speed = 60; syncControls(); go('#/flow'); },
      text: 'The auto-consumer picks up messages, but any message containing "poison" always fails. The clock runs at 60x, so watch the message return to the queue (blue dot) again and again.' },
    { title: 'The dead-letter queue catches it', run() {
        Sim.advance(240000);
        Sim.setConsumer('billing-queue', { enabled: false });
        ui.speed = 1; syncControls(); go('#/queues/orders-dlq'); },
      text: 'After 3 failed receives (the limit we set), SQS moved the message to orders-dlq so it stops clogging billing-queue. The healthy messages were processed normally.' },
    { title: 'Fix the bug, then redrive', run() {
        Sim.redriveDlq('orders-dlq'); go('#/flow'); },
      text: 'Once the bug is fixed, "redrive" sends dead-lettered messages back to where they came from (green dot). That is the full lifecycle: publish, fan-out, filter, receive, retry, dead-letter, redrive.' }
  ];

  function pub(message, attrs) {
    const r = Sim.publish('orders', { subject: 'New order', message, attributes: Sim.parseAttributes(attrs) });
    r.at = Sim.now(); ui.lastPublish.orders = r;
  }

  function demoNext() {
    if (ui.demo + 1 >= DEMO.length) { ui.demo = -1; renderDemo(); return; }
    ui.demo++;
    guard(() => DEMO[ui.demo].run());
    renderDemo(); tickUI();
  }
  function renderDemo() {
    const b = $('#demo-banner');
    if (ui.demo < 0) { b.hidden = true; b.innerHTML = ''; return; }
    const s = DEMO[ui.demo], last = ui.demo === DEMO.length - 1;
    b.hidden = false;
    b.innerHTML = `<div class="demo-inner"><div class="demo-text"><strong>Guided demo, step ${ui.demo + 1} of ${DEMO.length}: ${esc(s.title)}</strong><p>${esc(s.text)}</p></div>
      <div class="row"><button class="btn btn-primary" data-act="demo-next">${last ? 'Finish demo' : 'Next: ' + esc(DEMO[ui.demo + 1].title)}</button><button class="btn" data-act="demo-exit">Exit demo</button></div></div>`;
  }

  /* =====================================================================
   *  Rendering + main loop
   * ===================================================================== */
  function render() {
    const r = parseRoute();
    Flow.unmount();
    let v;
    if (r.view === 'queues') v = r.id ? queueView(r.id) : queuesView();
    else if (r.view === 'topics') v = r.id ? topicView(r.id) : topicsView();
    else if (r.view === 'activity') v = activityView();
    else if (r.view === 'learn') v = learnView();
    else v = flowView();
    ui.view = v;
    const main = $('#main');
    main.innerHTML = v.html;
    window.scrollTo(0, 0);
    const key = ['queues', 'topics', 'activity', 'learn'].includes(r.view) ? r.view : 'flow';
    $$('.side a').forEach(a => a.setAttribute('aria-current', a.dataset.nav === key ? 'page' : 'false'));
    if (v.after) v.after();
    tickUI();
  }

  function syncControls() {
    $$('[data-act=speed]').forEach(b => b.setAttribute('aria-pressed', String(Number(b.dataset.v) === ui.speed)));
    const p = $('[data-act=pause]');
    p.setAttribute('aria-pressed', String(ui.paused));
    p.textContent = ui.paused ? 'Resume' : 'Pause';
  }

  function tickUI() {
    const S = Sim.getState();
    $('#clock').textContent = fmtClock(S.simTime);
    $('#nav-q').textContent = Object.keys(S.queues).length;
    $('#nav-t').textContent = Object.keys(S.topics).length;
    if (ui.view && ui.view.refresh) ui.view.refresh();
    Flow.refresh();
    $$('.cd').forEach(s => { s.textContent = fmtDur((Number(s.dataset.until) - Sim.now()) / 1000); });
  }

  let saveTimer = null;
  function scheduleSave() {
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
      saveTimer = null;
      try { localStorage.setItem(STORAGE_KEY, Sim.serialize()); } catch (e) { /* storage full or blocked */ }
    }, 800);
  }

  function boot() {
    try { const saved = localStorage.getItem(STORAGE_KEY); if (saved) Sim.load(saved); } catch (e) { /* ignore */ }
    Sim.on(e => { scheduleSave(); Flow.handle(e); });
    window.addEventListener('hashchange', render);
    syncControls();
    render();

    let last = performance.now();
    setInterval(() => {
      const t = performance.now();
      const dt = Math.min(t - last, 2000);
      last = t;
      if (!ui.paused && dt > 0) Sim.advance(dt * ui.speed);
      tickUI();
    }, 250);
  }

  document.addEventListener('DOMContentLoaded', boot);
})();
