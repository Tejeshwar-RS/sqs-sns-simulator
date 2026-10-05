/* =====================================================================
 * engine.js  -  The SQS + SNS simulation engine
 * ---------------------------------------------------------------------
 * Pure JavaScript, no DOM access. Everything the "AWS" does lives here:
 *   - SQS: queues (Standard + FIFO), send / receive / delete, visibility
 *          timeout, delivery delay, retention, dead-letter queues, redrive
 *   - SNS: topics, subscriptions (SQS / email / HTTP), publish, fan-out,
 *          filter policies
 *   - A simulated clock so time-based behaviour can be sped up in a demo
 *   - An auto-consumer that can succeed or fail (to demonstrate the DLQ)
 *
 * The UI (app.js, flow.js) only calls the functions exposed on `Sim`
 * and listens to the events the engine emits.
 * ===================================================================== */
(function (global) {
  'use strict';

  const REGION = 'ap-south-1';
  const ACCOUNT = '123456789012';
  const DEDUP_WINDOW_MS = 5 * 60 * 1000;   // FIFO deduplication window (5 minutes, as in AWS)
  const MAX_BODY_BYTES = 256 * 1024;       // SQS message size limit (256 KB)
  const HISTORY_EVERY_MS = 5000;           // monitoring sample interval (simulated time)
  const HISTORY_MAX = 150;
  const LOG_MAX = 300;
  const NO_LOG = new Set(['tick', 'changed']);

  /* ---------- Errors that look like AWS errors ---------- */
  class AwsError extends Error {
    constructor(code, message) {
      super(message);
      this.name = 'AwsError';
      this.code = code;
    }
  }

  /* ---------- State ---------- */
  function freshState() {
    return {
      simTime: Date.now(),   // simulated clock (ms since epoch)
      seq: 0,                // global message sequence number (keeps FIFO order)
      queues: {},
      topics: {},
      consumers: {},         // auto-consumer settings per queue
      endpointLog: [],       // what simulated email / HTTP endpoints "received"
      log: []                // activity log
    };
  }
  let state = freshState();
  const listeners = [];

  const now = () => state.simTime;
  const byteLen = s => new TextEncoder().encode(s).length;
  const uid = () =>
    (global.crypto && global.crypto.randomUUID)
      ? global.crypto.randomUUID()
      : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
          const r = Math.random() * 16 | 0;
          return (c === 'x' ? r : (r & 3) | 8).toString(16);
        });
  const hash = s => { let h = 5381; for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) | 0; return (h >>> 0).toString(16); };
  const handleId = () => 'AQEB' + Math.random().toString(36).slice(2, 12) + Math.random().toString(36).slice(2, 12);

  const queueArn = n => `arn:aws:sqs:${REGION}:${ACCOUNT}:${n}`;
  const queueUrl = n => `https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/${n}`;
  const topicArn = n => `arn:aws:sns:${REGION}:${ACCOUNT}:${n}`;

  /* ---------- Events + activity log ---------- */
  function describe(e) {
    switch (e.type) {
      case 'queue-created': return `Queue "${e.queue}" created (${e.queueType === 'fifo' ? 'FIFO' : 'Standard'})`;
      case 'queue-updated': return `Settings of queue "${e.queue}" updated`;
      case 'queue-deleted': return `Queue "${e.queue}" deleted`;
      case 'sent': return `Message sent to "${e.queue}"` + (e.via ? ` by SNS topic "${e.via}"` : '') + (e.delay ? ` with a ${e.delay}s delivery delay` : '');
      case 'duplicate': return `FIFO deduplication dropped a duplicate message for "${e.queue}"`;
      case 'received': return `${e.by === 'consumer' ? 'Consumer' : 'You'} received a message from "${e.queue}" (receive #${e.receiveCount}). It is now in flight.`;
      case 'deleted': return `Message deleted from "${e.queue}"` + (e.by === 'consumer' ? ' by the consumer' : '');
      case 'visibility-changed': return `Visibility timeout of a message in "${e.queue}" changed to ${e.seconds}s`;
      case 'visibility-expired': return `Visibility timeout expired in "${e.queue}". The message is available again (received ${e.receiveCount} time${e.receiveCount === 1 ? '' : 's'}).`;
      case 'dead-lettered': return `Message moved from "${e.queue}" to dead-letter queue "${e.dlq}" after ${e.receiveCount} failed receives`;
      case 'redriven': return `Message redriven from "${e.dlq}" back to "${e.queue}"`;
      case 'expired': return `Message in "${e.queue}" was removed because it passed the retention period`;
      case 'purged': return `Queue "${e.queue}" purged (${e.count} message${e.count === 1 ? '' : 's'} removed)`;
      case 'topic-created': return `Topic "${e.topic}" created`;
      case 'topic-deleted': return `Topic "${e.topic}" deleted`;
      case 'subscribed': return `${e.protocol.toUpperCase()} endpoint "${e.endpoint}" subscribed to topic "${e.topic}"` + (e.filtered ? ' with a filter policy' : '');
      case 'unsubscribed': return `${e.protocol.toUpperCase()} endpoint "${e.endpoint}" unsubscribed from topic "${e.topic}"`;
      case 'published': return `Message published to topic "${e.topic}"`;
      case 'delivered': return `SNS delivered the message to ${e.protocol.toUpperCase()} endpoint "${e.endpoint}"`;
      case 'filtered': return `SNS did not deliver to "${e.endpoint}": ${e.reason}`;
      case 'delivery-failed': return `SNS could not deliver to "${e.endpoint}": ${e.reason}`;
      case 'consumer-ok': return `Consumer finished processing a message from "${e.queue}"`;
      case 'consumer-failed': return `Consumer FAILED to process a message from "${e.queue}" (it will not be deleted)`;
      case 'consumer-late': return `Consumer took longer than the visibility timeout in "${e.queue}"; its receipt handle is no longer valid`;
      default: return e.type;
    }
  }

  const CATEGORY = {
    'queue-created': 'sqs', 'queue-updated': 'sqs', 'queue-deleted': 'sqs', sent: 'sqs', duplicate: 'sqs', received: 'sqs',
    deleted: 'sqs', 'visibility-changed': 'sqs', 'visibility-expired': 'sqs', expired: 'sqs', purged: 'sqs',
    'dead-lettered': 'dlq', redriven: 'dlq',
    'topic-created': 'sns', 'topic-deleted': 'sns', subscribed: 'sns', unsubscribed: 'sns', published: 'sns',
    delivered: 'sns', filtered: 'sns', 'delivery-failed': 'sns',
    'consumer-ok': 'consumer', 'consumer-failed': 'consumer', 'consumer-late': 'consumer'
  };

  function emit(evt) {
    evt.t = now();
    evt.id = ++state.seq;
    if (!NO_LOG.has(evt.type)) {
      evt.text = describe(evt);
      evt.category = CATEGORY[evt.type] || 'sqs';
      state.log.unshift({ id: evt.id, t: evt.t, type: evt.type, category: evt.category, text: evt.text });
      if (state.log.length > LOG_MAX) state.log.length = LOG_MAX;
    }
    listeners.forEach(fn => { try { fn(evt); } catch (err) { console.error(err); } });
  }

  /* ---------- Small validators ---------- */
  function intIn(value, lo, hi, label, fallback) {
    if (value === undefined || value === null || value === '') return fallback;
    const n = Number(value);
    if (!Number.isInteger(n) || n < lo || n > hi) {
      throw new AwsError('InvalidParameterValue', `${label} must be a whole number between ${lo} and ${hi}.`);
    }
    return n;
  }

  function validateQueueName(name, type) {
    let base = name;
    if (type === 'fifo') {
      if (!name.endsWith('.fifo')) throw new AwsError('InvalidQueueName', 'FIFO queue names must end with ".fifo".');
      base = name.slice(0, -5);
    } else if (name.endsWith('.fifo')) {
      throw new AwsError('InvalidQueueName', 'Only FIFO queues can have a name ending in ".fifo".');
    }
    if (name.length > 80 || !/^[A-Za-z0-9_-]+$/.test(base)) {
      throw new AwsError('InvalidQueueName', 'Queue names can be up to 80 characters: letters, numbers, hyphens and underscores only.');
    }
  }

  function getQueue(name) {
    const q = state.queues[name];
    if (!q) throw new AwsError('QueueDoesNotExist', `Queue "${name}" does not exist.`);
    return q;
  }
  function getTopic(name) {
    const t = state.topics[name];
    if (!t) throw new AwsError('NotFound', `Topic "${name}" does not exist.`);
    return t;
  }

  /* =====================================================================
   *  SQS - queue management
   * ===================================================================== */
  function applyQueueSettings(q, o) {
    q.visibilityTimeout = intIn(o.visibilityTimeout, 0, 43200, 'Visibility timeout', q.visibilityTimeout);
    q.retention = intIn(o.retention, 60, 1209600, 'Message retention period', q.retention);
    q.delaySeconds = intIn(o.delaySeconds, 0, 900, 'Delivery delay', q.delaySeconds);
    if (q.type === 'fifo' && 'contentBasedDedup' in o) q.contentBasedDedup = !!o.contentBasedDedup;

    if ('dlqName' in o) {
      if (!o.dlqName) {
        q.redrive = null;
      } else {
        const dlq = state.queues[o.dlqName];
        if (!dlq) throw new AwsError('QueueDoesNotExist', `Dead-letter queue "${o.dlqName}" does not exist.`);
        if (dlq.name === q.name) throw new AwsError('InvalidParameterValue', 'A queue cannot be its own dead-letter queue.');
        if (dlq.type !== q.type) throw new AwsError('InvalidParameterValue', 'A dead-letter queue must be the same type (Standard or FIFO) as its source queue.');
        const prev = q.redrive ? q.redrive.maxReceiveCount : 3;
        q.redrive = { dlqName: dlq.name, maxReceiveCount: intIn(o.maxReceiveCount, 1, 1000, 'Maximum receives', prev) };
      }
    }
  }

  function createQueue(name, opts = {}) {
    const type = opts.type === 'fifo' ? 'fifo' : 'standard';
    name = String(name || '').trim();
    validateQueueName(name, type);
    if (state.queues[name]) throw new AwsError('QueueNameExists', `A queue named "${name}" already exists.`);
    const q = {
      name, type, createdAt: now(),
      visibilityTimeout: 30, retention: 345600, delaySeconds: 0,
      contentBasedDedup: false, redrive: null,
      messages: [], dedup: {}, history: [], lastSample: 0,
      stats: { sent: 0, received: 0, deleted: 0, deadLettered: 0 }
    };
    applyQueueSettings(q, opts);
    state.queues[name] = q;
    emit({ type: 'queue-created', queue: name, queueType: type });
    return q;
  }

  function updateQueue(name, opts) {
    const q = getQueue(name);
    applyQueueSettings(q, opts);
    emit({ type: 'queue-updated', queue: name });
    return q;
  }

  function deleteQueue(name) {
    getQueue(name);
    delete state.queues[name];
    delete state.consumers[name];
    // Real AWS leaves dangling references; the simulator tidies them up.
    Object.values(state.topics).forEach(t => {
      t.subscriptions = t.subscriptions.filter(s => !(s.protocol === 'sqs' && s.endpoint === name));
    });
    Object.values(state.queues).forEach(q => { if (q.redrive && q.redrive.dlqName === name) q.redrive = null; });
    emit({ type: 'queue-deleted', queue: name });
  }

  function counts(q) {
    const c = { available: 0, inflight: 0, delayed: 0 };
    q.messages.forEach(m => { c[m.state]++; });
    return c;
  }

  /* =====================================================================
   *  SQS - messages
   * ===================================================================== */
  function normalizeAttrs(a) {
    const out = {};
    if (!a) return out;
    Object.entries(a).forEach(([k, v]) => {
      if (v && typeof v === 'object' && 'value' in v) out[k] = { type: v.type === 'Number' ? 'Number' : 'String', value: String(v.value) };
      else out[k] = { type: typeof v === 'number' ? 'Number' : 'String', value: String(v) };
    });
    return out;
  }

  /** "type=physical\namount=499"  ->  { type:{String}, amount:{Number} } */
  function parseAttributes(text) {
    const out = {};
    String(text || '').split('\n').map(l => l.trim()).filter(Boolean).forEach(line => {
      const i = line.indexOf('=');
      if (i < 1) throw new AwsError('InvalidParameterValue', `Attribute line "${line}" must look like name=value.`);
      const k = line.slice(0, i).trim();
      const v = line.slice(i + 1).trim();
      out[k] = { type: /^-?\d+(\.\d+)?$/.test(v) ? 'Number' : 'String', value: v };
    });
    return out;
  }

  function pruneDedup(q) {
    Object.keys(q.dedup).forEach(k => { if (now() - q.dedup[k].at > DEDUP_WINDOW_MS) delete q.dedup[k]; });
  }

  function sendMessage(qname, p = {}) {
    const q = getQueue(qname);
    const body = String(p.body === undefined || p.body === null ? '' : p.body);
    if (!body.trim()) throw new AwsError('MissingParameter', 'The message body cannot be empty.');
    if (byteLen(body) > MAX_BODY_BYTES) throw new AwsError('InvalidParameterValue', 'The message body is larger than the 256 KB limit.');

    let delay = q.delaySeconds, groupId = null, dedupId = null;
    if (q.type === 'fifo') {
      groupId = String(p.groupId || '').trim();
      if (!groupId) throw new AwsError('MissingParameter', 'FIFO queues need a message group ID.');
      dedupId = String(p.dedupId || '').trim() || (q.contentBasedDedup ? 'content:' + hash(body) : '');
      if (!dedupId) throw new AwsError('MissingParameter', 'Give a deduplication ID, or turn on content-based deduplication for this queue.');
      pruneDedup(q);
      if (q.dedup[dedupId]) {
        emit({ type: 'duplicate', queue: q.name });
        return { message: q.messages.find(m => m.id === q.dedup[dedupId].messageId) || null, duplicate: true };
      }
    } else if (p.delaySeconds !== undefined && p.delaySeconds !== '' && p.delaySeconds !== null) {
      delay = intIn(p.delaySeconds, 0, 900, 'Delivery delay', 0);
    }

    const m = {
      id: uid(), body, attributes: normalizeAttrs(p.attributes),
      groupId, dedupId,
      state: delay > 0 ? 'delayed' : 'available',
      receiveCount: 0, sentAt: now(), availableAt: now() + delay * 1000,
      receiptHandle: null, seq: ++state.seq,
      source: p.source || 'direct', deadLetterSource: null
    };
    if (dedupId) q.dedup[dedupId] = { at: now(), messageId: m.id };
    q.messages.push(m);
    q.stats.sent++;
    emit({ type: 'sent', queue: q.name, id: m.id, via: p.via || null, delay });
    return { message: m, duplicate: false };
  }

  /**
   * ReceiveMessage. Returned messages become "in flight": hidden from every
   * other consumer until the visibility timeout ends or they are deleted.
   */
  function receiveMessages(qname, opts = {}) {
    const q = getQueue(qname);
    const max = intIn(opts.max, 1, 10, 'Maximum number of messages', 1);
    const vt = intIn(opts.visibilityTimeout, 0, 43200, 'Visibility timeout', q.visibilityTimeout);
    const available = q.messages.filter(m => m.state === 'available').sort((a, b) => a.seq - b.seq);
    const picked = [];

    if (q.type === 'fifo') {
      // Strict order. A group with an in-flight message is locked until that message is done.
      const locked = new Set(q.messages.filter(m => m.state === 'inflight').map(m => m.groupId));
      for (const m of available) {
        if (picked.length >= max) break;
        if (!locked.has(m.groupId)) picked.push(m);
      }
    } else {
      // Standard queues give "best-effort" ordering: pick randomly among the first few messages.
      const pool = available.slice();
      while (picked.length < max && pool.length) {
        const idx = Math.floor(Math.random() * Math.min(3, pool.length));
        picked.push(pool.splice(idx, 1)[0]);
      }
    }

    picked.forEach(m => {
      m.state = 'inflight';
      m.receiveCount++;
      m.receiptHandle = handleId();
      m.availableAt = now() + vt * 1000;
      q.stats.received++;
      emit({ type: 'received', queue: q.name, id: m.id, receiveCount: m.receiveCount, by: opts.by || 'you' });
    });
    return picked.map(m => ({ ...m }));
  }

  function deleteMessage(qname, handle, opts = {}) {
    const q = getQueue(qname);
    const i = q.messages.findIndex(m => m.state === 'inflight' && m.receiptHandle === handle);
    if (i < 0) {
      throw new AwsError('ReceiptHandleIsInvalid', 'This receipt handle is no longer valid. The visibility timeout probably expired and the message is available again.');
    }
    const [m] = q.messages.splice(i, 1);
    q.stats.deleted++;
    emit({ type: 'deleted', queue: q.name, id: m.id, by: opts.by || 'you' });
  }

  function changeVisibility(qname, handle, seconds) {
    const q = getQueue(qname);
    const m = q.messages.find(x => x.state === 'inflight' && x.receiptHandle === handle);
    if (!m) throw new AwsError('ReceiptHandleIsInvalid', 'This receipt handle is no longer valid.');
    seconds = intIn(seconds, 0, 43200, 'Visibility timeout', 0);
    m.availableAt = now() + seconds * 1000;
    emit({ type: 'visibility-changed', queue: q.name, seconds });
    if (seconds === 0) releaseOrDlq(q, m);
  }

  function purgeQueue(qname) {
    const q = getQueue(qname);
    const count = q.messages.length;
    q.messages = [];
    q.dedup = {};
    emit({ type: 'purged', queue: q.name, count });
  }

  /* ---------- Dead-letter queues ---------- */
  function removeFrom(q, m) { q.messages.splice(q.messages.indexOf(m), 1); }

  function moveToDlq(q, m) {
    const dlq = state.queues[q.redrive.dlqName];
    if (!dlq) { m.state = 'available'; m.receiptHandle = null; return; }
    removeFrom(q, m);
    const receives = m.receiveCount;
    m.state = 'available';
    m.receiveCount = 0;
    m.receiptHandle = null;
    m.availableAt = now();
    m.seq = ++state.seq;
    m.deadLetterSource = q.name;
    dlq.messages.push(m);
    q.stats.deadLettered++;
    emit({ type: 'dead-lettered', queue: q.name, dlq: dlq.name, id: m.id, receiveCount: receives });
  }

  /** Called when an in-flight message's visibility timeout is over. */
  function releaseOrDlq(q, m) {
    if (q.redrive && m.receiveCount >= q.redrive.maxReceiveCount) {
      moveToDlq(q, m);
    } else {
      m.state = 'available';
      m.receiptHandle = null;
      emit({ type: 'visibility-expired', queue: q.name, id: m.id, receiveCount: m.receiveCount });
    }
  }

  /** "Start DLQ redrive": move dead-lettered messages back to the queue they came from. */
  function redriveDlq(dlqName) {
    const dlq = getQueue(dlqName);
    let moved = 0;
    dlq.messages.filter(m => m.deadLetterSource && m.state === 'available').forEach(m => {
      const src = state.queues[m.deadLetterSource];
      if (!src) return;
      removeFrom(dlq, m);
      const from = m.deadLetterSource;
      m.deadLetterSource = null;
      m.receiveCount = 0;
      m.state = 'available';
      m.sentAt = now();
      m.seq = ++state.seq;
      src.messages.push(m);
      moved++;
      emit({ type: 'redriven', queue: from, dlq: dlqName, id: m.id });
    });
    return moved;
  }

  const dlqSources = name => Object.values(state.queues).filter(q => q.redrive && q.redrive.dlqName === name);

  /* =====================================================================
   *  Auto-consumer (a fake worker app that polls a queue)
   * ===================================================================== */
  const defaultConsumer = () => ({ enabled: false, pollEvery: 2, processTime: 3, failRate: 0, poisonFails: true, nextPollAt: 0, working: [] });

  function getConsumer(qname) { return state.consumers[qname] || defaultConsumer(); }

  function setConsumer(qname, cfg) {
    getQueue(qname);
    const c = state.consumers[qname] || (state.consumers[qname] = defaultConsumer());
    c.pollEvery = intIn(cfg.pollEvery, 1, 60, 'Poll interval', c.pollEvery);
    c.processTime = intIn(cfg.processTime, 0, 600, 'Processing time', c.processTime);
    c.failRate = intIn(cfg.failRate, 0, 100, 'Failure rate', c.failRate);
    if ('poisonFails' in cfg) c.poisonFails = !!cfg.poisonFails;
    if ('enabled' in cfg) c.enabled = !!cfg.enabled;
    if (!c.enabled) c.working = [];
    emit({ type: 'changed' });
    return c;
  }

  function runConsumers() {
    const t = now();
    Object.keys(state.consumers).forEach(qname => {
      const c = state.consumers[qname];
      const q = state.queues[qname];
      if (!q) { delete state.consumers[qname]; return; }
      if (!c.enabled) { c.working = []; return; }

      c.working = c.working.filter(w => {
        if (t < w.finishAt) return true;
        const m = q.messages.find(x => x.id === w.id && x.state === 'inflight' && x.receiptHandle === w.handle);
        if (!m) { emit({ type: 'consumer-late', queue: qname }); return false; }
        if (w.fail) {
          emit({ type: 'consumer-failed', queue: qname, id: m.id });   // no delete -> message will reappear
        } else {
          deleteMessage(qname, w.handle, { by: 'consumer' });
          emit({ type: 'consumer-ok', queue: qname, id: m.id });
        }
        return false;
      });

      if (t >= c.nextPollAt && c.working.length < 1) {
        c.nextPollAt = t + c.pollEvery * 1000;
        receiveMessages(qname, { max: 1, by: 'consumer' }).forEach(m => {
          const fail = (c.poisonFails && /poison/i.test(m.body)) || Math.random() * 100 < c.failRate;
          c.working.push({ id: m.id, handle: m.receiptHandle, finishAt: t + c.processTime * 1000, fail });
        });
      }
    });
  }

  /* =====================================================================
   *  SNS - topics, subscriptions, filter policies
   * ===================================================================== */
  function createTopic(name) {
    name = String(name || '').trim();
    if (!/^[A-Za-z0-9_-]{1,256}$/.test(name)) throw new AwsError('InvalidParameter', 'Topic names can be up to 256 characters: letters, numbers, hyphens and underscores only.');
    if (state.topics[name]) throw new AwsError('TopicNameExists', `A topic named "${name}" already exists.`);
    const t = { name, createdAt: now(), subscriptions: [], stats: { published: 0, delivered: 0, filtered: 0 } };
    state.topics[name] = t;
    emit({ type: 'topic-created', topic: name });
    return t;
  }

  function deleteTopic(name) {
    getTopic(name);
    delete state.topics[name];
    emit({ type: 'topic-deleted', topic: name });
  }

  function validateRule(key, r) {
    const bad = () => { throw new AwsError('InvalidParameter', `Invalid filter rule for "${key}": ${JSON.stringify(r)}. Use a plain value, {"prefix": "..."}, {"suffix": "..."}, {"anything-but": [...]}, {"numeric": [">", 5]} or {"exists": true}.`); };
    if (typeof r === 'string' || typeof r === 'number') return;
    if (!r || typeof r !== 'object' || Array.isArray(r)) return bad();
    const ks = Object.keys(r);
    if (ks.length !== 1) return bad();
    const op = ks[0], v = r[op];
    if (op === 'prefix' || op === 'suffix') { if (typeof v !== 'string') bad(); }
    else if (op === 'anything-but') { if (!(typeof v === 'string' || typeof v === 'number' || (Array.isArray(v) && v.length))) bad(); }
    else if (op === 'exists') { if (typeof v !== 'boolean') bad(); }
    else if (op === 'numeric') {
      const okOps = ['=', '>', '>=', '<', '<='];
      if (!Array.isArray(v) || !v.length || v.length % 2 || !v.every((x, i) => i % 2 === 0 ? okOps.includes(x) : typeof x === 'number')) bad();
    } else bad();
  }

  function validateFilterPolicy(policy) {
    if (policy === null || policy === undefined) return null;
    if (typeof policy !== 'object' || Array.isArray(policy)) throw new AwsError('InvalidParameter', 'A filter policy must be a JSON object, for example {"type": ["order"]}.');
    const keys = Object.keys(policy);
    if (!keys.length) return null;
    keys.forEach(k => {
      const rules = policy[k];
      if (!Array.isArray(rules) || !rules.length) throw new AwsError('InvalidParameter', `In a filter policy, "${k}" must map to a non-empty array, for example "${k}": ["value"].`);
      rules.forEach(r => validateRule(k, r));
    });
    return policy;
  }

  function parseFilterPolicy(text) {
    if (!String(text || '').trim()) return null;
    let obj;
    try { obj = JSON.parse(text); } catch (e) { throw new AwsError('InvalidParameter', 'The filter policy is not valid JSON: ' + e.message); }
    return validateFilterPolicy(obj);
  }

  function matchRule(rule, attr) {
    if (rule && typeof rule === 'object' && 'exists' in rule) return rule.exists === !!attr;
    if (!attr) return false;
    const val = String(attr.value);
    const num = Number(attr.value);
    if (typeof rule === 'string' || typeof rule === 'number') return val === String(rule);
    if ('prefix' in rule) return val.startsWith(rule.prefix);
    if ('suffix' in rule) return val.endsWith(rule.suffix);
    if ('anything-but' in rule) {
      const list = [].concat(rule['anything-but']).map(String);
      return !list.includes(val);
    }
    if ('numeric' in rule) {
      if (!Number.isFinite(num)) return false;
      const spec = rule.numeric;
      for (let i = 0; i < spec.length; i += 2) {
        const op = spec[i], n = spec[i + 1];
        const ok = op === '=' ? num === n : op === '>' ? num > n : op === '>=' ? num >= n : op === '<' ? num < n : num <= n;
        if (!ok) return false;
      }
      return true;
    }
    return false;
  }

  const ruleText = r => (r && typeof r === 'object') ? JSON.stringify(r) : JSON.stringify(r);

  /** Every key in the policy must match (AND); inside a key, any value may match (OR). */
  function matchFilter(policy, attrs) {
    if (!policy || !Object.keys(policy).length) return { matched: true, reason: 'no filter policy, so every message is delivered' };
    attrs = attrs || {};
    for (const [key, rules] of Object.entries(policy)) {
      const attr = attrs[key];
      if (!rules.some(r => matchRule(r, attr))) {
        const have = attr ? `is "${attr.value}"` : 'is missing';
        return { matched: false, reason: `attribute "${key}" ${have}, but the filter policy needs ${rules.map(ruleText).join(' or ')}` };
      }
    }
    return { matched: true, reason: 'all filter policy conditions matched' };
  }

  function subscribe(topicName, p = {}) {
    const t = getTopic(topicName);
    const protocol = p.protocol;
    let endpoint = String(p.endpoint || '').trim();
    if (!['sqs', 'email', 'http'].includes(protocol)) throw new AwsError('InvalidParameter', 'Protocol must be sqs, email or http.');
    if (!endpoint) throw new AwsError('InvalidParameter', 'An endpoint is required.');

    if (protocol === 'sqs') {
      const q = getQueue(endpoint);
      if (q.type === 'fifo') throw new AwsError('InvalidParameter', 'A Standard SNS topic can only deliver to Standard SQS queues, not FIFO queues.');
    } else if (protocol === 'email') {
      if (!/^\S+@\S+\.\S+$/.test(endpoint)) throw new AwsError('InvalidParameter', 'Enter a valid email address.');
    } else if (!/^https?:\/\/\S+$/.test(endpoint)) {
      throw new AwsError('InvalidParameter', 'Enter a URL that starts with http:// or https://.');
    }
    if (t.subscriptions.some(s => s.protocol === protocol && s.endpoint === endpoint)) {
      throw new AwsError('InvalidParameter', `"${endpoint}" is already subscribed to this topic.`);
    }
    const filterPolicy = typeof p.filterPolicy === 'string' ? parseFilterPolicy(p.filterPolicy) : validateFilterPolicy(p.filterPolicy);
    const sub = {
      id: uid().slice(0, 8), protocol, endpoint, filterPolicy,
      rawMessageDelivery: protocol === 'sqs' && !!p.rawMessageDelivery,
      createdAt: now(), stats: { delivered: 0, filtered: 0 }
    };
    t.subscriptions.push(sub);
    emit({ type: 'subscribed', topic: t.name, protocol, endpoint, filtered: !!filterPolicy });
    return sub;
  }

  function unsubscribe(topicName, subId) {
    const t = getTopic(topicName);
    const i = t.subscriptions.findIndex(s => s.id === subId);
    if (i < 0) throw new AwsError('NotFound', 'Subscription not found.');
    const [s] = t.subscriptions.splice(i, 1);
    emit({ type: 'unsubscribed', topic: t.name, protocol: s.protocol, endpoint: s.endpoint });
  }

  /**
   * Publish -> fan-out. For every subscription: check the filter policy,
   * then deliver (SQS queue / simulated email / simulated HTTP endpoint).
   */
  function publish(topicName, p = {}) {
    const t = getTopic(topicName);
    const message = String(p.message === undefined || p.message === null ? '' : p.message);
    if (!message.trim()) throw new AwsError('MissingParameter', 'The message cannot be empty.');
    if (byteLen(message) > MAX_BODY_BYTES) throw new AwsError('InvalidParameter', 'The message is larger than the 256 KB limit.');
    const attrs = normalizeAttrs(p.attributes);
    const messageId = uid();

    t.stats.published++;
    emit({ type: 'published', topic: t.name, id: messageId });

    const envelope = {
      Type: 'Notification', MessageId: messageId, TopicArn: topicArn(t.name),
      ...(p.subject ? { Subject: p.subject } : {}),
      Message: message,
      Timestamp: new Date(now()).toISOString(),
      ...(Object.keys(attrs).length ? { MessageAttributes: Object.fromEntries(Object.entries(attrs).map(([k, v]) => [k, { Type: v.type, Value: v.value }])) } : {})
    };

    const results = t.subscriptions.map(sub => {
      const res = { subscriptionId: sub.id, protocol: sub.protocol, endpoint: sub.endpoint, delivered: false, reason: '' };
      const verdict = matchFilter(sub.filterPolicy, attrs);
      if (!verdict.matched) {
        sub.stats.filtered++; t.stats.filtered++;
        res.reason = verdict.reason;
        emit({ type: 'filtered', topic: t.name, protocol: sub.protocol, endpoint: sub.endpoint, reason: verdict.reason });
        return res;
      }
      if (sub.protocol === 'sqs') {
        if (!state.queues[sub.endpoint]) {
          res.reason = 'the queue no longer exists';
          emit({ type: 'delivery-failed', topic: t.name, endpoint: sub.endpoint, reason: res.reason });
          return res;
        }
        sendMessage(sub.endpoint, {
          body: sub.rawMessageDelivery ? message : JSON.stringify(envelope, null, 2),
          attributes: sub.rawMessageDelivery ? attrs : {},
          source: 'sns:' + t.name, via: t.name
        });
      } else {
        state.endpointLog.unshift({
          id: uid().slice(0, 8), t: now(), topic: t.name, kind: sub.protocol, to: sub.endpoint,
          subject: p.subject || '', body: sub.protocol === 'email' ? message : JSON.stringify(envelope, null, 2)
        });
        if (state.endpointLog.length > 60) state.endpointLog.length = 60;
      }
      sub.stats.delivered++; t.stats.delivered++;
      res.delivered = true;
      res.reason = verdict.reason;
      emit({ type: 'delivered', topic: t.name, protocol: sub.protocol, endpoint: sub.endpoint, queue: sub.protocol === 'sqs' ? sub.endpoint : null });
      return res;
    });

    return { messageId, results };
  }

  /* =====================================================================
   *  Time
   * ===================================================================== */
  function sample(q) {
    if (now() - q.lastSample >= HISTORY_EVERY_MS) {
      q.lastSample = now();
      q.history.push({ t: now(), ...counts(q) });
      if (q.history.length > HISTORY_MAX) q.history.shift();
    }
  }

  function step(ms) {
    state.simTime += ms;
    const t = state.simTime;
    Object.values(state.queues).forEach(q => {
      if (q.type === 'fifo') pruneDedup(q);
      q.messages.slice().forEach(m => {
        if (t - m.sentAt > q.retention * 1000) {
          removeFrom(q, m);
          emit({ type: 'expired', queue: q.name, id: m.id });
        } else if (m.state === 'delayed' && t >= m.availableAt) {
          m.state = 'available';
        } else if (m.state === 'inflight' && t >= m.availableAt) {
          releaseOrDlq(q, m);
        }
      });
      sample(q);
    });
    runConsumers();
  }

  /** Move the simulated clock forward. Big jumps are split into steps so that consumers and timeouts still behave. */
  function advance(ms) {
    if (!(ms > 0)) return;
    const stepMs = Math.max(1000, ms / 300);
    let left = ms;
    while (left > 0) {
      const s = Math.min(stepMs, left);
      step(s);
      left -= s;
    }
    emit({ type: 'tick' });
  }

  /* =====================================================================
   *  Persistence + public API
   * ===================================================================== */
  function serialize() { return JSON.stringify(state); }

  function load(json) {
    try {
      const s = JSON.parse(json);
      if (!s || typeof s !== 'object' || !s.queues || !s.topics) throw new Error('bad shape');
      Object.values(s.consumers || {}).forEach(c => { c.working = []; });
      state = Object.assign(freshState(), s);
      return true;
    } catch (e) {
      state = freshState();
      return false;
    }
  }

  function reset() {
    state = freshState();
    emit({ type: 'changed' });
  }

  global.Sim = {
    AwsError, REGION, ACCOUNT,
    on: fn => listeners.push(fn),
    getState: () => state,
    now, serialize, load, reset, advance,
    queueArn, queueUrl, topicArn,
    // SQS
    createQueue, updateQueue, deleteQueue, counts,
    sendMessage, receiveMessages, deleteMessage, changeVisibility, purgeQueue,
    redriveDlq, dlqSources,
    getConsumer, setConsumer,
    // SNS
    createTopic, deleteTopic, subscribe, unsubscribe, publish,
    matchFilter, validateFilterPolicy, parseFilterPolicy, parseAttributes
  };
})(typeof window !== 'undefined' ? window : globalThis);
