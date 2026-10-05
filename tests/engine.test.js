// Run with:  node tests/engine.test.js
require('../js/engine.js');
const Sim = globalThis.Sim;
const assert = require('assert');
let passed = 0;
const test = (name, fn) => { Sim.reset(); fn(); passed++; console.log('  ok  ' + name); };
const q = n => Sim.getState().queues[n];

test('standard queue: send, receive, in-flight, delete', () => {
  Sim.createQueue('q1');
  Sim.sendMessage('q1', { body: 'hello' });
  assert.deepStrictEqual(Sim.counts(q('q1')), { available: 1, inflight: 0, delayed: 0 });
  const [m] = Sim.receiveMessages('q1');
  assert.strictEqual(Sim.counts(q('q1')).inflight, 1);
  assert.strictEqual(Sim.receiveMessages('q1').length, 0, 'in-flight message is hidden');
  Sim.deleteMessage('q1', m.receiptHandle);
  assert.strictEqual(q('q1').messages.length, 0);
});

test('visibility timeout returns the message', () => {
  Sim.createQueue('q1', { visibilityTimeout: 10 });
  Sim.sendMessage('q1', { body: 'x' });
  const [m] = Sim.receiveMessages('q1');
  Sim.advance(9000);
  assert.strictEqual(Sim.counts(q('q1')).inflight, 1);
  Sim.advance(2000);
  assert.deepStrictEqual(Sim.counts(q('q1')), { available: 1, inflight: 0, delayed: 0 });
  assert.throws(() => Sim.deleteMessage('q1', m.receiptHandle), /receipt handle/);
});

test('delivery delay', () => {
  Sim.createQueue('q1', { delaySeconds: 20 });
  Sim.sendMessage('q1', { body: 'x' });
  assert.strictEqual(Sim.counts(q('q1')).delayed, 1);
  Sim.advance(21000);
  assert.strictEqual(Sim.counts(q('q1')).available, 1);
});

test('retention removes old messages', () => {
  Sim.createQueue('q1', { retention: 60 });
  Sim.sendMessage('q1', { body: 'x' });
  Sim.advance(61000);
  assert.strictEqual(q('q1').messages.length, 0);
});

test('dead-letter queue after maxReceiveCount, then redrive', () => {
  Sim.createQueue('dlq');
  Sim.createQueue('main', { visibilityTimeout: 5, dlqName: 'dlq', maxReceiveCount: 3 });
  Sim.sendMessage('main', { body: 'bad' });
  for (let i = 0; i < 3; i++) { Sim.receiveMessages('main'); Sim.advance(6000); }
  assert.strictEqual(q('main').messages.length, 0);
  assert.strictEqual(q('dlq').messages.length, 1);
  assert.strictEqual(q('dlq').messages[0].deadLetterSource, 'main');
  assert.strictEqual(Sim.redriveDlq('dlq'), 1);
  assert.strictEqual(q('main').messages.length, 1);
  assert.strictEqual(q('main').messages[0].receiveCount, 0);
});

test('DLQ validation', () => {
  Sim.createQueue('a'); Sim.createQueue('b.fifo', { type: 'fifo' });
  assert.throws(() => Sim.updateQueue('a', { dlqName: 'a' }), /own dead-letter/);
  assert.throws(() => Sim.updateQueue('a', { dlqName: 'b.fifo' }), /same type/);
  assert.throws(() => Sim.updateQueue('a', { dlqName: 'nope' }), /does not exist/);
});

test('queue name rules', () => {
  assert.throws(() => Sim.createQueue('bad name'), /letters, numbers/);
  assert.throws(() => Sim.createQueue('x', { type: 'fifo' }), /\.fifo/);
  assert.throws(() => Sim.createQueue('x.fifo'), /FIFO/);
  Sim.createQueue('ok.fifo', { type: 'fifo' });
  assert.throws(() => Sim.createQueue('ok.fifo', { type: 'fifo' }), /already exists/);
});

test('FIFO: order, group lock, dedup', () => {
  Sim.createQueue('f.fifo', { type: 'fifo' });
  assert.throws(() => Sim.sendMessage('f.fifo', { body: '1', groupId: 'g' }), /deduplication/);
  ['1', '2', '3'].forEach(b => Sim.sendMessage('f.fifo', { body: b, groupId: 'g', dedupId: 'd' + b }));
  const dup = Sim.sendMessage('f.fifo', { body: '1', groupId: 'g', dedupId: 'd1' });
  assert.strictEqual(dup.duplicate, true);
  assert.strictEqual(q('f.fifo').messages.length, 3);
  const [a] = Sim.receiveMessages('f.fifo', { max: 1 });
  assert.strictEqual(a.body, '1');
  assert.strictEqual(Sim.receiveMessages('f.fifo', { max: 5 }).length, 0, 'group g is locked');
  Sim.deleteMessage('f.fifo', a.receiptHandle);
  const rest = Sim.receiveMessages('f.fifo', { max: 5 });
  assert.deepStrictEqual(rest.map(m => m.body), ['2', '3']);
  Sim.advance(6 * 60 * 1000);
  Sim.sendMessage('f.fifo', { body: '1', groupId: 'g', dedupId: 'd1' }); // window over -> accepted
});

test('FIFO content-based dedup', () => {
  Sim.createQueue('f.fifo', { type: 'fifo', contentBasedDedup: true });
  Sim.sendMessage('f.fifo', { body: 'same', groupId: 'g' });
  assert.strictEqual(Sim.sendMessage('f.fifo', { body: 'same', groupId: 'g' }).duplicate, true);
});

test('SNS fan-out + filter policies', () => {
  Sim.createQueue('all'); Sim.createQueue('physical'); Sim.createQueue('big');
  Sim.createTopic('orders');
  Sim.subscribe('orders', { protocol: 'sqs', endpoint: 'all' });
  Sim.subscribe('orders', { protocol: 'sqs', endpoint: 'physical', filterPolicy: '{"type":["physical"]}' });
  Sim.subscribe('orders', { protocol: 'sqs', endpoint: 'big', filterPolicy: { amount: [{ numeric: ['>=', 1000] }] }, rawMessageDelivery: true });
  Sim.subscribe('orders', { protocol: 'email', endpoint: 'ops@example.com' });

  let r = Sim.publish('orders', { message: 'order 1', attributes: Sim.parseAttributes('type=physical\namount=499') });
  assert.deepStrictEqual(r.results.map(x => x.delivered), [true, true, false, true]);
  r = Sim.publish('orders', { message: 'order 2', attributes: Sim.parseAttributes('type=digital\namount=2500') });
  assert.deepStrictEqual(r.results.map(x => x.delivered), [true, false, true, true]);
  assert.strictEqual(q('all').messages.length, 2);
  assert.strictEqual(q('physical').messages.length, 1);
  assert.strictEqual(q('big').messages[0].body, 'order 2', 'raw delivery = body only');
  assert.strictEqual(JSON.parse(q('all').messages[0].body).Message, 'order 1', 'envelope otherwise');
  assert.strictEqual(Sim.getState().endpointLog.length, 2);
});

test('filter rules: prefix, anything-but, exists, OR values', () => {
  const A = Sim.parseAttributes('region=ap-south-1\nvip=yes');
  assert.ok(Sim.matchFilter({ region: [{ prefix: 'ap-' }] }, A).matched);
  assert.ok(!Sim.matchFilter({ region: [{ 'anything-but': ['ap-south-1'] }] }, A).matched);
  assert.ok(Sim.matchFilter({ vip: [{ exists: true }] }, A).matched);
  assert.ok(Sim.matchFilter({ missing: [{ exists: false }] }, A).matched);
  assert.ok(Sim.matchFilter({ vip: ['no', 'yes'] }, A).matched, 'OR inside a key');
  assert.ok(!Sim.matchFilter({ vip: ['yes'], region: ['us-east-1'] }, A).matched, 'AND across keys');
  assert.ok(!Sim.matchFilter({ vip: ['yes'] }, {}).matched);
});

test('filter policy validation', () => {
  assert.throws(() => Sim.parseFilterPolicy('{oops'), /not valid JSON/);
  assert.throws(() => Sim.parseFilterPolicy('{"a":"b"}'), /non-empty array/);
  assert.throws(() => Sim.parseFilterPolicy('{"a":[{"wat":1}]}'), /Invalid filter rule/);
  assert.strictEqual(Sim.parseFilterPolicy('  '), null);
});

test('subscription validation', () => {
  Sim.createTopic('t'); Sim.createQueue('f.fifo', { type: 'fifo' }); Sim.createQueue('s');
  assert.throws(() => Sim.subscribe('t', { protocol: 'sqs', endpoint: 'f.fifo' }), /Standard/);
  assert.throws(() => Sim.subscribe('t', { protocol: 'email', endpoint: 'nope' }), /valid email/);
  Sim.subscribe('t', { protocol: 'sqs', endpoint: 's' });
  assert.throws(() => Sim.subscribe('t', { protocol: 'sqs', endpoint: 's' }), /already subscribed/);
});

test('auto-consumer succeeds and poison message ends up in the DLQ', () => {
  Sim.createQueue('dlq');
  Sim.createQueue('work', { visibilityTimeout: 10, dlqName: 'dlq', maxReceiveCount: 3 });
  Sim.sendMessage('work', { body: 'fine' });
  Sim.sendMessage('work', { body: 'poison pill' });
  Sim.setConsumer('work', { enabled: true, pollEvery: 1, processTime: 1 });
  Sim.advance(120000);
  assert.strictEqual(q('work').messages.length, 0);
  assert.strictEqual(q('dlq').messages.length, 1);
  assert.match(q('dlq').messages[0].body, /poison/);
  assert.strictEqual(q('work').stats.deleted, 1);
});

test('changeVisibility(0) releases immediately', () => {
  Sim.createQueue('q');
  Sim.sendMessage('q', { body: 'x' });
  const [m] = Sim.receiveMessages('q');
  Sim.changeVisibility('q', m.receiptHandle, 0);
  assert.strictEqual(Sim.counts(q('q')).available, 1);
});

test('purge, delete queue cleans subscriptions and redrive', () => {
  Sim.createQueue('d'); Sim.createQueue('s', { dlqName: 'd' }); Sim.createTopic('t');
  Sim.subscribe('t', { protocol: 'sqs', endpoint: 's' });
  Sim.sendMessage('s', { body: 'x' }); Sim.purgeQueue('s');
  assert.strictEqual(q('s').messages.length, 0);
  Sim.deleteQueue('d'); assert.strictEqual(q('s').redrive, null);
  Sim.deleteQueue('s'); assert.strictEqual(Sim.getState().topics.t.subscriptions.length, 0);
});

test('persistence round trip', () => {
  Sim.createQueue('q'); Sim.sendMessage('q', { body: 'x' });
  const json = Sim.serialize();
  Sim.reset(); assert.strictEqual(Object.keys(q('') ? {} : Sim.getState().queues).length, 0);
  assert.ok(Sim.load(json));
  assert.strictEqual(q('q').messages.length, 1);
  assert.strictEqual(Sim.load('garbage'), false);
});

console.log(`\n${passed} tests passed`);
