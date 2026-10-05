# SQS + SNS Simulator

A browser-based simulator of **Amazon SQS** (Simple Queue Service) and **Amazon SNS** (Simple Notification Service). Create queues and topics, publish messages, and watch them travel through the system in real time. Everything runs in the browser, with no backend and no AWS account.

**Live demo:** `https://<your-username>.github.io/<repo-name>/` (replace after deploying)

> Built for the *AWS Service Simulation & Enhancement* assignment.

## What it simulates

### Amazon SQS
- **Standard and FIFO queues**, with AWS-style name validation (FIFO names end in `.fifo`).
- **Send, receive and delete** messages, with message attributes.
- **Visibility timeout**: a received message becomes *in flight* and is hidden from other consumers, and returns to the queue if it is not deleted in time.
- **Delivery delay**, **message retention**, receipt handles, receive counts.
- **FIFO behaviour**: strict ordering per message group, group locking while a message is in flight, and 5 minute deduplication (explicit ID or content-based).
- **Dead-letter queues** with a configurable max-receive count.

### Amazon SNS
- **Topics** and **subscriptions** (SQS queue, email, HTTP, the last two simulated).
- **Publish and fan-out**: one message, one copy per matching subscriber.
- **SNS message envelope** (the JSON wrapper SNS puts around messages delivered to SQS), or **raw message delivery**.

## Enhancements beyond basic SQS and SNS

| # | Enhancement | What it adds |
|---|---|---|
| 1 | **Live message-flow animation** | An SVG diagram of producer, topics, queues and consumers. Dots travel along the real paths as events happen, and queue counters update live. |
| 2 | **Dead-letter queue workflow** | An auto-consumer that can fail (random rate, or "poison" messages that always fail). After N failed receives the message moves to the DLQ, and a **redrive** button sends it back. |
| 3 | **Filter-policy engine with explanations** | Subscribers can filter on message attributes (exact, `prefix`, `suffix`, `anything-but`, `numeric`, `exists`). Every publish shows *why* each subscriber did or did not receive the message. |
| 4 | **Simulated clock** | Pause, run at 10x or 60x, or jump forward by 10 s, 1 min, 1 h or 1 day to see timeouts and retention without waiting. |
| 5 | **Guided demo** | An 8-step narrated walkthrough of the whole lifecycle. |
| 6 | **Monitoring chart** | Per-queue graph of available, in-flight and delayed messages over simulated time. |

## Run it locally

No build step. Open `index.html` in a browser, or serve the folder:

```bash
python3 -m http.server 8000
# then open http://localhost:8000
```

Run the engine tests (Node 18+):

```bash
node tests/engine.test.js
```

## Deploy on GitHub Pages

1. Create a new GitHub repository and push this folder to it:
   ```bash
   git init
   git add .
   git commit -m "SQS + SNS simulator"
   git branch -M main
   git remote add origin https://github.com/<your-username>/<repo-name>.git
   git push -u origin main
   ```
2. On GitHub open **Settings, then Pages**.
3. Under **Build and deployment**, choose **Deploy from a branch**, branch `main`, folder `/ (root)`, and save.
4. After a minute the site is live at `https://<your-username>.github.io/<repo-name>/`. Put that link at the top of this README.

Netlify and Vercel also work: drag in the folder, no settings needed.

## Project structure

```
sqs-sns-simulator/
├── index.html          Page shell: header, navigation, script tags
├── css/style.css       Styling
├── js/
│   ├── engine.js       The simulation: SQS, SNS, clock, consumers (no DOM code)
│   ├── flow.js         Animated message-flow diagram (SVG)
│   └── app.js          UI: routing, views, forms, guided demo, persistence
├── tests/
│   └── engine.test.js  17 unit tests for the engine (run with Node)
└── docs/
    └── VIVA_GUIDE.md   Demo script and likely questions
```

**Design choice:** the engine knows nothing about the page. The UI only calls `Sim.*` functions and listens to the events the engine emits. This keeps the "AWS logic" separate and testable, and it is why the animation can react to real events instead of being scripted.

## How the key parts work

- **Simulated clock.** `Sim.advance(ms)` moves time forward. Every step, the engine releases delayed messages, returns in-flight messages whose visibility timeout ended (or dead-letters them), and deletes messages past retention. Large jumps are split into steps so consumers still behave.
- **Visibility timeout.** `receiveMessages` marks a message `inflight`, increments its receive count, issues a new receipt handle, and sets `availableAt = now + visibilityTimeout`. Only a matching handle can delete it.
- **Dead-letter queue.** When an in-flight message times out and its receive count has reached `maxReceiveCount`, it moves to the DLQ instead of becoming available.
- **Fan-out.** `publish` loops over subscriptions, evaluates each filter policy against the message attributes, and delivers a copy to every match.
- **Persistence.** State is saved to `localStorage` and restored on reload. **Reset all** clears it.

## Simplifications compared with real AWS

- One in-browser state, not a distributed system.
- The DLQ move happens when the visibility timeout ends on the last allowed receive (real SQS moves it on the next receive attempt).
- Standard queues approximate "best-effort ordering" by picking randomly among the first three available messages.
- Email and HTTP subscriptions are simulated (shown in an inbox panel), and are confirmed automatically. SQS access policies are not modelled.
- The message inspector table is a simulator feature. Real SQS cannot list messages without receiving them.

## Tech

HTML, CSS and vanilla JavaScript. No frameworks, no build tools, no dependencies. Fonts: IBM Plex Sans and IBM Plex Mono via Google Fonts (the app falls back to system fonts offline).
