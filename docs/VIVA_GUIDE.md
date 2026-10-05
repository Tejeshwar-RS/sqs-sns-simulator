# Demo and viva guide

## Mapping to the evaluation rubric

| Criteria (marks) | Where to point |
|---|---|
| **Understanding of the AWS service (2)** | The **How it works** page in the app: the problem, SQS (pull) vs SNS (push), fan-out, the message lifecycle diagram, Standard vs FIFO, a glossary, and a table of what is simulated vs simplified. |
| **Core functionality / simulation (3)** | Queues (Standard and FIFO), send / receive / delete, visibility timeout, delay, retention, receipt handles, topics, subscriptions, publish and fan-out. All in `js/engine.js`, covered by 17 tests. |
| **Enhancement / tweak (2)** | Live message-flow animation, DLQ workflow with a failing consumer and redrive, filter-policy engine with explanations, simulated clock. |
| **UI/UX and working website (1)** | Console-style layout, live counters, toasts for every action, clear error messages, empty states, responsive layout, keyboard-friendly. |
| **Deployment and GitHub (1)** | Live GitHub Pages link in the README, structured repo, commit history. |
| **Demonstration and explanation (1)** | Script below. |

## 4 minute demo script

Press **Start guided demo** on the Message flow page and use **Next** to move through the steps. Say this at each one:

1. **Setup.** "This is an SNS topic called *orders* feeding two SQS queues. The queue on the right of the diagram is a dead-letter queue. Shipping only wants physical orders and finance only wants orders of 1000 or more. Those are filter policies."
2. **Physical order.** "One publish, two copies. This is **fan-out**. The email was skipped because the amount is under 1000."
3. **Digital order.** "Now shipping is blocked by its filter (the red cross). If I open the topic I can read the exact reason for each decision."
4. **Receive.** "Polling makes the message **in flight**. For the visibility timeout it is invisible to other consumers. If the worker crashes, it comes back. That is why SQS is at-least-once."
5. **Delete.** "Deleting with the receipt handle means the work is finished."
6. **Poison message.** "This message always fails. I am running the clock at 60x. Watch it return to the queue again and again."
7. **DLQ.** "After 3 failed receives it moved to the dead-letter queue, so it stops blocking the queue."
8. **Redrive.** "After the bug is fixed, redrive sends it back to the source queue."

Then spend 30 seconds on the code: open `engine.js` and show `receiveMessages`, `releaseOrDlq` and `publish`.

## Likely questions and answers

**What happens if a consumer crashes after receiving a message?**
The message stays in flight until the visibility timeout ends, then becomes available again and another consumer receives it. So delivery is *at least once*, and consumers should be able to handle the same message twice.

**What is the difference between SQS and SNS?**
SQS is a queue you *pull* from; each message is processed by one consumer and kept until deleted. SNS is publish/subscribe; it *pushes* every message to all subscribers immediately and does not store it for later.

**Why use them together?**
Fan-out. The producer publishes once; each service gets its own queue, so a slow or offline service simply builds a backlog without affecting the others, and adding a new service needs no change to the producer.

**Standard vs FIFO?**
Standard: very high throughput, best-effort ordering, possible duplicates. FIFO: strict order within a message group and deduplication within 5 minutes, with lower throughput. FIFO names end in `.fifo`.

**What is a dead-letter queue and why do we need one?**
A side queue for messages that fail repeatedly. Without it a poison message would be retried forever. With it, the message is parked after `maxReceiveCount` attempts so engineers can inspect and redrive it.

**What is the visibility timeout for, and what if processing takes longer?**
It gives the consumer time to process without anyone else taking the message. If processing takes longer, the message reappears and may be processed twice. Fix it by raising the timeout or extending it per message. (You can see this in the app: set the consumer's processing time higher than the queue's visibility timeout.)

**How does a filter policy work?**
It is a JSON document on a subscription, matched against message attributes. Every key must match; inside a key any value may match. Non-matching messages are not delivered to that subscriber.

**How did you simulate time?**
One simulated clock in the engine. A timer advances it by real elapsed time multiplied by the speed, and every step re-evaluates message states. The buttons at the top change the speed or jump forward.

**How is this different from real AWS?**
Real AWS is a distributed system that stores messages across multiple availability zones. This is a single in-browser state. The README lists each simplification.

**How does the animation know when to move a dot?**
The engine emits an event for every action (`published`, `delivered`, `filtered`, `received`, `dead-lettered`, ...). `flow.js` listens and animates a dot along the SVG path for that event, so the animation shows what really happened.

**What would you add next?**
Long polling, SQS access policies, SNS retry policies and delivery-failure handling, a Lambda subscriber, and message batching.

## Good things to try live if asked

- Set a queue's visibility timeout to 5 s, receive a message, and jump +10s: it reappears.
- Create a FIFO queue, send the same deduplication ID twice: the second is dropped.
- Send a message with a 60 s delivery delay, then jump +1m.
- Set retention to 60 s on a queue, send, jump +1m: the message is gone.
- Subscribe an email with `{"amount": [{"numeric": [">=", 1000]}]}` and publish `amount=5000`, then `amount=5`.
