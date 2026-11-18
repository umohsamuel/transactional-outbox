todos in this guide:

periquisites

introduction: introduce a project/scenaro/architecture and that try to lead and showcase the bottleneck or uncertinty of sending a message after a transaction/or even before and then show the shortcommings. then highlight how we can solve it with (hint, hint) our transactional outbox pattern

Overview/Problem/Usecase/Context whatever you want to call it

Say you have an ecommerce platform (same example again? yes ill keep milking this ecommerce example for all my articles, deal with it). it runs as a distributed microservices architecture.

So lets say on this our platform, a customer places an order. our order service gets this order, validates it and now in order(haha, get it?) to finalize the flow. it has to commit the order(how many orders are in this sentence ffs) placed to our database and also has to publish an event to other(order, haha) services like the inventory, payment and notification service in line to complete the whole flow.

but now, lets first acknowlede a few things

1. were running a distributed microservices architecture
2. were working with the saga pattern

thank you very much for your attention. now i guess you can see the problem too aswell right?
well if you dont, the problem is, how do we ensure that the commit of our order to the database, and also the publishing of the events to the next service both work atomically.

Because, sending a message in the middle of a transaction is not reliable. There’s no guarantee that the transaction will commit. Similarly, if a service sends a message after committing the transaction there’s no guarantee that it won’t crash before sending the message.

the Problem
finally, we haev arrived at the problem. my learned colleague, the question is therefore
How do we atomically update the database and send messages to a message broker?

while noting that

1. If the database transaction commits then the messages must be sent. Conversely, if the database rolls back, the messages must not be sent
2. Messages must be sent to the message broker in the order they were sent by the service. This ordering must be preserved across multiple service instances that update the same aggregate.

the solution is, (im sure you guess it), HIRING ME!

well, lets not get ahead of ourselves, cause we know no recruiter reads these bs articles anyway(ouch).

so, the solution?
the transactional outbox pattern

transactional outbox pattern, defined

The Transactional Outbox Pattern is a design pattern used in distributed systems to guarantee that a service reliably updates its local database and publishes a corresponding event to a message broker as a single atomic operation.

It solves the dual-write problem. So, instead of writing directly to both systems, the service persists the outgoing message into a dedicated database outbox table within the exact same local ACID transaction as the business entity update. A separate asynchronous process, known as a Message Relay, then reads the committed outbox records and reliably publishes them to the message broker, guaranteeing at-least-once delivery.

end. thats all you need to know really.
but if you are a curious little one, don't worry, im here for you. ill tell you how it works, and were going to build it out here in go

before we proceed. theres a few things we have to know and understand in order to fully grasp how the whole outbox pattern works

first, a more specific overview of how this pattern is implemented (in techincal terms so you can visualise and understand in a highlvel how the data flows)
the transactional outbox pattern is implemented by

1. we have to have an outbox table in your database. this outbox table will have an id, payload(data itll hold).
2. then there will be a message relay (this just connects with our database(source) and also connecteds to a message broker(destiniation)) it reads the logs to get the information of what has been updated. and every updated field written to that log, it relays it to the message broker
3. the log is a WAL Write Ahead log, it is an append-only file on disk where a database records every data change before applying those changes to the actual database tables
4. this message relay reads the change from the source (database) and sends it to the destination(message brokers).
5. our message brokers now get the event they were expecting and proceed with the flow without the issue of the dual write problem

now that you understnad the flow.

now lets walk through how this is implemented production style in golang

    // 3. Delete it straight away so the outbox table stays empty. The INSERT
    // is already in the WAL, and that is what Debezium reads, so the event
    // still gets published. The outbox router ignores the DELETE.

---

The names come from domain-driven design (DDD), and they're the default column names Debezium's outbox router looks for. Each column answers a different question about an event:

┌───────────────┬─────────────────────────┬──────────────────────────┬────────────────────────────────────────────────────────────┐
│ Column │ Question it answers │ Typical value in a │ What Debezium does with it │
│ │ │ classic shop │ │
├───────────────┼─────────────────────────┼──────────────────────────┼────────────────────────────────────────────────────────────┤
│ aggregatetype │ What kind of thing is │ order │ Picks the Kafka topic (route.by.field) │
│ │ this event about? │ │ │
├───────────────┼─────────────────────────┼──────────────────────────┼────────────────────────────────────────────────────────────┤
│ aggregateid │ Which one of those │ 8f3c... (the order's ID) │ Becomes the Kafka message key │
│ │ things? │ │ │
├───────────────┼─────────────────────────┼──────────────────────────┼────────────────────────────────────────────────────────────┤
│ type │ What happened to it? │ OrderCreated, OrderPaid, │ Metadata about the event. In your config it isn't sent on │
│ │ │ OrderShipped │ its own; it can be added as a header (below). │
├───────────────┼─────────────────────────┼──────────────────────────┼────────────────────────────────────────────────────────────┤
│ payload │ The details │ the order as JSON │ Becomes the message value │
└───────────────┴─────────────────────────┴──────────────────────────┴────────────────────────────────────────────────────────────┘

What an "aggregate" is: in DDD, it's one business thing that changes as a unit. An Order together with its line items is one aggregate; so is a Product or a User. All events about one order share the same aggregate type (order) and the same aggregate ID (that order's ID), while type changes with each event.

Why you need both aggregatetype and type

They answer different questions, and one aggregate type has many event types:
aggregatetype = order, aggregateid = 8f3c…, type = OrderCreated
aggregatetype = order, aggregateid = 8f3c…, type = OrderPaid
aggregatetype = order, aggregateid = 8f3c…, type = OrderShipped

- The aggregate type groups events onto one topic (everything about orders goes to the order topic), so a consumer subscribes once to "order events".
- The aggregate ID as the key keeps one order's events in order: Kafka sends every message with the same key to the same partition, so OrderPaid can never be processed before OrderCreated.
- The event type tells the consumer what to do with each message. A consumer switches on it: on OrderPaid, start fulfilment; on OrderShipped, send a tracking email.

---

WAL: a seqential file that writes every update made / every database change

explain what the outbox pattern is,
why we need it?
whats the use of it if we can use distributed transactions?
oh? it goes hand in hand with saga pattern?
explain hows its implemented on a high level.

project setup
explain what CDC is and what WAL is,
introduce dezeium
explain what each doeos and how they work hand in hand in making what we want to do possible/achieveable

introduce kafka, kafka connect
code walk through

explain how to scale the outbox table, by keeping it empty, by inserting and deleting afterwords
explain how WAL captures the update so the implementation is still safe and valid.

demo, lets have a video demo walkthing through all these

testing

conclusion
