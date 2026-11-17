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

Because, without using 2PC, sending a message in the middle of a transaction is not reliable. There’s no guarantee that the transaction will commit. Similarly, if a service sends a message after committing the transaction there’s no guarantee that it won’t crash before sending the message.

whats is the transactional outbox pattern

The Transactional Outbox Pattern is a design pattern used in distributed systems to guarantee that a service reliably updates its local database and publishes a corresponding event to a message broker as a single atomic operation.

The Transactional Outbox Pattern is a microservices design pattern that solves the dual-write problem by enabling a service to atomically update its local database and publish a corresponding message or event to a message broker without requiring a distributed transaction (such as Two-Phase Commit). Instead of writing directly to both systems, the service persists the outgoing message into a dedicated database outbox table within the exact same local ACID transaction as the business entity update. A separate asynchronous process—known as a Message Relay—then reads the committed outbox records and reliably publishes them to the message broker, guaranteeing at-least-once delivery.

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
