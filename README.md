# Custom Message Queue
### Problem Statement:

Design an efficient in-memory queueing system with low latency requirements.

##### Functional specification:
1. Queue holds JSON messages
2. Allow subscription of Consumers to messages that match a particular expression
3. Consumers register callbacks that will be invoked whenever there is a new message
4. Queue will have one producer and multiple consumers
5. Consumers might have dependency relationships between them. For ex, if there are three consumers A, B and C. One dependency relationship can be that C cannot consume a particular message before A and B have consumed it. C -> (A,B) (-> means must process after)
6. Queue is bounded in size and completely held in-memory. Size is configurable.
7. Handle concurrent writes and reads consistently between producer and consumers.
8. Provide retry mechanism to handle failures in message processing.

