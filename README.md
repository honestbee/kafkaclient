# Prelude

The context of this retry is not retrying to publish or consume from kafka if kafka is not reachable. In some kafka library, for example [sarama](https://github.com/Shopify/sarama), they have implemented the retrier if they are unable to publish or consume from kafka. So what does this retry mean ?

This retry means that retry to consume the same message after some amount of time (configurable) because at the moment, there is some issue on processing that message (maybe there is some flaky error in the database or other reasons). By delaying that message for some time and retrying it again, hope that the issue has been recovered and able to process that message successfully.

# Why do we need it

Sometimes we can get an intermittent error while processing the kafka message because of some issues, for example:
- There is some flaky issue on db / other microservices / external API
- Bad deployment on some services
- Database schema is mismatched
- etc

One of the naive solution is to run a publisher manually to re-publish the message that is failed to be processed.

# How it works

The general overview of the flow:

`sample_topic` -> `sample_retry_1_topic` -> `sample_retry_2_topic` -> ... -> `dead_letter_queue_topic`

Number of retry topic depends to the maximum number of retry that we set. If it reaches maximum number of retry but it is still failed, the message will go to `dead_letter_queue`. DLQ (Dead Letter Queue) can be used to collect these failures for visibility and diagnosis and maybe manual resolution for the message.

Each topic has their own retry topic(s), but they share the same DLQ.

### Processing retry in separate queue
Doing retry on the client (without publishing to separate topic) can block the other message from processing. For example if there is a bad message in the queue, the client will consume it and fail and will wait for some delay before consuming it again. Without a success response, the client will not commit a new offset and the bad message will block the entire queue until the client reaches the limit of maximum retry.

Doing retry by publishing it to separate topic won't block the entire queue. If there is a bad message, the client will publish it to another retry topic and continue to consume another message. On each retry topic, there is separate consumer to consume and retry the message.

The structure of the message for every retry will be the same. There won't be any additional field added to the message.

### Every retry attempt will have their own topic and consumer
We will have some retry topics based on the number of maximum attempt. For example if the number of maximum attempt is 3, then we will have 3 retry topics (`sample_retry_1`, `sample_retry_2`, `sample_retry_3`).  If we consume the message from `sample_retry_3` topic, it means that we are retrying the message for the third attempt and we can calculate the delay based on it. Each retry topic has a dedicated consumer. On each consumer, we injected the number of attempt as a property for the consumer. Number of attemp count won't be recorded in the message, but we can get it from the consumer.

# Don't use it if ...

This retry scenario is not for every usecases. For some usecases where ordering in the topic is important, this retry scenario will mess up the ordering.

# Example

Go to the `example` folder for some examples of how to use the library.

# Reference

[https://eng.uber.com/reliable-reprocessing/](https://eng.uber.com/reliable-reprocessing/)
