# Priority Queue Implementation with AWS SQS using .NET

This in-depth code sample implements priority queues in .NET using AWS SQS service. 
> This sample is provided as is, with no warranty of any kind. Use at your own discretion.

## Motivation and Goals

Priority queues is a common enterprise use case, and implementing it using AWS SQS is a common need, while also being somewhat non-trivial to get it right.

The main issue to deal with when implementing priority queues with SQS is stemming from the fact that SQS queues are not pushing messages to consumers. Instead, consumers must poll queues for messages. Polling SQS queues can get expensive if done too frequently, and so polling of multiple queues _the right way_ is the main goal of this sample. 
> The "right way" here means providing a _small set of configuration settings that regulate the **balance between SQS request $$ spend, CPU utilization efficiency, and message wait time**_.

## Design Outline

The heart of the solution is well-thought-out implementation of moderately tricky logic required to cycle through checking all priority queues in such a way that _long-polls (blocks & waits for) the highest priority queue only when we know all queues are empty_. If we suspect any queue could be non-empty, then we short-poll all queues. That's the main feature of this sample. 

Another facet presenting a distinct set of trade-offs addressed by this sample is how message processing _parallelism_ should be configured: 
1. Multiple threads (inside a few or a single process) option. This enables highest density of processing and CPU utilization. This option is preferable mostly when running this code on a machine, physical or VM.
2. Multiple processes (with a single processing thread) option. This means higher overhead due to costs of processes and containers, but may be simpler in operations. Here each container is allocated some vCPU and whether the container waits or processes is not that important for CPU utilization since other co-hosted containers can utilized available CPU.

## Configuration Settings and corresponding trade-offs

Following are configuration settings of the solution, with explanation how different values affect trade-offs balanced by this sample.

* QueueArns. Comma-delimited list of SQS ARNs. First one in the list is the top priority, the last one is lowest priority. Fewer queues = less SQS spend. More queues = more flexibility in priorities.
* ProcessorCount. Number of parallel worker threads fetching and processing messages. Fewer processors = less SQS spent. More processors = fuller CPU utilization of a VM. Typically, use value of 1 if running this code in multiple co-located containers. Use value of more than 1 when running w/o containers (on a VM) and want to achieve highest useful CPU utilization.
* MessageBatchSize. Number of messages picked in a single SQS request. Value of 1 = better CPU utilization and least possible amount of wait time for higher priority messages. Value > 1 = less SQS spend. 
* HighPriorityWaitTimeoutSeconds: long-polling wait time for highest-priority queue  when all queues are empty (all other lower-priority queues are always short-polled). Longer wait = less SQS spend. Shorter wait = less wait time for new messages in lower-priority queues.
* VisibilityTimeoutOnProcessingFailureSeconds. Defines default processing re-try delay for failed messages. Use it together with AWS DLQ for proper quick retries and routing to the dead-letter queue.

## I want to...

* ...minimize my SQS $$ spend.
  * Use fewer queues.
  * Use fewer processors (as few processors as needed to keep queues close to empty, but not empty).
  * Use larger message batch size (more than 1, up to 10).
  * Use longer long-polling delay of the high-priority queue.

* ...maximize CPU utilization of expensive CPU-optimized EC2 instances:
  * Use more processors.
  * Use smaller message batch size (1 is best).
  * Use shorter long-polling delay of the high-priority queue.

* ...have minimum delay before a message is picked from a queue.
  * Use more processors.
  * Use shorter long-polling delay of the high-priority queue.

* ...have higher-priority messages wait shortest possible amount of time.
  * Use more processors.
  * Use smaller message batch size (1 is best).
