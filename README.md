# Welcome to AWS SQS Prioritizer

> Provided as is, with no warranty or liability on the part of creators and maintainers. 

Priority queue implementation with AWS SQS can be tricky, and this project aims to create well-isolated, reusable AWS infrastructure component that can be stood up in seconds with AWS CDK and have your **message processors pick items from a single output queue**, while your message publishers post messages to **multiple priority queues**. What happens in the middle is the concern of this project.

Main drivers for implementing priority queue on SQS stem from SQS features:
* Very high scalability
* Very high durability
* Low cost
* Simplicity of infrastructure
* Simplicity of usage

To use the solution you may simply run `cdk deploy` command to provision SQS queues and the  Amazon ECS container running prioriotization logic. Solution is highly parameterized, allowing operators to dial in specific behaviour, with tradeoffs being:
* Performance
* Cost
* Prioritization precision