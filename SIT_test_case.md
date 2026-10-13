# System Integration Test (SIT) Strategy

## Question

Our pipeline works like this:

1. **Service1** publishes change events to Kafka.
2. The **system under test (SUT)** consumes those events from Kafka.
3. The SUT persists each event to a Postgres inbox.
4. The SUT sends the inbox records to Salesforce.

Service1 is owned elsewhere and is **not** part of the SUT. What is the ideal system integration test
setup for the SUT, and which test cases should it cover?

## Answer

Treat Kafka as the system boundary on the input side and Salesforce as the boundary on the output side.
The test plays the role of Service1 by publishing events straight to a real Kafka topic. Postgres stays
real, and Salesforce is replaced by a faithful HTTP stub.

### Ideal setup (C#)

- **Kafka and Postgres:** run both as real instances via Testcontainers. Don't mock them, because offset
  commits, consumer groups and transactions are where integration bugs live.
- **Salesforce:** use WireMock.Net to stub the composite API endpoints and the OAuth token call, with
  responses copied from real Salesforce payloads, including partial failures (`allOrNone=false`
  responses, or a Composite Graph response where one graph has `isSuccessful: false`).
- **SUT:** run it in-process with `WebApplicationFactory`/`IHost`, configured to point at the containers.
- **Assertions:** check every boundary: the inbox row and its status in Postgres, the exact request body
  WireMock received, and the committed consumer offset. Poll with a timeout instead of using
  `Thread.Sleep`.

### Test cases

| # | Scenario | Steps | Expected result |
|---|---|---|---|
| 1 | Happy path | Publish an Account change event. | The inbox row exists, the composite request to Salesforce carries the correctly mapped fields, and the inbox row is marked sent. |
| 2 | Duplicate event | Publish the same event twice. | One inbox row and one Salesforce call. This proves the idempotency key works. |
| 3 | Ordering | Publish two updates for the same Account. | Salesforce ends up with the latest update, never a stale one. |
| 4 | Salesforce transient failure | Stub returns 503 or 429 first, then 200. | The retry happens and the row ends up sent. |
| 5 | Salesforce permanent failure | Stub returns 400 or a partial failure in the composite response. | The row is marked failed or dead-lettered and doesn't retry forever. |
| 6 | Poison or malformed message | Publish invalid JSON, then a valid event. | The consumer doesn't get stuck, the bad message is quarantined, and later valid events still flow. |
| 7 | Crash recovery | Stop the SUT after the inbox write but before the send, then restart it. Optionally, also kill it before the offset commit. | The event is sent exactly once, and no data is lost. |
| 8 | Salesforce unreachable at startup | Start with the stub down, publish events, then bring the stub back. | Events still land in the inbox and drain once the stub is back. This is the point of having an inbox. |

### Covering the Service1 gap

Since Service1 isn't in the SUT, its event shape is an assumption. Close that gap with a consumer-driven
contract test (Pact, or schema-registry compatibility checks if you use Avro or JSON Schema). Build test
events from those same contract or sample files rather than hand-written JSON, so a schema change on
Service1's side breaks a test instead of production.

### Real Salesforce sandbox

Run a smaller set of tests against a real Salesforce sandbox as well, for example nightly. The stub proves
the SUT's logic, but only the sandbox proves the field mappings and authentication actually work.
