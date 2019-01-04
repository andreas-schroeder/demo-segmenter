# Kafka Streams Event Sourcing Demo

Demos various event sourcing features of Kafka Streams.

## Example Domain

The app simulates devices sending sensor data. Sending of data is initiated by a 
`DeviceWokeUp` event, and completed with a `AllDataSent` event.
This is an example of three devices successfully sending data:

```
[device-3] DeviceWokeUp()
[device-3] DataReceived(pressure,0.275)
[device-3] DataReceived(humidity,0.775)
[device-3] AllDataSent()
[device-5] DeviceWokeUp()
[device-5] DataReceived(pressure,0.858)
[device-4] DeviceWokeUp()
[device-4] DataReceived(humidity,0.500)
[device-5] AllDataSent()
[device-4] DataReceived(wind,0.327)
[device-4] DataReceived(temperature,0.980)
[device-4] DataReceived(wind,0.665)
[device-4] AllDataSent()
```

## Application Logic

The demo app segments the events received into sessions. The incoming events
are keyed by device id only. A session (started with `DeviceWokeUp` and completed
with `AllDataSent` or timeout) is used to group events so that subsequent 
processors can aggregate data session by session.

This is achieved by grouping all events by key (device), and aggregating the
events for each single key. Creating a new session on `DeviceWokeUp`, and completing 
it when receiving `AllDataSent`. Effects of message loss and retries are considered
in the aggregation logic, while out-of-order or late-arriving events are not considered.
This logic is implemented in the `Segmenter` class.

Sessions of devices need to be eventually purged from the segmenter state store. This 
is implemented in the `SessionPurger`.

## Demo Structure

* ZooKeeper/Kafka/Schema Registry: infrastructure.
* Driver: produces random events for 5 devices
* Segmenter: create sessions from events
* Reader: print session events to console 

## Demoed features

This is a list of challenges addressed in the code

* How to aggregate state from events (with groupByKey & aggregate)
* How to purge / cleanup state
* How to use Kafka Streams Scala DSL
* How to use Avro as serialization format (with avro4s for mapping to case classes)
* How to use the Confluent schema registry to register avro schemas
* How to model events with Avro type unions (and how to handle shapeless Coproduct)
* How to use event time extractors
* How to configure exactly-once processing and standby replicas
* How to unit test streams applications
* How to setup a local setup harness with docker-compose

## Commands and Tasks
Building all Docker containers - requires sbt
```bash
./build.sh
``` 

Run in demo mode (that can be read) - requires docker-compose
```bash
./run-demo.sh
``` 

Follow driver, reader, and segmenter
```bash
docker logs --since 1m -f session-segmenter_driver_1
docker logs --since 1m -f session-segmenter_reader_1
docker logs --since 1m -f session-segmenter_app_1
```


Run driver as fast as possible
```bash
run-fullspeed.sh
``` 

scale up, crash one node, observe task reassignments, scale down.