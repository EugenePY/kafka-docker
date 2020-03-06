from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
import json
import datetime
#import logging
# logging.basicConfig(level=logging.DEBUG)

# To consume latest messages and auto-commit offsets

consumer1 = KafkaConsumer(group_id="agent-1",
                          value_deserializer=lambda m: json.loads(
                              m.decode("utf-8")),
                          max_partition_fetch_bytes=100*1048576,
                          bootstrap_servers=['localhost:9092'],
                          auto_offset_reset='earliest',
                          session_timeout_ms=100000)

TOPIC = "json-topic"
tp = TopicPartition(TOPIC, 0)
# consumer1.end_offsets([TopicPartition("json-topic", 0)])
# consume json messages
# for message in consumer1:
#    # message value and key are raw bytes -- decode if necessary!
# e.g., for unicode: `message.value.decode('utf-8')`
#    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                         message.offset, message.key,
#                                         message.value))

# consumer1.close()
#
consumer1.assign([tp])
# datetime_offset = int((datetime.datetime.utcnow() - datetime.timedelta(days=1)
#                       ).timestamp()*1000)
# print(datetime_offset)
# print(consumer1.offsets_for_times({tp: datetime_offset}))

consumer1.commit({tp: OffsetAndMetadata(0, None)})
for message in consumer1:
    #    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: timestamp=%i key=%s value=%s" % (message.topic,
                                                      message.partition,
                                                      message.offset,
                                                      message.timestamp,
                                                      message.key,
                                                      message.value))

consumer1.close()

# ps = [TopicPartition(TOPIC, p) for p in consumer1.partitions_for_topic(TOPIC)]
# print(ps)
# print(consumer1.end_offsets(ps))
