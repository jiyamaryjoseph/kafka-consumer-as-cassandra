import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from src.entity.generic import Generic
from src.kafka_config import sasl_conf
from src.kafka_logger import logging

from src.cloud_storage.cassendra_confg import CassandraOperation





def consumer_using_sample_file(topic,file_path):
    schema_str = Generic.get_schema_to_produce_consume_data(file_path=file_path)
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Generic.dict_to_object)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group2',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    cassendra = CassandraOperation()
    
    cassendra.create_table_from_schema(schema_str)
    records = []
    x = 0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            
            record: Generic = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            logging.info("record done")
            # mongodb.insert(collection_name="car",record=car.record)

            if record is not None:
                records.append(record.to_dict())
                if x % 1000 == 0:
                    
                    
                    print(records)
                    cassendra.divide_and_insert_data(records)
                    print(records)
                    records = []
                    logging.info("next 5000 starting")
            x = x + 1
        except KeyboardInterrupt:
            break

    consumer.close()
