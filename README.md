# Aiven exercise

Two simple tools to complete the following assignment:

    Your task is to implement a system that generates some events and passes the events
    through Aiven Kafka instance to Aiven PostgreSQL database.
    For this, you need a Kafka producer which sends data to a Kafka topic,
    and a Kafka consumer storing the data to Aiven PostgreSQL database.

kafka_producer.py will generate an event every second and send it to Kafka.

kafka_consumer.py will consume said events and store in a PostgreSQL database called **exercise**, table **test_data**

Unit tests included in the tests folder
