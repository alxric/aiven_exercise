#!/usr/bin/env python
import json
import kafka
import logging
import psycopg2
import sys

from aiven import Config, Consumer, Psql

def main():
    cfg = Config("config.yml")
    c = Consumer(
            cfg.yaml["kafka"]["host"],
            cfg.yaml["kafka"]["group"],
            cfg.yaml["kafka"]["topic"],
            cfg.yaml["kafka"]["ssl_cafile"],
            cfg.yaml["kafka"]["ssl_certfile"],
            cfg.yaml["kafka"]["ssl_keyfile"],)
    pg = Psql(
            cfg.yaml["postgres"]["host"],
            cfg.yaml["postgres"]["port"],
            cfg.yaml["postgres"]["username"],
            cfg.yaml["postgres"]["password"],
            cfg.yaml["postgres"]["database"])

    try:
        c.open_conn()
    except kafka.errors.KafkaUnavailableError as e:
        log.fatal("Unable to connect to Kafka! Error: {0}".format(e))
        sys.exit(1)
    try:
        pg.open_conn()
    except psycopg2.OperationalError as e:
        log.fatal("Unable to connect to PostgreSQL! Error: {0}".format(e))
        sys.exit(1)


    log.info("Now consuming from topic '{0}'".format(cfg.yaml["kafka"]["topic"]))
    while not c.stop_event.is_set():
        try:
            for msg in c.consumer:
                try: #Assume JSON formatted message
                    data = json.loads(msg.value.decode('utf-8'))
                    for key in data.keys():
                        if pg.insert(pg.conn, key, data[key]):
                            log.info("Message consumed: {0}".format(data))
                            c.consumer.commit() #Commit after each message
                        else:
                            log.error("Unable to consume message: {0}".format(data))

                except AttributeError as e:
                    log.error("Invalid message format! Error: {0}".format(e))
        except Exception as e:
            log.Error("Could not fetch message! Error: {0}".format(e))
            continue
    c.consumer.close()

if __name__ == "__main__":
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s [%(name)s %(process)d] %(message)s',
        level=logging.INFO
        )
    log = logging.getLogger('aiven_consumer')
    main()
