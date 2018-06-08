#!/usr/bin/env python
import kafka
import logging
from aiven import Producer, Config
import sys
import time


def main():

    cfg = Config("config.yml")
    p = Producer(
            cfg.yaml["kafka"]["host"],
            cfg.yaml["kafka"]["topic"],
            cfg.yaml["kafka"]["ssl_cafile"],
            cfg.yaml["kafka"]["ssl_certfile"],
            cfg.yaml["kafka"]["ssl_keyfile"],)
    try:
        p.open_conn()
    except kafka.errors.NoBrokersAvailable as e:
        log.fatal("Unable to connect to Kafka! Error: {0}".format(e))
        sys.exit(1)

    log.info("Ready to produce to topic '{0}'".format(cfg.yaml["kafka"]["topic"]))
    counter = 0

    # Start sending a message every second, indefinitely
    while True:
        try:
            log.info("Sending 'foo':'{0}' to '{1}'".format(
                counter,
                cfg.yaml["kafka"]["topic"]))
            result = p.producer.send(cfg.yaml["kafka"]["topic"]
                    , bytes('{"foo":"%d"}' % counter, "utf-8"))
            result.get(timeout=5) # Check to make sure kafka is still running
            counter+=1
            time.sleep(1)
        except kafka.errors.KafkaTimeoutError as e:
            log.fatal("Can not send message! Error: {0}".format(e)) #Kafka down
            sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s [%(name)s %(process)d] %(message)s',
        level=logging.INFO
        )
    log = logging.getLogger('aiven_producer')
    main()
