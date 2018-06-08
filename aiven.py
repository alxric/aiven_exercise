#!/usr/bin/env python
import multiprocessing
import psycopg2
import sys
import threading
import yaml

from kafka import KafkaProducer, KafkaConsumer

class Config():
    def __init__(self, path):
        self.parse_config(path)

    def parse_config(self, path):
        with open(path, 'r') as stream:
            try:
                self.yaml = yaml.load(stream)
            except yaml.YAMLError as e:
                print(e)
                sys.exit(1)

class Producer(threading.Thread):
    def __init__(self, host, topic, cafile, certfile, keyfile):
        threading.Thread.__init__(self)
        self.host = host
        self.topic = topic
        self.cafile = cafile
        self.certfile = certfile
        self.keyfile = keyfile

    def open_conn(self):
        self.producer = KafkaProducer(
                security_protocol='SSL',
                ssl_check_hostname=True,
                bootstrap_servers=self.host,
                ssl_cafile=self.cafile,
                ssl_certfile=self.certfile,
                ssl_keyfile=self.keyfile)

class Consumer(multiprocessing.Process):
    def __init__(self, host, group, topic, cafile, certfile, keyfile):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        self.host = host
        self.group = group
        self.topic = topic
        self.cafile = cafile
        self.certfile = certfile
        self.keyfile = keyfile

    def stop(self):
        self.stop_event.set()

    def open_conn(self):
        self.consumer = KafkaConsumer(
                self.topic,
                group_id=self.group,
                security_protocol='SSL',
                ssl_check_hostname=True,
                bootstrap_servers=self.host,
                ssl_cafile=self.cafile,
                ssl_certfile=self.certfile,
                ssl_keyfile=self.keyfile)

class Psql():
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def open_conn(self):
        self.conn = psycopg2.connect(
                "host={0} port={1} user={2} password={3}\
                dbname={4} sslmode='require'".format(
            self.host, self.port, self.user,
            self.password, self.database))

    def close_conn(self):
        self.conn.close()

    def insert(self, conn, name, value):
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO test_data VALUES (%s, %s)",
                    (name, int(value)))
            conn.commit()
            cur.close()
        except (ValueError, psycopg2.ProgrammingError):
            return False
        except psycopg2.OperationalError:
            raise
        return True
