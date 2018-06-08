#!/usr/bin/env python
import socket
import testing.postgresql
import unittest
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from aiven import Psql
from kafka.conn import BrokerConnection, ConnectionStates

class TestKafka(unittest.TestCase):
    def test_initialization(self):
        self.assertEqual(2+2, 4)

    def test_connection(self):
        conn = BrokerConnection('localhost', 9092, socket.AF_INET)
        assert conn.connected() is False
        conn.state = ConnectionStates.CONNECTED
        assert conn.connected() is True
        assert conn.connecting() is False
        conn.state = ConnectionStates.CONNECTING
        assert conn.connecting() is True
        conn.state = ConnectionStates.CONNECTED
        assert conn.connecting() is False
        conn.state = ConnectionStates.DISCONNECTED
        assert conn.connected() is False

class Postgres(unittest.TestCase):
    def test_initialization(self):
        self.assertEqual(2+2, 4)

    def test_insert(self):
        with testing.postgresql.Postgresql() as postgresql:
            import psycopg2
            conn = psycopg2.connect(**postgresql.dsn())
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE test_data(name varchar(6), value int)")
            cursor.close()
            conn.commit()

            psql = Psql("postgres", "test")
            psql.insert(conn, "foo", "1")
            cursor = conn.cursor()
            cursor.execute("select * from test_data")
            self.assertEqual(str(cursor.fetchone()), "('foo', 1)")
            cursor.close()
            conn.close()


if __name__ == '__main__':
    unittest.main()
