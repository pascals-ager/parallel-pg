import unittest
import os
from configuration.config import PostgresConfig
import psycopg2
import psycopg2.extras
from persistence.postgresImpl import PostgresImpl
from persistence.persist import DataAccessLayer

class TestDB(unittest.TestCase):
    def setUp(self):
       self.engine = DataAccessLayer(PostgresImpl, PostgresConfig)
