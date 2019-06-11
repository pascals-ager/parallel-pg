import psycopg2
import psycopg2.extras
import logging
from multiprocessing import pool


class PostgresImpl:

    def __init__(self, config):
        self.db_name = config.DB_NAME
        self.host = config.DB_URI
        self.port = config.DB_PORT
        self.user = config.DB_USER
        self.password = config.DB_PASSWORD
        self.logger = logging.getLogger()

    def connect(self):
        return psycopg2.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port)

    def execute_batch(self, statement, generator, chunk):
        """
        :param statement: SQL Insert statement to execute
        :param generator: generator that yields json formatted strings
        :param chunk: chunk of records that have to be extracted from the generator
        :return: None
        """
        connection = None
        try:
            connection = self.connect()
            cursor = connection.cursor()
            json_list = [next(generator, (None,)) for line in range(chunk)]
            while json_list[0] != (None,):
                # SqlAlchemy batch mode uses execute_batch, hence using execute_batch to do bulk insert for chunk
                psycopg2.extras.execute_batch(cursor, statement, json_list)
                connection.commit()
                json_list = [next(generator, (None,)) for line in range(chunk)]
            cursor.close()
        except psycopg2.DatabaseError as error:
            self.logger.error(error)
            raise error
        finally:
            if connection:
                connection.close()

    def execute_ddl(self, statement):
        """
        :param statement: SQL DDL statement to execute
        :return: None
        """
        connection = None
        try:
            connection = self.connect()
            cursor = connection.cursor()
            cursor.execute(statement)
            connection.commit()
            cursor.close()
        except psycopg2.DatabaseError as error:
            self.logger.error(error)
            raise error
        finally:
            if connection:
                connection.close()
