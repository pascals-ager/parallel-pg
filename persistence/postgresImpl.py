import psycopg2
import psycopg2.extras
import logging
from concurrent.futures import ThreadPoolExecutor as PoolExecutor


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

    def parallel_insert(self, statement, json_list):
        """
        :param statement: SQL Insert statement to execute
        :param json_list: List of json that is persisted by the thread
        :return: None
        """
        connection = None
        try:
            connection = self.connect()
            cursor = connection.cursor()
            # SqlAlchemy batch mode uses execute_batch, hence using execute_batch to do bulk insert for chunk
            psycopg2.extras.execute_batch(cursor, statement, json_list)
            connection.commit()
            cursor.close()
            self.logger.info("Finished parallel persist on thread".format())
        except psycopg2.DatabaseError as error:
            self.logger.error(error)
            raise error
        finally:
            if connection:
                connection.close()

    def execute_batch(self, statement, generator, chunk):
        """
        :param statement: SQL Insert statement to execute
        :param generator: generator that yields json formatted strings
        :param chunk: chunk of records that have to be extracted from the generator
        :return: None
        """
        json_list = [next(generator, (None,)) for line in range(chunk)]
        with PoolExecutor(max_workers=8) as executor:
            while json_list[0] != (None,):
                self.logger.info("Submitting parallel task to persist into the database with {}".format(statement))
                executor.submit(self.parallel_insert, statement, json_list)
                json_list = [next(generator, (None,)) for line in range(chunk)]

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
