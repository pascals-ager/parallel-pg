import psycopg2
import psycopg2.extras


class PostgresImpl:

    def __init__(self, config):
        self.db_name = config.DB_NAME
        self.host = config.DB_URI
        self.port = config.DB_PORT
        self.user = config.DB_USER
        self.password = config.DB_PASSWORD

    def connect(self):
        return psycopg2.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port)

    def execute_batch(self, query, chunk, generator):
        connection = self.connect()
        cursor = connection.cursor()
        psycopg2.extras.execute_batch(cursor, query, [next(generator, None) for line in range(chunk)])
        connection.commit()
        cursor.close()
