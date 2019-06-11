import unittest, time
import psycopg2
import psycopg2.extras
from configuration import PostgresConfig
from persistence.persist import DataAccessLayer
from persistence.postgresImpl import PostgresImpl


class TestPersistence(unittest.TestCase):

    def setUp(self):
        self.config = PostgresConfig()
        self.engine = DataAccessLayer(PostgresImpl, self.config)
        self.engine.seed("test_json")

    def test_persist(self):
        generator = (('{"a" : "one", "b": "two"}',) for x in range(28))
        self.engine.persist('test_json', generator, 10)
        time.sleep(5)
        connection = None
        try:
            connection = psycopg2.connect(dbname=self.config.DB_NAME, user=self.config.DB_USER,
                                          password=self.config.DB_PASSWORD, host=self.config.DB_URI,
                                          port=self.config.DB_PORT)
            cursor = connection.cursor()
            statement = "SELECT count(*) from test_json where data is not null"
            cursor.execute(statement)
            assert (28,) == cursor.fetchone()
            connection.commit()
            cursor.close()
        except psycopg2.DatabaseError as error:
            raise error
        finally:
            if connection:
                connection.close()

    def tearDown(self):
        connection = None
        try:
            connection = psycopg2.connect(dbname=self.config.DB_NAME, user=self.config.DB_USER,
                                          password=self.config.DB_PASSWORD, host=self.config.DB_URI,
                                          port=self.config.DB_PORT)
            cursor = connection.cursor()
            statement = "TRUNCATE test_json"
            cursor.execute(statement)
            connection.commit()
            cursor.close()
        except psycopg2.DatabaseError as error:
            raise error
        finally:
            if connection:
                connection.close()
        self.config = None
        self.engine = None


if __name__ == "__main__":
    unittest.main()
