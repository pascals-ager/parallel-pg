def singleton(cls, *args, **kwargs):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
            return instances[cls]
    return _singleton


@singleton
class Config(object):
    APP_NAME = 'TierLoader'


@singleton
class PostgresConfig(object):
    DB_USER = 'postgres'
    DB_NAME = 'postgres'
    DB_PASSWORD = 'docker'
    DB_URI = 'localhost'
    DB_PORT = '5432'

