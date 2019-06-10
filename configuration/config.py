class Config:
    APP_NAME = 'TierLoader'


class PostgresConfig(Config):
    DB_USER = 'postgres'
    DB_NAME = 'postgres'
    DB_PASSWORD = 'docker'
    DB_URI = 'localhost'
    DB_PORT = '5432'

