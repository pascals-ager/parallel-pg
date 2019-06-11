class DataAccessLayer:

    def __init__(self, service, config):
        self.service = service(config)

    def persist(self, dst_table, chunk, generator):
        insert_statement = "INSERT INTO {}(data) values (%s)".format(dst_table)
        self.service.execute_batch(insert_statement, chunk, generator)

    def seed(self, dst_table):
        ddl_statement = "CREATE TABLE IF NOT EXISTS {}(data JSONB)".format(dst_table)
        self.service.execute_ddl(ddl_statement)
