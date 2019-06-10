class Persist:

    def __init__(self, service, config):
        self.service = service(config)

    def persist(self, dst_table, chunk, generator):
        query = "INSERT INTO {}(data) values (%s)".format(dst_table)
        self.service.execute_batch(query, chunk, generator)
