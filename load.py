import argparse
import logging
import time
from persistence.postgresImpl import PostgresImpl
from persistence.persist import DataAccessLayer
from configuration import PostgresConfig
from api.yielder import LineYldr


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            log.info('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed


@timeit
def load(args):

    if args.command == "load":
        params = {'src_file': args.src_file, 'dst_table': args.dst_table, 'chunk': args.chunk}
        log.info("Received {} command with {}".format(args.command, params.__str__()))
        generator = LineYldr().yield_line(params.get('src_file'))
        DataAccessLayer(PostgresImpl, PostgresConfig()).persist(params.get('dst_table'), generator, int(params.get('chunk')))
        log.info("Data load success into table {}".format(params.get('dst_table')))

    if args.command == "seed":
        params = {'dst_table': args.dst_table}
        log.info("Received {} command with {}".format(args.command, params.__str__()))
        DataAccessLayer(PostgresImpl, PostgresConfig()).seed(params.get('dst_table'))
        log.info("Seeded table {} in the database".format(params.get('dst_table')))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, filename="logs/tierloader.log")
    log = logging.getLogger()
    parser = argparse.ArgumentParser(description="Bulk Load flat files into Postgres")
    subparsers = parser.add_subparsers(help="sub command help", dest='command')
    parser_load = subparsers.add_parser('load', help="load flat file")
    parser_load.add_argument('--src_file', help="Location of the sourcefile",
                             required=True)
    parser_load.add_argument('--dst_table', help="Name of the destination table",
                             required=True, default=None)
    parser_load.add_argument('--chunk', help="chunks to load per insert",
                             required=False, default=1000)
    parser_seed = subparsers.add_parser('seed', help="seed the database with the table")
    parser_seed.add_argument('--dst_table', help="Name of destination table with JSONB data column",
                             required=True)
    load(parser.parse_args())
