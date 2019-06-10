import argparse
import logging
from typing import Any, Tuple, Generator, Dict
from persistence.postgres_impl import PostgresImpl
from persistence.persist import Persist
from configuration.config import PostgresConfig

from api.yielder import LineYldr


def load(args):
    log = logging.getLogger()
    if args.command == "load":
        params: Dict[str, Any] = {'src_file': args.src_file, 'dst_table': args.dst_table, 'chunk': args.chunk}
        log.info("Received {} command with {}".format(args.command, params.__str__()))
        generator = LineYldr().yield_line(params.get('src_file'))
        Persist(PostgresImpl, PostgresConfig).persist(params.get('dst_table'), int(params.get('chunk')), generator)


if __name__ == "__main__":
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
    parser_seed.add_argument('--dst_table', help="Location of the sourcefile",
                             required=True)
    load(parser.parse_args())
