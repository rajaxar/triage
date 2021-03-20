import argparse
from pathlib import Path

from argcmdr import local, LocalRoot, Local

ROOT_PATH = Path(__file__).parent.resolve()

class DirtyDuck(LocalRoot):
    """Commands for the Dirtyducks's tutorial"""
    pass

@DirtyDuck.register
def db_setup(context, args):
    """Setting up dirtyducks's  database
    The following environment variables should available:
    PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGPORT
    and obvioulsy they should point to a PostgreSQL database"""
    for sql_file in Path('dirtyduck').rglob('*.sql'):
        yield context.local['psql']['-f', str(sql_file)]
