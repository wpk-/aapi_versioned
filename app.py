import logging
from os import getenv

from aapi import API

from aapi_versioned.db import DB
from aapi_versioned.base import connect_db
from aapi_versioned.sync import Sync
from aapi_versioned.sync_log import SyncLog
from aapi_versioned.web import write_stats


def main(db_config: dict[str, str]) -> None:
    with connect_db(db_config) as conn:
        api = API()
        db = DB(conn)
        log = SyncLog(conn)

        log.create_table()

        sync = Sync(api, db, log)

        # Sync all:
        sync.sync_all()

        # Or sync individual endpoints:
        # sync.afval_bijplaatsingen.pull()
        # sync.afval_containers.pull()
        # sync.afval_vulgraad_sidcon.pull()
        # sync.buurten.pull()
        # ...

        # Export summary data for a web UI.
        write_stats('web/data', sync, log)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    main({
        'host': getenv('AAPI_HOST'),
        'dbname': getenv('AAPI_NAME'),
        'user': getenv('AAPI_USER'),
        'password': getenv('AAPI_PASS'),
        'port': 5432,
        'sslmode': 'require',
    })
