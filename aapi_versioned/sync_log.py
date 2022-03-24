import logging
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import NamedTuple

from aapi_versioned.base import Query, SimpleDatabase

logger = logging.getLogger(__name__)


class LogItem(NamedTuple):
    id: int
    target: str
    status: str
    error: str
    created: int
    deleted: int
    started: datetime
    finished: datetime

    @classmethod
    def fields_sql(self) -> dict[str, str]:
        return {
            'id': 'SERIAL PRIMARY KEY',
            'target': 'TEXT NOT NULL',
            'status': 'TEXT NOT NULL',
            'error': 'TEXT',
            'created': 'INTEGER DEFAULT 0',
            'deleted': 'INTEGER DEFAULT 0',
            'started': 'TIMESTAMP WITHOUT TIME ZONE',
            'finished': 'TIMESTAMP WITHOUT TIME ZONE',
        }


class SyncLog(SimpleDatabase):
    table_name = 'sync_log'
    fields = tuple(LogItem.__annotations__)

    # Job log verbs
    # -------------

    def all(self, **params) -> Iterator[LogItem]:
        batch_size = 5000
        query = (Query.select(self.table_name, self.fields)
                 .where(**params)
                 .order_by('target', 'id'))
        return (LogItem(*row) for row in self.fetchmany(query, batch_size))

    def create_table(self) -> None:
        """Maakt de sync log tabel als deze niet al bestaat.
        """
        query = Query.create_table(self.table_name, LogItem.fields_sql())
        self.execute(query)

    def recent(self) -> Iterator[LogItem]:
        """Geeft alle gelogde jobs van de afgelopen dertig dagen.
        """
        since = (datetime.now() - timedelta(days=30)
                 ).replace(hour=0, minute=0, second=0, microsecond=0)
        return self.all(started__gte=since)

    def start(self, target: str, started: datetime) -> int:
        """Start een nieuwe sync job log.
        """
        query = (Query.insert(self.table_name)
                 .values(target=target, status='start', started=started)
                 .returning('id'))
        row = self.fetchone(query)
        logger.info(f'Job {row[0]} started to sync {target!r}.')
        return row[0]

    def status(self, job_id: int, status: str, **kwargs) -> None:
        """Logt de nieuwe job status.
        """
        logger.debug(f'Job {job_id} status change: {status!r}, {kwargs}.')
        query = (Query.update(self.table_name)
                 .set(status=status, **kwargs)
                 .where(id=job_id))
        self.execute(query)
