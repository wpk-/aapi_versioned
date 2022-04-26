import logging
from collections.abc import Iterator, Iterable
from dataclasses import dataclass
from datetime import datetime, date, time
from typing import Generic, Type, Union

from psycopg import Connection

from aapi.models import Multipolygon, Point, Polygon

from aapi_versioned.base import SimpleDatabase, Query
from aapi_versioned.models import (
    Model, datetimetz,
    Afvalbijplaatsing, Afvalcluster, Afvalclusterfractie,
    Afvalcontainerlocatie, Afvalcontainer, Afvalcontainertype, Afvalweging,
    AfvalvulgraadSidcon, MeldingMijnAmsterdam, MeldingOpenbareRuimte,
    Buurt, Stadsdeel, Wijk, Winkelgebied,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Dit is een dataclass ipv NamedTuple zodat Generic[Model] werkt.
@dataclass(frozen=True, slots=True)     # NB. Slots arg is Python 3.10+.
class Versioned(Generic[Model]):
    id: int
    created: datetime
    deleted: Union[datetime, None]
    data: Model


class DB:
    def __init__(self, connection: Connection) -> None:
        def endpoint(path: str, model: Type[Model]) -> Endpoint[Model]:
            return Endpoint(path, model, connection)

        # Like API session.
        self.connection = connection

        # Huishoudelijk afval
        # -------------------
        self.afval_bijplaatsingen = endpoint(
            'v1_huishoudelijkafval_bijplaatsingen',
            Afvalbijplaatsing
        )
        self.afval_clusters = endpoint(
            'v1_huishoudelijkafval_cluster',
            Afvalcluster
        )
        self.afval_clusterfracties = endpoint(
            'v1_huishoudelijkafval_clusterfractie',
            Afvalclusterfractie
        )
        self.afval_containerlocaties = endpoint(
            'v1_huishoudelijkafval_containerlocatie',
            Afvalcontainerlocatie
        )
        self.afval_containers = endpoint(
            'v1_huishoudelijkafval_container',
            Afvalcontainer
        )
        self.afval_containertypes = endpoint(
            'v1_huishoudelijkafval_containertype',
            Afvalcontainertype
        )
        self.afval_vulgraad_sidcon = endpoint(
            'afval_suppliers_sidcon_filllevels',
            AfvalvulgraadSidcon
        )
        self.afval_wegingen = endpoint(
            'v1_huishoudelijkafval_weging',
            Afvalweging
        )

        # Meldingen
        # ---------
        self.meldingen = endpoint(
            'v1_meldingen_meldingen',
            MeldingOpenbareRuimte
        )
        self.meldingen_buurt = endpoint(
            'v1_meldingen_meldingen_buurt',
            MeldingMijnAmsterdam
        )

        # Gebieden
        # --------
        self.buurten = endpoint(
            'v1_gebieden_buurten',
            Buurt
        )
        self.stadsdelen = endpoint(
            'v1_gebieden_stadsdelen',
            Stadsdeel
        )
        self.wijken = endpoint(
            'v1_gebieden_wijken',
            Wijk
        )

        # Winkelgebieden
        # --------------
        self.winkelgebieden = endpoint(
            'v1_winkelgebieden_winkelgebieden',
            Winkelgebied
        )


class Endpoint(SimpleDatabase, Generic[Model]):
    type_map = {
        bool: 'BOOLEAN',
        date: 'DATE',
        datetime: 'TIMESTAMP WITHOUT TIME ZONE',
        datetimetz: 'TIMESTAMP WITH TIME ZONE',
        float: 'DOUBLE PRECISION',
        int: 'INTEGER',
        Multipolygon: 'TEXT',
        Point: 'POINT',
        Polygon: 'TEXT',
        str: 'TEXT',
        time: 'TIME',
    }

    def __init__(self, table_name: str, model: Type[Model],
                 connection: Connection) -> None:
        """Creates the endpoint interface fetching item_types from url.

        :param table_name: The database table holding all endpoint records.
        :param model: The type of items this endpoint returns.
        :param connection: Connection to the database.
        """
        super().__init__(connection)
        self.table_name = table_name
        self.model = model
        # self.connection = connection

    @property
    def fields(self) -> tuple[str, ...]:
        return self.version_fields + self.model_fields

    @property
    def model_fields(self) -> tuple[str, ...]:
        return tuple(self.model.__annotations__)

    @property
    def version_fields(self) -> tuple[str, str, str]:
        return '_id', '_created', '_deleted'

    # General interface
    # -----------------

    def add(self, created: datetime, items: Iterable[Model]) -> None:
        query = Query.copy(self.table_name, ('_created',) + self.model_fields)
        data = ((created,) + item for item in items)
        self.copy(query, data)

    def all(self, **params) -> Iterator[Versioned[Model]]:
        batch_size = 5000
        query = Query.select(self.table_name, self.fields).where(**params)
        return (Versioned(row[0], row[1], row[2], self.model(*row[3:]))
                for row in self.fetchmany(query, batch_size))

    def count(self, **params) -> int:
        query, params = Query.count(self.table_name).where(**params)
        return self.fetchone(query)[0]

    def create_table(self) -> None:
        query = self.query_create_table()
        self.execute(query)

    def delete(self, deleted: datetime, record_ids: Iterable[int]) -> None:
        query = (Query.update(self.table_name)
                 .set(_deleted=deleted)
                 .where(_deleted=None, _id__in=record_ids))
        self.execute(query)

    def one(self, **params) -> Versioned[Model]:
        query = Query.select(self.table_name, self.fields).where(**params)
        row = self.fetchone(query)
        return Versioned(row[0], row[1], row[2], self.model(*row[3:]))

    def twenty(self, job_id: datetime) -> Iterator[tuple]:
        fields = self.fields    # Includes model and versioning.
        order_by = ('id', '_created') if 'id' in fields else ('_id',)
        query = (Query.select(self.table_name, fields)
                 .where(_created=job_id)
                 .or_(_deleted=job_id)
                 .order_by(*order_by)
                 .limit(20))
        return self.fetchmany(query)

    # Preset queries
    # --------------

    def query_create_table(self) -> Query:
        type_map = self.__class__.type_map
        fields_def = dict(
            _id='SERIAL PRIMARY KEY',
            _created='TIMESTAMP NOT NULL',
            _deleted='TIMESTAMP',
            **{k: type_map[v] for k, v in self.model.__annotations__.items()}
        )
        return Query.create_table(self.table_name, fields_def)
