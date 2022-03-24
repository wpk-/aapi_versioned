import logging
import os.path
import sys
from datetime import datetime, timedelta
from operator import itemgetter
from typing import Any, Callable, Generic, Optional, Type, TypeVar, Union

import requests
from aapi.api import API, Endpoint as EndpointAPI
from aapi.models import Model as ModelAPI, Point, Polygon, Multipolygon

from aapi_versioned.db import DB, Endpoint as EndpointDB
from aapi_versioned.models import Model as ModelDB
from aapi_versioned.sync_log import SyncLog

logger = logging.getLogger(__name__)

T = TypeVar('T')

vandaag = datetime.today().date()
dertig_dagen_terug = vandaag - timedelta(days=30)


class Sync:
    """Interface voor alle API -> DB synchronisatie.
    """
    def __init__(self, api: API, db: DB, log: SyncLog) -> None:
        """Maakt een sync interface.

        :param api: De Amsterdam API waaruit gegevens worden gelezen.
        :param db: De database waarin alle mutaties worden bijgehouden.
        :param log: Een sync log om statistieken en informatie over de
            synchronisatie bij te houden.
        """
        def task(ep_api: EndpointAPI[ModelAPI],
                 ep_db: EndpointDB[ModelDB],
                 kw_api: Optional[dict[str, str]] = None,
                 kw_db: Optional[dict[str, str]] = None
                 ) -> Task[ModelAPI, ModelDB]:
            return Task(ep_api, ep_db, log, kw_api, kw_db)

        toen = dertig_dagen_terug.isoformat()

        # Huishoudelijk afval
        # -------------------
        self.afval_bijplaatsingen = task(
            api.afval_bijplaatsingen,
            db.afval_bijplaatsingen,
            {'datumTijdWaarneming[gte]': toen},
            {'datumTijdWaarneming__gte': toen}
        )
        self.afval_clusters = task(
            api.afval_clusters,
            db.afval_clusters
        )
        self.afval_clusterfracties = task(
            api.afval_clusterfracties,
            db.afval_clusterfracties
        )
        self.afval_containerlocaties = task(
            api.afval_containerlocaties,
            db.afval_containerlocaties
        )
        self.afval_containers = task(
            api.afval_containers,
            db.afval_containers
        )
        self.afval_containertypes = task(
            api.afval_containertypes,
            db.afval_containertypes
        )
        # Het klopt dat de SIDCON vulgraad API "__gt" hanteert.
        self.afval_vulgraad_sidcon = task(
            api.afval_vulgraad_sidcon,
            db.afval_vulgraad_sidcon,
            {'communication_date_time__gt': f'{toen}T00:00:00Z',
             'page_size': 5000},
            {'communication_date_time__gt': f'{toen}T00:00:00Z'}
        )
        self.afval_wegingen = task(
            api.afval_wegingen,
            db.afval_wegingen,
            {'datumWeging[gte]': toen},
            {'datumWeging__gte': toen}
        )

        # Meldingen
        # ---------
        self.meldingen = task(
            api.meldingen,
            db.meldingen,
            {'datumMelding[gte]': toen},
            {'datumMelding__gte': toen}
        )

        # Gebieden
        # --------
        self.buurten = task(
            api.buurten,
            db.buurten
        )
        self.stadsdelen = task(
            api.stadsdelen,
            db.stadsdelen
        )
        self.wijken = task(
            api.wijken,
            db.wijken
        )

        # Winkelgebieden
        # --------------
        self.winkelgebieden = task(
            api.winkelgebieden,
            db.winkelgebieden
        )

    @property
    def tasks(self) -> list['Task']:
        """Geeft de lijst van alle taken.
        """
        return [task
                for _, task in vars(self).items()
                if isinstance(task, Task)]

    def sync_all(self) -> None:
        """Synchroniseert alle API endpoints waarvoor ook een DB endpoint
        bestaat.
        """
        for task in self.tasks:
            task.pull()


class Task(Generic[ModelAPI, ModelDB]):
    """Een task omvat de synchronisatie van 1 API op 1 DB endpoint.

    Deze klasse voert de feitelijke synchronisatie uit.

    Variabelen vormen zo goed als gaat een analogie met Git origin main
    zoals erg mooi uitgelegd hier: https://stackoverflow.com/a/18137512
    """
    def __init__(self, origin: EndpointAPI[ModelAPI],
                 main: EndpointDB[ModelDB],
                 sync_log: SyncLog,
                 origin_kwargs: Optional[dict[str, Any]] = None,
                 main_kwargs: Optional[dict[str, Any]] = None) -> None:
        """

        :param origin: Het API endpoint.
        :param main: Het database endpoint.
        :param sync_log: De sync log om informatie over de synchronisatie
            bij te houden.
        """
        self.origin = origin
        self.main = main
        self.log = sync_log
        self.origin_kwargs = origin_kwargs or {}
        self.main_kwargs = main_kwargs or {}

    @property
    def task_name(self) -> str:
        return self.main.table_name

    def fetch(self) -> tuple[set[ModelDB], bool]:
        """Haalt alle actieve records op uit de API.
        """
        to_main = model_transformer(self.main.model, self.origin.model)
        origin_main = set()
        partial = False

        try:
            for item in self.origin.all(**self.origin_kwargs):
                origin_main.add(to_main(item))
        except requests.HTTPError as err:
            logger.warning(err)
            partial = True

        return origin_main, partial

    def diff(self, origin_main: set[ModelDB]) -> list[int]:
        """Markeert alle records die verschillen tussen DB en API.
        Let op: `origin_main` wordt aangepast zodat alleen de nieuwe
        records overblijven (alle + in een diff).
        De functie retourneert een referentie naar alle te verwijderen
        records (alle - in een diff).
        """
        to_main = model_transformer(self.main.model)
        deleted = []

        for rec in self.main.all(_deleted=None, **self.main_kwargs):
            main_item = to_main(rec.data)
            try:
                origin_main.remove(main_item)
            except KeyError:
                deleted.append(rec.id)

        return deleted

    def pull(self) -> None:
        """Synchroniseert alle mutaties van origin (remote) naar main
        (lokaal).

        Elke mogelijke fout wordt opgevangen en gelogd zodat het falen
        van een synchronisatie de overige synchronisaties niet kan
        hinderen.
        """
        ts = datetime.now()
        error = ''

        try:
            self.main.create_table()
            job_id = self.log.start(self.task_name, ts)
        except Exception as err:
            logger.error(err)
            return

        try:
            self.log.status(job_id, 'fetch')
            added, partial = self.fetch()

            # NB. Even when partial is True, added will be updated to
            #     contain the new records.
            self.log.status(job_id, 'sync')
            deleted = self.diff(added)

            if partial:
                error = 'HTTP request failed. Cannot sync deletions.'
                deleted = []

            if added:
                self.log.status(job_id, 'create', created=len(added))
                self.main.add(ts, iter(added))

            if deleted:
                self.log.status(job_id, 'delete', deleted=len(deleted))
                self.main.delete(ts, deleted)

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            filename = os.path.relpath(tb.tb_frame.f_code.co_filename)
            lineno = tb.tb_lineno
            error = f'Exception in {filename!r} on line {lineno}: {str(err)}'
            logger.error(f'Job {job_id}: {error}')

        ts = datetime.now()

        if error:
            self.log.status(job_id, 'failed', finished=ts, error=error)
        else:
            self.log.status(job_id, 'done', finished=ts)


def model_transformer(model_to: Type[ModelDB],
                      model_from: Optional[Type[ModelAPI]] = None,
                      ) -> Callable[[ModelDB], ModelDB]:
    """Maakt een functie die efficient API modellen omzet naar DB rijen.
    De rijen zijn hashable tuples die 1:1 matchen met de DB records.
    """
    def _identity(v: T) -> T:
        return v

    def parse(data: Union[ModelAPI, ModelDB]) -> ModelDB:
        return model_to(*(None if data_x is None else mapper(data_x)
                          for data_x, mapper in zip(f(data), field_methods)))

    type_methods = {
        Point: lambda x: str(x).replace(' ', ''),
        Polygon: lambda x: str(x).replace(' ', ''),
        Multipolygon: lambda x: str(x).replace(' ', ''),
    }

    field_methods = [
        type_methods.get(typ, _identity)
        for fld, typ in model_to.__annotations__.items()
    ]

    f = (_identity if model_from is None else
         itemgetter(*(model_from._fields.index(field_to)
                      for field_to in model_to._fields)))

    return parse
