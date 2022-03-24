from datetime import date, datetime, time, timezone
from typing import NamedTuple, TypeVar

from aapi.models import (
    Afvalbijplaatsing, Afvalcluster, Afvalclusterfractie, Afvalcontainer,
    Afvalcontainerlocatie, Afvalcontainertype, Afvalweging,
    Buurt, Stadsdeel, Wijk, Winkelgebied,
)


def datetimetz(val: str, default_tz: timezone = timezone.utc) -> datetime:
    try:
        dt = datetime.fromisoformat(val)
    except TypeError:
        dt = val
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
    return dt


class AfvalvulgraadSidcon(NamedTuple):
    filling: int                            # 6
    communication_date_time: datetimetz     # 2022-03-17T15:19:04.960000Z
    # id: int                                 # 11026789
    container_id: str                       # "REA00252"
    short_id: str                           # null


class MeldingOpenbareRuimte(NamedTuple):
    id: str                                 # "SIA-1000"
    hoofdcategorie: str                     # "Wegen, verkeer, straatmeubilair"
    subcategorie: str                       # "Prullenbak is kapot"
    datumMelding: date                      # "2018-08-09"
    tijdstipMelding: time                   # "09:52:36"
    datumOverlast: date                     # "2018-08-09"
    tijdstipOverlast: time                  # "09:52:36"
    meldingType: str                        # "SIG"
    meldingSoort: str                       # "standaard"
    meldingsnummerBovenliggend: str         # "SIA-999894"
    gbdBuurtCode: str                       # "A04g"
    gbdBuurtNaam: str                       # "Valkenburg"
    gbdWijkCode: str                        # "A04"
    gbdWijkNaam: str                        # "Nieuwmarkt/Lastage"
    gbdGgwgebiedCode: str                   # "DX02"
    gbdGgwgebiedNaam: str                   # "Centrum-Oost"
    gbdStadsdeelCode: str                   # "A"
    gbdStadsdeelNaam: str                   # "Centrum"
    bagWoonplaatsNaam: str                  # "Amsterdam"
    bron: str                               # "SIA"
    # laatstGezienBron: datetime              # "2022-03-07T04:21:37"
    # Wordt elke nacht aangepast voor alle 1,2 mln records. Elke dag zijn dus
    # alle records veranderd om niks. Een hinderlijk en nutteloos veld.


Model = TypeVar(
    'Model',
    Afvalbijplaatsing, Afvalcluster, Afvalclusterfractie, Afvalcontainer,
    Afvalcontainerlocatie, Afvalcontainertype, AfvalvulgraadSidcon,
    Afvalweging, Buurt, MeldingOpenbareRuimte, Stadsdeel, Wijk, Winkelgebied,
)
