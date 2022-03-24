aapi_versioned
--------------

Versiebeheer van de Amsterdam API. Draai elke dag om een geschiedenis op te
bouwen van alle mutaties.


## Install

Installeer de dependencies ([orjson][orjson], [psycopg][psycopg],
[requests][requests] en [aapi][aapi]):
```shell
pip install -r requirements.txt
```


## Gebruik

Stel de volgende environment variabelen in met de verbindingsgegevens van de
PostgreSQL database: `AAPI_HOST`, `AAPI_NAME`, `AAPI_USER` en `AAPI_PASS`.

Draai vervolgens:
```shell
python app.py
```

Dit maakt alle nodige tabellen aan en synchroniseert de live gegevens uit de
Amsterdam API naar de database. Draai het elke dag om een log van alle mutaties
aan te leggen.

Zie ook [app.py](app.py).


### Nieuw endpoint toevoegen

1. Voeg het model toe in `aapi`: het model in `models.py` en het endpoint in
   `api.py`. Importeer vervolgens `aapi` opnieuw in dit project.
2. Voeg het model toe in `aapi_versioned`: het model in `models.py`, het
   endpoint in `db.py` en de synchronisatie in `sync.py`.
3. Klaar.


## Toekomst

* Uitbreiden met meer endpoints.
* Async.


## Licentie

[MIT](LICENSE)


[orjson]: https://github.com/ijl/orjson
[psycopg]: https://www.psycopg.org/psycopg3/
[requests]: https://docs.python-requests.org/
[aapi]: https://github.com/wpk-/aapi
