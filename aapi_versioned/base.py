import logging
from typing import Any, Iterable, Iterator, Optional

import psycopg
from psycopg import Connection

logger = logging.getLogger(__name__)


def connect_db(db_config: dict[str, str]) -> Connection:
    return psycopg.connect(**db_config)


class SimpleDatabase:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def copy(self, query: 'Query', rows: Iterable[tuple]) -> None:
        logger.debug(query)
        try:
            with self.connection.cursor() as cur:
                with cur.copy(query.query) as copy:
                    for row in rows:
                        copy.write_row(row)
            self.connection.commit()
        except psycopg.Error as err:
            self.connection.rollback()
            raise err

    def execute(self, query: 'Query') -> None:
        logger.debug(query)
        try:
            with self.connection.cursor() as cur:
                cur.execute(query.query, query.params)
            self.connection.commit()
        except psycopg.Error as err:
            self.connection.rollback()
            raise err

    def fetchmany(self, query: 'Query', batch_size: int = 5000
                   ) -> Iterator[tuple]:
        logger.debug(query)
        try:
            with self.connection.cursor() as cur:
                cur.execute(query.query, query.params)

                while batch := cur.fetchmany(size=batch_size):
                    for row in batch:
                        yield row
        except psycopg.Error as err:
            self.connection.rollback()
            raise err

    def fetchone(self, query: 'Query') -> tuple:
        logger.debug(query)
        try:
            with self.connection.cursor() as cur:
                cur.execute(query.query, query.params)
                row = cur.fetchone()
            self.connection.commit()
        except psycopg.Error as err:
            self.connection.rollback()
            raise err
        return row


class Query:
    """Een klasse om op leesbare manier SQL te schrijven.

    model.select()
        .where(field=3)
        .or_(other_field__lte=datetime(2022,5,22,8,20),
             other_field__gte=datetime(2021,5,22,8,20))
        .order_by('field4', 'field5 DESC')
        .limit(20)
    ->
    SELECT * FROM table
    WHERE "field" = 3
    OR ("other_field" <= %s AND "other_field" >= %s)
    ORDER BY "field4", "field5" DESC
    LIMIT 20
    """
    def __init__(self, base_query: str,
                 params: Optional[Iterable] = None,
                 clauses: Optional[dict[str, Any]] = None) -> None:
        # base_query = 'SELECT COUNT(*) FROM table'
        #              'UPDATE table SET ... = ...'
        #              'SELECT fields FROM table'
        self.base_query = base_query
        self.params = list(params or [])
        self._clauses = clauses or {}

    def __str__(self) -> str:
        query = self.base_query
        clauses = self._clauses.copy()

        value = clauses.pop('VALUES', {})
        if value:
            cols, vals = zip(*value.items())
            query += f' ({", ".join(cols)}) VALUES ({", ".join(vals)})'

        value = clauses.pop('SET', [])
        if value:
            query += ' SET ' + ', '.join(value)

        value = clauses.pop('WHERE', [])
        if value:
            query += (' WHERE ' + ' OR '.join(f'({" AND ".join(terms)})'
                                              if len(terms) > 1 else terms[0]
                                              for terms in value))

        value = clauses.pop('ORDER_BY', [])
        if value:
            query += ' ORDER BY ' + ', '.join(value)

        value = clauses.pop('LIMIT', None)
        if value is not None:
            query += f' LIMIT {value:d}'

        value = clauses.pop('RETURNING', [])
        if value:
            query += ' RETURNING ' + ', '.join(value)

        return query

    @property
    def query(self) -> str:
        return str(self)

    @classmethod
    def copy(cls, table_name: str, fields) -> 'Query':
        fields = ', '.join(quote_fields(fields))
        return cls(f'COPY {table_name} ({fields}) FROM STDIN')

    @classmethod
    def count(cls, table_name: str) -> 'Query':
        return cls(f'SELECT COUNT(*) FROM {table_name}', [],
                   {'WHERE': [], 'ORDER_BY': [], 'LIMIT': None})

    @classmethod
    def create_table(cls, table_name: str, fields_def: dict[str, str]
                     ) -> 'Query':
        fields = ', '.join(f'"{f}" {t}' for f, t in fields_def.items())
        return cls(f'CREATE TABLE IF NOT EXISTS {table_name} ({fields})')

    @classmethod
    def insert(cls, table_name: str) -> 'Query':
        return cls(f'INSERT INTO {table_name}', [],
                   {'VALUES': {}, 'RETURNING': []})

    @classmethod
    def select(cls, table_name: str, fields: Iterable[str]) -> 'Query':
        fields = ', '.join(quote_fields(fields))
        return cls(f'SELECT {fields} FROM {table_name}', [],
                   {'WHERE': [], 'ORDER_BY': [], 'LIMIT': None})

    @classmethod
    def update(cls, table_name: str) -> 'Query':
        return cls(f'UPDATE {table_name}', [],
                   {'SET': [], 'WHERE': [], 'RETURNING': []})

    def accept_clause(self, clause: str) -> None:
        if clause not in self._clauses:
            raise TypeError(f'{clause!r} is not valid for this query.')

    def limit(self, n: int) -> 'Query':
        self.accept_clause('LIMIT')
        return Query(self.base_query, self.params,
                     dict(self._clauses, LIMIT=n))

    def or_(self, **kwargs) -> 'Query':
        return self.where(**kwargs)

    def order_by(self, *args) -> 'Query':
        def order(field: str) -> str:
            field = field.split()
            mod = field[1] if len(field) == 2 else ''
            return f'"{field[0]}" {mod}'
        self.accept_clause('ORDER_BY')
        order_by = [order(f) for f in args]
        return Query(self.base_query, self.params,
                     dict(self._clauses, ORDER_BY=order_by))

    def returning(self, *args) -> 'Query':
        self.accept_clause('RETURNING')
        returning = args    # NB. Quoting fields would be a syntax error (!?)
        return Query(self.base_query, self.params,
                     dict(self._clauses, RETURNING=returning))

    def set(self, **kwargs) -> 'Query':
        self.accept_clause('SET')
        terms, params = parse_qargs(kwargs)
        set_ = self._clauses['SET'] + terms
        return Query(self.base_query, self.params + params,
                     dict(self._clauses, SET=set_))

    def values(self, **kwargs) -> 'Query':
        self.accept_clause('VALUES')
        terms = quote_fields(kwargs)
        params = list(kwargs.values())
        values = {**self._clauses['VALUES'], **{t: '%s' for t in terms}}
        return Query(self.base_query, self.params + params,
                     dict(self._clauses, VALUES=values))

    def where(self, **kwargs) -> 'Query':
        self.accept_clause('WHERE')
        terms, params = parse_qargs(kwargs)
        where = self._clauses['WHERE'] + [tuple(terms)]
        return Query(self.base_query, self.params + params,
                     dict(self._clauses, WHERE=where))


def parse_qargs(qargs: dict[str, Any]) -> tuple[list[str], list]:
    """Verwerkt keyword-argumenten tot SQL-conditie termen.

    :param qargs: Een dict-beschrijving van de condities:
        {'a__lte': 3, 'b__in': (4, 5, 6), 'c': True}
    :return: Twee lijsten: de query tekst tokens en de vereiste waardes.
        ['"a" <= %s', '"b" = ANY(%s)', '"c" = %s']
        [3, [4, 5, 6], True]
    """
    op_map = {'lt': '<', 'lte': '<=', 'le': '<=', 'eq': '=',
              'ne': '!=', 'ge': '>=', 'gte': '>=', 'gt': '>'}
    terms = []
    values = []

    for k, v in qargs.items():
        if '__' in k:
            k, op = k.rsplit('__', 1)
        else:
            op = 'eq'

        if op == 'eq':
            if v is None:
                terms.append(f'"{k}" IS NULL')
            else:
                terms.append(f'"{k}" = %s')
                values.append(v)
        elif op in op_map:
            terms.append(f'"{k}" {op_map[op]} %s')
            values.append(v)
        elif op == 'isnull':
            terms.append(f'"{k}" IS {"" if v else "NOT"} NULL')
        elif op == 'in':
            terms.append(f'"{k}" = ANY(%s)')
            values.append(list(v))

    return terms, values


def quote_fields(fields: Iterable[str]) -> Iterator[str]:
    """Plaatst alle strings in dubbele quotes.
    """
    return (f'"{f}"' for f in fields)
