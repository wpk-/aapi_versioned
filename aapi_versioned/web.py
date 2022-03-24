from datetime import datetime
from pathlib import Path
from typing import Union

from orjson import orjson

from aapi_versioned.sync import Sync, Task
from aapi_versioned.sync_log import SyncLog, LogItem


def write_stats(path: Union[str, Path], sync: Sync, log: SyncLog) -> None:
    """Schrijft (en verwijdert) de JSON-bestanden voor web/index.html UI.
    """
    path = Path(path)

    recent_logs = list(log.recent())

    with open(path / 'jobs.json', 'wb') as f:
        f.write(orjson.dumps({
            'modified': datetime.now(),
            'fields': list(LogItem._fields),
            'items': [tuple(item) for item in recent_logs],
        }))

    log_ids: dict[int, LogItem] = {item.id: item for item in recent_logs}
    file_ids = {int(f.stem): f for f in path.glob('[0-9]*.json')}

    for file_id, file in file_ids.items():
        if file_id not in log_ids:
            file.unlink()

    task_for_target: dict[str, Task] = {
        task.task_name: task
        for task in sync.tasks
    }

    for log_id, log in log_ids.items():
        if log_id not in file_ids:
            task = task_for_target[log.target]
            fields = list(task.main.fields)
            items = list(task.main.twenty(log.started))

            with open(path / f'{log_id}.json', 'wb') as f:
                f.write(orjson.dumps({
                    'modified': log.started,
                    'fields': fields,
                    'items': items,
                }))
