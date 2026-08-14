"""
Structured JSONL log for automated trading daemon decisions (incident replay).
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

from main.web_gui.trade_journal_service import journal_path

logger = logging.getLogger(__name__)

_LOG_LOCK = threading.Lock()


def execution_log_path() -> str:
    journal_file = journal_path()
    journal_dir = os.path.dirname(journal_file)
    return os.path.join(journal_dir, 'trade_execution.jsonl')


def append_execution_event(event_type: str, payload: dict[str, Any]) -> None:
    record: dict[str, Any] = {
        'ts_utc': datetime.now(timezone.utc).isoformat(),
        'event': event_type,
    }
    record.update(payload)
    path = execution_log_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + '\n'
    with _LOG_LOCK:
        with open(path, 'a', encoding='utf-8') as log_file:
            log_file.write(line)
    logger.info('trade_execution %s %s', event_type, json.dumps(payload, ensure_ascii=False))
