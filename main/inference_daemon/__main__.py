import argparse
import asyncio
import logging
import sys

import main.spawn_process as spawn_process
from main.runtime_limits import apply_runtime_limits
from main.offline_inference.artifacts import (
    write_latest_inference_computing,
    write_latest_inference_error,
)
from main.spawn_process import run_in_spawned_process
from main.web_gui.request_workers import _worker_inference_cycle_safe
from settings import settings

logger = logging.getLogger(__name__)


async def _daemon_loop(symbol_id: str) -> None:
    interval_sec = settings.INFERENCE_DAEMON_INTERVAL_SEC
    logger.info(
        'Inference daemon started: symbol=%s interval=%ds bars_limit=%d verbose=%s',
        symbol_id,
        interval_sec,
        settings.INFERENCE_DAEMON_BARS_LIMIT,
        spawn_process.VERBOSE,
    )
    while True:
        write_latest_inference_computing(symbol_id=symbol_id)
        try:
            await asyncio.to_thread(
                run_in_spawned_process,
                _worker_inference_cycle_safe,
                symbol_id,
            )
        except Exception as exception:
            logger.error('Inference daemon spawn cycle failed: %s', exception)
            write_latest_inference_error(
                symbol_id=symbol_id,
                error_message=str(exception),
            )
        await asyncio.sleep(interval_sec)


def main() -> None:
    apply_runtime_limits()

    parser = argparse.ArgumentParser(description='Offline inference daemon')
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='INFO logging (DB reads, dataset preparation, inference steps)',
    )
    arguments = parser.parse_args()

    spawn_process.VERBOSE = arguments.verbose

    logging.basicConfig(
        level=logging.INFO if arguments.verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        stream=sys.stdout,
    )

    symbol_id = settings.INFERENCE_DAEMON_SYMBOL
    asyncio.run(_daemon_loop(symbol_id=symbol_id))


if __name__ == '__main__':
    main()
