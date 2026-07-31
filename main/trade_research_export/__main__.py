import argparse
import logging
import sys

import main.spawn_process as spawn_process
from main.runtime_limits import apply_runtime_limits
from main.offline_inference.paths import DEFAULT_SYMBOL_ID
from main.spawn_process import run_in_spawned_process
from main.web_gui.request_workers import _worker_trade_research_export_safe
from settings import settings

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Export trade research predictions to NPZ (always incremental)',
    )
    parser.add_argument(
        '--symbol',
        type=str,
        default=settings.INFERENCE_DAEMON_SYMBOL,
        help=f'SymbolId name (default: {DEFAULT_SYMBOL_ID})',
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='INFO logging (DB reads, dataset preparation, batch inference progress)',
    )
    return parser.parse_args()


def main() -> None:
    apply_runtime_limits()

    arguments = parse_arguments()
    spawn_process.VERBOSE = arguments.verbose

    logging.basicConfig(
        level=logging.INFO if arguments.verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        stream=sys.stdout,
    )

    run_in_spawned_process(
        _worker_trade_research_export_safe,
        arguments.symbol,
    )


if __name__ == '__main__':
    main()
