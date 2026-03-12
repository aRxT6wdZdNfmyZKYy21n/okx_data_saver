#!/usr/bin/env python3
"""
Запуск веб-GUI: uvicorn main.web_gui.api:app.

Пример:
  python -m main.web_gui --host 0.0.0.0 --port 8000
"""

import argparse
import logging
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description='OKX Data Set Web GUI')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind')
    parser.add_argument('--reload', action='store_true', help='Enable reload for development')
    parser.add_argument('-v', '--verbose', action='store_true', help='INFO logging (e.g. Processed row #...)')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        stream=sys.stdout,
    )

    import main.web_gui.request_workers as _request_workers
    _request_workers.VERBOSE = args.verbose

    from main.web_gui import api  # noqa: F401 — монтирование static при импорте

    import uvicorn
    uvicorn.run(
        'main.web_gui.api:app',
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == '__main__':
    main()
