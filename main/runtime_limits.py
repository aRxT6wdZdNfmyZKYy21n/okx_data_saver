import os

from settings import settings


def apply_runtime_limits() -> None:
    os.environ['POLARS_MAX_THREADS'] = str(settings.POLARS_MAX_THREADS)
