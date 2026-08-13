"""Cache-busting helpers for web GUI static assets."""

from __future__ import annotations

import os

from settings import settings

_STATIC_FILENAMES = (
    'app.js',
    'style.css',
    'index.html',
)

_INDEX_TEMPLATE_PLACEHOLDER = '__ASSET_VERSION__'


def static_dir_path() -> str:
    return os.path.join(os.path.dirname(__file__), 'static')


def compute_static_asset_version(static_dir: str) -> str:
    if settings.WEB_GUI_ASSET_VERSION is not None:
        configured = str(settings.WEB_GUI_ASSET_VERSION).strip()
        if configured:
            return configured
    mtimes: list[int] = []
    for filename in _STATIC_FILENAMES:
        path = os.path.join(static_dir, filename)
        if os.path.isfile(path):
            mtimes.append(int(os.path.getmtime(path)))
    if not mtimes:
        return '0'
    return str(max(mtimes))


def render_index_html(static_dir: str) -> str:
    index_path = os.path.join(static_dir, 'index.html')
    if not os.path.isfile(index_path):
        return (
            '<html><body><p>OKX Data Set Web GUI. '
            'Place index.html in main/web_gui/static/</p></body></html>'
        )
    asset_version = compute_static_asset_version(static_dir)
    with open(index_path, encoding='utf-8') as index_file:
        template = index_file.read()
    return template.replace(_INDEX_TEMPLATE_PLACEHOLDER, asset_version)
