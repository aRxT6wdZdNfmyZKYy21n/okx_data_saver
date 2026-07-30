import os

DEFAULT_SYMBOL_ID = 'BTC_USDT'


def repo_root() -> str:
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..'),
    )


def inference_artifact_dir(symbol_id: str) -> str:
    return os.path.join(repo_root(), 'data', 'inference', symbol_id)


def inference_artifact_path(symbol_id: str) -> str:
    return os.path.join(inference_artifact_dir(symbol_id), 'latest_inference.json')


def last_inference_ok_path(symbol_id: str) -> str:
    return os.path.join(inference_artifact_dir(symbol_id), 'last_inference_ok.json')


def trade_research_dir(symbol_id: str) -> str:
    return os.path.join(repo_root(), 'data', 'trade_research', symbol_id)


def trade_research_horizon_dir(symbol_id: str, eval_horizon: str) -> str:
    return os.path.join(trade_research_dir(symbol_id), eval_horizon)


def trade_research_npz_path(symbol_id: str, eval_horizon: str) -> str:
    return os.path.join(
        trade_research_horizon_dir(symbol_id, eval_horizon),
        'predictions.npz',
    )


def trade_research_inference_npz_path(symbol_id: str, eval_horizon: str) -> str:
    return os.path.join(
        trade_research_horizon_dir(symbol_id, eval_horizon),
        'predictions_inference.npz',
    )


def trade_research_meta_path(symbol_id: str, eval_horizon: str) -> str:
    return os.path.join(
        trade_research_horizon_dir(symbol_id, eval_horizon),
        'meta.json',
    )


def trade_research_inference_meta_path(symbol_id: str, eval_horizon: str) -> str:
    return os.path.join(
        trade_research_horizon_dir(symbol_id, eval_horizon),
        'meta_inference.json',
    )


def trade_research_legacy_npz_path(symbol_id: str) -> str:
    return os.path.join(trade_research_dir(symbol_id), 'predictions.npz')


def trade_research_legacy_meta_path(symbol_id: str) -> str:
    return os.path.join(trade_research_dir(symbol_id), 'meta.json')
