from __future__ import annotations

import json

import numpy as np

from main.web_gui.trade_research_service import (
    TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS,
    _direction_action_from_inference,
    _hybrid_backtest_allows_entry,
    _recommended_entry_action,
)


class TradeResearchNpzStore:
    """
    Row-oriented доступ к trade research NPZ без построения dict на все sample_index.
    sample_index в артефакте отсортирован (см. trade_research_export).
    """

    def __init__(
        self,
        npz_data: np.lib.npyio.NpzFile,
    ) -> None:
        for key in TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS:
            if key not in npz_data.files:
                raise RuntimeError(
                    'Trade research NPZ missing policy columns; re-run '
                    'main.trade_research_export against the current inference_api config',
                )

        sample_index = npz_data['sample_index'].astype(np.int64)
        if sample_index.shape[0] > 1:
            if not bool(np.all(sample_index[:-1] <= sample_index[1:])):
                raise RuntimeError('Trade research NPZ sample_index must be sorted')

        self._sample_index = sample_index
        self._policy_action = npz_data['policy_action']
        self._policy_prob_hold = npz_data['policy_prob_hold'].astype(np.float64)
        self._policy_prob_long = npz_data['policy_prob_long'].astype(np.float64)
        self._policy_prob_short = npz_data['policy_prob_short'].astype(np.float64)
        self._entry_hint_json = npz_data['entry_hint_json']
        self._entry_hint_cache: dict[int, dict[str, object]] = {}
        if 'train_sample_index' in npz_data.files:
            self._train_sample_index = npz_data['train_sample_index'].astype(np.int64)
        else:
            self._train_sample_index = None
        if 'train_size' in npz_data.files:
            self._train_size = int(npz_data['train_size'][0])
        else:
            self._train_size = None
        if 'train_size_ratio' in npz_data.files:
            self._train_size_ratio = float(npz_data['train_size_ratio'][0])
        else:
            self._train_size_ratio = None

    @property
    def val_split_available(self) -> bool:
        return (
            self._train_sample_index is not None
            and self._train_size is not None
            and self._train_size_ratio is not None
        )

    @property
    def train_size(self) -> int | None:
        return self._train_size

    @property
    def train_size_ratio(self) -> float | None:
        return self._train_size_ratio

    def has_train_aligned_targets_at_row(self, row_index: int) -> bool:
        if self._train_sample_index is None:
            return True
        return int(self._train_sample_index[row_index]) >= 0

    def row_matches_split(self, row_index: int, split: str) -> bool:
        if split == 'all':
            return True
        if not self.val_split_available:
            return False
        train_sample_index = int(self._train_sample_index[row_index])
        if train_sample_index < 0:
            return False
        if self._train_size is None:
            return False
        if split == 'val':
            return train_sample_index >= self._train_size
        if split == 'train':
            return train_sample_index < self._train_size
        raise ValueError(f'Unknown split: {split!r}')

    @property
    def sample_index_array(self) -> np.ndarray:
        return self._sample_index

    def row_count(self) -> int:
        return int(self._sample_index.shape[0])

    def row_for_sample(self, sample_index_value: int) -> int | None:
        row_index = int(np.searchsorted(self._sample_index, sample_index_value))
        if row_index >= self.row_count():
            return None
        if int(self._sample_index[row_index]) != sample_index_value:
            return None
        return row_index

    def has_sample(self, sample_index_value: int) -> bool:
        return self.row_for_sample(sample_index_value) is not None

    def entry_hint_at_row(self, row_index: int) -> dict[str, object]:
        if row_index in self._entry_hint_cache:
            return self._entry_hint_cache[row_index]

        entry_hint_raw = self._entry_hint_json[row_index]
        if isinstance(entry_hint_raw, bytes):
            entry_hint_text = entry_hint_raw.decode('utf-8')
        else:
            entry_hint_text = str(entry_hint_raw)
        entry_hint = json.loads(entry_hint_text)
        if not isinstance(entry_hint, dict):
            sample_index_value = int(self._sample_index[row_index])
            raise RuntimeError(f'Invalid entry_hint JSON for sample {sample_index_value}')

        self._entry_hint_cache[row_index] = entry_hint
        return entry_hint

    def inference_result_at_row(self, row_index: int) -> dict[str, object]:
        return {
            'policy': {
                'action': str(self._policy_action[row_index]),
                'probabilities': {
                    'hold': float(self._policy_prob_hold[row_index]),
                    'long': float(self._policy_prob_long[row_index]),
                    'short': float(self._policy_prob_short[row_index]),
                },
            },
            'entry_hint': self.entry_hint_at_row(row_index),
        }

    def policy_action_at_row(self, row_index: int) -> str:
        return str(self._policy_action[row_index])

    def allows_entry_at_row(self, row_index: int) -> bool:
        return _hybrid_backtest_allows_entry(self.inference_result_at_row(row_index))

    def recommended_action_at_row(self, row_index: int) -> str | None:
        return _recommended_entry_action(self.inference_result_at_row(row_index))

    def direction_action_at_row(self, row_index: int) -> str:
        return _direction_action_from_inference(self.inference_result_at_row(row_index))

    def next_cached_sample_index(self, sample_index_value: int) -> int | None:
        row_index = int(np.searchsorted(self._sample_index, sample_index_value, side='left'))
        if row_index >= self.row_count():
            return None
        return int(self._sample_index[row_index])
