from __future__ import annotations

import pytest
import torch
from torch.utils.data import TensorDataset

from main.offline_inference.trade_research_payload_dataloader_common import (
    build_trade_research_payload_dataloader,
)


class _FakePayloadDataset(TensorDataset):
    def __getitem__(self, index: int) -> tuple[int, dict[str, object]]:
        _tensor = super().__getitem__(index)
        return index, {'value': _tensor}


def test_build_trade_research_payload_dataloader_single_process() -> None:
    dataset = _FakePayloadDataset(torch.zeros(8, 2))
    loader = build_trade_research_payload_dataloader(
        payload_dataset=dataset,
        batch_size=4,
        num_workers=0,
        prefetch_factor=2,
    )
    assert loader.num_workers == 0
    batches = list(loader)
    assert len(batches) == 2
    sample_indices, payloads = batches[0]
    assert sample_indices == [0, 1, 2, 3]
    assert len(payloads) == 4


def test_build_trade_research_payload_dataloader_multiprocess_spawn() -> None:
    dataset = _FakePayloadDataset(torch.zeros(8, 2))
    loader = build_trade_research_payload_dataloader(
        payload_dataset=dataset,
        batch_size=4,
        num_workers=2,
        prefetch_factor=2,
    )
    assert loader.num_workers == 2
    assert loader.prefetch_factor == 2
    batches = list(loader)
    assert len(batches) == 2


def test_build_trade_research_payload_dataloader_rejects_invalid_num_workers() -> None:
    dataset = _FakePayloadDataset(torch.zeros(4, 2))
    with pytest.raises(ValueError, match='num_workers'):
        build_trade_research_payload_dataloader(
            payload_dataset=dataset,
            batch_size=2,
            num_workers=-1,
            prefetch_factor=2,
        )
