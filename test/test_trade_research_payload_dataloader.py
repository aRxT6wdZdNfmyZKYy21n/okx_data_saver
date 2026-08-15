from __future__ import annotations

import pytest
import torch
from torch.utils.data import Dataset

from main.offline_inference.trade_research_payload_dataloader_common import (
    build_trade_research_payload_dataloader,
    payload_dicts_from_loader_batch,
)


class _FakeSampleTensorDataset(Dataset[tuple[int, dict[str, torch.Tensor], torch.Tensor]]):
    def __init__(self, sample_count: int) -> None:
        self._sample_count = sample_count

    def __len__(self) -> int:
        return self._sample_count

    def __getitem__(
        self,
        index: int,
    ) -> tuple[int, dict[str, torch.Tensor], torch.Tensor]:
        return (
            index,
            {'x1': torch.full((4, 2), float(index))},
            torch.full((3,), float(index)),
        )


def test_build_trade_research_payload_dataloader_single_process() -> None:
    dataset = _FakeSampleTensorDataset(8)
    loader = build_trade_research_payload_dataloader(
        payload_dataset=dataset,
        batch_size=4,
        num_workers=0,
        prefetch_factor=2,
    )
    assert loader.num_workers == 0
    batches = list(loader)
    assert len(batches) == 2
    sample_indices, payloads = payload_dicts_from_loader_batch(batches[0])
    assert sample_indices == [0, 1, 2, 3]
    assert len(payloads) == 4


def test_build_trade_research_payload_dataloader_multiprocess_spawn() -> None:
    dataset = _FakeSampleTensorDataset(8)
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


def test_build_trade_research_payload_dataloader_multiprocess_large_batch_count() -> None:
    dataset = _FakeSampleTensorDataset(128)
    loader = build_trade_research_payload_dataloader(
        payload_dataset=dataset,
        batch_size=8,
        num_workers=2,
        prefetch_factor=2,
    )
    batches = list(loader)
    assert len(batches) == 16
    all_sample_indices: list[int] = []
    for loader_batch in batches:
        sample_indices, _payloads = payload_dicts_from_loader_batch(loader_batch)
        all_sample_indices.extend(sample_indices)
    assert all_sample_indices == list(range(128))


def test_build_trade_research_payload_dataloader_rejects_invalid_num_workers() -> None:
    dataset = _FakeSampleTensorDataset(4)
    with pytest.raises(ValueError, match='num_workers'):
        build_trade_research_payload_dataloader(
            payload_dataset=dataset,
            batch_size=2,
            num_workers=-1,
            prefetch_factor=2,
        )
