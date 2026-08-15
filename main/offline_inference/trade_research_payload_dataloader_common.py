from __future__ import annotations

import torch
from torch.utils.data import DataLoader, Dataset

from main.web_gui.inference_service import (
    _payload_dicts_from_batched_tensors,
)


SampleTensorBatch = tuple[
    list[int],
    dict[str, torch.Tensor],
    torch.Tensor,
]

SampleTensorItem = tuple[int, dict[str, torch.Tensor], torch.Tensor]


def _payload_tensor_batch_collate(
    batch: list[SampleTensorItem],
) -> SampleTensorBatch:
    sample_indices = [item[0] for item in batch]
    scale_names = batch[0][1].keys()
    batched_x_seq = {
        scale_name: torch.stack(
            [item[1][scale_name] for item in batch],
            dim=0,
        )
        for scale_name in scale_names
    }
    batched_x_static = torch.stack(
        [item[2] for item in batch],
        dim=0,
    )
    return sample_indices, batched_x_seq, batched_x_static


class TrainSamplePayloadDataset(Dataset[SampleTensorItem]):
    def __init__(
        self,
        train_dataset: object,
        sample_indices: list[int],
        train_sample_index_by_inference_sample: dict[int, int],
    ) -> None:
        self._train_dataset = train_dataset
        self._sample_indices = sample_indices
        self._train_sample_index_by_inference_sample = train_sample_index_by_inference_sample

    def __len__(self) -> int:
        return len(self._sample_indices)

    def __getitem__(self, index: int) -> SampleTensorItem:
        sample_index = self._sample_indices[index]
        train_sample_index = self._train_sample_index_by_inference_sample[sample_index]
        x_seq, x_static, _targets = self._train_dataset[train_sample_index]
        return sample_index, x_seq, x_static


class InferenceSamplePayloadDataset(Dataset[SampleTensorItem]):
    def __init__(
        self,
        inference_dataset: object,
        sample_indices: list[int],
    ) -> None:
        self._inference_dataset = inference_dataset
        self._sample_indices = sample_indices

    def __len__(self) -> int:
        return len(self._sample_indices)

    def __getitem__(self, index: int) -> SampleTensorItem:
        sample_index = self._sample_indices[index]
        x_seq, x_static = self._inference_dataset[sample_index]
        return sample_index, x_seq, x_static


def build_trade_research_payload_dataloader(
    payload_dataset: Dataset[SampleTensorItem],
    batch_size: int,
    num_workers: int,
    prefetch_factor: int,
) -> DataLoader[SampleTensorBatch]:
    if batch_size <= 0:
        raise ValueError(f'batch_size must be positive, got: {batch_size}')
    if num_workers < 0:
        raise ValueError(f'num_workers must be non-negative, got: {num_workers}')
    if prefetch_factor <= 0:
        raise ValueError(
            f'prefetch_factor must be positive, got: {prefetch_factor}',
        )
    loader_kwargs: dict[str, object] = {
        'batch_size': batch_size,
        'shuffle': False,
        'num_workers': num_workers,
        'drop_last': False,
        'collate_fn': _payload_tensor_batch_collate,
    }
    if num_workers > 0:
        loader_kwargs['prefetch_factor'] = prefetch_factor
        loader_kwargs['persistent_workers'] = True
        loader_kwargs['multiprocessing_context'] = 'spawn'
    return DataLoader(payload_dataset, **loader_kwargs)


def payload_dicts_from_loader_batch(
    batch: SampleTensorBatch,
) -> tuple[list[int], list[dict[str, object]]]:
    sample_indices, batched_x_seq, batched_x_static = batch
    payloads = _payload_dicts_from_batched_tensors(
        batched_x_seq=batched_x_seq,
        batched_x_static=batched_x_static,
    )
    return sample_indices, payloads
