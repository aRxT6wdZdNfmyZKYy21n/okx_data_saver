from __future__ import annotations

from torch.utils.data import DataLoader, Dataset

from main.web_gui.inference_service import (
    _prepare_payload_dict_from_sample,
    _prepare_payload_dict_from_train_sample,
)


def _payload_batch_collate(
    batch: list[tuple[int, dict[str, object]]],
) -> tuple[list[int], list[dict[str, object]]]:
    sample_indices = [item[0] for item in batch]
    payloads = [item[1] for item in batch]
    return sample_indices, payloads


class TrainSamplePayloadDataset(Dataset[tuple[int, dict[str, object]]]):
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

    def __getitem__(self, index: int) -> tuple[int, dict[str, object]]:
        sample_index = self._sample_indices[index]
        train_sample_index = self._train_sample_index_by_inference_sample[sample_index]
        payload = _prepare_payload_dict_from_train_sample(
            train_dataset=self._train_dataset,
            train_sample_index=train_sample_index,
        )
        return sample_index, payload


class InferenceSamplePayloadDataset(Dataset[tuple[int, dict[str, object]]]):
    def __init__(
        self,
        inference_dataset: object,
        sample_indices: list[int],
    ) -> None:
        self._inference_dataset = inference_dataset
        self._sample_indices = sample_indices

    def __len__(self) -> int:
        return len(self._sample_indices)

    def __getitem__(self, index: int) -> tuple[int, dict[str, object]]:
        sample_index = self._sample_indices[index]
        payload = _prepare_payload_dict_from_sample(
            dataset=self._inference_dataset,
            sample_index=sample_index,
        )
        return sample_index, payload


def build_trade_research_payload_dataloader(
    payload_dataset: Dataset[tuple[int, dict[str, object]]],
    batch_size: int,
    num_workers: int,
    prefetch_factor: int,
) -> DataLoader[tuple[int, dict[str, object]]]:
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
        'collate_fn': _payload_batch_collate,
    }
    if num_workers > 0:
        loader_kwargs['prefetch_factor'] = prefetch_factor
        loader_kwargs['persistent_workers'] = True
        loader_kwargs['multiprocessing_context'] = 'spawn'
    return DataLoader(payload_dataset, **loader_kwargs)
