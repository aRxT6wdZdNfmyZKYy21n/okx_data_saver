"""Parity checks for rc7 target-first aggregation with forward-target padding."""

from __future__ import annotations

import pytest
import torch
from omegaconf import OmegaConf

from main.web_gui.inference_service import (
    _build_level0_to_raw_row_indices,
    _build_train_dataset,
    fetch_inference_metadata,
)
from main.web_gui.trade_research_dataset_common import (
    TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    append_forward_target_padding,
)


def _mid_history_sample_indices(
    level0_to_raw: list[int],
    start_index: int,
    dataset_length: int,
    real_bar_count: int,
    sample_count: int,
) -> list[int]:
    indices: list[int] = []
    for sample_index in range(dataset_length):
        entry_bar_index = start_index + sample_index
        raw_row = level0_to_raw[entry_bar_index]
        if raw_row >= real_bar_count:
            continue
        if raw_row < real_bar_count // 2:
            indices.append(sample_index)
        if len(indices) >= sample_count:
            break
    return indices


@pytest.mark.integration
def test_padded_train_mid_history_x_seq_matches_unpadded() -> None:
    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    bars_limit = required_rows + TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS + 50_000

    from enumerations import SymbolId
    from main.web_gui.data_service import fetch_last_bars_sync

    raw_df = fetch_last_bars_sync(
        symbol_id=SymbolId.BTC_USDT,
        limit=bars_limit,
        offset=0,
    )
    if raw_df is None or raw_df.height < required_rows + 10_000:
        pytest.skip('Insufficient BTC_USDT bars for parity probe')

    real_bar_count = int(raw_df.height)
    padded_df, _padding_real_count = append_forward_target_padding(
        raw_df=raw_df,
        forward_bars=TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    )

    unpadded_train = _build_train_dataset(df=raw_df, metadata=metadata)
    padded_train = _build_train_dataset(df=padded_df, metadata=metadata)

    unpadded_level0 = unpadded_train.aggregated_data[0]
    padded_level0 = padded_train.aggregated_data[0]
    tail_gain = int(padded_level0.height) - int(unpadded_level0.height)
    assert tail_gain > 0
    assert tail_gain <= TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS

    unpadded_to_raw = _build_level0_to_raw_row_indices(raw_df, unpadded_level0)
    padded_to_raw = _build_level0_to_raw_row_indices(padded_df, padded_level0)

    start_index = int(unpadded_train.start_index)
    sample_indices = _mid_history_sample_indices(
        level0_to_raw=unpadded_to_raw,
        start_index=start_index,
        dataset_length=len(unpadded_train),
        real_bar_count=real_bar_count,
        sample_count=8,
    )
    if len(sample_indices) == 0:
        pytest.skip('No mid-history samples found')

    raw_to_padded_sample: dict[int, int] = {}
    padded_start = int(padded_train.start_index)
    for padded_sample_index in range(len(padded_train)):
        entry_bar_index = padded_start + padded_sample_index
        raw_row = padded_to_raw[entry_bar_index]
        raw_to_padded_sample[raw_row] = padded_sample_index

    for unpadded_sample_index in sample_indices:
        raw_row = unpadded_to_raw[start_index + unpadded_sample_index]
        padded_sample_index = raw_to_padded_sample[raw_row]

        unpadded_x_seq, _, _ = unpadded_train[unpadded_sample_index]
        padded_x_seq, _, _ = padded_train[padded_sample_index]

        for scale_name in unpadded_x_seq:
            left = unpadded_x_seq[scale_name]
            right = padded_x_seq[scale_name]
            assert left.shape == right.shape
            diff = (left - right).abs().max().item()
            assert diff == 0.0, (
                f'scale {scale_name} sample raw_row={raw_row}: max_abs_diff={diff}'
            )
