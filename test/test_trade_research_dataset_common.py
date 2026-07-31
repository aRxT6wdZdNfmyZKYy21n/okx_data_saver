import polars as pl

from main.web_gui.trade_research_dataset_common import (
    TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    append_forward_target_padding,
    build_last_grid_inference_provenance,
    inference_provenance_fields,
    inference_tail_grid_sample_indices,
    inference_tail_pnl_sample_indices,
    inference_tail_selection_note,
    is_trade_research_entry_point_segment,
    last_real_sample_index,
    pnl_max_sample_index,
    prepare_trade_research_raw_dataframe,
    sample_indices_for_display_grid,
    sample_indices_for_pnl_grid,
)


def test_last_real_sample_index_skips_synthetic_tail() -> None:
    start_index = 2
    level0_to_raw = [0, 1, 2, 3, 4, 5, 6]
    real_bar_count = 5
    sample_index = last_real_sample_index(
        dataset_length=4,
        start_index=start_index,
        level0_to_raw=level0_to_raw,
        real_bar_count=real_bar_count,
    )
    assert sample_index == 2
    assert level0_to_raw[start_index + sample_index] == 4


def test_append_forward_target_padding_extends_raw_df() -> None:
    raw_df = pl.DataFrame(
        {
            'symbol_id': ['BTC_USDT'],
            'start_trade_id': [10],
            'end_trade_id': [19],
            'start_timestamp_ms': [0.0],
            'end_timestamp_ms': [60000.0],
            'open_price': [100.0],
            'high_price': [100.5],
            'low_price': [99.5],
            'close_price': [100.0],
            'total_volume': [1.0],
            'buy_volume': [0.5],
            'total_quantity': [1.0],
            'buy_quantity': [0.5],
            'total_trades_count': [1.0],
            'buy_trades_count': [1.0],
        }
    )
    padded, real_bar_count = append_forward_target_padding(raw_df, 2)
    assert real_bar_count == 1
    assert padded.height == 3
    assert padded.row(-1, named=True)['close_price'] == 100.0


def test_prepare_trade_research_raw_dataframe_uses_default_padding() -> None:
    raw_df = pl.DataFrame(
        {
            'symbol_id': ['BTC_USDT'],
            'start_trade_id': [10],
            'end_trade_id': [19],
            'start_timestamp_ms': [0.0],
            'end_timestamp_ms': [60000.0],
            'open_price': [100.0],
            'high_price': [100.5],
            'low_price': [99.5],
            'close_price': [100.0],
            'total_volume': [1.0],
            'buy_volume': [0.5],
            'total_quantity': [1.0],
            'buy_quantity': [0.5],
            'total_trades_count': [1.0],
            'buy_trades_count': [1.0],
        }
    )
    padded, real_bar_count = prepare_trade_research_raw_dataframe(
        raw_df,
        TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    )
    assert real_bar_count == 1
    assert padded.height == 1 + TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS


def test_inference_tail_sample_indices() -> None:
    train_map = {0: 0, 1536: 1}
    grid = [0, 1536, 3072]
    pnl = [512, 3072, 3584]
    assert inference_tail_grid_sample_indices(grid, train_map) == [3072]
    assert inference_tail_pnl_sample_indices(pnl, train_map) == [512, 3072, 3584]
    assert inference_tail_selection_note(1, 3) == (
        'inference tail 1 grid + 3 pnl samples (train_sample_index=-1)'
    )
    assert inference_tail_selection_note(0, 0) is None


def test_display_grid_extends_past_pnl_grid() -> None:
    dataset_length = 100000
    horizon_steps = 1536
    step_bars = 1536
    pnl_max = pnl_max_sample_index(dataset_length, horizon_steps)
    display_indices, _note = sample_indices_for_display_grid(dataset_length, step_bars)
    pnl_indices, _pnl_note = sample_indices_for_pnl_grid(
        dataset_length,
        step_bars,
        horizon_steps,
    )
    assert display_indices[-1] <= dataset_length - 1
    assert display_indices[-1] > pnl_indices[-1]
    assert len(display_indices) == len(pnl_indices) + 1


def test_is_trade_research_entry_point_segment() -> None:
    pnl_max = 98463
    start_index = 1000
    horizon_steps = 1536
    level0_height = 200000
    assert is_trade_research_entry_point_segment(
        sample_index=99840,
        pnl_max_sample_index=pnl_max,
        start_index=start_index,
        horizon_steps=horizon_steps,
        level0_height=level0_height,
    )
    assert not is_trade_research_entry_point_segment(
        sample_index=98304,
        pnl_max_sample_index=pnl_max,
        start_index=start_index,
        horizon_steps=horizon_steps,
        level0_height=level0_height,
    )
    assert is_trade_research_entry_point_segment(
        sample_index=100,
        pnl_max_sample_index=pnl_max,
        start_index=start_index,
        horizon_steps=horizon_steps,
        level0_height=start_index + 100 + horizon_steps,
    )


def test_inference_provenance_fields() -> None:
    fields = inference_provenance_fields(
        inference_x1_timestamp_ms=1_725_000_000_000,
        inference_entry_close=62758.2,
    )
    assert fields['inference_x1_timestamp_ms'] == 1_725_000_000_000
    assert fields['inference_entry_close'] == 62758.2


def test_build_last_grid_inference_provenance() -> None:
    entry_timestamp_ms = [1000, 2000, 3000]
    entry_close = [100.0, 101.0, 102.0]
    pred_start_price = [100.5, 101.5, 102.5]

    def row_for_sample(sample_index: int) -> int | None:
        mapping = {0: 0, 1536: 1, 3072: 2}
        if sample_index in mapping:
            return mapping[sample_index]
        return None

    segments_by_sample_index = {
        3072: {
            'sample_index': 3072,
            'segment_kind': 'entry_point',
            'action': 'long',
            'inference_x1_timestamp_ms': 3000,
            'inference_entry_close': 102.5,
        },
    }
    provenance = build_last_grid_inference_provenance(
        grid_sample_indices=[0, 1536, 3072],
        segments_by_sample_index=segments_by_sample_index,
        entry_timestamp_ms_by_row=entry_timestamp_ms,
        entry_close_by_row=entry_close,
        pred_start_price_by_row=pred_start_price,
        row_for_sample=row_for_sample,
    )
    assert provenance is not None
    assert provenance['sample_index'] == 3072
    assert provenance['inference_x1_timestamp_ms'] == 3000
    assert provenance['inference_entry_close'] == 102.5
    assert provenance['segment_kind'] == 'entry_point'
    assert provenance['action'] == 'long'
