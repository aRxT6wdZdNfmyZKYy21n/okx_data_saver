import polars as pl

from main.web_gui.trade_research_dataset_common import (
    TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    append_forward_target_padding,
    last_real_sample_index,
    prepare_trade_research_raw_dataframe,
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
