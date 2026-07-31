import polars as pl

from main.web_gui.trade_research_dataset_common import (
    TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    append_forward_target_padding,
    prepare_trade_research_raw_dataframe,
)


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
    padded, real_bar_count = prepare_trade_research_raw_dataframe(raw_df)
    assert real_bar_count == 1
    assert padded.height == 1 + TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS
