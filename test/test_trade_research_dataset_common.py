import polars as pl

from main.web_gui.trade_research_dataset_common import (
    append_forward_target_padding,
    real_last_start_trade_id,
    sample_exit_on_real_bars,
)


def _build_raw_dataframe(rows_count: int) -> pl.DataFrame:
    index = pl.Series('index', range(rows_count))
    return pl.DataFrame(
        {
            'symbol_id': ['BTC_USDT'] * rows_count,
            'start_trade_id': (index * 10).cast(pl.Int32),
            'end_trade_id': (index * 10 + 9).cast(pl.Int32),
            'start_timestamp_ms': (index * 60000).cast(pl.Int64),
            'end_timestamp_ms': ((index + 1) * 60000).cast(pl.Int64),
            'open_price': (100.0 + index).cast(pl.Float64),
            'high_price': (100.5 + index).cast(pl.Float64),
            'low_price': (99.5 + index).cast(pl.Float64),
            'close_price': (100.0 + index).cast(pl.Float64),
            'total_volume': (1.0 + index).cast(pl.Float64),
            'buy_volume': (0.5 + index).cast(pl.Float64),
            'total_quantity': (2.0 + index).cast(pl.Float64),
            'buy_quantity': (1.0 + index).cast(pl.Float64),
            'total_trades_count': (3.0 + index).cast(pl.Float64),
            'buy_trades_count': (1.0 + index).cast(pl.Float64),
        }
    )


def test_append_forward_target_padding_extends_and_preserves_real_tail() -> None:
    raw_df = _build_raw_dataframe(100)
    padded_df, real_bar_count = append_forward_target_padding(
        raw_df=raw_df,
        forward_bars=4,
    )
    assert real_bar_count == 100
    assert padded_df.height == 104
    assert padded_df.row(99, named=True)['close_price'] == 199.0
    assert padded_df.row(100, named=True)['close_price'] == 199.0
    assert padded_df.row(100, named=True)['total_volume'] == 0.0
    assert padded_df.row(103, named=True)['start_trade_id'] == 1030
    assert padded_df.schema['start_trade_id'] == pl.Int32


def test_real_last_start_trade_id_uses_real_tail() -> None:
    raw_df = _build_raw_dataframe(10)
    padded_df, real_bar_count = append_forward_target_padding(
        raw_df=raw_df,
        forward_bars=3,
    )
    assert real_last_start_trade_id(padded_df, real_bar_count) == 90


def test_sample_exit_on_real_bars() -> None:
    level0_to_raw = list(range(20))
    assert sample_exit_on_real_bars(
        sample_index=2,
        start_index=5,
        horizon_steps=3,
        level0_to_raw=level0_to_raw,
        real_bar_count=12,
    )
    assert not sample_exit_on_real_bars(
        sample_index=2,
        start_index=5,
        horizon_steps=3,
        level0_to_raw=level0_to_raw,
        real_bar_count=10,
    )
