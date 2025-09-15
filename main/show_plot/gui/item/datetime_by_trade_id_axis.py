import typing

from datetime import (
    datetime,
)

import pandas
import pyqtgraph

if typing.TYPE_CHECKING:
    from main.show_plot.processor import (
        FinPlotChartProcessor,
    )


class DateTimeByTradeIDAxisItem(pyqtgraph.AxisItem):
    __slots__ = ('__processor',)

    def __init__(
        self,
        processor,  # type: FinPlotChartProcessor
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.setLabel(
            text='Time by trade ID',
            units=None,
        )

        self.enableAutoSIPrefix(
            False,
        )

        self.__processor = processor

    def tickStrings(self, values, scale, spacing):
        processor = self.__processor

        trades_dataframe = processor.get_trades_dataframe()

        tick_strings: list[str] = []

        trade_id_float: float

        for trade_id_float in values:
            trade_id = int(
                trade_id_float,
            )

            tick_string = f'{trade_id}'

            if trades_dataframe is not None:
                if trade_id in trades_dataframe.index:
                    row = trades_dataframe.loc[trade_id]

                    timestamp: pandas.Timestamp = row.timestamp_ms

                    try:
                        tick_string = '\n'.join(
                            (
                                datetime.fromtimestamp(
                                    timestamp.value / 10**9,
                                ).isoformat(),
                                tick_string,
                            )
                        )
                    except ValueError:
                        pass

            tick_strings.append(
                tick_string,
            )

        return tick_strings
