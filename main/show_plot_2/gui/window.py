from __future__ import annotations

import asyncio
import traceback
import typing
from collections import (
    defaultdict,
)
from datetime import (
    datetime,
)
from functools import (
    partial,
)

import pyqtgraph
from chrono import (
    Timer,
)
from PyQt6.QtCore import (
    QPointF,
    Qt,
)
from PyQt6.QtGui import (
    QColor,
)
from PyQt6.QtWidgets import (
    QGridLayout,
    QMainWindow,
    QVBoxLayout,
    QWidget,
)
from qasync import (
    asyncSlot,
)

from constants.plot import (
    PlotConstants,
)
from main.show_plot_2.gui.item.candlestick import (
    CandlestickItem,
)
from main.show_plot_2.gui.item.datetime_axis import (
    DateTimeAxisItem,
)
from utils.async_ import (
    create_task_with_exceptions_logging,
)
from utils.qt import (
    QtUtils,
)

if typing.TYPE_CHECKING:
    from main.show_plot_2.processor import FinPlotChartProcessor

    print(
        FinPlotChartProcessor.__name__,  # Do not delete FinPlotChartProcessor by ruff
    )


_IS_NEED_DRAW_CLOSE_PRICE_PLOT_WITH_POINTS = False
# _IS_NEED_SHOW_BOLLINGER_BANDS = False
_IS_NEED_SHOW_CANDLES = False
# _IS_NEED_SHOW_RSI = False
_IS_NEED_SHOW_VELOCITY = True

_BOLLINGER_BANDS_FILL_COLOR = QColor(
    33,
    150,
    243,
    12,
)

_BOLLINGER_BASE_LINE_COLOR = '#2962ff'
_BOLLINGER_LOWER_BAND_COLOR = '#089981'
_BOLLINGER_UPPER_BAND_COLOR = '#f23645'

_PLOT_BACKGROUND_UPPER_COLOR = QColor(
    126,
    87,
    194,
)

_PLOT_BACKGROUND_LOWER_COLOR = QColor(
    67,
    70,
    81,
)

_RSI_PLOT_GRADIENT_UPPER_START_COLOR = QColor(
    76,
    175,
    80,
)

_RSI_PLOT_GRADIENT_UPPER_END_COLOR = QColor(
    76,
    175,
    80,
    0,
)

_RSI_PLOT_GRADIENT_LOWER_START_COLOR = QColor(
    255,
    82,
    82,
    0,
)

_RSI_PLOT_GRADIENT_LOWER_END_COLOR = QColor(
    255,
    82,
    82,
    255,
)

_RSI_LINE_COLOR = '#7e57c2'
_TEST_LINE_COLOR = '#ffffff'
_VELOCITY_LINE_COLOR = '#ffffff'


class FinPlotChartWindow(QMainWindow):
    def __init__(
        self,
        processor,  # type: FinPlotChartProcessor
        parent=None,
    ):
        super().__init__(
            parent,
        )

        functionality_layout = QGridLayout()

        window_layout_widget = QWidget()

        window_layout = QVBoxLayout(
            window_layout_widget,
        )

        self.setCentralWidget(
            window_layout_widget,
        )

        # self.setLayout(
        #     window_layout
        # )

        self.setMinimumSize(
            1366,  # 1920
            768,  # 1080
        )

        self.setWindowFlags(
            Qt.WindowType(
                Qt.WindowType.WindowMaximizeButtonHint
                | Qt.WindowType.WindowMinimizeButtonHint
                | Qt.WindowType.WindowCloseButtonHint,
            )
        )

        self.setWindowTitle(
            'Chart',
        )

        # extreme_lines_image_item = pyqtgraph.ImageItem()
        # order_book_volumes_asks_image_item = pyqtgraph.ImageItem()

        # position_array = numpy.array(
        #     [
        #         0.0,
        #         1.0,
        #     ],
        # )

        # green_color_array = numpy.array(
        #     [
        #         [
        #             0,  # R
        #             0,  # G
        #             0,  # B
        #             0,  # A
        #         ],
        #         [
        #             0,  # R
        #             255,  # G
        #             0,  # B
        #             255,  # A
        #         ],
        #     ],
        # )
        #
        # green_color_map = pyqtgraph.ColorMap(
        #     position_array,
        #     green_color_array,
        # )

        # order_book_volumes_asks_image_item.setColorMap(green_color_map)
        #
        # order_book_volumes_bids_image_item = pyqtgraph.ImageItem()
        #
        # red_color_array = numpy.array(
        #     [
        #         [
        #             0,  # R
        #             0,  # G
        #             0,  # B
        #             0,  # A
        #         ],
        #         [
        #             255,  # R
        #             0,  # G
        #             0,  # B
        #             255,  # A
        #         ],
        #     ],
        # )
        #
        # red_color_map = pyqtgraph.ColorMap(
        #     position_array,
        #     red_color_array,
        # )
        #
        # order_book_volumes_bids_image_item.setColorMap(
        #     red_color_map,
        # )

        graphics_layout_widget: pyqtgraph.GraphicsLayout = (  # noqa
            pyqtgraph.GraphicsLayoutWidget()
        )

        candle_plot = graphics_layout_widget.addPlot(
            title='Candle',
        )

        candle_date_axis = DateTimeAxisItem(
            orientation='bottom',
            processor=processor,
        )

        candle_plot.setAxisItems(
            {
                'bottom': candle_date_axis,
            },
        )

        candle_plot.showGrid(
            x=True,
            y=True,
        )

        candle_plot.sigXRangeChanged.connect(
            partial(
                self.__update_plots_x_range,
                candle_plot,
            ),
        )

        candle_plot.sigYRangeChanged.connect(
            partial(
                self.__update_plots_y_range,
                candle_plot,
            ),
        )

        # candle_plot.addItem(
        #     extreme_lines_image_item,
        # )

        # candle_plot.addItem(
        #     order_book_volumes_asks_image_item,
        # )

        # candle_plot.addItem(
        #     order_book_volumes_bids_image_item,
        # )

        graphics_layout_widget.nextRow()

        # candles_plot_by_interval_name_map: dict[str, typing.Any] = {}  # TODO: typing
        #
        # for interval_name in PlotConstants.IntervalNames:
        #     candles_plot = graphics_layout_widget.addPlot(
        #         title=f'Candles ({interval_name})',
        #     )
        #
        #     candles_date_axis = DateTimeByTradeIDAxisItem(
        #         orientation='bottom',
        #         processor=processor,
        #     )
        #
        #     candles_plot.setAxisItems(
        #         {
        #             'bottom': candles_date_axis,
        #         },
        #     )
        #
        #     candles_plot.showGrid(
        #         x=True,
        #         y=True,
        #     )
        #
        #     assert interval_name not in candles_plot_by_interval_name_map, (
        #         interval_name,
        #     )
        #
        #     candles_plot_by_interval_name_map[interval_name] = candles_plot
        #
        #     candles_plot.sigXRangeChanged.connect(
        #         partial(
        #             self.__update_plots_x_range,
        #             candles_plot,
        #         ),
        #     )
        #
        #     candles_plot.sigYRangeChanged.connect(
        #         partial(
        #             self.__update_plots_y_range,
        #             candles_plot,
        #         ),
        #     )
        #
        #     graphics_layout_widget.nextRow()

        # if _IS_NEED_SHOW_RSI:
        #     rsi_plot = graphics_layout_widget.addPlot(
        #         title='RSI',
        #     )
        #
        #     rsi_date_axis = DateTimeByTradeIDAxisItem(
        #         orientation='bottom',
        #         processor=processor,
        #     )
        #
        #     rsi_plot.setAxisItems(
        #         {
        #             'bottom': rsi_date_axis,
        #         },
        #     )
        #
        #     rsi_plot.showGrid(
        #         x=True,
        #         y=True,
        #     )
        #
        #     rsi_plot.sigXRangeChanged.connect(
        #         partial(
        #             self.__update_plots_x_range,
        #             rsi_plot,
        #         ),
        #     )
        #
        #     graphics_layout_widget.nextRow()

        if _IS_NEED_SHOW_VELOCITY:
            velocity_plot = graphics_layout_widget.addPlot(
                title=f'Trades per {PlotConstants.VelocityIntervalName}',
            )

            velocity_date_axis = DateTimeAxisItem(
                orientation='bottom',
                processor=processor,
            )

            velocity_plot.setAxisItems(
                {
                    'bottom': velocity_date_axis,
                },
            )

            velocity_plot.showGrid(
                x=True,
                y=True,
            )

            velocity_plot.sigXRangeChanged.connect(
                partial(
                    self.__update_plots_x_range,
                    velocity_plot,
                ),
            )

            graphics_layout_widget.nextRow()

        # Create a linear gradient for any plot background

        """
        plot_background_gradient = (
            QLinearGradient(
                0,
                0,
                0,
                1
            )
        )

        plot_background_gradient.setColorAt(
            0.0,
            _PLOT_BACKGROUND_UPPER_COLOR
        )

        plot_background_gradient.setColorAt(
            1.0,
            _PLOT_BACKGROUND_LOWER_COLOR
        )

        plot_background_gradient.setCoordinateMode(
            QLinearGradient.CoordinateMode.ObjectMode
        )

        plot_background_brush = (
            QBrush(
                plot_background_gradient
            )
        )

        price_view_box.setBackgroundColor(
            plot_background_brush
        )

        rsi_view_box.setBackgroundColor(
            plot_background_brush
        )
        """

        (
            symbol_name_label,
            symbol_name_combo_box,
        ) = QtUtils.create_label_and_combo_box(
            'Символ',
            self.__on_symbol_name_changed,
            alignment=Qt.AlignmentFlag.AlignLeft,
        )

        (
            data_set_idx_label,
            data_set_idx_combo_box,
        ) = QtUtils.create_label_and_combo_box(
            'ID датасета',
            self.__on_data_set_idx_changed,
            alignment=Qt.AlignmentFlag.AlignLeft,
        )

        # if _IS_NEED_SHOW_RSI:
        #     (
        #         rsi_interval_name_label,
        #         rsi_interval_name_combo_box,
        #     ) = QtUtils.create_label_and_combo_box(
        #         'RSI timeframe',
        #         self.__on_rsi_interval_name_changed,
        #         alignment=Qt.AlignmentFlag.AlignLeft,
        #         values=PlotConstants.IntervalNames,
        #     )

        # (
        #     trades_smoothing_level_label,
        #     trades_smoothing_level_combo_box,
        # ) = QtUtils.create_label_and_combo_box(
        #     'Trades smoothing level',
        #     self.__on_trades_smoothing_level_changed,
        #     alignment=Qt.AlignmentFlag.AlignLeft,
        #     values=PlotConstants.TradesSmoothingLevels,
        # )

        self.__graphics_layout_widget = graphics_layout_widget

        # self.__bollinger_base_line_plot_data_item = candle_plot.plot(
        #     pen=_BOLLINGER_BASE_LINE_COLOR,
        #     name='Bollinger Base Line',
        # )

        # self.__bollinger_lower_band_plot_data_item = candle_plot.plot(
        #     pen=_BOLLINGER_LOWER_BAND_COLOR,
        #     name='Bollinger Lower Band',
        # )

        # self.__bollinger_upper_band_plot_data_item = candle_plot.plot(
        #     pen=_BOLLINGER_UPPER_BAND_COLOR,
        #     name='Bollinger Upper Band',
        # )

        # TODO
        """
        self.__bollinger_bands_fill_between_item: (
            pyqtgraph.FillBetweenItem | None
        ) = None
        """

        # self.__candles_plot_by_interval_name_map = candles_plot_by_interval_name_map
        # self.__extreme_lines_image_item = extreme_lines_image_item

        self.__drawing_lock = asyncio.Lock()

        # self.__order_book_volumes_asks_image_item = order_book_volumes_asks_image_item
        # self.__order_book_volumes_bids_image_item = order_book_volumes_bids_image_item
        self.__candle_plot = candle_plot

        candle_plot_close_price_data_item_kwargs = {
            'pen': (
                255,
                255,
                255,
            ),
        }

        if _IS_NEED_DRAW_CLOSE_PRICE_PLOT_WITH_POINTS:
            candle_plot_close_price_data_item_kwargs.update(
                dict(
                    symbolBrush=(
                        127,
                        0,
                        0,
                    ),
                    symbolPen='w',
                ),
            )

        self.__candle_plot_close_price_data_item = candle_plot.plot(
            name='Close price',
            **candle_plot_close_price_data_item_kwargs,
        )

        self.__candle_plot_high_price_data_item = candle_plot.plot(
            name='High price',
            pen=(
                127,
                127,
                127,
            ),
        )

        self.__candle_plot_low_price_data_item = candle_plot.plot(
            name='Low price',
            pen=(
                127,
                127,
                127,
            ),
        )

        self.__candle_plot_max_ask_price_data_item = candle_plot.plot(
            name='Max ask price',
            pen=(
                0,  # Red
                127,  # Green
                0,
            ),
        )

        self.__candle_plot_max_bid_price_data_item = candle_plot.plot(
            name='Max bid price',
            pen=(
                127,  # Red
                0,  # Green
                0,
            ),
        )

        self.__candle_plot_min_ask_price_data_item = candle_plot.plot(
            name='Min ask price',
            pen=(
                0,  # Red
                127,  # Green
                0,
            ),
        )

        self.__candle_plot_min_bid_price_data_item = candle_plot.plot(
            name='Min bid price',
            pen=(
                127,  # Red
                0,  # Green
                0,
            ),
        )

        self.__price_candlestick_item_by_start_timestamp_ms_map_by_interval_name_map: defaultdict[
            str, dict[int, CandlestickItem]
        ] = defaultdict(
            dict,
        )

        self.__processor = processor

        # self.__quantity_plot = (
        #     quantity_plot
        # )

        # self.__quantity_plot = (
        #     quantity_plot
        # )

        # if _IS_NEED_SHOW_RSI:
        #     self.__rsi_plot = rsi_plot
        #
        #     self.__rsi_plot_data_item = rsi_plot.plot(
        #         pen=_RSI_LINE_COLOR,
        #         name='RSI',
        #     )

        # if _IS_NEED_SHOW_RSI:
        #     self.__rsi_interval_name_combo_box = rsi_interval_name_combo_box
        #     self.__rsi_interval_name_label = rsi_interval_name_label

        # self.__trades_smoothing_level_combo_box = trades_smoothing_level_combo_box
        # self.__trades_smoothing_level_label = trades_smoothing_level_label

        self.__data_set_idx_combo_box = data_set_idx_combo_box
        self.__data_set_idx_label = data_set_idx_label

        self.__symbol_name_combo_box = symbol_name_combo_box
        self.__symbol_name_label = symbol_name_label

        if _IS_NEED_SHOW_VELOCITY:
            self.__velocity_plot = velocity_plot

            self.__velocity_plot_data_item = velocity_plot.plot(
                pen=_VELOCITY_LINE_COLOR,
                name=f'Trades per {PlotConstants.VelocityIntervalName}',
            )

        # self.__volume_plot = (
        #     volume_plot
        # )

        # self.__volume_plot = (
        #     volume_plot
        # )

        # if _IS_NEED_SHOW_RSI:
        #     functionality_layout.addWidget(rsi_interval_name_label, 0, 4, 2, 1)
        #     functionality_layout.addWidget(rsi_interval_name_combo_box, 2, 4, 2, 1)

        functionality_layout.addWidget(symbol_name_label, 0, 2, 2, 1)
        functionality_layout.addWidget(symbol_name_combo_box, 2, 2, 2, 1)

        # functionality_layout.addWidget(trades_smoothing_level_label, 0, 6, 2, 1)
        # functionality_layout.addWidget(trades_smoothing_level_combo_box, 2, 6, 2, 1)

        functionality_layout.addWidget(data_set_idx_label, 0, 8, 2, 1)
        functionality_layout.addWidget(data_set_idx_combo_box, 2, 8, 2, 1)

        window_layout.addWidget(graphics_layout_widget)

        window_layout.addLayout(functionality_layout)

    def __update_plots_x_range(
        self,
        current_plot: pyqtgraph.PlotWidget,  # TODO: check typing
    ):
        x_range = current_plot.getViewBox().viewRange()[0]

        plots = [
            # *self.__candles_plot_by_interval_name_map.values(),
            self.__candle_plot,
        ]

        # if _IS_NEED_SHOW_RSI:
        #     plots.append(
        #         self.__rsi_plot,
        #     )

        if _IS_NEED_SHOW_VELOCITY:
            plots.append(
                self.__velocity_plot,
            )

        for plot in plots:
            if plot is current_plot:
                continue

            plot.setXRange(
                *x_range,
                padding=0,
            )

    def __update_plots_y_range(
        self,
        current_plot: pyqtgraph.PlotWidget,  # TODO: check typing
    ):
        y_range = current_plot.getViewBox().viewRange()[1]

        for plot in (
            # *self.__candles_plot_by_interval_name_map.values(),
            self.__candle_plot,
        ):
            if plot is current_plot:
                continue

            plot.setYRange(
                *y_range,
                padding=0,
            )

    async def plot(
        self,
        delay: float | None = None,
        is_need_run_once: bool = True,
    ) -> None:
        drawing_lock = self.__drawing_lock

        if is_need_run_once and drawing_lock.locked():
            return

        async with drawing_lock:
            if delay is not None:
                await asyncio.sleep(
                    delay,
                )

            try:
                with Timer() as timer:
                    await self.__plot()

                print(
                    f'Plotted by {timer.elapsed:.3f}s',
                )
            except Exception as exception:
                print(
                    'Handled exception'
                    f': {"".join(traceback.format_exception(exception))}',
                )

            if not is_need_run_once:
                create_task_with_exceptions_logging(
                    self.plot(
                        delay=5.0,  # 5s  # 0.1  # 100ms  # TODO
                        is_need_run_once=False,
                    )
                )

    # @asyncSlot()
    # async def __on_rsi_interval_name_changed(
    #     self,
    #     # idx: int
    # ) -> None:
    #     current_rsi_interval_name = self.__rsi_interval_name_combo_box.currentText()
    #
    #     processor = self.__processor
    #
    #     if not current_rsi_interval_name or (
    #         current_rsi_interval_name == processor.get_current_rsi_interval_name()
    #     ):
    #         return
    #
    #     print(
    #         f'Selected RSI interval name: {current_rsi_interval_name!r}'
    #         # f' ({idx})'
    #     )
    #
    #     if not await processor.update_current_rsi_interval_name(
    #         current_rsi_interval_name,
    #     ):
    #         # TODO: response to user UI
    #
    #         return

    # @asyncSlot()
    # async def __on_trades_smoothing_level_changed(
    #     self,
    # ) -> None:
    #     current_trades_smoothing_level = (
    #         self.__trades_smoothing_level_combo_box.currentText()
    #     )
    #
    #     processor = self.__processor
    #
    #     if not current_trades_smoothing_level or (
    #         current_trades_smoothing_level
    #         == processor.get_current_trades_smoothing_level()
    #     ):
    #         return
    #
    #     print(
    #         f'Selected trades smoothing level: {current_trades_smoothing_level!r}'
    #         # f' ({idx})'
    #     )
    #
    #     if not await processor.update_current_trades_smoothing_level(
    #         current_trades_smoothing_level,
    #     ):
    #         # TODO: response to user UI
    #
    #         return

    @asyncSlot()
    async def __on_data_set_idx_changed(
        self,
        # idx: int
    ) -> None:
        current_data_set_idx_raw = self.__data_set_idx_combo_box.currentText()

        processor = self.__processor

        if not current_data_set_idx_raw or (
            current_data_set_idx_raw == processor.get_current_symbol_name()
        ):
            return

        current_data_set_idx = int(
            current_data_set_idx_raw,
        )

        print(
            f'Selected data set idx: {current_data_set_idx!r}'
            # f' ({idx})'
        )

        if not await processor.update_current_data_set_idx(
            current_data_set_idx,
        ):
            # TODO: response to user UI

            return

    @asyncSlot()
    async def __on_symbol_name_changed(
        self,
        # idx: int
    ) -> None:
        current_symbol_name = self.__symbol_name_combo_box.currentText()

        processor = self.__processor

        if not current_symbol_name or (
            current_symbol_name == processor.get_current_symbol_name()
        ):
            return

        print(
            f'Selected symbol name: {current_symbol_name!r}'
            # f' ({idx})'
        )

        if not await processor.update_current_symbol_name(
            current_symbol_name,
        ):
            # TODO: response to user UI

            return

    async def __plot(
        self,
    ) -> None:
        candle_plot = self.__candle_plot
        # candles_plot_by_interval_name_map = self.__candles_plot_by_interval_name_map

        main_candle_interval_name = '100ms'

        processor = self.__processor

        current_available_symbol_names = (
            await processor.get_current_available_symbol_names()
        )

        QtUtils.update_combo_box_values(
            self.__symbol_name_combo_box,
            self.__symbol_name_label,
            current_available_symbol_names,
        )

        current_available_data_set_idxs = (
            await processor.get_current_available_data_set_idxs()
        )

        QtUtils.update_combo_box_values(
            self.__data_set_idx_combo_box,
            self.__data_set_idx_label,
            current_available_data_set_idxs,
        )

        candles_dataframe = processor.get_candles_dataframe()

        price_candlestick_item_by_start_timestamp_ms_map_by_interval_name_map = (
            self.__price_candlestick_item_by_start_timestamp_ms_map_by_interval_name_map
        )

        if candles_dataframe is None:
            print(
                'candles_dataframe is None',
            )

            for (
                interval_name,
                price_candlestick_item_by_start_timestamp_ms_map,
            ) in price_candlestick_item_by_start_timestamp_ms_map_by_interval_name_map.items():
                if interval_name == main_candle_interval_name:
                    current_candle_plot = candle_plot
                else:
                    raise NotImplementedError(
                        interval_name,
                    )

                for (
                    price_candlestick_item
                ) in price_candlestick_item_by_start_timestamp_ms_map.values():
                    current_candle_plot.removeItem(
                        price_candlestick_item,
                    )

                price_candlestick_item_by_start_timestamp_ms_map.clear()

            price_candlestick_item_by_start_timestamp_ms_map_by_interval_name_map.clear()

            return

        start_timestamp_ms_series = candles_dataframe.get_column(
            'start_timestamp_ms',
        )

        start_timestamp_ms_numpy_array = start_timestamp_ms_series.to_numpy()

        # if _IS_NEED_SHOW_BOLLINGER_BANDS:
        #     bollinger_base_line_series = processor.get_bollinger_base_line_series()
        #     bollinger_lower_band_series = processor.get_bollinger_lower_band_series()
        #     bollinger_upper_band_series = processor.get_bollinger_upper_band_series()
        #
        #     if bollinger_base_line_series is not None:
        #         assert bollinger_lower_band_series is not None, None
        #         assert bollinger_upper_band_series is not None, None
        #
        #         self.__bollinger_base_line_plot_data_item.setData(
        #             trade_id_numpy_array,
        #             bollinger_base_line_series.to_numpy(),
        #         )
        #
        #         self.__bollinger_lower_band_plot_data_item.setData(
        #             trade_id_numpy_array,
        #             bollinger_lower_band_series.to_numpy(),
        #         )
        #
        #         self.__bollinger_upper_band_plot_data_item.setData(
        #             trade_id_numpy_array,
        #             bollinger_upper_band_series.to_numpy(),
        #         )
        #
        #         """
        #         bollinger_bands_fill_between_item = (
        #             self.__bollinger_bands_fill_between_item
        #         )
        #
        #         if bollinger_bands_fill_between_item is None:
        #             self.__bollinger_bands_fill_between_item = (
        #                 bollinger_bands_fill_between_item  # noqa
        #             ) = (
        #                 finplot.fill_between(
        #                     bollinger_lower_band_plot.item,
        #                     bollinger_upper_band_plot.item,
        #
        #                     color=(
        #                         _BOLLINGER_BANDS_FILL_COLOR
        #                     )
        #                 )
        #             )
        #         """
        #     else:
        #         assert bollinger_lower_band_series is None, None
        #         assert bollinger_upper_band_series is None, None

        # plot.reset()
        # self.__plot_overlay.reset()

        # candle_dataframe_by_interval_name_map = (
        #     processor.get_candle_dataframe_by_interval_name_map()
        # )

        if _IS_NEED_SHOW_CANDLES:
            price_candlestick_item_by_start_timestamp_ms_map = (
                price_candlestick_item_by_start_timestamp_ms_map_by_interval_name_map[
                    main_candle_interval_name
                ]
            )

            candle_start_timestamp_ms_set: set[int] = set()

            for candle_row_data in candles_dataframe.iter_rows(named=True):
                # start_trade_id: int = candle_row_data['start_trade_id']

                candle_close_price: float = candle_row_data['close_price']
                candle_high_price: float = candle_row_data['high_price']
                candle_low_price: float = candle_row_data['low_price']
                candle_open_price: float = candle_row_data['open_price']

                # end_datetime: datetime = candle_row_data['end_datetime']
                end_timestamp_ms: int = candle_row_data['end_timestamp_ms']
                # end_trade_id: int = candle_row_data['end_trade_id']

                # start_datetime: datetime = candle_row_data['start_datetime']
                start_timestamp_ms: int = candle_row_data['start_timestamp_ms']

                candle_start_timestamp_ms_set.add(
                    start_timestamp_ms,
                )

                price_candlestick_item = (
                    price_candlestick_item_by_start_timestamp_ms_map.get(
                        start_timestamp_ms,
                    )
                )

                if price_candlestick_item is not None:
                    price_candlestick_item.update_data(
                        candle_close_price,
                        end_timestamp_ms,
                        candle_high_price,
                        candle_low_price,
                        candle_open_price,
                        start_timestamp_ms,
                    )
                else:
                    price_candlestick_item = price_candlestick_item_by_start_timestamp_ms_map[
                        start_timestamp_ms
                    ] = CandlestickItem(
                        candle_close_price,
                        end_timestamp_ms,
                        candle_high_price,
                        candle_low_price,
                        candle_open_price,
                        start_timestamp_ms,
                    )

                    candle_plot.addItem(
                        price_candlestick_item,
                    )

            for start_timestamp_ms in tuple(
                price_candlestick_item_by_start_timestamp_ms_map,
            ):
                if start_timestamp_ms not in candle_start_timestamp_ms_set:
                    print(
                        'Removing candlestick item'
                        f' by start timestamp (ms) {start_timestamp_ms}',
                    )

                    price_candlestick_item = (
                        price_candlestick_item_by_start_timestamp_ms_map.pop(
                            start_timestamp_ms,
                        )
                    )

                    candle_plot.removeItem(
                        price_candlestick_item,
                    )

        # extreme_lines_array = processor.get_extreme_lines_array()
        #
        # extreme_lines_position = processor.get_extreme_lines_position()
        #
        # extreme_lines_scale = processor.get_extreme_lines_scale()
        #
        # extreme_lines_image_item = self.__extreme_lines_image_item
        #
        # if (
        #     extreme_lines_position is not None
        #     and extreme_lines_array is not None
        #     and extreme_lines_scale is not None
        # ):
        #     extreme_lines_image_item.setPos(
        #         QPointF(
        #             *extreme_lines_position,
        #         ),
        #     )
        #
        #     extreme_lines_image_item.setImage(
        #         extreme_lines_array,
        #     )
        #
        #     extreme_lines_image_item.setScale(
        #         extreme_lines_scale,
        #     )
        #
        #     extreme_lines_image_item.show()
        # else:
        #     extreme_lines_image_item.hide()
        #
        # order_book_volumes_asks_array = processor.get_order_book_volumes_asks_array()
        #
        # order_book_volumes_bids_array = processor.get_order_book_volumes_bids_array()
        #
        # order_book_volumes_position = processor.get_order_book_volumes_position()
        #
        # order_book_volumes_scale = processor.get_order_book_volumes_scale()
        #
        # order_book_volumes_asks_image_item = self.__order_book_volumes_asks_image_item
        # order_book_volumes_bids_image_item = self.__order_book_volumes_bids_image_item
        #
        # if (
        #     order_book_volumes_position is not None
        #     and order_book_volumes_asks_array is not None
        #     and order_book_volumes_bids_array is not None
        #     and order_book_volumes_scale is not None
        # ):
        #     order_book_volumes_position_point = QPointF(
        #         *order_book_volumes_position,
        #     )
        #
        #     order_book_volumes_asks_image_item.setPos(
        #         order_book_volumes_position_point,
        #     )
        #
        #     order_book_volumes_asks_image_item.setImage(
        #         order_book_volumes_asks_array,
        #     )
        #
        #     order_book_volumes_asks_image_item.setScale(
        #         order_book_volumes_scale,
        #     )
        #
        #     order_book_volumes_asks_image_item.show()
        #
        #     order_book_volumes_bids_image_item.setPos(
        #         order_book_volumes_position_point,
        #     )
        #
        #     order_book_volumes_bids_image_item.setImage(
        #         order_book_volumes_bids_array,
        #     )
        #
        #     order_book_volumes_bids_image_item.setScale(
        #         order_book_volumes_scale,
        #     )
        #
        #     order_book_volumes_bids_image_item.show()
        # else:
        #     order_book_volumes_asks_image_item.hide()
        #     order_book_volumes_bids_image_item.hide()

        close_price_series = candles_dataframe.get_column(
            'close_price',
        )

        self.__candle_plot_close_price_data_item.setData(
            start_timestamp_ms_numpy_array,
            close_price_series.to_numpy(),
        )

        high_price_series = candles_dataframe.get_column(
            'high_price',
        )

        self.__candle_plot_high_price_data_item.setData(
            start_timestamp_ms_numpy_array,
            high_price_series.to_numpy(),
        )

        low_price_series = candles_dataframe.get_column(
            'low_price',
        )

        self.__candle_plot_low_price_data_item.setData(
            start_timestamp_ms_numpy_array,
            low_price_series.to_numpy(),
        )

        max_end_ask_price_series = candles_dataframe.get_column(
            'max_end_ask_price',
        )

        self.__candle_plot_max_ask_price_data_item.setData(
            start_timestamp_ms_numpy_array,
            max_end_ask_price_series.to_numpy(),
        )

        # max_end_bid_price_series = candles_dataframe.get_column(
        #     'max_end_bid_price',
        # )
        #
        # self.__candle_plot_max_bid_price_data_item.setData(
        #     start_timestamp_ms_numpy_array,
        #     max_end_bid_price_series.to_numpy(),
        # )

        # min_end_ask_price_series = candles_dataframe.get_column(
        #     'min_end_ask_price',
        # )
        #
        # self.__candle_plot_min_ask_price_data_item.setData(
        #     start_timestamp_ms_numpy_array,
        #     min_end_ask_price_series.to_numpy(),
        # )

        min_end_bid_price_series = candles_dataframe.get_column(
            'min_end_bid_price',
        )

        self.__candle_plot_min_bid_price_data_item.setData(
            start_timestamp_ms_numpy_array,
            min_end_bid_price_series.to_numpy(),
        )

        # if _IS_NEED_SHOW_RSI:
        #     rsi_series = processor.get_rsi_series()
        #
        #     if rsi_series is not None:
        #         self.__rsi_plot_data_item.setData(
        #             trade_id_numpy_array,
        #             rsi_series.to_numpy(),
        #         )

        if _IS_NEED_SHOW_VELOCITY:
            velocity_series = processor.get_velocity_series()

            if velocity_series is not None:
                self.__velocity_plot_data_item.setData(
                    start_timestamp_ms_numpy_array,
                    velocity_series.to_numpy(),
                )

    def auto_range_candle_plot(
        self,
    ) -> None:
        self.__candle_plot.getViewBox().autoRange()
