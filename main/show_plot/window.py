from __future__ import annotations

import asyncio
import traceback
import typing

from datetime import datetime

import pandas
from qasync import asyncSlot

import pyqtgraph

from PyQt6.QtCore import Qt, QRectF, QPointF, QSizeF

from PyQt6.QtGui import (
    QColor,
)

from PyQt6.QtWidgets import (
    QGridLayout,
    QMainWindow,
    QVBoxLayout,
    QWidget
)

from utils.async_ import (
    create_task_with_exceptions_logging
)

from utils.qt import (
    QtUtils
)

if typing.TYPE_CHECKING:
    from main.show_plot.processor import (
        FinPlotChartProcessor
    )


_IS_NEED_SHOW_BOLLINGER_BANDS = False

# Bogdan's color preset

_BOLLINGER_BANDS_FILL_COLOR = (
    QColor(
        33,
        150,
        243,
        12
    )
)

_BOLLINGER_BASE_LINE_COLOR = '#2962ff'
_BOLLINGER_LOWER_BAND_COLOR = '#089981'
_BOLLINGER_UPPER_BAND_COLOR = '#f23645'

_CANDLE_BEAR_COLOR = (
    '#000000'
)

_CANDLE_BULL_COLOR = (
    '#ffffff'
)

_PLOT_BACKGROUND_UPPER_COLOR = (
    QColor(
        126,
        87,
        194
    )
)

_PLOT_BACKGROUND_LOWER_COLOR = (
    QColor(
        67,
        70,
        81
    )
)

_RSI_PLOT_GRADIENT_UPPER_START_COLOR = (
    QColor(
        76,
        175,
        80
    )
)

_RSI_PLOT_GRADIENT_UPPER_END_COLOR = (
    QColor(
        76,
        175,
        80,
        0
    )
)

_RSI_PLOT_GRADIENT_LOWER_START_COLOR = (
    QColor(
        255,
        82,
        82,
        0
    )
)

_RSI_PLOT_GRADIENT_LOWER_END_COLOR = (
    QColor(
        255,
        82,
        82,
        255
    )
)

_RSI_LINE_COLOR = '#d1c4e9'
_TEST_LINE_COLOR = '#ffffff'

# Pavel's color preset

# _BOLLINGER_BANDS_FILL_COLOR = (
#     QColor(
#         33,
#         150,
#         243,
#         0.05
#     )
# )
#
# _BOLLINGER_BASE_LINE_COLOR = '#2962ff'
# _BOLLINGER_LOWER_BAND_COLOR = '#089981'
# _BOLLINGER_UPPER_BAND_COLOR = '#f23645'
#
# _CANDLE_BEAR_COLOR = (
#     '#f23645'
# )
#
# _CANDLE_BULL_COLOR = (
#     '#089981'
# )
#
# _PLOT_BACKGROUND_UPPER_COLOR = (
#     QColor(
#         24,
#         28,
#         39
#     )
# )
#
# _PLOT_BACKGROUND_LOWER_COLOR = (
#     QColor(
#         19,
#         23,
#         34
#     )
# )
#
# _RSI_PLOT_GRADIENT_UPPER_START_COLOR = (
#     QColor(
#         76,
#         175,
#         80
#     )
# )
#
# _RSI_PLOT_GRADIENT_UPPER_END_COLOR = (
#     QColor(
#         76,
#         175,
#         80,
#         0
#     )
# )
#
# _RSI_PLOT_GRADIENT_LOWER_START_COLOR = (
#     QColor(
#         255,
#         82,
#         82,
#         0
#     )
# )
#
# _RSI_PLOT_GRADIENT_LOWER_END_COLOR = (
#     QColor(
#         255,
#         82,
#         82,
#         255
#     )
# )
#
# _RSI_LINE_COLOR = '#7e57c2'


class RectItem(pyqtgraph.RectROI):
    def __init__(self, brush, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__brush = brush

    def paint(self, painter, *args):
        size: pyqtgraph.Point = self.state['size']

        rect = QRectF(
            0,
            0,
            size[0],
            size[1]
        ).normalized()

        painter.setPen(self.currentPen)
        painter.setBrush(self.__brush)
        painter.translate(rect.left(), rect.top())
        painter.scale(rect.width(), rect.height())
        painter.drawRect(0, 0, 1, 1)

    def set_brush(self, brush) -> None:
        self.__brush = brush

    def addScaleHandle(self, *args, **kwargs):
        if self.resizable:
            super().addScaleHandle(*args, **kwargs)


class CandlestickItem(RectItem):
    def __init__(
            self,

            *args,
            **kwargs,
    ) -> None:
        super().__init__(
            *args,
            **kwargs,
        )


class DateTimeAxisItem(pyqtgraph.AxisItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setLabel(text='Time', units=None)
        self.enableAutoSIPrefix(False)

    def tickStrings(self, values, scale, spacing):
        return [
            datetime.fromtimestamp(
                value //
                10 ** 9
            ).isoformat()

            for value in values
        ]


class FinPlotChartWindow(QMainWindow):
    def __init__(
            self,

            processor,  # type: FinPlotChartProcessor
            parent=None
    ):
        super(
            FinPlotChartWindow,
            self
        ).__init__(
            parent
        )

        functionality_layout = (
            QGridLayout()
        )

        window_layout_widget = (
            QWidget()
        )

        window_layout = (
            QVBoxLayout(
                window_layout_widget
            )
        )

        self.setCentralWidget(
            window_layout_widget
        )

        # self.setLayout(
        #     window_layout
        # )

        self.setMinimumSize(
            1366,  # 1920
            768    # 1080
        )

        self.setWindowFlags(
            Qt.WindowType(
                Qt.WindowType.WindowMaximizeButtonHint |
                Qt.WindowType.WindowMinimizeButtonHint |
                Qt.WindowType.WindowCloseButtonHint
            )
        )

        self.setWindowTitle(
            'Chart'
        )

        graphics_layout_widget: pyqtgraph.GraphicsLayout = pyqtgraph.GraphicsLayoutWidget()  # noqa

        price_plot = graphics_layout_widget.addPlot(
            title='Plot'
        )

        price_date_axis = DateTimeAxisItem(
            orientation='bottom',
        )

        price_plot.setAxisItems({
            'bottom': price_date_axis
        })

        price_plot.showGrid(
            x=True,
            y=True
        )

        graphics_layout_widget.nextRow()

        rsi_plot = graphics_layout_widget.addPlot(
            title='RSI'
        )

        rsi_date_axis = DateTimeAxisItem(
            orientation='bottom',
        )

        rsi_plot.setAxisItems({
            'bottom': rsi_date_axis
        })

        rsi_plot.showGrid(
            x=True,
            y=True
        )

        def update_rsi_plot_x_range():  # TODO: move to method
            rsi_plot.setXRange(
                *price_plot.getViewBox().viewRange()[0],
                padding=0
            )

        def update_price_plot_x_range():  # TODO: move to method
            price_plot.setXRange(
                *rsi_plot.getViewBox().viewRange()[0],
                padding=0
            )

        price_plot.sigXRangeChanged.connect(update_rsi_plot_x_range)
        rsi_plot.sigXRangeChanged.connect(update_price_plot_x_range)

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
            interval_name_label,
            interval_name_combo_box
        ) = (
            QtUtils.create_label_and_combo_box(
                'Интервал',
                self.__on_interval_name_changed,

                alignment=(
                    Qt.AlignmentFlag.AlignLeft
                )
            )
        )

        (
            symbol_name_label,
            symbol_name_combo_box
        ) = (
            QtUtils.create_label_and_combo_box(
                'Символ',
                self.__on_symbol_name_changed,

                alignment=(
                    Qt.AlignmentFlag.AlignLeft
                )
            )
        )

        self.__graphics_layout_widget = graphics_layout_widget

        self.__bollinger_base_line_plot_data_item = (
            price_plot.plot(
                pen=_BOLLINGER_BASE_LINE_COLOR,
                name="Bollinger Base Line",
            )
        )

        self.__bollinger_lower_band_plot_data_item = (
            price_plot.plot(
                pen=_BOLLINGER_LOWER_BAND_COLOR,
                name="Bollinger Lower Band",
            )
        )

        self.__bollinger_upper_band_plot_data_item = (
            price_plot.plot(
                pen=_BOLLINGER_UPPER_BAND_COLOR,
                name="Bollinger Upper Band",
            )
        )

        # TODO
        """
        self.__bollinger_bands_fill_between_item: (
            typing.Optional[
                pyqtgraph.FillBetweenItem
            ]
        ) = None
        """

        self.__drawing_lock = (
            asyncio.Lock()
        )

        self.__interval_name_combo_box = (
            interval_name_combo_box
        )

        self.__interval_name_label = (
            interval_name_label
        )

        self.__price_plot = (
            price_plot
        )

        self.__price_plot_data_item = (
            price_plot.plot(
                pen=(255, 255, 255),  # TODO: TradingView
                name='Price',
            )
        )

        self.__processor = (
            processor
        )

        # self.__quantity_plot = (
        #     quantity_plot
        # )

        # self.__quantity_plot = (
        #     quantity_plot
        # )

        self.__rsi_plot = (
            rsi_plot
        )

        self.__rsi_plot_data_item = (
            rsi_plot.plot(
                pen=_RSI_LINE_COLOR,
                name='RSI',
            )
        )

        self.__test_analytics_rect_item_by_start_timestamp_ms_map: (
            dict[int, RectItem]
        ) = {}

        self.__test_plot_data_item = (
            price_plot.plot(
                pen=(127, 127, 127),
                name='Test',
            )
        )

        self.__symbol_name_combo_box = (
            symbol_name_combo_box
        )

        self.__symbol_name_label = (
            symbol_name_label
        )

        # self.__volume_plot = (
        #     volume_plot
        # )

        # self.__volume_plot = (
        #     volume_plot
        # )

        functionality_layout.addWidget(
            symbol_name_label,
            0, 2, 2, 1
        )

        functionality_layout.addWidget(
            symbol_name_combo_box,
            2, 2, 2, 1
        )

        functionality_layout.addWidget(
            interval_name_label,
            0, 4, 2, 1
        )

        functionality_layout.addWidget(
            interval_name_combo_box,
            2, 4, 2, 1
        )

        window_layout.addWidget(
            graphics_layout_widget
        )

        window_layout.addLayout(
            functionality_layout
        )

    async def plot(
            self,

            delay: (
                typing.Optional[
                    float
                ]
            ) = None,

            is_need_run_once: bool = True
    ) -> None:
        drawing_lock = (
            self.__drawing_lock
        )

        if (
                is_need_run_once and
                drawing_lock.locked()
        ):
            return

        async with drawing_lock:
            if delay is not None:
                await (
                    asyncio.sleep(
                        delay
                    )
                )

            try:
                await self.__plot()
            except Exception as exception:
                print(
                    'Handled exception'
                    f': {"".join(traceback.format_exception(exception))}',
                )

            if not is_need_run_once:
                create_task_with_exceptions_logging(
                    self.plot(
                        delay=5.0,  # 5s  # 0.1  # 100ms  # TODO
                        is_need_run_once=False
                    )
                )

    @asyncSlot()
    async def __on_interval_name_changed(
            self,

            # idx: str
    ) -> None:
        current_interval_name = (
            self.__interval_name_combo_box.currentText()
        )

        if not current_interval_name:
            return

        processor = (
            self.__processor
        )

        if (
                current_interval_name ==
                processor.get_current_interval_name()
        ):
            return

        print(
            'Selected interval name'
            f': {current_interval_name!r}'
            # f' ({idx})'
        )

        await processor.update_current_interval_name(
            current_interval_name
        )

    @asyncSlot()
    async def __on_symbol_name_changed(
            self,

            # idx: int
    ) -> None:
        current_symbol_name = (
            self.__symbol_name_combo_box.currentText()
        )

        processor = (
            self.__processor
        )

        if (
                not current_symbol_name or

                (
                    current_symbol_name ==
                    processor.get_current_symbol_name()
                )
        ):
            return

        print(
            'Selected symbol name'
            f': {current_symbol_name!r}'
            # f' ({idx})'
        )

        if not (
                await processor.update_current_symbol_name(
                    current_symbol_name
                )
        ):
            # TODO: response to user UI

            return

    async def __plot(self) -> None:
        processor = (
            self.__processor
        )

        current_available_symbol_names = (
            await processor.get_current_available_symbol_names()
        )

        QtUtils.update_combo_box_values(
            self.__symbol_name_combo_box,
            self.__symbol_name_label,
            current_available_symbol_names
        )

        current_available_interval_names = (
            processor.get_current_available_interval_names()
        )

        QtUtils.update_combo_box_values(
            self.__interval_name_combo_box,
            self.__interval_name_label,
            current_available_interval_names
        )

        candles_dataframe = (
            processor.get_candles_dataframe()
        )

        if candles_dataframe is None:
            print(
                'candles_dataframe is None'
            )

            return

        if _IS_NEED_SHOW_BOLLINGER_BANDS:
            bollinger_base_line_series = (
                processor.get_bollinger_base_line_series()
            )

            bollinger_lower_band_series = (
                processor.get_bollinger_lower_band_series()
            )

            bollinger_upper_band_series = (
                processor.get_bollinger_upper_band_series()
            )

            if bollinger_base_line_series is not None:
                assert (
                    bollinger_lower_band_series is not None
                ), None

                assert (
                    bollinger_upper_band_series is not None
                ), None

                self.__bollinger_base_line_plot_data_item.setData(
                    bollinger_base_line_series.index,
                    bollinger_base_line_series.array,
                )

                self.__bollinger_lower_band_plot_data_item.setData(
                    bollinger_lower_band_series.index,
                    bollinger_lower_band_series.array,
                )

                self.__bollinger_upper_band_plot_data_item.setData(
                    bollinger_upper_band_series.index,
                    bollinger_upper_band_series.array,
                )

                """
                bollinger_bands_fill_between_item = (
                    self.__bollinger_bands_fill_between_item
                )

                if bollinger_bands_fill_between_item is None:
                    self.__bollinger_bands_fill_between_item = (
                        bollinger_bands_fill_between_item  # noqa
                    ) = (
                        finplot.fill_between(
                            bollinger_lower_band_plot.item,
                            bollinger_upper_band_plot.item,

                            color=(
                                _BOLLINGER_BANDS_FILL_COLOR
                            )
                        )
                    )
                """
            else:
                assert (
                    bollinger_lower_band_series is None
                ), None

                assert (
                    bollinger_upper_band_series is None
                ), None

        # plot.reset()
        # self.__plot_overlay.reset()

        # TODO
        """
        self.__price_plot.candlestick_ochl(
            candles_dataframe[[
                'open',
                'close',
                'high',
                'low'
            ]],
            # legend='price',
            ax=price_plot  # .overlay()
        )
        """

        """
        max_price = (
            processor.get_max_price()
        )

        assert (
            max_price is not None
        ), None

        min_price = (
            processor.get_min_price()
        )

        assert (
            min_price is not None
        ), None

        finplot.set_y_range(
            float(
                min_price *

                (
                    1 -

                    Decimal(
                        '0.05'  # 5%
                    )
                )
            ),

            float(
                max_price *

                (
                    1 +

                    Decimal(
                        '0.05'  # 5%
                    )
                )
            ),

            ax=price_plot
        )
        """

        # quantity_plot = (
        #     self.__quantity_plot
        # )

        # self.__quantity_plot.plot(  # TODO
        #     candles_dataframe.quantity,
        #     legend='quantity',
        #     ax=quantity_plot
        # )

        rsi_series = (
            processor.get_rsi_series()
        )

        if rsi_series is not None:
            self.__rsi_plot_data_item.setData(
                rsi_series.index,
                rsi_series.array,
            )

        price_plot = self.__price_plot

        test_analytics_raw_data_list = processor.get_test_analytics_raw_data_list()

        test_analytics_rect_item_by_start_timestamp_ms_map = (
            self.__test_analytics_rect_item_by_start_timestamp_ms_map
        )

        if test_analytics_raw_data_list is not None:
            test_analytics_raw_data_by_start_timestamp_ms_map: dict[int, dict[str, typing.Any]] = {}

            for test_analytics_raw_data in test_analytics_raw_data_list:
                start_timestamp: pandas.Timestamp = test_analytics_raw_data[
                    'first_start_timestamp'
                ]

                start_timestamp_ms = int(
                    start_timestamp.timestamp() *
                    1000
                )

                test_analytics_raw_data_by_start_timestamp_ms_map[
                    start_timestamp_ms
                ] = test_analytics_raw_data

                test_analytics_rect_item = test_analytics_rect_item_by_start_timestamp_ms_map.get(
                    start_timestamp_ms
                )

                end_price: float = test_analytics_raw_data[
                    'second_price'
                ]

                end_timestamp: pandas.Timestamp = test_analytics_raw_data[
                    'second_start_timestamp'
                ]

                is_bull: bool = test_analytics_raw_data[
                    'is_bull'
                ]

                start_price: float = test_analytics_raw_data[
                    'first_price'
                ]

                test_analytics_color: QColor

                if is_bull:
                    test_analytics_color = QColor(
                        0,
                        255,
                        0,

                        int(
                            255 *
                            0.25
                        )
                    )
                else:
                    test_analytics_color = QColor(
                        255,
                        0,
                        0,

                        int(
                            255 *
                            0.25
                        )
                    )

                test_analytics_rect_item_position = (
                    start_timestamp.value,
                    start_price,
                )

                test_analytics_rect_item_size = (
                    (
                        end_timestamp.value -
                        start_timestamp.value
                    ),

                    (
                        end_price -
                        start_price
                    ),
                )

                if test_analytics_rect_item is not None:
                    test_analytics_rect_item.setPen(
                        test_analytics_color
                    )

                    test_analytics_rect_item.set_brush(
                        test_analytics_color
                    )

                    test_analytics_rect_item.setPos(
                        test_analytics_rect_item_position
                    )

                    test_analytics_rect_item.setSize(
                        test_analytics_rect_item_size
                    )
                else:
                    # Add new rect

                    test_analytics_rect_item = (  # noqa
                        test_analytics_rect_item_by_start_timestamp_ms_map[
                            start_timestamp_ms
                        ]
                    ) = RectItem(
                        brush=(
                            test_analytics_color
                        ),

                        pen=(
                            test_analytics_color
                        ),

                        pos=(
                            test_analytics_rect_item_position
                        ),

                        size=(
                            test_analytics_rect_item_size
                        ),

                        movable=False,
                        rotatable=False,
                        resizable=False,
                        removable=False,
                    )

                    price_plot.addItem(
                        test_analytics_rect_item
                    )

            # Remove other items

            for start_timestamp_ms in tuple(
                    test_analytics_rect_item_by_start_timestamp_ms_map
            ):
                if start_timestamp_ms in test_analytics_raw_data_by_start_timestamp_ms_map:
                    continue

                test_analytics_rect_item = test_analytics_rect_item_by_start_timestamp_ms_map.pop(
                    start_timestamp_ms
                )

                price_plot.removeItem(
                    test_analytics_rect_item
                )
        else:
            for test_analytics_rect_item in test_analytics_rect_item_by_start_timestamp_ms_map.values():
                price_plot.removeItem(
                    test_analytics_rect_item
                )

            test_analytics_rect_item_by_start_timestamp_ms_map.clear()

        test_series = (
            processor.get_test_series()
        )

        if test_series is not None:
            self.__test_plot_data_item.setData(
                test_series.index,
                test_series.array,
            )

        # volume_plot = (
        #     self.__volume_plot
        # )

        # self.__volume_plot.volume_ocv(
        #     candles_dataframe['open close volume'.split()],  # .volume,
        #     # legend='volume',
        #     # kind='volume',
        #     ax=volume_plot  # .overlay()
        # )

        # # finplot.refresh()  # refresh autoscaling when all plots complete

        """
        def update(txt):
        df = download(txt)
        if len(df) < 20: # symbol does not exist
            return
        info.setText('Loading symbol name...')
        price = df['Open Close High Low'.split()]
        volume = df['Open Close Volume'.split()]
        ax.reset()  # remove previous plots
        axo.reset()  # remove previous plots
        fplt.candlestick_ochl(price)
        fplt.volume_ocv(volume, ax=axo)
        fplt.refresh() # refresh autoscaling when all plots complete
        Thread(target=lambda: info.setText(get_name(txt))).start() # slow, so use thread
        """

    def auto_range_price_plot(self) -> None:
        self.__price_plot.getViewBox().autoRange()
