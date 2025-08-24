from __future__ import annotations

import asyncio
import traceback
import typing

from datetime import datetime, timedelta

import pandas
from pyqtgraph import Point
from qasync import asyncSlot

import pyqtgraph

from PyQt6.QtCore import Qt, QRectF

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


_INTERVAL_DURATION_BY_NAME_MAP = {
    '15m': timedelta(
        minutes=15,
    ),

    '1H': timedelta(
        hours=1,
    ),
}

_IS_NEED_SHOW_BOLLINGER_BANDS = True

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

_CANDLE_BEAR_BODY_COLOR = QColor(
    0xf2,
    0x36,
    0x45,
)

_CANDLE_BEAR_SHADOW_COLOR = QColor(
    0xf2,
    0x36,
    0x45,
)

_CANDLE_BULL_BODY_COLOR = QColor(
    0x08,
    0x99,
    0x81,
)

_CANDLE_BULL_SHADOW_COLOR = QColor(
    0x08,
    0x99,
    0x81,
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

_RSI_LINE_COLOR = '#7e57c2'
_TEST_LINE_COLOR = '#ffffff'


class RectItem(pyqtgraph.GraphicsObject):
    def __init__(
            self,
            brush_color: QColor,
            pen_color: QColor,
            position: Point,
            size: Point,
    ) -> None:
        super().__init__()

        self.setPos(
            position,
        )

        self.__brush_color = brush_color
        self.__pen_color = pen_color
        self.__size = size

    def boundingRect(self):
        size = self.__size

        return QRectF(
            0,
            0,
            size[0],
            size[1]
        ).normalized()

    def paint(self, painter, *args):
        rect = self.boundingRect()

        painter.setPen(
            pyqtgraph.mkPen(
                self.__pen_color,
            ),
        )

        painter.setBrush(
            pyqtgraph.mkBrush(
                self.__brush_color,
            ),
        )

        painter.translate(
            rect.left(),
            rect.top(),
        )

        painter.scale(
            rect.width(),
            rect.height()
        )

        painter.drawRect(
            0,
            0,
            1,
            1
        )

    def set_brush_color(self, value: QColor) -> None:
        self.__brush_color = value

    def set_pen_color(self, value: QColor) -> None:
        self.__pen_color = value

    def set_size(self, value: Point) -> None:
        self.__size = value

    def _get_pen_color(
            self
    ) -> QColor:
        return self.__pen_color


class CandlestickItem(RectItem):
    def __init__(
            self,

            close_price: float,
            end_timestamp_ns: float,
            high_price: float,
            low_price: float,
            open_price: float,
            start_timestamp_ns: float,
    ) -> None:
        body_color, shadow_color = self.__generate_body_and_shadow_color_pair(
            close_price,
            open_price,
        )

        super().__init__(
            brush_color=body_color,
            pen_color=shadow_color,
            position=self.__generate_position(
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            ),
            size=self.__generate_size(
                close_price,
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            ),
        )

        self.__close_price = close_price
        self.__end_timestamp_ns = end_timestamp_ns
        self.__high_price = high_price
        self.__low_price = low_price
        self.__open_price = open_price
        self.__start_timestamp_ns = start_timestamp_ns

    def paint(self, painter, *args):
        painter.setPen(
            pyqtgraph.mkPen(
                self._get_pen_color(),
                width=2,
            ),
        )

        timestamp_ms_delta = (
            (
                self.__end_timestamp_ns -
                self.__start_timestamp_ns
            ) * 0.25
        )

        painter.drawLine(
            Point(timestamp_ms_delta, self.__high_price - self.__open_price),
            Point(timestamp_ms_delta, self.__low_price - self.__open_price)
        )

        super().paint(
            painter,
            *args,
        )

    def update_data(
            self,

            close_price: float,
            end_timestamp_ns: float,
            high_price: float,
            low_price: float,
            open_price: float,
            start_timestamp_ns: float,
    ) -> None:
        self.__close_price = close_price
        self.__end_timestamp_ns = end_timestamp_ns
        self.__high_price = high_price
        self.__low_price = low_price
        self.__open_price = open_price
        self.__start_timestamp_ns = start_timestamp_ns

        self.setPos(
            self.__generate_position(
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            )
        )

        self.set_size(
            self.__generate_size(
                close_price,
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            )
        )

        body_color, shadow_color = self.__generate_body_and_shadow_color_pair(
            close_price,
            open_price,
        )

        self.set_brush_color(
            body_color,
        )

        self.set_pen_color(
            shadow_color,
        )

    @staticmethod
    def __generate_body_and_shadow_color_pair(
            close_price: float,
            open_price: float,
    ) -> tuple[QColor, QColor]:
        is_bull = close_price >= open_price

        body_color: QColor
        shadow_color: QColor

        if is_bull:
            body_color = _CANDLE_BULL_BODY_COLOR
            shadow_color =_CANDLE_BULL_SHADOW_COLOR
        else:
            body_color = _CANDLE_BEAR_BODY_COLOR
            shadow_color = _CANDLE_BEAR_SHADOW_COLOR

        return body_color, shadow_color

    @staticmethod
    def __generate_position(
            end_timestamp_ns: float,
            open_price: float,
            start_timestamp_ns: float,
    ) -> Point:
        return Point(
            start_timestamp_ns + (end_timestamp_ns - start_timestamp_ns) * 0.25,
            open_price
        )

    @staticmethod
    def __generate_size(
            close_price: float,
            end_timestamp_ns: float,
            open_price: float,
            start_timestamp_ns: float,
    ) -> Point:
        return Point(
            (end_timestamp_ns - start_timestamp_ns) * 0.5,
            close_price - open_price
        )


class DateTimeAxisItem(pyqtgraph.AxisItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setLabel(text='Time', units=None)
        self.enableAutoSIPrefix(False)

    def tickStrings(self, values, scale, spacing):
        tick_strings = []

        for value in values:
            try:
                tick_string = datetime.fromtimestamp(
                    value //
                    10 ** 9
                ).isoformat()
            except ValueError:
                tick_string = 'N/A'

            tick_strings.append(
                tick_string,
            )

        return tick_strings


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

        self.__price_candlestick_item_by_start_timestamp_ms_map: (
            dict[int, CandlestickItem]
        ) = {}

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

        price_candlestick_item_by_start_timestamp_ms_map = (
            self.__price_candlestick_item_by_start_timestamp_ms_map
        )

        price_plot = self.__price_plot

        if candles_dataframe is None:
            print(
                'candles_dataframe is None'
            )

            for price_candlestick_item in price_candlestick_item_by_start_timestamp_ms_map.values():
                price_plot.removeItem(
                    price_candlestick_item,
                )

            price_candlestick_item_by_start_timestamp_ms_map.clear()

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

        current_interval_name = processor.get_current_interval_name()

        assert current_interval_name is not None, None

        current_interval_duration = _INTERVAL_DURATION_BY_NAME_MAP[
            current_interval_name
        ]

        candle_start_timestamp_ms_set: set[int] = set()

        for start_timestamp, candle_data in candles_dataframe.iterrows():
            end_timestamp = start_timestamp + current_interval_duration

            candle_close_price = candle_data.close
            candle_high_price = candle_data.high
            candle_low_price = candle_data.low
            candle_open_price = candle_data.open

            start_timestamp_ms = int(
                start_timestamp.timestamp() *
                1000
            )

            candle_start_timestamp_ms_set.add(
                start_timestamp_ms,
            )

            price_candlestick_item = price_candlestick_item_by_start_timestamp_ms_map.get(
                start_timestamp_ms
            )

            if price_candlestick_item is not None:
                price_candlestick_item.update_data(
                    candle_close_price,
                    end_timestamp.value,
                    candle_high_price,
                    candle_low_price,
                    candle_open_price,
                    start_timestamp.value
                )
            else:
                price_candlestick_item = price_candlestick_item_by_start_timestamp_ms_map[
                    start_timestamp_ms
                ] = CandlestickItem(
                    candle_close_price,
                    end_timestamp.value,
                    candle_high_price,
                    candle_low_price,
                    candle_open_price,
                    start_timestamp.value
                )

                price_plot.addItem(
                    price_candlestick_item
                )

        rsi_series = (
            processor.get_rsi_series()
        )

        if rsi_series is not None:
            self.__rsi_plot_data_item.setData(
                rsi_series.index,
                rsi_series.array,
            )

        test_analytics_raw_data_list = processor.get_test_analytics_raw_data_list()

        test_analytics_rect_item_by_start_timestamp_ms_map = (
            self.__test_analytics_rect_item_by_start_timestamp_ms_map
        )

        if test_analytics_raw_data_list is not None:
            test_analytics_raw_data_by_start_timestamp_ms_map: dict[int, dict[str, typing.Any]] = {}  # TODO: test_analytics_raw_data_list -> test_analytics_raw_data_by_start_timestamp_ms_map

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

                test_analytics_rect_item_position = Point(
                    start_timestamp.value,
                    start_price,
                )

                test_analytics_rect_item_size = Point(
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
                    test_analytics_rect_item.set_pen_color(
                        test_analytics_color
                    )

                    test_analytics_rect_item.set_brush_color(
                        test_analytics_color
                    )

                    test_analytics_rect_item.setPos(
                        test_analytics_rect_item_position
                    )

                    test_analytics_rect_item.set_size(
                        test_analytics_rect_item_size
                    )
                else:
                    # Add new rect

                    test_analytics_rect_item = (  # noqa
                        test_analytics_rect_item_by_start_timestamp_ms_map[
                            start_timestamp_ms
                        ]
                    ) = RectItem(
                        brush_color=(
                            test_analytics_color
                        ),

                        pen_color=(
                            test_analytics_color
                        ),

                        position=(
                            test_analytics_rect_item_position
                        ),

                        size=(
                            test_analytics_rect_item_size
                        ),
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

    def auto_range_price_plot(self) -> None:
        self.__price_plot.getViewBox().autoRange()
