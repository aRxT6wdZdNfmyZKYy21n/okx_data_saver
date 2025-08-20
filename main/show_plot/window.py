from __future__ import annotations

import asyncio
import traceback
import typing

from decimal import (
    Decimal
)

from qasync import asyncSlot

import finplot
import pyqtgraph

from finplot import (  # TODO: use
    FinRect,
)

from PyQt6.QtCore import Qt

from PyQt6.QtGui import (
    QBrush,
    QColor,
    QLinearGradient
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

        (
            price_axis,
            rsi_axis

            # quantity_axis,
            # volume_axis
        ) = (
            finplot.create_plot(
                init_zoom_periods=1,
                rows=2  # 1  # 3
            )
        )

        # TODO: remove
        """
        print(
            'price_axis:',
            price_axis
        )

        print(
            'dir(price_axis):',
            dir(price_axis)
        )
        """

        # TODO: fix
        """
        axis_pen = (
            finplot.internal_utils._makepen(  # noqa  # TODO: make this better
                color=(
                    FinPlotConstants.foreground
                )
            )
        )

        price_ax_data_by_name_map: (
            typing.Dict[
                str,
                typing.Dict
            ]
        ) = (
            price_axis.axes
        )

        price_axis_right_ax_data = (
            price_ax_data_by_name_map[
                'right'
            ]
        )

        price_axis_right_item: (
            pyqtgraph.AxisItem
        ) = (
            price_axis_right_ax_data[
                'item'
            ]
        )

        price_axis_right_item.setPen(
            axis_pen
        )

        price_axis_right_item.setTextPen(
            axis_pen
        )
        """

        """
        print(
            'rsi_axis.curves:',
            rsi_axis.curves
        )

        print(
            'type(rsi_axis):',
            type(rsi_axis)
        )

        print(
            'dir(rsi_axis):',
            dir(rsi_axis)
        )
        """

        rsi_plot_linear_region_item = (
            finplot.add_horizontal_band(
                30,
                70,
                ax=rsi_axis
            )
        )

        # TODO
        """
        rsi_plot_gradient = (
            QLinearGradient(
                0,
                0,
                0,
                1
            )
        )

        rsi_plot_gradient.setColorAt(
            0.0,
            _RSI_PLOT_GRADIENT_UPPER_START_COLOR
        )

        rsi_plot_gradient.setColorAt(
            0.3,
            _RSI_PLOT_GRADIENT_UPPER_END_COLOR
        )

        rsi_plot_gradient.setColorAt(
            0.7,
            _RSI_PLOT_GRADIENT_LOWER_START_COLOR
        )

        rsi_plot_gradient.setColorAt(
            1.0,
            _RSI_PLOT_GRADIENT_LOWER_END_COLOR
        )

        rsi_plot_gradient.setCoordinateMode(
            QLinearGradient.CoordinateMode.ObjectMode
        )

        rsi_plot_fill_between_item = (
            # finplot.fill_between(
            pyqtgraph.FillBetweenItem(
                rsi_axis,
                rsi_plot_linear_region_item.lines[0],

                brush=(
                    QBrush(
                        pyqtgraph.mkColor(
                            '#ffffff'
                        )
                    )
                )
            )
        )

        rsi_plot_fill_between_item.ax = rsi_axis

        rsi_plot_fill_between_item.setZValue(
            -40
        )

        rsi_axis.addItem(
            rsi_plot_fill_between_item
        )
        
        """

        # TODO
        """
        rsi_plot_linear_region_item.setBrush(
            QBrush(
                rsi_plot_gradient
            )
        )
        """

        for rsi_plot_linear_region_item_line in (
                rsi_plot_linear_region_item.lines
        ):
            rsi_plot_linear_region_item_line.setPen(
                pyqtgraph.mkPen(
                    '#787b86',
                    dash=[
                        7,
                        7
                    ],

                    style=(
                        Qt.PenStyle.CustomDashLine
                    )
                )
            )

        finplot.set_y_range(
            0,
            100,
            ax=rsi_axis
        )

        # price_axis.set_visible(
        #     xgrid=True,
        #     ygrid=True
        # )

        price_view_box = (
            price_axis.vb
        )

        price_axis_graphics_view = (
            price_view_box.win
        )

        # volume_axis_graphics_view = (
        #     volume_axis.vb.win
        # )
        #
        # assert (
        #     price_axis_graphics_view is
        #     volume_axis_graphics_view
        # ), (
        #     price_axis_graphics_view,
        #     volume_axis_graphics_view
        # )

        plot_graphics_view = (
            price_axis_graphics_view
        )

        rsi_view_box = (
            rsi_axis.vb
        )

        # Create a linear gradient for any plot background

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

        # plot_graphics_view.setBackgroundBrush(
        #     QBrush(
        #         plot_background_gradient
        #     )
        # )

        price_view_box.setBackgroundColor(
            plot_background_brush
        )

        rsi_view_box.setBackgroundColor(
            plot_background_brush
        )

        (
            bollinger_base_line_plot,
            bollinger_lower_band_plot,
            bollinger_upper_band_plot,
            price_plot,
            rsi_plot

            # quantity_plot,
            # volume_plot

        ) = (
            finplot.live(
                # 1
                # 2
                # 3
                # 4
                5
            )
        )

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

        self.axs = [  # finplot requires this property
            price_axis,
            rsi_axis

            # quantity_axis,
            # volume_axis

        ]

        self.__bollinger_base_line_plot = (
            bollinger_base_line_plot
        )

        self.__bollinger_lower_band_plot = (
            bollinger_lower_band_plot
        )

        self.__bollinger_upper_band_plot = (
            bollinger_upper_band_plot
        )

        self.__bollinger_bands_fill_between_item: (
            typing.Optional[
                pyqtgraph.FillBetweenItem
            ]
        ) = None

        self.__drawing_lock = (
            asyncio.Lock()
        )

        self.__interval_name_combo_box = (
            interval_name_combo_box
        )

        self.__interval_name_label = (
            interval_name_label
        )

        self.__price_axis = (
            price_axis
        )

        self.__price_plot = (
            price_plot
        )

        self.__processor = (
            processor
        )

        # self.__quantity_axis = (
        #     quantity_axis
        # )

        # self.__quantity_plot = (
        #     quantity_plot
        # )

        self.__rsi_axis = (
            rsi_axis
        )

        self.__rsi_plot = (
            rsi_plot
        )

        self.__symbol_name_combo_box = (
            symbol_name_combo_box
        )

        self.__symbol_name_label = (
            symbol_name_label
        )

        # self.__volume_axis = (
        #     volume_axis
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
            plot_graphics_view
        )

        window_layout.addLayout(
            functionality_layout
        )

        # use TradingView colors

        price_plot.colors.update({
            'bear_shadow': _CANDLE_BEAR_COLOR,
            'bear_frame': _CANDLE_BEAR_COLOR,
            'bear_body': _CANDLE_BEAR_COLOR,
            'bull_shadow': _CANDLE_BULL_COLOR,
            'bull_frame': _CANDLE_BULL_COLOR,
            'bull_body': _CANDLE_BULL_COLOR
        })

        """
        order_book_plot.colors.update({
            'bull_shadow': '#26a69a7f',
            'bull_frame': '#26a69a7f',
            'bull_body': '#26a69a7f',
            'bear_shadow': '#ef53507f',
            'bear_frame': '#ef53507f',
            'bear_body': '#ef53507f'
        })
        """

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

    def show(self) -> None:
        finplot.show(
            qt_exec=False
        )  # prepares plots when they're all setup

        super(
            FinPlotChartWindow,
            self
        ).show()

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

            idx: int
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
            f' ({idx})'
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

        bollinger_base_line_series = (
            processor.get_bollinger_base_line_series()
        )

        bollinger_lower_band_series = (
            processor.get_bollinger_lower_band_series()
        )

        bollinger_upper_band_series = (
            processor.get_bollinger_upper_band_series()
        )

        price_axis = (
            self.__price_axis
        )

        if bollinger_base_line_series is not None:
            assert (
                bollinger_lower_band_series is not None
            ), None

            assert (
                bollinger_upper_band_series is not None
            ), None

            self.__bollinger_base_line_plot.plot(
                bollinger_base_line_series,
                ax=price_axis,
                color=_BOLLINGER_BASE_LINE_COLOR,
                legend='Bollinger base line'
            )

            bollinger_lower_band_plot = (
                self.__bollinger_lower_band_plot
            )

            bollinger_lower_band_plot.plot(
                bollinger_lower_band_series,
                ax=price_axis,
                color=_BOLLINGER_LOWER_BAND_COLOR,
                legend='Bollinger lower band'
            )

            bollinger_upper_band_plot = (
                self.__bollinger_upper_band_plot
            )

            bollinger_upper_band_plot.plot(
                bollinger_upper_band_series,
                ax=price_axis,
                color=_BOLLINGER_UPPER_BAND_COLOR,
                legend='Bollinger upper band'
            )

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
        else:
            assert (
                bollinger_lower_band_series is None
            ), None

            assert (
                bollinger_upper_band_series is None
            ), None

        # axis.reset()
        # self.__axis_overlay.reset()

        self.__price_plot.candlestick_ochl(
            candles_dataframe[[
                'open',
                'close',
                'high',
                'low'
            ]],
            # legend='price',
            ax=price_axis  # .overlay()
        )

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

            ax=price_axis
        )

        # quantity_axis = (
        #     self.__quantity_axis
        # )

        # self.__quantity_plot.plot(  # TODO
        #     candles_dataframe.quantity,
        #     legend='quantity',
        #     ax=quantity_axis
        # )

        rsi_series = (
            processor.get_rsi_series()
        )

        if rsi_series is not None:
            self.__rsi_plot.plot(
                rsi_series,
                ax=self.__rsi_axis,
                color=_RSI_LINE_COLOR,
                legend='RSI (6)'
            )

        # volume_axis = (
        #     self.__volume_axis
        # )

        # self.__volume_plot.volume_ocv(
        #     candles_dataframe['open close volume'.split()],  # .volume,
        #     # legend='volume',
        #     # kind='volume',
        #     ax=volume_axis  # .overlay()
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
