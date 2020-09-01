# -*- coding:utf-8 -*-
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker  # 用于日期刻度定制
from matplotlib import colors as mcolors  # 用于颜色转换成渲染时顶点需要的颜色格式
from matplotlib.collections import LineCollection, PolyCollection  # 用于绘制直线集合和多边形集合
from chart import tools  # 计算MACD和KDJ(没有使用talib，有缺陷)


class SingleMultiCursor:
    """
    一个用于多个子图（横排或者竖排）的十字星光标，可以在多个子图上同时出现
    single=0表示仅仅一个子图显示水平线，所有子图显示垂直线，用于竖排的子图
    single=1表示仅仅一个子图显示垂直线，所有子图显示水平线，用于横排的子图
    注意：为了能让光标响应事件处理，必须保持对它的引用（比如有个变量保存）
    用法::
        import matplotlib.pyplot as plt
        import numpy as np
        fig, (ax1, ax2) = plt.subplots(nrows=2, sharex=True)
        t = np.arange(0.0, 2.0, 0.01)
        ax1.plot(t, np.sin(2*np.pi*t))
        ax2.plot(t, np.sin(4*np.pi*t))
        cursor = SingleMultiCursor(fig.canvas, (ax1, ax2), single=0, color='w', lw=0.5)
        plt.show()
    """

    def __init__(self, canvas, axes, single=0, **lineprops):
        self.canvas = canvas
        self.axes = axes
        self.single = single
        if single not in [0, 1]:
            raise ValueError('Unrecognized single value: ' + str(single) + ', must be 0 or 1')

        xmin, xmax = axes[-1].get_xlim()
        ymin, ymax = axes[-1].get_ylim()
        xmid = 0.5 * (xmin + xmax)
        ymid = 0.5 * (ymin + ymax)

        self.background = None
        self.needclear = False

        lineprops['animated'] = True  # for blt

        self.lines = [
            [ax.axhline(ymid, visible=False, **lineprops) for ax in axes],
            [ax.axvline(xmid, visible=False, **lineprops) for ax in axes]
        ]

        self.canvas.mpl_connect('motion_notify_event', self.onmove)
        self.canvas.mpl_connect('draw_event', self.clear)

    def clear(self, event):
        self.background = (self.canvas.copy_from_bbox(self.canvas.figure.bbox))
        for line in self.lines[0] + self.lines[1]:
            line.set_visible(False)

    def onmove(self, event):
        if event.inaxes is None: return
        if not self.canvas.widgetlock.available(self): return

        self.needclear = True

        for i in range(len(self.axes)):
            if event.inaxes == self.axes[i]:
                if self.single == 0:
                    for line in self.lines[1]:
                        line.set_xdata((event.xdata, event.xdata))
                        line.set_visible(True)

                    line = self.lines[0][i]
                    line.set_ydata((event.ydata, event.ydata))
                    line.set_visible(True)
                else:
                    for line in self.lines[0]:
                        line.set_ydata((event.ydata, event.ydata))
                        line.set_visible(True)

                    line = self.lines[1][i]
                    line.set_xdata((event.xdata, event.xdata))
                    line.set_visible(True)
            else:
                self.lines[self.single][i].set_visible(False)

        if self.background is not None:
            self.canvas.restore_region(self.background)

        for lines in self.lines:
            for line in lines:
                if line.get_visible():
                    line.axes.draw_artist(line)

        self.canvas.blit()


class myPlot:
    @staticmethod
    def kline(dataframe):
        tools.calc_macd(dataframe)  # 计算MACD值，数据存于DataFrame中
        tools.calc_kdj(dataframe)  # 计算KDJ值，数据存于DataFrame中
        df = dataframe[-150:].copy()  # 取最近的N天测试，列顺序为 open,high,low,close,volume,dif,dea,bar,k,d,j  index的名字为timestamp

        # 日期转换成整数序列
        date_tickers = df.index.strftime("%m-%d %H")
        print(date_tickers)
        df.loc[:, 'date'] = range(0, len(df))  # 日期改变成序号
        matix = df.values  # 转换成绘制蜡烛图需要的数据格式(open, high, low, close, volume, date)
        xdates = matix[:, -1]  # X轴数据(这里用的天数索引)

        # 设置外观效果
        plt.rc('font', family='SimHei')  # 用中文字体，防止中文显示不出来
        plt.rc('figure', fc='k')  # 绘图对象背景图
        plt.rc('text', c='#800000')  # 文本颜色
        plt.rc('axes', axisbelow=True, xmargin=0, fc='k', ec='#800000', lw=1.5, labelcolor='#800000',
               unicode_minus=False)  # 坐标轴属性(置底，左边无空隙，背景色，边框色，线宽，文本颜色，中文负号修正)
        plt.rc('xtick', c='#d43221')  # x轴刻度文字颜色
        plt.rc('ytick', c='#d43221')  # y轴刻度文字颜色
        plt.rc('grid', c='#800000', alpha=0.9, ls=':', lw=0.8)  # 网格属性(颜色，透明值，线条样式，线宽)
        plt.rc('lines', lw=0.8)  # 全局线宽

        # 创建绘图对象和4个坐标轴
        fig = plt.figure(figsize=(16, 8))
        left, width = 0.05, 0.9
        ax1 = fig.add_axes([left, 0.6, width, 0.35])  # left, bottom, width, height
        ax2 = fig.add_axes([left, 0.45, width, 0.15], sharex=ax1)  # 共享ax1轴
        ax3 = fig.add_axes([left, 0.25, width, 0.2], sharex=ax1)  # 共享ax1轴
        ax4 = fig.add_axes([left, 0.05, width, 0.2], sharex=ax1)  # 共享ax1轴
        plt.setp(ax1.get_xticklabels(), visible=False)  # 使x轴刻度文本不可见，因为共享，不需要显示
        plt.setp(ax2.get_xticklabels(), visible=False)  # 使x轴刻度文本不可见，因为共享，不需要显示
        plt.setp(ax3.get_xticklabels(), visible=False)  # 使x轴刻度文本不可见，因为共享，不需要显示


        # 绘制蜡烛图
        def format_date(x, pos=None): return '' if x < 0 or x > len(date_tickers) - 1 else date_tickers[
            int(x)]  # 日期格式化函数，根据天数索引取出日期值

        ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))  # 设置自定义x轴格式化日期函数
        ax1.xaxis.set_major_locator(ticker.MultipleLocator(max(int(len(df) / 15), 5)))  # 横向最多排15个左右的日期，最少5个，防止日期太拥挤
        # ax1.xaxis.set_major_locator(mdates.HourLocator())
        # mpf.candlestick_ochl(ax1, matix, width=0.5, colorup='#ff3232', colordown='#54fcfc')
        # # 下面这一段代码，替换了上面注释的这个函数，因为上面的这个函数达不到同花顺的效果
        opens, highs, lows, closes = matix[:, 0], matix[:, 1], matix[:, 2], matix[:, 3]  # 取出ochl值
        avg_dist_between_points = (xdates[-1] - xdates[0]) / float(len(xdates))  # 计算每个日期之间的距离
        delta = avg_dist_between_points / 4.0  # 用于K线实体(矩形)的偏移坐标计算
        barVerts = [((date - delta, open), (date - delta, close), (date + delta, close), (date + delta, open)) for
                    date, open, close in zip(xdates, opens, closes)]  # 生成K线实体(矩形)的4个顶点坐标
        rangeSegLow = [((date, low), (date, min(open, close))) for date, low, open, close in
                       zip(xdates, lows, opens, closes)]  # 生成下影线顶点列表
        rangeSegHigh = [((date, high), (date, max(open, close))) for date, high, open, close in
                        zip(xdates, highs, opens, closes)]  # 生成上影线顶点列表
        rangeSegments = rangeSegLow + rangeSegHigh  # 上下影线顶点列表
        cmap = {True: mcolors.to_rgba('#53c156', 1.0),
                False: mcolors.to_rgba('#ff1717', 1.0)}  # K线实体(矩形)中间的背景色(True是上涨颜色，False是下跌颜色)
        inner_colors = [cmap[opn < cls] for opn, cls in zip(opens, closes)]  # K线实体(矩形)中间的背景色列表
        cmap = {True: mcolors.to_rgba('#53c156', 1.0),
                False: mcolors.to_rgba('#ff1717', 1.0)}  # K线实体(矩形)边框线颜色(上下影线和后面的成交量颜色也共用)
        updown_colors = [cmap[opn < cls] for opn, cls in zip(opens, closes)]  # K线实体(矩形)边框线颜色(上下影线和后面的成交量颜色也共用)列表
        ax1.add_collection(LineCollection(rangeSegments, colors=updown_colors, linewidths=0.5, antialiaseds=False))  # 生成上下影线的顶点数据(颜色，线宽，反锯齿，反锯齿关闭好像没效果)
        ax1.add_collection(PolyCollection(barVerts, facecolors=inner_colors, edgecolors=updown_colors, antialiaseds=False, linewidths=0.5))  # 生成多边形(矩形)顶点数据(背景填充色，边框色，反锯齿，线宽)
        plt.gcf().autofmt_xdate()  # 自动旋转日期标记


        # 绘制均线
        mav_colors = ['#ffffff', '#d4ff07', '#ff80ff', '#00e600', '#02e2f4', '#ffffb9', '#2a6848']  # 均线循环颜色
        mav_period = [5, 10, 20, 30, 60, 120, 180]  # 定义要绘制的均线周期，可增减
        n = len(df)
        for i in range(len(mav_period)):
            if n >= mav_period[i]:
                mav_vals = df['close'].rolling(mav_period[i]).mean().values
                ax1.plot(xdates, mav_vals, c=mav_colors[i % len(mav_colors)], label='MA' + str(mav_period[i]))
        ax1.set_title('my kLine')  # 标题
        ax1.grid(True)  # 画网格
        ax1.legend(loc='upper right')  # 图例放置于右上角
        ax1.xaxis_date()  # 好像要不要效果一样？

        # 绘制成交量和成交量均线（5日，10日）
        # ax2.bar(xdates, matix[:, 5], width= 0.5, color=updown_colors) # 绘制成交量柱状图
        barVerts = [((date - delta, 0), (date - delta, vol), (date + delta, vol), (date + delta, 0)) for date, vol in
                    zip(xdates, matix[:, 4])]  # 生成K线实体(矩形)的4个顶点坐标
        ax2.add_collection(PolyCollection(barVerts, facecolors=inner_colors, edgecolors=updown_colors, antialiaseds=False,
                                          linewidths=0.5))  # 生成多边形(矩形)顶点数据(背景填充色，边框色，反锯齿，线宽)
        if n >= 5:  # 5日均线，作法类似前面的均线
            vol5 = df['volume'].rolling(5).mean().values
            ax2.plot(xdates, vol5, c='y', label='VOL5')
        if n >= 10:  # 10日均线，作法类似前面的均线
            vol10 = df['volume'].rolling(10).mean().values
            ax2.plot(xdates, vol10, c='w', label='VOL10')
        ax2.yaxis.set_ticks_position('right')  # y轴显示在右边
        ax2.legend(loc='upper right')  # 图例放置于右上角
        ax2.grid(True)  # 画网格
        # ax2.set_ylabel('成交量') # y轴名称

        # 绘制MACD
        difs, deas, bars = matix[:, 5], matix[:, 6], matix[:, 7]  # 取出MACD值
        ax3.axhline(0, ls='-', c='g', lw=0.5)  # 水平线
        ax3.plot(xdates, difs, c='w', label='DIFF')  # 绘制DIFF线
        ax3.plot(xdates, deas, c='y', label='DEA')  # 绘制DEA线
        # ax3.bar(xdates, df['bar'], width= 0.05, color=bar_colors) # 绘制成交量柱状图(发现用bar绘制，线的粗细不一致，故使用下面的直线列表)
        cmap = {True: mcolors.to_rgba('r', 1.0), False: mcolors.to_rgba('g', 1.0)}  # MACD线颜色，大于0为红色，小于0为绿色
        bar_colors = [cmap[bar > 0] for bar in bars]  # MACD线颜色列表
        vlines = [((date, 0), (date, bars[date])) for date in range(len(bars))]  # 生成MACD线顶点列表
        ax3.add_collection(LineCollection(vlines, colors=bar_colors, linewidths=0.5, antialiaseds=False))  # 生成MACD线的顶点数据(颜色，线宽，反锯齿)
        ax3.legend(loc='upper right')  # 图例放置于右上角
        ax3.grid(True)  # 画网格

        # 绘制KDJ
        K, D, J = matix[:, 8], matix[:, 9], matix[:, 10]  # 取出KDJ值
        ax4.axhline(0, ls='-', c='g', lw=0.5)  # 水平线
        ax4.yaxis.set_ticks_position('right')  # y轴显示在右边
        ax4.plot(xdates, K, c='y', label='K')  # 绘制K线
        ax4.plot(xdates, D, c='c', label='D')  # 绘制D线
        ax4.plot(xdates, J, c='m', label='J')  # 绘制J线
        ax4.legend(loc='upper right')  # 图例放置于右上角
        ax4.grid(True)  # 画网格

        # set useblit = True on gtkagg for enhanced performance
        from matplotlib.widgets import Cursor, MultiCursor  # 处理鼠标

        cursor = Cursor(ax1, useblit=True, color='w', linewidth=0.5, linestyle=':')  # 绘制单个子图十字光标
        # cursor = MultiCursor(fig.canvas, (ax1, ax2, ax3, ax4), useblit=True, horizOn=True, vertOn=True, color='w', lw=0.5)  # 绘制十字光标
        # cursor = SingleMultiCursor(fig.canvas, (ax1), single=0, color='r', lw=0.5)  # 单子图多光标
        plt.show()  # 窗口手动关闭

