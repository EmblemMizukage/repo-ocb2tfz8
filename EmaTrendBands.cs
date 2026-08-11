// EmaTrendBands — NT8 副图双色带趋势指标 + Delta直方图
// 上带(本周期)：EMA21>EMA55>EMA144 = 绝对多头(红)；EMA21<EMA55<EMA144 = 绝对空头(绿)；其他 = 中性(灰)
// 下带(大周期)：15分钟 EMA144 趋势——15m K线实体完全站上EMA144=多头(红)，实体完全跌破=空头(绿)，实体穿越EMA时保持前一状态；任何主图周期都自动挂15m副序列
// Delta直方图(中部)：每根主图K线内 主动买量-主动卖量（1-tick序列按tick rule判定方向），周期自适应——主图任意分钟/秒级周期都自动按当前K线累计。
//   正delta=红柱，负delta=绿柱（与用户配色约定一致：多头=红，空头=绿），柱高=delta大小，不显示数字；另有21周期Delta EMA均线。
// 布局：delta直方图以0轴为中心占据面板主体；上带/下带的y位置随近期delta幅度自适应缩放，始终贴在面板顶部/底部，不与直方图重叠。
#region Using declarations
using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Windows.Media;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.NinjaScript;
#endregion

namespace NinjaTrader.NinjaScript.Indicators
{
	public class EmaTrendBands : Indicator
	{
		private EMA emaFast, emaMid, emaSlow;		// 本周期 21/55/144
		private EMA emaHtf;							// 大周期(15m) 144
		private int htfTrend;						// 1多/-1空/0未定（15m序列更新）

		// ---- Delta（1-tick序列，tick rule：本tick价>上tick价=主动买，<=主动卖，相等沿用上次方向） ----
		private double deltaAccum;					// 当前主图K线累计delta
		private double lastTickPrice;
		private int lastTickDir;					// 1买/-1卖/0未定
		private double deltaEma;					// Delta EMA（手动递推，避免多序列时序问题）
		private bool deltaEmaInit;

		// ---- 色带位置自适应：近期 |delta| 滚动最大值 ----
		private double[] absRing;
		private int absIdx;
		private const int AbsWindow = 200;

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description					= "双色带趋势(副图)+Delta直方图：上带=本周期EMA21/55/144排列，下带=15m EMA144趋势；中部=每根K线Delta(正红/负绿)及其EMA均线，周期自适应";
				Name						= "EmaTrendBands";
				Calculate					= Calculate.OnBarClose;
				IsOverlay					= false;
				DisplayInDataBox			= true;
				DrawOnPricePanel			= false;
				IsSuspendedWhileInactive	= true;
				FastPeriod					= 21;
				MidPeriod					= 55;
				SlowPeriod					= 144;
				HtfMinutes					= 15;
				HtfEmaPeriod				= 144;
				DeltaEmaPeriod				= 21;

				// PlotStyle.Square + 粗笔宽：在自适应 y 值上形成横向矩形色带（Bar 样式会从0起画竖柱，不适合色带）
				AddPlot(new Gui.Stroke(Brushes.DimGray, 14), PlotStyle.Square, "本周期排列");
				AddPlot(new Gui.Stroke(Brushes.DimGray, 14), PlotStyle.Square, "大周期趋势");
				// Delta直方图：从0轴起画竖柱，颜色逐根设置（正=红/负=绿）
				AddPlot(new Gui.Stroke(Brushes.Red, 3), PlotStyle.Bar, "Delta");
				// Delta均线
				AddPlot(new Gui.Stroke(Brushes.DodgerBlue, 2), PlotStyle.Line, "DeltaEMA");
				AddLine(new Gui.Stroke(Brushes.Gray, DashStyleHelper.Dot, 1), 0, "零轴");
			}
			else if (State == State.Configure)
			{
				AddDataSeries(BarsPeriodType.Minute, HtfMinutes);	// BIP1 大周期
				AddDataSeries(BarsPeriodType.Tick, 1);				// BIP2 1-tick：delta计算，周期自适应
			}
			else if (State == State.DataLoaded)
			{
				emaFast = EMA(BarsArray[0], FastPeriod);
				emaMid  = EMA(BarsArray[0], MidPeriod);
				emaSlow = EMA(BarsArray[0], SlowPeriod);
				emaHtf  = EMA(BarsArray[1], HtfEmaPeriod);
				deltaAccum = 0; lastTickPrice = 0; lastTickDir = 0;
				deltaEma = 0; deltaEmaInit = false;
				absRing = new double[AbsWindow]; absIdx = 0;
			}
		}

		protected override void OnBarUpdate()
		{
			if (BarsInProgress == 1)					// 15m 副序列：更新大周期趋势
			{
				if (CurrentBars[1] >= HtfEmaPeriod)
				{
					double bodyHigh = Math.Max(Opens[1][0], Closes[1][0]);
					double bodyLow  = Math.Min(Opens[1][0], Closes[1][0]);
					if (bodyLow > emaHtf[0])
						htfTrend = 1;
					else if (bodyHigh < emaHtf[0])
						htfTrend = -1;
					// 实体与EMA相交时保持原趋势
				}
				return;
			}
			if (BarsInProgress == 2)					// 1-tick 序列：tick rule 累计delta
			{
				double px = Closes[2][0];
				int dir = lastTickDir;
				if (lastTickPrice > 0)
				{
					if (px > lastTickPrice) dir = 1;
					else if (px < lastTickPrice) dir = -1;
				}
				if (dir != 0)
					deltaAccum += dir * Volumes[2][0];
				lastTickPrice = px;
				lastTickDir = dir;
				return;
			}
			if (BarsInProgress != 0) return;

			// ---- Delta直方图：取本K线累计值并清零（下一根重新累计→任意主图周期自适应） ----
			double delta = deltaAccum;
			deltaAccum = 0;
			Values[2][0] = delta;
			PlotBrushes[2][0] = delta >= 0 ? Brushes.Red : Brushes.Green;	// 配色约定：多头=红，空头=绿

			// ---- Delta EMA(21) ----
			if (!deltaEmaInit) { deltaEma = delta; deltaEmaInit = true; }
			else deltaEma += 2.0 / (DeltaEmaPeriod + 1) * (delta - deltaEma);
			Values[3][0] = deltaEma;

			// ---- 色带y位置自适应：贴面板顶部/底部，不与直方图打架 ----
			absRing[absIdx] = Math.Abs(delta);
			absIdx = (absIdx + 1) % AbsWindow;
			double maxAbs = 1;
			for (int i = 0; i < AbsWindow; i++)
				if (absRing[i] > maxAbs) maxAbs = absRing[i];
			double bandTop = maxAbs * 1.25;			// 上带位置
			double bandBot = -maxAbs * 1.25;		// 下带位置

			// 上带：本周期 EMA 排列
			Values[0][0] = bandTop;
			if (CurrentBar >= SlowPeriod)
			{
				double f = emaFast[0], m = emaMid[0], s = emaSlow[0];
				PlotBrushes[0][0] = (f > m && m > s) ? Brushes.Red
					: (f < m && m < s) ? Brushes.Green
					: Brushes.DimGray;
			}
			else
				PlotBrushes[0][0] = Brushes.DimGray;

			// 下带：15m EMA144 趋势
			Values[1][0] = bandBot;
			PlotBrushes[1][0] = htfTrend > 0 ? Brushes.Red
				: htfTrend < 0 ? Brushes.Green
				: Brushes.Transparent;			// EMA144(15m) 预热期不显示
		}

		#region Properties
		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "本周期EMA快线", Order = 1, GroupName = "1.参数")]
		public int FastPeriod { get; set; }

		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "本周期EMA中线", Order = 2, GroupName = "1.参数")]
		public int MidPeriod { get; set; }

		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "本周期EMA慢线", Order = 3, GroupName = "1.参数")]
		public int SlowPeriod { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1440)]
		[Display(Name = "大周期(分钟)", Order = 4, GroupName = "1.参数")]
		public int HtfMinutes { get; set; }

		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "大周期EMA周期", Order = 5, GroupName = "1.参数")]
		public int HtfEmaPeriod { get; set; }

		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "Delta均线周期", Order = 6, GroupName = "1.参数")]
		public int DeltaEmaPeriod { get; set; }
		#endregion
	}
}
