// EmaTrendBands — NT8 副图双色带趋势指标
// 上带(本周期)：EMA21>EMA55>EMA144 = 绝对多头(红)；EMA21<EMA55<EMA144 = 绝对空头(绿)；其他 = 中性(灰)
// 下带(大周期)：15分钟 EMA144 趋势——15m K线实体完全站上EMA144=多头(红)，实体完全跌破=空头(绿)，实体穿越EMA时保持前一状态；任何主图周期都自动挂15m副序列
// 配色约定与用户其他指标一致：多头=红，空头=绿
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

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description					= "双色带趋势(副图)：上带=本周期EMA21/55/144排列(红绝对多/绿绝对空/灰中性)，下带=15m EMA144趋势(红多/绿空)";
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

				// PlotStyle.Square + 粗笔宽：在固定 y 值上形成横向矩形色带（Bar 样式会从0起画竖柱，不适合色带）
				AddPlot(new Gui.Stroke(Brushes.DimGray, 14), PlotStyle.Square, "本周期排列");
				AddPlot(new Gui.Stroke(Brushes.DimGray, 14), PlotStyle.Square, "大周期趋势");
			}
			else if (State == State.Configure)
			{
				AddDataSeries(BarsPeriodType.Minute, HtfMinutes);
			}
			else if (State == State.DataLoaded)
			{
				emaFast = EMA(BarsArray[0], FastPeriod);
				emaMid  = EMA(BarsArray[0], MidPeriod);
				emaSlow = EMA(BarsArray[0], SlowPeriod);
				emaHtf  = EMA(BarsArray[1], HtfEmaPeriod);
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
			if (BarsInProgress != 0) return;

			// 上带：本周期 EMA 排列（画在 y=2 一条水平带）
			Values[0][0] = 2;
			if (CurrentBar >= SlowPeriod)
			{
				double f = emaFast[0], m = emaMid[0], s = emaSlow[0];
				PlotBrushes[0][0] = (f > m && m > s) ? Brushes.Red
					: (f < m && m < s) ? Brushes.Green
					: Brushes.DimGray;
			}
			else
				PlotBrushes[0][0] = Brushes.DimGray;

			// 下带：15m EMA144 趋势（画在 y=1 一条水平带）
			Values[1][0] = 1;
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
		#endregion
	}
}
