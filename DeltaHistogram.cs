// DeltaHistogram — NT8 独立副图 Delta 直方图指标
// 每根主图K线内 Delta = 主动买量 - 主动卖量（1-tick序列按 tick rule 判定方向：本tick价>上tick价=主动买，<=主动卖，相等沿用上次方向）。
// 周期自适应：主图任意秒级/分钟周期都自动按当前K线累计。
// 显示：正delta=红柱，负delta=绿柱（配色约定：多头=红，空头=绿），柱高=delta大小，不显示数字，无0轴线。
// 均线：|delta| 的 EMA（默认21周期）——衡量近期多空博弈的平均力度。
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
	public class DeltaHistogram : Indicator
	{
		private double deltaAccum;					// 当前主图K线累计delta
		private double lastTickPrice;
		private int lastTickDir;					// 1买/-1卖/0未定
		private double absEma;						// |delta| 的EMA（手动递推，避免多序列时序问题）
		private bool absEmaInit;

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description					= "独立副图Delta直方图：每根K线主动买量-主动卖量(tick rule)，正红/负绿，柱高=大小；均线=|delta|的EMA(默认21)；周期自适应";
				Name						= "DeltaHistogram";
				Calculate					= Calculate.OnBarClose;
				IsOverlay					= false;
				DisplayInDataBox			= true;
				DrawOnPricePanel			= false;
				IsSuspendedWhileInactive	= true;
				DeltaEmaPeriod				= 21;

				// Delta直方图：从0起画竖柱，颜色逐根设置（正=红/负=绿）
				AddPlot(new Gui.Stroke(Brushes.Red, 3), PlotStyle.Bar, "Delta");
				// |Delta| 均线
				AddPlot(new Gui.Stroke(Brushes.DodgerBlue, 2), PlotStyle.Line, "AbsDeltaEMA");
			}
			else if (State == State.Configure)
			{
				AddDataSeries(BarsPeriodType.Tick, 1);				// BIP1 1-tick：delta计算，周期自适应
			}
			else if (State == State.DataLoaded)
			{
				deltaAccum = 0; lastTickPrice = 0; lastTickDir = 0;
				absEma = 0; absEmaInit = false;
			}
		}

		protected override void OnBarUpdate()
		{
			if (BarsInProgress == 1)					// 1-tick 序列：tick rule 累计delta
			{
				double px = Closes[1][0];
				int dir = lastTickDir;
				if (lastTickPrice > 0)
				{
					if (px > lastTickPrice) dir = 1;
					else if (px < lastTickPrice) dir = -1;
				}
				if (dir != 0)
					deltaAccum += dir * Volumes[1][0];
				lastTickPrice = px;
				lastTickDir = dir;
				return;
			}
			if (BarsInProgress != 0) return;

			// 取本K线累计值并清零（下一根重新累计→任意主图周期自适应）
			double delta = deltaAccum;
			deltaAccum = 0;
			Values[0][0] = delta;
			PlotBrushes[0][0] = delta >= 0 ? Brushes.Red : Brushes.Green;	// 配色约定：多头=红，空头=绿

			// |delta| 的 EMA（最近N个周期delta绝对值的均线）
			double absDelta = Math.Abs(delta);
			if (!absEmaInit) { absEma = absDelta; absEmaInit = true; }
			else absEma += 2.0 / (DeltaEmaPeriod + 1) * (absDelta - absEma);
			Values[1][0] = absEma;
		}

		#region Properties
		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "Delta均线周期(|delta|的EMA)", Order = 1, GroupName = "1.参数")]
		public int DeltaEmaPeriod { get; set; }
		#endregion
	}
}
