// EmaBandTrendEA — NT8 策略：基于 EMA21/55/144 色带趋势的三档限价挂单 EA（美盘/非美盘双周期版）
// 趋势判定（与 EmaTrendBands 第一条带一致）：
//   红(EMA21>EMA55>EMA144) 出现 → 进入多头趋势；期间灰/红均视为多头；直到出现绿(EMA21<EMA55<EMA144) 才逆转为空头趋势。做空镜像。
// 交易序列（图表周期不影响交易，策略内部自带两条秒级序列）：
//   美盘时段用"美盘交易周期(秒)"序列（默认10s），其余时段用"非美盘交易周期(秒)"序列（默认30s）。
//   NT8 无法在运行中更改图表本身的周期，故用 AddDataSeries 双序列自动切换实现。
// 美盘时间窗（时间均按图表/平台时区的HHmm设置）：
//   美盘开盘前N分钟：撤单+市价平掉全部持仓，禁止开仓；
//   开盘后N分钟内：允许持有/止盈，但不挂新入场单；
//   美盘收盘前N分钟：撤单+平仓，禁止开仓；收盘后切回非美盘序列。
// 趋势下（每根K线收盘刷新）：
//   在 EMA21/55/144 各挂一档限价单（手数可调，默认 2/1/2，各档可单独开关）；某档已成交则不再重复挂。
//   离场（两种模式，开关参数切换）：
//     固定止盈模式：各档统一限价平仓 @ 持仓成本线(加权平均开仓价) ± 止盈点数（默认20点）；新档位成交后成本线变化，止盈价随之刷新。
//     移动止损模式：价格从成本线每浮盈一个步长(默认20点)，止损线上移一档（浮盈1档→保本，浮盈2档→成本+1档...），只进不退，触发止损市价全平。
//   离场：色带反色 → 撤单+全平+镜像反向。
// 15m EMA144 过滤（可勾选）：实体完全站上=只多 / 完全跌破=只空 / 相交保持。
// 实盘下单只在实时状态执行；「允许历史段下单」仅在 Strategy Analyzer 回测时开启。
// 面板：大周期过滤→时段/周期状态→本周期色带→昨日回测(12行)→今日回测(12行)→实盘统计→档位→持仓。
//   影子回测与实盘同样按美盘/非美盘双周期+时间窗运行（收盘级近似）。
#region Using declarations
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Windows.Media;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.DrawingTools;
using NinjaTrader.NinjaScript.Indicators;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
	public class EmaBandTrendEA : Strategy
	{
		// 序列索引：0=图表主序列(仅面板) 1=15m过滤 2=美盘秒级 3=非美盘秒级
		private const int BipHtf = 1, BipUs = 2, BipNonUs = 3;
		private EMA emaHtf;
		private EMA[] emaF = new EMA[4], emaM = new EMA[4], emaS = new EMA[4];
		private int htfTrend;					// 15m过滤带：1红多 / -1绿空 / 0未定
		private int trend;						// 交易序列色带趋势：1多 / -1空 / 0未定（灰色保持原值）
		private int curRegimeBip = -1;			// 当前生效交易序列（BipUs / BipNonUs）
		private string phaseS = "—";
		private bool filledFast, filledMid, filledSlow;
		private Order ordFast, ordMid, ordSlow;
		private double trailStop;				// 实盘移动止损线（0=未激活）

		private const string SigFast = "E21";
		private const string SigMid  = "E55";
		private const string SigSlow = "E144";

		private SimpleFont panelFont;
		private string lastAction = "—";
		private DateTime lastPanelAt = DateTime.MinValue;
		private DateTime rtStartAt = DateTime.MinValue;

		private SessionIterator sessionIter;
		private DateTime firstTradingDay = DateTime.MinValue;
		private DateTime warmupDoneDay   = DateTime.MinValue;
		private DateTime curTradingDay   = DateTime.MinValue;

		// ---------- 影子回测（收盘级近似）：TP档位 × 加/不加过滤，共12套并行 ----------
		private class DayStat
		{
			public int Trades, Wins;
			public double GrossWin, GrossLoss, Comm;
			public double Equity, Peak, MaxDD;
			public double Net { get { return GrossWin + GrossLoss - Comm; } }
			public void Add(double netPnl)
			{
				Equity += netPnl;
				if (Equity > Peak) Peak = Equity;
				if (Peak - Equity > MaxDD) MaxDD = Peak - Equity;
			}
		}
		private class ShadowSim
		{
			public bool UseFilter;
			public double TpPoints;
			public int Trend;
			public double[] Pend = new double[3];
			public bool[]   Open = new bool[3];
			public double[] Cost = new double[3];
			public double Stop;					// 移动止损线（0=未激活）
			public Dictionary<DateTime, DayStat> Days = new Dictionary<DateTime, DayStat>();

			public DayStat Day(DateTime d)
			{
				DayStat st;
				if (!Days.TryGetValue(d, out st)) { st = new DayStat(); Days[d] = st; }
				return st;
			}
			public void CloseLot(int i, double px, int[] qty, double pv, double commPerSide, int dir, DateTime day)
			{
				double pnl  = (px - Cost[i]) * dir * qty[i] * pv;
				double comm = commPerSide * qty[i] * 2;
				DayStat st = Day(day);
				st.Comm += comm;
				if (pnl >= 0) st.GrossWin += pnl; else st.GrossLoss += pnl;
				st.Trades++; if (pnl - comm > 0) st.Wins++;
				st.Add(pnl - comm);
				Open[i] = false;
			}
			public void FlattenAll(double px, int[] qty, double pv, double commPerSide, DateTime day)
			{
				for (int i = 0; i < 3; i++)
					if (Open[i]) CloseLot(i, px, qty, pv, commPerSide, Trend, day);
				for (int i = 0; i < 3; i++) Pend[i] = 0;
				Stop = 0;
			}
			public double AvgCost(int[] qty)
			{
				double c = 0; int q = 0;
				for (int i = 0; i < 3; i++)
					if (Open[i]) { c += Cost[i] * qty[i]; q += qty[i]; }
				return q == 0 ? 0 : c / q;
			}
			public bool AnyOpen { get { return Open[0] || Open[1] || Open[2]; } }
		}
		private readonly List<ShadowSim> sims = new List<ShadowSim>();

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description					= "EMA色带趋势EA：红→绿(或绿→红)才逆转趋势；EMA21/55/144三档限价挂单，离场=成本线固定止盈或成本线步进移动止损(开关切换)；美盘/非美盘双周期自动切换；可选15m EMA144色带过滤";
				Name						= "EmaBandTrendEA";
				Calculate					= Calculate.OnBarClose;
				EntriesPerDirection			= 3;
				EntryHandling				= EntryHandling.UniqueEntries;
				IsExitOnSessionCloseStrategy= false;
				OrderFillResolution			= OrderFillResolution.Standard;
				TimeInForce					= TimeInForce.Gtc;
				BarsRequiredToTrade			= 0;

				FastPeriod	= 21;
				MidPeriod	= 55;
				SlowPeriod	= 144;
				QtyFast		= 2;
				QtyMid		= 1;
				QtySlow		= 2;
				UseFastLevel	= true;
				UseMidLevel		= true;
				UseSlowLevel	= true;
				ProfitPoints	= 20;
				UseTrailingMode	= false;
				TrailStepPoints	= 20;
				TpPointsA	= 20;
				TpPointsB	= 30;
				TpPointsC	= 40;
				TpPointsD	= 5;
				TpPointsE	= 10;
				TpPointsF	= 15;
				UseHtfFilter	= false;
				HtfMinutes		= 15;
				HtfEmaPeriod	= 144;
				CommissionPerSide = 0.62;
				AllowHistoricalTrading = false;

				UseSessionSwitch	= true;
				UsPeriodSec			= 10;
				NonUsPeriodSec		= 30;
				UsOpenHHmm			= 2130;		// 北京时间 21:30 = 美盘开盘 09:30 ET(夏令)
				UsCloseHHmm			= 400;		// 北京时间 04:00 = 美盘收盘 16:00 ET(夏令)
				FlattenMinutes		= 10;
				DelayAfterOpenMin	= 10;
			}
			else if (State == State.Configure)
			{
				panelFont = new SimpleFont("Consolas", 12);
				AddDataSeries(BarsPeriodType.Minute, HtfMinutes);				// BIP1
				AddDataSeries(BarsPeriodType.Second, UsPeriodSec);				// BIP2 美盘
				AddDataSeries(BarsPeriodType.Second, NonUsPeriodSec);			// BIP3 非美盘
			}
			else if (State == State.DataLoaded)
			{
				emaHtf = EMA(BarsArray[BipHtf], HtfEmaPeriod);
				foreach (int b in new[] { BipUs, BipNonUs })
				{
					emaF[b] = EMA(BarsArray[b], FastPeriod);
					emaM[b] = EMA(BarsArray[b], MidPeriod);
					emaS[b] = EMA(BarsArray[b], SlowPeriod);
				}
				trend = 0; htfTrend = 0; curRegimeBip = -1;
				sessionIter = new SessionIterator(BarsArray[0]);

				sims.Clear();
				foreach (double tp in AllTps())
				{
					sims.Add(new ShadowSim { UseFilter = true,  TpPoints = tp });
					sims.Add(new ShadowSim { UseFilter = false, TpPoints = tp });
				}
			}
			else if (State == State.Realtime)
			{
				// 历史段不下真实单，切实时前清干净内部状态，避免同步单/残留挂单造成混乱
				filledFast = filledMid = filledSlow = false;
				ordFast = ordMid = ordSlow = null;
				trailStop = 0;
				rtStartAt = Core.Globals.Now;
				lastAction = "进入实时 " + rtStartAt.ToString("HH:mm:ss");
			}
		}

		// ---------- 时段工具：分钟数（0~1439，按图表/平台时区） ----------
		private static int MinOfDay(DateTime t) { return t.Hour * 60 + t.Minute; }
		private int OpenMin  { get { return UsOpenHHmm / 100 * 60 + UsOpenHHmm % 100; } }
		private int CloseMin { get { return UsCloseHHmm / 100 * 60 + UsCloseHHmm % 100; } }
		private bool InUsSession(int m)
		{
			int o = OpenMin, c = CloseMin;
			return o <= c ? (m >= o && m < c) : (m >= o || m < c);
		}
		// 距 target 还有多少分钟（0~1439）
		private static int MinUntil(int m, int target) { return (target - m + 1440) % 1440; }

		protected override void OnBarUpdate()
		{
			if (BarsInProgress == BipHtf)			// 15m 过滤带（实体完全站上/跌破，相交保持）
			{
				if (CurrentBars[BipHtf] >= HtfEmaPeriod)
				{
					double bHi = Math.Max(Opens[BipHtf][0], Closes[BipHtf][0]);
					double bLo = Math.Min(Opens[BipHtf][0], Closes[BipHtf][0]);
					if (bLo > emaHtf[0]) htfTrend = 1;
					else if (bHi < emaHtf[0]) htfTrend = -1;
				}
				return;
			}
			if (BarsInProgress == 0) { UpdatePanel(); return; }		// 主序列只负责面板刷新
			if (BarsInProgress != BipUs && BarsInProgress != BipNonUs) return;

			int bip = BarsInProgress;
			DateTime barTime = Times[bip][0];
			int m = MinOfDay(barTime);

			// 判定当前生效序列：开启切换时按美盘时间窗；关闭切换时始终用非美盘序列
			bool inUs = UseSessionSwitch && InUsSession(m);
			int activeBip = inUs ? BipUs : BipNonUs;
			if (bip != activeBip) return;			// 非生效序列的K线只更新EMA，不执行逻辑

			curTradingDay = sessionIter.GetTradingDay(barTime);
			if (firstTradingDay == DateTime.MinValue) firstTradingDay = curTradingDay;
			if (CurrentBars[bip] < SlowPeriod) return;
			if (warmupDoneDay == DateTime.MinValue) warmupDoneDay = curTradingDay;

			double pv = Instrument.MasterInstrument.PointValue;
			int[] qty = { QtyFast, QtyMid, QtySlow };
			bool realtime = State == State.Realtime || AllowHistoricalTrading;

			// ---- 序列切换：撤单、清状态（时间窗设计上此时应已平仓，稳妥起见强制归零） ----
			if (curRegimeBip != bip)
			{
				if (realtime && curRegimeBip != -1)
				{
					CancelEntryOrders();
					if (Position.MarketPosition == MarketPosition.Long)  ExitLong();
					else if (Position.MarketPosition == MarketPosition.Short) ExitShort();
				}
				filledFast = filledMid = filledSlow = false;
				trailStop = 0;
				trend = 0;
				foreach (ShadowSim sim in sims)
				{
					sim.FlattenAll(Closes[bip][0], qty, pv, CommissionPerSide, curTradingDay);
					sim.Trend = 0;
				}
				curRegimeBip = bip;
			}

			// ---- 美盘时间窗：平仓窗 / 禁开仓窗 ----
			bool forceFlat = false, noEntry = false;
			if (UseSessionSwitch)
			{
				int toOpen  = MinUntil(m, OpenMin);
				int toClose = MinUntil(m, CloseMin);
				if ((toOpen > 0 && toOpen <= FlattenMinutes) || (toClose > 0 && toClose <= FlattenMinutes))
					forceFlat = true;												// 开盘前/收盘前 N 分钟
				int sinceOpen = MinUntil(OpenMin, m);
				if (inUs && sinceOpen < DelayAfterOpenMin) noEntry = true;			// 开盘后 N 分钟禁开仓
				phaseS = forceFlat ? (toOpen <= FlattenMinutes && toOpen > 0 ? "开盘前平仓窗,距开盘" + toOpen + "分" : "收盘前平仓窗,距收盘" + toClose + "分")
						: noEntry ? "开盘后禁开仓,已过" + sinceOpen + "分"
						: inUs ? "美盘正常交易,距收盘" + toClose + "分"
						: "非美盘正常交易,距美盘开盘" + toOpen + "分";
			}
			else phaseS = "时段切换已关闭";

			// 0) 影子回测：先用"上一根收盘挂出的单"结算本K线
			foreach (ShadowSim sim in sims) StepSim(sim, bip, pv, qty);

			// 1) 色带颜色 → 趋势状态机（灰色保持原趋势）
			double f = emaF[bip][0], md = emaM[bip][0], s = emaS[bip][0];
			int band = (f > md && md > s) ? 1 : (f < md && md < s) ? -1 : 0;
			if (band != 0 && band != trend)
			{
				if (realtime)
				{
					CancelEntryOrders();
					if (Position.MarketPosition == MarketPosition.Long)  ExitLong();
					else if (Position.MarketPosition == MarketPosition.Short) ExitShort();
					filledFast = filledMid = filledSlow = false;
					trailStop = 0;
				}
				trend = band;
			}
			foreach (ShadowSim sim in sims)
			{
				if (band != 0 && band != sim.Trend)
				{
					sim.FlattenAll(Closes[bip][0], qty, pv, CommissionPerSide, curTradingDay);
					sim.Trend = band;
				}
			}

			// 2) 平仓窗：撤单+全平（实盘与影子回测一致）
			if (forceFlat)
			{
				if (realtime)
				{
					CancelEntryOrders();
					if (Position.MarketPosition == MarketPosition.Long)  ExitLong();
					else if (Position.MarketPosition == MarketPosition.Short) ExitShort();
					filledFast = filledMid = filledSlow = false;
					trailStop = 0;
				}
				foreach (ShadowSim sim in sims)
					sim.FlattenAll(Closes[bip][0], qty, pv, CommissionPerSide, curTradingDay);
				UpdatePanel();
				return;
			}

			// 3) 影子回测：按本根收盘挂出下一根的单（禁开仓窗内不挂入场单，只保持止盈）
			foreach (ShadowSim sim in sims) ArmSim(sim, bip, noEntry);

			// 4) 实盘下单
			if (realtime && trend != 0)
			{
				bool allowEntry = !noEntry && (!UseHtfFilter || trend == htfTrend);
				if (allowEntry)
				{
					if (trend == 1)
					{
						if (UseFastLevel && !filledFast) ordFast = EnterLongLimit(bip, true, QtyFast, emaF[bip][0], SigFast);
						if (UseMidLevel  && !filledMid)  ordMid  = EnterLongLimit(bip, true, QtyMid,  emaM[bip][0], SigMid);
						if (UseSlowLevel && !filledSlow) ordSlow = EnterLongLimit(bip, true, QtySlow, emaS[bip][0], SigSlow);
					}
					else
					{
						if (UseFastLevel && !filledFast) ordFast = EnterShortLimit(bip, true, QtyFast, emaF[bip][0], SigFast);
						if (UseMidLevel  && !filledMid)  ordMid  = EnterShortLimit(bip, true, QtyMid,  emaM[bip][0], SigMid);
						if (UseSlowLevel && !filledSlow) ordSlow = EnterShortLimit(bip, true, QtySlow, emaS[bip][0], SigSlow);
					}
				}
				else
					CancelEntryOrders();

				if (Position.MarketPosition == MarketPosition.Flat) trailStop = 0;
				if (Position.MarketPosition != MarketPosition.Flat && Position.Quantity > 0)
				{
					double avg = Position.AveragePrice;
					if (!UseTrailingMode)
					{
						// 固定止盈：成本线 ± 止盈点数（新档位成交→成本线变化→止盈价随之刷新）
						double target = Instrument.MasterInstrument.RoundToTickSize(
							trend == 1 ? avg + ProfitPoints : avg - ProfitPoints);
						if (trend == 1)
						{
							if (filledFast) ExitLongLimit(bip, true, QtyFast, target, "TP" + SigFast, SigFast);
							if (filledMid)  ExitLongLimit(bip, true, QtyMid,  target, "TP" + SigMid,  SigMid);
							if (filledSlow) ExitLongLimit(bip, true, QtySlow, target, "TP" + SigSlow, SigSlow);
						}
						else
						{
							if (filledFast) ExitShortLimit(bip, true, QtyFast, target, "TP" + SigFast, SigFast);
							if (filledMid)  ExitShortLimit(bip, true, QtyMid,  target, "TP" + SigMid,  SigMid);
							if (filledSlow) ExitShortLimit(bip, true, QtySlow, target, "TP" + SigSlow, SigSlow);
						}
					}
					else
					{
						// 移动止损：从成本线每浮盈一个步长，止损线上移一档（只进不退）
						if (trend == 1)
						{
							int steps = (int)Math.Floor((Highs[bip][0] - avg) / TrailStepPoints);
							if (steps >= 1)
							{
								double cand = Instrument.MasterInstrument.RoundToTickSize(avg + (steps - 1) * TrailStepPoints);
								if (trailStop == 0 || cand > trailStop) trailStop = cand;
							}
							if (trailStop > 0)
							{
								if (filledFast) ExitLongStopMarket(bip, true, QtyFast, trailStop, "TS" + SigFast, SigFast);
								if (filledMid)  ExitLongStopMarket(bip, true, QtyMid,  trailStop, "TS" + SigMid,  SigMid);
								if (filledSlow) ExitLongStopMarket(bip, true, QtySlow, trailStop, "TS" + SigSlow, SigSlow);
							}
						}
						else
						{
							int steps = (int)Math.Floor((avg - Lows[bip][0]) / TrailStepPoints);
							if (steps >= 1)
							{
								double cand = Instrument.MasterInstrument.RoundToTickSize(avg - (steps - 1) * TrailStepPoints);
								if (trailStop == 0 || cand < trailStop) trailStop = cand;
							}
							if (trailStop > 0)
							{
								if (filledFast) ExitShortStopMarket(bip, true, QtyFast, trailStop, "TS" + SigFast, SigFast);
								if (filledMid)  ExitShortStopMarket(bip, true, QtyMid,  trailStop, "TS" + SigMid,  SigMid);
								if (filledSlow) ExitShortStopMarket(bip, true, QtySlow, trailStop, "TS" + SigSlow, SigSlow);
							}
						}
					}
				}
			}

			UpdatePanel();
		}

		// ---------- 影子回测：结算本K线（先判入场触及，再按成本线判离场；收盘级近似） ----------
		private void StepSim(ShadowSim sim, int bip, double pv, int[] qty)
		{
			if (sim.Trend == 0) return;
			int dir = sim.Trend;
			bool allow = !sim.UseFilter || sim.Trend == htfTrend;
			for (int i = 0; i < 3; i++)
			{
				if (!sim.Open[i] && allow && sim.Pend[i] > 0)
				{
					bool touch = dir == 1 ? Lows[bip][0] <= sim.Pend[i] : Highs[bip][0] >= sim.Pend[i];
					if (touch) { sim.Open[i] = true; sim.Cost[i] = sim.Pend[i]; }
				}
			}
			if (!sim.AnyOpen) { sim.Stop = 0; return; }
			double avg = sim.AvgCost(qty);
			if (!UseTrailingMode)
			{
				// 固定止盈：成本线 ± N点，全部持仓同价平仓
				double tpPx = dir == 1 ? avg + sim.TpPoints : avg - sim.TpPoints;
				bool hit = dir == 1 ? Highs[bip][0] >= tpPx : Lows[bip][0] <= tpPx;
				if (hit)
				{
					for (int i = 0; i < 3; i++)
						if (sim.Open[i]) sim.CloseLot(i, tpPx, qty, pv, CommissionPerSide, dir, curTradingDay);
					sim.Stop = 0;
				}
			}
			else
			{
				// 移动止损：从成本线每浮盈一个步长(=TpPoints)，止损线上移一档（只进不退）
				double step = sim.TpPoints;
				int steps = (int)Math.Floor((dir == 1 ? Highs[bip][0] - avg : avg - Lows[bip][0]) / step);
				if (steps >= 1)
				{
					double cand = dir == 1 ? avg + (steps - 1) * step : avg - (steps - 1) * step;
					if (sim.Stop == 0 || (dir == 1 ? cand > sim.Stop : cand < sim.Stop)) sim.Stop = cand;
				}
				if (sim.Stop != 0)
				{
					bool hit = dir == 1 ? Lows[bip][0] <= sim.Stop : Highs[bip][0] >= sim.Stop;
					if (hit)
					{
						for (int i = 0; i < 3; i++)
							if (sim.Open[i]) sim.CloseLot(i, sim.Stop, qty, pv, CommissionPerSide, dir, curTradingDay);
						sim.Stop = 0;
					}
				}
			}
		}

		private void ArmSim(ShadowSim sim, int bip, bool noEntry)
		{
			if (sim.Trend == 0) return;
			if (noEntry)
			{
				for (int i = 0; i < 3; i++) sim.Pend[i] = 0;
			}
			else
			{
				sim.Pend[0] = UseFastLevel ? emaF[bip][0] : 0;
				sim.Pend[1] = UseMidLevel  ? emaM[bip][0] : 0;
				sim.Pend[2] = UseSlowLevel ? emaS[bip][0] : 0;
			}
		}

		protected override void OnExecutionUpdate(Execution execution, string executionId, double price, int quantity,
			MarketPosition marketPosition, string orderId, DateTime time)
		{
			if (execution.Order == null || execution.Order.OrderState != OrderState.Filled)
				return;

			string sig = execution.Order.Name;
			if (sig == SigFast) filledFast = true;
			else if (sig == SigMid)  filledMid  = true;
			else if (sig == SigSlow) filledSlow = true;
			else if (sig == "TP" + SigFast || sig == "TS" + SigFast) filledFast = false;
			else if (sig == "TP" + SigMid  || sig == "TS" + SigMid)  filledMid  = false;
			else if (sig == "TP" + SigSlow || sig == "TS" + SigSlow) filledSlow = false;
			lastAction = sig + " 成交@" + price.ToString("F2") + " x" + quantity;
		}

		private void CancelEntryOrders()
		{
			foreach (Order o in new[] { ordFast, ordMid, ordSlow })
				if (o != null && (o.OrderState == OrderState.Working || o.OrderState == OrderState.Accepted))
					CancelOrder(o);
			ordFast = ordMid = ordSlow = null;
		}

		// 实时状态下面板每秒随tick刷新
		protected override void OnMarketData(MarketDataEventArgs e)
		{
			if (State != State.Realtime || e.MarketDataType != MarketDataType.Last) return;
			if ((Core.Globals.Now - lastPanelAt).TotalMilliseconds < 1000) return;
			lastPanelAt = Core.Globals.Now;
			TriggerCustomEvent(o => { if (CurrentBar > 0) UpdatePanel(); }, null);
		}

		private static string Money(double v) { return (v < 0 ? "-$" : "$") + Math.Abs(v).ToString("N0"); }

		// 统一列宽：方案(8) 笔数(5) 胜率(6) 总盈利(9) 总亏损(9) 手续费(8) 最大回撤(9) 净利(9)
		private static string StatRow(string name, int trades, int wins,
			double grossWin, double grossLoss, double comm, double maxDD, double net)
		{
			string wr = trades == 0 ? "—" : (100.0 * wins / trades).ToString("F0") + "%";
			return name.PadRight(8)
				+ trades.ToString().PadLeft(5) + wr.PadLeft(6)
				+ Money(grossWin).PadLeft(9) + Money(grossLoss).PadLeft(9)
				+ Money(comm).PadLeft(8) + Money(-maxDD).PadLeft(9) + Money(net).PadLeft(9);
		}
		private double[] AllTps()
		{
			return new[] { TpPointsD, TpPointsE, TpPointsF, TpPointsA, TpPointsB, TpPointsC };
		}
		private string SimRows(DateTime day)
		{
			string r = "";
			foreach (double tp in AllTps()) r += "│ " + SimRow(tp, true, day) + "\n";
			foreach (double tp in AllTps()) r += "│ " + SimRow(tp, false, day) + "\n";
			return r;
		}
		private string SimRow(double tp, bool filter, DateTime day)
		{
			ShadowSim s = sims.Find(x => x.TpPoints == tp && x.UseFilter == filter);
			string name = (UseTrailingMode ? "TS" : "TP") + tp.ToString("F0") + (filter ? "·滤" : "·原");
			DayStat st;
			if (s == null || day == DateTime.MinValue || !s.Days.TryGetValue(day, out st))
				return StatRow(name, 0, 0, 0, 0, 0, 0, 0);
			return StatRow(name, st.Trades, st.Wins, st.GrossWin, st.GrossLoss, st.Comm, st.MaxDD, st.Net);
		}

		// 上一交易日（影子回测出现过的、早于今日的最近交易日）
		private DateTime PrevTradingDay()
		{
			DateTime best = DateTime.MinValue;
			foreach (ShadowSim s in sims)
				foreach (DateTime d in s.Days.Keys)
					if (d < curTradingDay && d > best) best = d;
			return best;
		}

		private void UpdatePanel()
		{
			if (ChartControl == null) return;
			string st   = State == State.Realtime ? "● 实时运行中" : "○ 历史回补中";
			string trS  = trend == 1 ? "多↑" : trend == -1 ? "空↓" : "未定";
			string htfS = htfTrend == 1 ? "红(只允许做多)" : htfTrend == -1 ? "绿(只允许做空)" : "预热中";
			string sesS = !UseSessionSwitch ? ("单周期 " + NonUsPeriodSec + "s")
				: (curRegimeBip == BipUs ? "美盘 " + UsPeriodSec + "s" : "非美盘 " + NonUsPeriodSec + "s")
				  + "  [" + phaseS + "]  美盘(北京时间)" + (UsOpenHHmm / 100).ToString("D2") + ":" + (UsOpenHHmm % 100).ToString("D2")
				  + "-" + (UsCloseHHmm / 100).ToString("D2") + ":" + (UsCloseHHmm % 100).ToString("D2");

			DateTime prevDay = PrevTradingDay();
			string prevFlag = prevDay == DateTime.MinValue ? "无数据"
				: (firstTradingDay < prevDay && warmupDoneDay < prevDay) ? "数据完整" : "数据不全,请加大加载天数";

			int nRt = 0, wRt = 0; double gw = 0, gl = 0, cm = 0, eq = 0, pk = 0, dd = 0;
			foreach (Trade t in SystemPerformance.RealTimeTrades)
			{
				double p = t.ProfitCurrency;
				double c = t.Quantity * CommissionPerSide * 2;
				nRt++; cm += c;
				if (p >= 0) gw += p; else gl += p;
				if (p - c > 0) wRt++;
				eq += p - c;
				if (eq > pk) pk = eq;
				if (pk - eq > dd) dd = pk - eq;
			}

			string head = "方案".PadRight(8) + "笔数".PadLeft(4) + "胜率".PadLeft(5)
				+ "总盈利".PadLeft(7) + "总亏损".PadLeft(7) + "手续费".PadLeft(6) + "最大回撤".PadLeft(6) + "净利".PadLeft(7);

			string lvS = "E" + FastPeriod + ":" + (!UseFastLevel ? "已关闭" : filledFast ? "已开仓" : "挂单中")
				+ "   E" + MidPeriod + ":" + (!UseMidLevel ? "已关闭" : filledMid ? "已开仓" : "挂单中")
				+ "   E" + SlowPeriod + ":" + (!UseSlowLevel ? "已关闭" : filledSlow ? "已开仓" : "挂单中");
			string posS = "无持仓";
			if (Position.MarketPosition != MarketPosition.Flat)
				posS = (Position.MarketPosition == MarketPosition.Long ? "多 " : "空 ") + Position.Quantity + "手 @"
					+ Position.AveragePrice.ToString("F2") + "   浮盈 "
					+ Money(Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, Close[0]));

			string txt =
				  "┌─ EmaBandTrendEA ─ " + st + "\n"
				+ "│ 大周期15m带: " + htfS + "   过滤: " + (UseHtfFilter ? "开" : "关") + "\n"
				+ "│ 交易时段: " + sesS + "\n"
				+ "│ 交易序列色带: " + trS + "\n"
				+ "├─ 昨日回测 " + (prevDay == DateTime.MinValue ? "—" : prevDay.ToString("MM-dd")) + " (" + prevFlag + ")\n"
				+ "│ " + head + "\n"
				+ SimRows(prevDay)
				+ "├─ 今日回测 " + (curTradingDay == DateTime.MinValue ? "—" : curTradingDay.ToString("MM-dd")) + "\n"
				+ "│ " + head + "\n"
				+ SimRows(curTradingDay)
				+ "├─ EA实盘 启动时间: " + (rtStartAt == DateTime.MinValue ? "—" : rtStartAt.ToString("MM-dd HH:mm:ss")) + "\n"
				+ "│ " + head + "\n"
				+ "│ " + StatRow("实盘", nRt, wRt, gw, gl, cm, dd, gw + gl - cm) + "\n"
				+ "├─ 档位: " + lvS + "\n"
				+ "│ 持仓: " + posS + "\n"
				+ "│ 最近动作: " + lastAction + "\n"
				+ "└ 注: 回测为收盘级近似,已按美盘/非美盘双周期+时间窗执行; " + (UseTrailingMode ? "TS=成本线移动止损步长" : "TP=成本线固定止盈") + "; 滤=加15m带过滤, 原=不过滤; 佣金$" + CommissionPerSide.ToString("F2") + "/手/边已计入";
			Draw.TextFixed(this, "bandPanel", txt, TextPosition.TopLeft, Brushes.White,
				panelFont, Brushes.DarkSlateGray, Brushes.Black, 60);
		}

		#region Properties
		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "EMA快线周期", Order = 1, GroupName = "1.参数")]
		public int FastPeriod { get; set; }

		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "EMA中线周期", Order = 2, GroupName = "1.参数")]
		public int MidPeriod { get; set; }

		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "EMA慢线周期", Order = 3, GroupName = "1.参数")]
		public int SlowPeriod { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "启用快线档(E21)", Order = 1, GroupName = "2.手数")]
		public bool UseFastLevel { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "启用中线档(E55)", Order = 2, GroupName = "2.手数")]
		public bool UseMidLevel { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "启用慢线档(E144)", Order = 3, GroupName = "2.手数")]
		public bool UseSlowLevel { get; set; }

		[NinjaScriptProperty]
		[Range(1, 100)]
		[Display(Name = "快线档手数", Order = 4, GroupName = "2.手数")]
		public int QtyFast { get; set; }

		[NinjaScriptProperty]
		[Range(1, 100)]
		[Display(Name = "中线档手数", Order = 5, GroupName = "2.手数")]
		public int QtyMid { get; set; }

		[NinjaScriptProperty]
		[Range(1, 100)]
		[Display(Name = "慢线档手数", Order = 6, GroupName = "2.手数")]
		public int QtySlow { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "启用移动止损模式(关=成本线固定止盈)", Order = 6, GroupName = "3.止盈")]
		public bool UseTrailingMode { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "实盘止盈点数(距成本线)", Order = 7, GroupName = "3.止盈")]
		public double ProfitPoints { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "实盘移动止损步长(点)", Order = 20, GroupName = "3.止盈")]
		public double TrailStepPoints { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "回测止盈档A(点)", Order = 8, GroupName = "3.止盈")]
		public double TpPointsA { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "回测止盈档B(点)", Order = 9, GroupName = "3.止盈")]
		public double TpPointsB { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "回测止盈档C(点)", Order = 10, GroupName = "3.止盈")]
		public double TpPointsC { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "回测止盈档D(点)", Order = 21, GroupName = "3.止盈")]
		public double TpPointsD { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "回测止盈档E(点)", Order = 22, GroupName = "3.止盈")]
		public double TpPointsE { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1000)]
		[Display(Name = "回测止盈档F(点)", Order = 23, GroupName = "3.止盈")]
		public double TpPointsF { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "启用15m EMA144色带过滤", Order = 11, GroupName = "4.过滤")]
		public bool UseHtfFilter { get; set; }

		[NinjaScriptProperty]
		[Range(1, 1440)]
		[Display(Name = "过滤带周期(分钟)", Order = 12, GroupName = "4.过滤")]
		public int HtfMinutes { get; set; }

		[NinjaScriptProperty]
		[Range(1, 500)]
		[Display(Name = "过滤带EMA周期", Order = 13, GroupName = "4.过滤")]
		public int HtfEmaPeriod { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "启用美盘/非美盘双周期切换", Order = 1, GroupName = "6.时段")]
		public bool UseSessionSwitch { get; set; }

		[NinjaScriptProperty]
		[Range(1, 3600)]
		[Display(Name = "美盘交易周期(秒)", Order = 2, GroupName = "6.时段")]
		public int UsPeriodSec { get; set; }

		[NinjaScriptProperty]
		[Range(1, 3600)]
		[Display(Name = "非美盘交易周期(秒)", Order = 3, GroupName = "6.时段")]
		public int NonUsPeriodSec { get; set; }

		[NinjaScriptProperty]
		[Range(0, 2359)]
		[Display(Name = "美盘开盘时间(HHmm,北京时间,默认2130)", Order = 4, GroupName = "6.时段")]
		public int UsOpenHHmm { get; set; }

		[NinjaScriptProperty]
		[Range(0, 2359)]
		[Display(Name = "美盘收盘时间(HHmm,北京时间,默认0400)", Order = 5, GroupName = "6.时段")]
		public int UsCloseHHmm { get; set; }

		[NinjaScriptProperty]
		[Range(0, 120)]
		[Display(Name = "开/收盘前平仓分钟数", Order = 6, GroupName = "6.时段")]
		public int FlattenMinutes { get; set; }

		[NinjaScriptProperty]
		[Range(0, 120)]
		[Display(Name = "开盘后禁开仓分钟数", Order = 7, GroupName = "6.时段")]
		public int DelayAfterOpenMin { get; set; }

		[NinjaScriptProperty]
		[Range(0, 100)]
		[Display(Name = "每手每边佣金($)", Order = 14, GroupName = "5.统计")]
		public double CommissionPerSide { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "允许历史段下单(仅Strategy Analyzer回测时开启)", Order = 15, GroupName = "5.统计")]
		public bool AllowHistoricalTrading { get; set; }
		#endregion
	}
}
