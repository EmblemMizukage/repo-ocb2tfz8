"""
天勤 GUI (TQSDK 期权监控)
=============================================
- 数据源: 天勤量化 TqSdk (完全脱离无限易)
- 自动发现 SHFE / DCE / CZCE 期权品种
- 识别近月标的、构建 ATM±N 期权链，实时计算 IV / 代理 VIX
- GUI 左侧显示品种卡片，右侧为 T 型报价表格 + 信号面板

运行方式: python "天勤-gui.py"
"""
import os, sys, time, threading, traceback, datetime, sqlite3, math, signal, logging, warnings
warnings.filterwarnings("ignore")
from collections import OrderedDict, deque
from typing import Dict, Optional, List
from datetime import timezone, timedelta

# 抑制 tqsdk 日志噪音
logging.disable(logging.CRITICAL)
for _m in ["tqsdk","tqsdk.objs","tqsdk.api","tqsdk.sim","tqsdk.ta"]:
    logging.getLogger(_m).setLevel(logging.ERROR)

try:
    from zoneinfo import ZoneInfo
    _BJ_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    _BJ_TZ = timezone(timedelta(hours=8))

_PYQT_OK = True
try:
    from PyQt5 import QtCore, QtGui, QtWidgets
except Exception:
    _PYQT_OK = False

# ========================= 天勤账号 =========================
TQ_USER = "18365470981"
TQ_PASS = "Woaini1314@"

# ========================= 数据库路径 =========================
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH_QUOTES = os.path.join(_BASE_DIR, "option_quotes.db")
DB_PATH_SIM    = os.path.join(_BASE_DIR, "option_sim.db")
DB_PATH        = DB_PATH_QUOTES

# SHFE 品种列表（逐一检测是否有期权）
SHFE_PRODUCTS = [
    ("ag","白银"),("cu","铜"),("au","黄金"),("rb","螺纹钢"),("al","铝"),("zn","锌"),
    ("ru","天然橡胶"),("ao","氧化铝"),("ni","镍"),("sn","锡"),("pb","铅"),
    ("ss","不锈钢"),("sp","纸浆"),("bu","沥青"),("hc","热轧卷板"),("fu","燃料油"),("br","合成橡胶"),
]

DCE_PRODUCTS = [
    ("m","豆粕"),("i","铁矿石"),("c","玉米"),("y","豆油"),("p","棕榈油"),
    ("pp","聚丙烯"),("pg","LPG"),("l","聚乙烯"),("v","PVC"),("eg","乙二醇"),
    ("cs","玉米淀粉"),("a","豆一"),("b","豆二"),("j","焦炭"),("jm","焦煤"),
]

CZCE_PRODUCTS = [
    ("SR","白糖"),("CF","棉花"),("MA","甲醇"),("TA","PTA"),("SA","纯碱"),("FG","玻璃"),
    ("RM","菜粕"),("OI","菜油"),("SF","硅铁"),("SM","锰硅"),("PF","短纤"),
    ("PK","花生"),("AP","苹果"),("CJ","红枣"),("UR","尿素"),("SH","烧碱"),
]

TRADING_SESSIONS=[("09:00","11:30"),("13:30","15:00"),("21:00","23:59"),("00:00","02:30")]
SUB_BATCH_LIMIT=400   # 官方文档建议单次订阅≤512，收紧到400为CZCE预留余量
SUB_COOLDOWN_SECONDS=0.5  # unsub完成→下批sub开始的冷却间隔（秒）
MIN_NEAR_OPT_DAYS=1  # 期权到期日距今不足此天数(即已到期)时跳到下月；剩1天的保留但 BS 守卫会跳过

# 合约乘数表（来源：交易所官方合约规格，与旧版 TqSdk 脚本保持一致）
CONTRACT_MULTIPLIERS={
    "DCE" :{"m":10,"i":100,"c":10,"y":10,"p":10,"pp":5,"pg":20,"l":5,"v":5,"eg":10,"cs":10,"a":10,"b":10,"j":100,"jm":60},
    "CZCE":{"SR":10,"CF":5,"MA":10,"TA":5,"SA":20,"FG":20,"RM":10,"OI":10,"SF":5,"SM":5,"PF":5,"PK":5,"AP":10,"CJ":5,"UR":20,"SH":30},
    "SHFE":{"cu":5,"al":5,"zn":5,"pb":5,"ni":1,"sn":1,"au":1000,"ag":15,"rb":10,"hc":10,"ss":5,"ru":10,"bu":10,"sp":10,"fu":10,"ao":20,"br":5},
}
def _get_multiplier(exchange, product):
    ex=str(exchange or "").upper(); pd=str(product or "")
    return float(CONTRACT_MULTIPLIERS.get(ex,{}).get(pd) or CONTRACT_MULTIPLIERS.get(ex,{}).get(pd.upper()) or 1)

def _is_trading_time(now=None):
    now=now or datetime.datetime.now()
    if now.weekday()>=5:
        return False
    hhmm=now.strftime("%H:%M")
    for start,end in TRADING_SESSIONS:
        if start<=end:
            if start<=hhmm<=end:
                return True
        else:
            if hhmm>=start or hhmm<=end:
                return True
    return False

# ========================= TQSDK 参数配置 =========================
FETCH_INTERVAL_SEC            = 30
TARGET_REFRESH_SEC            = 3600
CONN_CHECK_TIMEOUT            = 15
QUOTE_BATCH_SIZE              = int(os.environ.get("OPTION_QUOTE_BATCH", "500"))
GREEKS_BATCH_SIZE             = int(os.environ.get("OPTION_GREEKS_BATCH", "800"))
GREEKS_REFRESH_SEC            = int(os.environ.get("OPTION_GREEKS_REFRESH_SEC", "60"))
RISK_FREE_RATE                = 0.025
QUOTE_RETRY_MIN_BATCH         = 50
QUOTE_CACHE_WAIT_SEC          = 0.2
QUOTE_BACKFILL_INTERVAL_SEC   = float(os.environ.get("OPTION_QUOTE_BACKFILL_INTERVAL_SEC", "10.0"))
QUOTE_BACKFILL_BATCH_SIZE     = max(1, int(os.environ.get("OPTION_QUOTE_BACKFILL_BATCH_SIZE", "50")))
QUOTE_COVERAGE_LOG_INTERVAL_SEC = float(os.environ.get("OPTION_QUOTE_COVERAGE_LOG_INTERVAL_SEC", "300.0"))
EVENT_DEADLINE_SEC            = float(os.environ.get("OPTION_EVENT_DEADLINE_SEC", "1.0"))
DB_FLUSH_INTERVAL_SEC         = float(os.environ.get("OPTION_DB_FLUSH_INTERVAL_SEC", "1.0"))
DB_FLUSH_INTERVAL_OFF_SEC     = float(os.environ.get("OPTION_DB_FLUSH_INTERVAL_OFF_SEC", "0"))
QUOTE_LATEST_FLUSH_INTERVAL_SEC = float(os.environ.get("OPTION_QUOTE_LATEST_FLUSH_INTERVAL_SEC", str(DB_FLUSH_INTERVAL_SEC)))
GREEKS_DB_FLUSH_INTERVAL_SEC  = float(os.environ.get("OPTION_GREEKS_DB_FLUSH_INTERVAL_SEC", "20.0"))
GREEKS_MAX_GROUPS_PER_FLUSH   = int(os.environ.get("OPTION_GREEKS_MAX_GROUPS_PER_FLUSH", "6"))
GUI_REFRESH_INTERVAL_SEC      = float(os.environ.get("OPTION_GUI_REFRESH_INTERVAL_SEC", "1.0"))
PNL_SNAPSHOT_SEC              = float(os.environ.get("OPTION_PNL_SNAPSHOT_SEC", "60"))
RECONNECT_AFTER_IDLE_SEC      = float(os.environ.get("OPTION_RECONNECT_AFTER_IDLE_SEC", "300"))
QUOTE_STALE_RECONNECT_SEC     = float(os.environ.get("OPTION_QUOTE_STALE_RECONNECT_SEC", "90"))
FETCH_SCOPE                   = os.environ.get("OPTION_FETCH_SCOPE", "all").lower()
FULL_REFRESH_EVERY_N_ROUNDS   = int(os.environ.get("OPTION_FULL_REFRESH_EVERY", "20"))
OPTION_ATM_WINDOW             = max(1, int(os.environ.get("OPTION_ATM_WINDOW", "30")))
OPTION_NEAR_CANDIDATE_LIMIT   = max(1, int(os.environ.get("OPTION_NEAR_CANDIDATE_LIMIT", "8")))
OPTION_MIN_EXPIRE_DAYS        = max(1.0, float(os.environ.get("OPTION_MIN_EXPIRE_DAYS", "1")))
OPTION_MONTH_SCOPE            = os.environ.get("OPTION_MONTH_SCOPE", "near").lower()
SIM_OPTION_MULTIPLIER         = float(os.environ.get("OPTION_SIM_MULTIPLIER", "1"))
OPTION_LOG_GREEKS_FIELDS      = os.environ.get("OPTION_LOG_GREEKS_FIELDS", "0").lower() in ("1","true","yes","on")
CONNECT_RETRY_MAX             = int(os.environ.get("OPTION_CONNECT_RETRY_MAX", "2"))
CONNECT_RETRY_SLEEP_SEC       = float(os.environ.get("OPTION_CONNECT_RETRY_SLEEP_SEC", "15"))
NON_TRADING_INTERVAL          = 300

def _env_csv(name, default):
    raw = os.environ.get(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

FOCUS_EXCHANGES = _env_csv("OPTION_FOCUS_EXCHANGES", "DCE")
FOCUS_PRODUCTS = {
    "DCE":  _env_csv("OPTION_FOCUS_DCE", "m"),
    "CZCE": _env_csv("OPTION_FOCUS_CZCE", ""),
    "SHFE": _env_csv("OPTION_FOCUS_SHFE", ""),
}

_DCE_PRODUCTS_DICT  = {k: v for k, v in DCE_PRODUCTS}
_CZCE_PRODUCTS_DICT = {k: v for k, v in CZCE_PRODUCTS}
_SHFE_PRODUCTS_DICT = {k: v for k, v in SHFE_PRODUCTS}

EXCHANGE_PRODUCT_CONFIG = [
    ("DCE",  "大商所", _DCE_PRODUCTS_DICT),
    ("CZCE", "郑商所", _CZCE_PRODUCTS_DICT),
    ("SHFE", "上期所", _SHFE_PRODUCTS_DICT),
]

# 全局运行标志 (数据线程检测)
running = True

# ========================= T型报价表格 =========================
C = type("C", (), {
    "WIN_BG": "#FDFBF7",
    "TOP_BAR_BG": "#E8E0D0",
    "TOOL_BAR_BG": "#F4EAD6",
    "TABLE_BG": "#FFFAF0",
    "TABLE_ALT_ROW": "#F7EFDF",
    "GRID_LINE": "#E1D4BF",
    "HDR_BG": "#EADFC9",
    "BORDER_COLOR": "#BCAAA4",
    "TEXT_DARK": "#3E2723",
    "TEXT_MID": "#6D4C41",
    "TEXT_LIGHT": "#8D6E63",
    "BRAND": "#5D4037",
    "TRADING_ON": "#2E7D32",
    "TRADING_OFF": "#E65100",
    "PRICE_UP": "#AD1457",
    "PRICE_DOWN": "#33691E",
    "CALL_OTM_BG": "#F9E0E0",
    "PUT_OTM_BG": "#E3F4E1",
    "ITM_BG": "#F5EAD3",
    "CALL_OTM_FG": "#B24C43",
    "PUT_OTM_FG": "#2F6F3A",
    "VOL_1_BG": "#FFF59D",
    "VOL_2_BG": "#FFF176",
    "VOL_3_BG": "#FFEE58",
    "SPECIAL_STRIKE_BG": "#FFE6B3",
    "ATM_BAND_5_BG": "#FFF4D6",
    "STRIKE_TEXT": "#4E342E",
    "ATM_HIGHLIGHT": "#FFF3E0",
    "ATM_ROW_BG": "#FFF0C1",
})

def _to_ratio_float(x):
    """把 ratio 安全转 float：兼容 float / '2.34' 文本 / '1:2' 比例文本（signal_log 重启恢复）。"""
    if isinstance(x, (int, float)):
        try:
            v = float(x)
            return 0.0 if v != v else v
        except Exception:
            return 0.0
    s = str(x or "").strip()
    if not s:
        return 0.0
    try:
        v = float(s)
        return 0.0 if v != v else v
    except ValueError:
        pass
    if ":" in s:
        a, _, b = s.partition(":")
        try:
            bn = float(b)
            return float(a) / bn if bn else 0.0
        except ValueError:
            return 0.0
    return 0.0


class SignalRecord:
    """比例价差信号记录（VIX前5 × OTM量前3 × 价比1:2~1:3 × 间距条件）"""
    __slots__=("ts","product","product_name","vix_rank","vix","opt_type",
               "near_iid","far_iid","near_strike","far_strike",
               "near_price","far_price","ratio","strike_gap","gap_threshold",
               "near_vol","far_vol","near_vol_rank","far_vol_rank",
               "multiplier","underlying_price","key")
    def __init__(self,ts,product,product_name,vix_rank,vix,opt_type,
                 near_iid,far_iid,near_strike,far_strike,
                 near_price,far_price,ratio,strike_gap,gap_threshold,
                 near_vol,far_vol,near_vol_rank,far_vol_rank,multiplier,underlying_price=0.0):
        self.ts=ts;self.product=product;self.product_name=product_name
        self.vix_rank=vix_rank;self.vix=vix;self.opt_type=opt_type
        self.near_iid=near_iid;self.far_iid=far_iid
        self.near_strike=near_strike;self.far_strike=far_strike
        self.near_price=near_price;self.far_price=far_price
        self.ratio=_to_ratio_float(ratio);self.strike_gap=float(strike_gap or 0);self.gap_threshold=float(gap_threshold or 0)
        self.near_vol=near_vol;self.far_vol=far_vol
        self.near_vol_rank=near_vol_rank;self.far_vol_rank=far_vol_rank
        self.multiplier=multiplier;self.underlying_price=float(underlying_price or 0)
        self.key=f"{product}_{opt_type}_{near_iid}_{far_iid}"

class _BandDelegate(QtWidgets.QStyledItemDelegate):
    """行权价±5/7/9%标注：行权价右上角超小字 + 对应虚值侧整行外框（一个矩形，不每列单独画）"""
    def __init__(self, strike_ci, call_cis, put_cis, parent=None):
        super().__init__(parent)
        self._sk=strike_ci
        # 记录每侧最左/最右列索引，用于只在边界列画外框
        self._cc=call_cis; self._pc=put_cis
        self._cc_first=min(call_cis) if call_cis else -1
        self._cc_last =max(call_cis) if call_cis else -1
        self._pc_first=min(put_cis)  if put_cis  else -1
        self._pc_last =max(put_cis)  if put_cis  else -1
    def paint(self, painter, option, index):
        super().paint(painter, option, index)
        band=index.sibling(index.row(), self._sk).data(QtCore.Qt.UserRole)
        if not band: return
        painter.save(); r=option.rect; ci=index.column()
        if ci==self._sk:
            painter.setFont(QtGui.QFont("SimSun",7,QtGui.QFont.Bold))
            painter.setPen(QtGui.QColor("#BF360C"))
            fm=painter.fontMetrics(); tw=fm.horizontalAdvance(band)
            painter.drawText(r.right()-tw-3, r.top()+fm.ascent()+2, band)
        else:
            is_call_side=("+" in band and ci in self._cc)
            is_put_side =("-" in band and ci in self._pc)
            if is_call_side or is_put_side:
                pen=QtGui.QPen(QtGui.QColor("#F9A825"),1.5,QtCore.Qt.DashLine)
                painter.setPen(pen); painter.setBrush(QtCore.Qt.NoBrush)
                adj=r.adjusted(0,1,-1,-1)
                # 上边和下边：每个格子都画
                painter.drawLine(adj.topLeft(), adj.topRight())
                painter.drawLine(adj.bottomLeft(), adj.bottomRight())
                # 左边：仅最左列画
                if (is_call_side and ci==self._cc_first) or (is_put_side and ci==self._pc_first):
                    painter.drawLine(adj.topLeft(), adj.bottomLeft())
                # 右边：仅最右列画
                if (is_call_side and ci==self._cc_last) or (is_put_side and ci==self._pc_last):
                    painter.drawLine(adj.topRight(), adj.bottomRight())
        painter.restore()

class TOptionTable(QtWidgets.QTableWidget):
    leg_clicked=QtCore.pyqtSignal(str,str,float,str,float)  # iid, opt_type(CALL/PUT), strike, side(BUY/SELL), ref_price
    leg_removed=QtCore.pyqtSignal(str)                      # iid — 取消勾选时通知篮子删除
    def __init__(self, parent=None):
        # 列已精简：删除"隐含波(官方)"两列（无官方IV数据源，query_option_greeks 不返回 iv/sigma）
        self.call_cols = ["iv_chg_c","iv_c_bs","delta_c","oi_c","volume_c","ask_c","bid_c","chg_c","last_c"]
        self.put_cols = ["last_p","chg_p","bid_p","ask_p","volume_p","oi_p","delta_p","iv_p_bs","iv_chg_p"]
        self.all_cols = ["buy_c","sell_c"] + self.call_cols + ["strike"] + self.put_cols + ["buy_p","sell_p"]
        super().__init__(0, len(self.all_cols), parent)
        self.col_headers = {
            "buy_c":"买C", "sell_c":"卖C", "buy_p":"买P", "sell_p":"卖P",
            "iv_chg_c":"隐波涨跌", "iv_c_bs":"隐含波动率", "delta_c":"Delta", "oi_c":"持仓量", "volume_c":"成交量",
            "ask_c":"卖价", "bid_c":"买价", "chg_c":"涨跌", "last_c":"最新价",
            "strike":"行权价",
            "last_p":"最新价", "chg_p":"涨跌", "bid_p":"买价", "ask_p":"卖价", "volume_p":"成交量",
            "oi_p":"持仓量", "delta_p":"Delta", "iv_p_bs":"隐含波动率", "iv_chg_p":"隐波涨跌",
        }
        # 勾选列固定窄；其余数据列自适应（Stretch）填满表宽，消除留白与截断
        self._checkbox_cols = ("buy_c","sell_c","buy_p","sell_p")
        self.col_width = {k:30 for k in self._checkbox_cols}

        self.setHorizontalHeaderLabels([self.col_headers.get(k, k) for k in self.all_cols])
        self.verticalHeader().setVisible(False)
        self.verticalHeader().setDefaultSectionSize(26)   # 行高加大，字体更易读
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        self.setFocusPolicy(QtCore.Qt.NoFocus)
        self.setAlternatingRowColors(True)
        hh = self.horizontalHeader()
        hh.setFixedHeight(28)
        hh.setMinimumSectionSize(34)
        for ci, ck in enumerate(self.all_cols):
            if ck in self._checkbox_cols:
                hh.setSectionResizeMode(ci, QtWidgets.QHeaderView.Fixed)
                self.setColumnWidth(ci, self.col_width.get(ck, 30))
            else:
                # 数据列自适应：随窗口宽度均分，自动消除右侧留白、避免文字折叠/截断
                hh.setSectionResizeMode(ci, QtWidgets.QHeaderView.Stretch)
            it=self.horizontalHeaderItem(ci)
            if ck == "strike":
                it.setForeground(QtGui.QColor(C.STRIKE_TEXT))
            elif ck in self.call_cols or ck == "last_c":
                it.setForeground(QtGui.QColor(C.PRICE_DOWN))
            elif ck in self.put_cols or ck == "last_p":
                it.setForeground(QtGui.QColor(C.PRICE_UP))
            else:
                it.setForeground(QtGui.QColor(C.TEXT_LIGHT))
        self.setStyleSheet(
            f"QTableWidget{{background:{C.TABLE_BG};color:{C.TEXT_DARK};gridline-color:{C.GRID_LINE};font:13px 'SimSun';alternate-background-color:{C.TABLE_ALT_ROW};}}"
            f"QTableWidget::item{{padding:1px 2px;}}"
            f"QHeaderView::section{{background:{C.TOP_BAR_BG};color:{C.TEXT_MID};border:none;border-right:1px solid {C.GRID_LINE};border-bottom:1px solid {C.BORDER_COLOR};font:bold 12px 'SimSun';padding:1px 2px;}}"
        )
        self._sr = OrderedDict()
        self._last_center_strike = None
        self._current_fe = OrderedDict()
        self._current_tc = {}
        self._cb_refs = {}  # {(row, col_key): wrapper_widget} —— 按行复用，切换品种不销毁
        self._last_basket = {}  # 最近一次篮子状态，用于复用 checkbox 时恢复勾选
        self.cellClicked.connect(self._on_table_cell_clicked)
        _sk=self.all_cols.index("strike")
        _cc=set(i for i,k in enumerate(self.all_cols) if k in set(self.call_cols))
        _pc=set(i for i,k in enumerate(self.all_cols) if k in set(self.put_cols))
        self.setItemDelegate(_BandDelegate(_sk, _cc, _pc, self))

    _color_cache = {}      # hex -> QColor，避免每格重复构造
    _brush_cache = {}      # hex -> QBrush
    _EMPTY_BRUSH = None

    @classmethod
    def _brush(cls, hex_color):
        if hex_color is None:
            if cls._EMPTY_BRUSH is None:
                cls._EMPTY_BRUSH = QtGui.QBrush()
            return cls._EMPTY_BRUSH
        b = cls._brush_cache.get(hex_color)
        if b is None:
            b = QtGui.QBrush(QtGui.QColor(hex_color)); cls._brush_cache[hex_color] = b
        return b

    def _c(self,r,c,t,ar=True,fg=None,bg=None,bold=False,udata=None):
        # 复用 item + 缓存画刷 + 仅在变化时调用 Qt，最小化每帧开销（性能关键）
        it=self.item(r,c)
        if it is None:
            it=QtWidgets.QTableWidgetItem()
            it.setTextAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignVCenter if ar else QtCore.Qt.AlignLeft|QtCore.Qt.AlignVCenter)
            self.setItem(r,c,it)
            it._st=(None,None,None,None,None)  # 上次应用的 (text,fg,bg,bold,udata)
        last=getattr(it,"_st",(None,None,None,None,None))
        s=str(t)
        if last[0]!=s: it.setText(s)
        if last[1]!=fg: it.setForeground(self._brush(fg))
        if last[2]!=bg: it.setBackground(self._brush(bg))
        if last[3]!=bold:
            f=it.font(); f.setBold(bold); it.setFont(f)
        if last[4]!=udata: it.setData(QtCore.Qt.UserRole, udata)
        it._st=(s,fg,bg,bold,udata)

    def _fmt_px(self, v):
        if v is None or v == 0: return "—"
        return f"{v:.2f}"

    def _fmt_int(self, v):
        if v is None or v == 0: return "—"
        return f"{int(v)}"

    def _center_atm(self, row, strike):
        if row < 0:
            return
        if self._last_center_strike == strike:
            return
        self._last_center_strike = strike
        item = self.item(row, 0) or self.item(row, self.columnCount() // 2)
        if item:
            self.scrollToItem(item, QtWidgets.QAbstractItemView.PositionAtCenter)

    def update_all(self, strike_dict, tc, up, iv_map=None, iv_open_map=None, iv_sources=None):
        self._current_fe=strike_dict; self._current_tc=tc
        strikes=sorted(strike_dict.keys(),reverse=True)
        self.setUpdatesEnabled(False)
        try:
            self._update_all_impl(strike_dict, tc, up, strikes, iv_map, iv_open_map, iv_sources)
        finally:
            self.setUpdatesEnabled(True)

    def _update_all_impl(self, strike_dict, tc, up, strikes, iv_map=None, iv_open_map=None, iv_sources=None):
        if list(self._sr.keys())!=strikes:
            new_n=len(strikes); old_n=self.rowCount()
            if new_n<old_n:
                # 删除多余行（Qt 会销毁这些行的 cellWidget），同步清理 _cb_refs
                for r in range(old_n-1, new_n-1, -1):
                    self.removeRow(r)
                    for ck in ("buy_c","sell_c","buy_p","sell_p"):
                        self._cb_refs.pop((r,ck), None)
            elif new_n>old_n:
                for _ in range(new_n-old_n):
                    self.insertRow(self.rowCount())
            self._sr.clear()
            for i,s in enumerate(strikes): self._sr[s]=i

        iv_map = iv_map or {}
        iv_open_map = iv_open_map or {}
        iv_sources = iv_sources or {}
        def _fmt_iv(v):
            return f"{v:.4f}" if (v is not None and v == v) else "—"
        col_idx={k:i for i,k in enumerate(self.all_cols)}
        atm_strike=None
        if up>0 and strikes:
            atm_strike=min(strikes, key=lambda s: abs(s-up))

        band_targets=[]
        if up>0:
            for p in (0.05, 0.07, 0.09):
                band_targets.extend([(f"+{int(p*100)}%", up*(1.0+p)), (f"-{int(p*100)}%", up*(1.0-p))])
        band_marks={}
        for tag, tgt in band_targets:
            if not strikes:
                continue
            s=min(strikes, key=lambda x: abs(x-tgt))
            if s in band_marks:
                if tag not in band_marks[s]:
                    band_marks[s]+=f"/{tag}"
            else:
                band_marks[s]=tag

        # CALL虚值 和 PUT虚值 分别独立排名，各取成交量>5000的前3
        # 理想情况下共有6个合约高亮：CALL虚值前3 + PUT虚值前3
        call_otm_vols=[]
        put_otm_vols=[]
        for strike,codes in strike_dict.items():
            ct=tc.get(codes.get("CALL",""));pt=tc.get(codes.get("PUT",""))
            if up>0 and strike>up and ct:
                call_otm_vols.append((getattr(ct, "volume", 0) or 0, codes.get("CALL","")))
            if up>0 and strike<up and pt:
                put_otm_vols.append((getattr(pt, "volume", 0) or 0, codes.get("PUT","")))
        call_top3_rank={cid:(idx+1) for idx,(v,cid) in
                        enumerate(sorted(call_otm_vols, key=lambda x: x[0], reverse=True)[:3])
                        if cid and v>5000}
        put_top3_rank ={cid:(idx+1) for idx,(v,cid) in
                        enumerate(sorted(put_otm_vols,  key=lambda x: x[0], reverse=True)[:3])
                        if cid and v>5000}
        otm_top3_rank={**call_top3_rank, **put_top3_rank}

        for strike,codes in strike_dict.items():
            row=self._sr.get(strike, None)
            if row is None: continue
            call_sym = codes.get("CALL","")
            put_sym  = codes.get("PUT","")
            ct=tc.get(call_sym);pt=tc.get(put_sym)
            atm=(atm_strike is not None and strike==atm_strike)
            band_mark=band_marks.get(strike, "")

            # strike > up: Call虚值(OTM)红背景 + Put实值(ITM)护眼绿
            # strike < up: Call实值(ITM)护眼绿 + Put虚值(OTM)绿背景
            call_otm=(up>0 and strike>up)
            put_otm=(up>0 and strike<up)
            if atm:
                bgc=bgp=C.ATM_ROW_BG
            else:
                bgc=C.CALL_OTM_BG if call_otm else C.ITM_BG
                bgp=C.PUT_OTM_BG if put_otm else C.ITM_BG
            call_price_color=C.CALL_OTM_FG if call_otm else C.PRICE_DOWN
            put_price_color=C.PUT_OTM_FG if put_otm else C.PRICE_UP
            if atm:
                call_price_color=put_price_color=C.BRAND

            for _ck,_ot,_sd in [("buy_c","CALL","BUY"),("sell_c","CALL","SELL")]:
                self._ensure_leg_cb(row,_ck,col_idx[_ck],codes.get(_ot,""),_ot,strike,_sd,bgc)

            if ct:
                pc=max(0.1,ct.pre_close or ct.last_price or 0.1);chg=(ct.last_price or 0)-pc
                c_iv=iv_map.get(call_sym)
                c_iv_open=iv_open_map.get(call_sym)
                c_iv_chg=(c_iv-c_iv_open) if (c_iv is not None and c_iv_open is not None) else None
                c_iv_src = iv_sources.get(call_sym, {})
                c_iv_bs  = c_iv_src.get("bs")
                self._c(row,col_idx["iv_chg_c"],(f"{c_iv_chg:+.4f}" if c_iv_chg is not None else "—"),bg=bgc)
                self._c(row,col_idx["iv_c_bs"],_fmt_iv(c_iv_bs),bg=bgc)
                self._c(row,col_idx["delta_c"],"—",bg=bgc)
                self._c(row,col_idx["oi_c"],self._fmt_int(ct.open_interest),bg=bgc)
                rank=otm_top3_rank.get(call_sym, 0)
                vol_bg=bgc if atm else {1:C.VOL_1_BG,2:C.VOL_2_BG,3:C.VOL_3_BG}.get(rank,bgc)
                self._c(row,col_idx["volume_c"],self._fmt_int(ct.volume),bg=vol_bg,bold=(vol_bg!=bgc))
                self._c(row,col_idx["ask_c"],self._fmt_px(ct.ask_price1),bg=bgc)
                self._c(row,col_idx["bid_c"],self._fmt_px(ct.bid_price1),bg=bgc)
                self._c(row,col_idx["chg_c"],f"{chg:+.2f}",fg=C.PRICE_UP if chg>=0 else C.PRICE_DOWN,bg=bgc)
                self._c(row,col_idx["last_c"],self._fmt_px(ct.last_price),fg=call_price_color,bg=bgc,bold=atm)
            else:
                for k in self.call_cols: self._c(row,col_idx[k],"—",bg=bgc)

            self._c(
                row,col_idx["strike"],f"{strike:.0f}",False,fg=C.STRIKE_TEXT,
                bg=(C.ATM_BAND_5_BG if band_mark else (C.ATM_ROW_BG if atm else "#FFF4E6")),
                bold=(atm or bool(band_mark)),
                udata=(band_mark if band_mark else None)
            )

            if pt:
                pc=max(0.1,pt.pre_close or pt.last_price or 0.1);chg=(pt.last_price or 0)-pc
                p_iv=iv_map.get(put_sym)
                p_iv_open=iv_open_map.get(put_sym)
                p_iv_chg=(p_iv-p_iv_open) if (p_iv is not None and p_iv_open is not None) else None
                p_iv_src = iv_sources.get(put_sym, {})
                p_iv_bs  = p_iv_src.get("bs")
                self._c(row,col_idx["last_p"],self._fmt_px(pt.last_price),fg=put_price_color,bg=bgp,bold=atm)
                self._c(row,col_idx["chg_p"],f"{chg:+.2f}",fg=C.PRICE_UP if chg>=0 else C.PRICE_DOWN,bg=bgp)
                self._c(row,col_idx["bid_p"],self._fmt_px(pt.bid_price1),bg=bgp)
                self._c(row,col_idx["ask_p"],self._fmt_px(pt.ask_price1),bg=bgp)
                rank=otm_top3_rank.get(put_sym, 0)
                vol_bg=bgp if atm else {1:C.VOL_1_BG,2:C.VOL_2_BG,3:C.VOL_3_BG}.get(rank,bgp)
                self._c(row,col_idx["volume_p"],self._fmt_int(pt.volume),bg=vol_bg,bold=(vol_bg!=bgp))
                self._c(row,col_idx["oi_p"],self._fmt_int(pt.open_interest),bg=bgp)
                self._c(row,col_idx["delta_p"],"—",bg=bgp)
                self._c(row,col_idx["iv_p_bs"],_fmt_iv(p_iv_bs),bg=bgp)
                self._c(row,col_idx["iv_chg_p"],(f"{p_iv_chg:+.4f}" if p_iv_chg is not None else "—"),bg=bgp)
            else:
                for k in self.put_cols: self._c(row,col_idx[k],"—",bg=bgp)

            for _ck,_ot,_sd in [("buy_p","PUT","BUY"),("sell_p","PUT","SELL")]:
                self._ensure_leg_cb(row,_ck,col_idx[_ck],codes.get(_ot,""),_ot,strike,_sd,bgp)

        if atm_strike in self._sr:
            self._center_atm(self._sr[atm_strike], atm_strike)

    def update_all_from_snapshot(self, rows, up):
        """消费预格式化 rows（与 old build_snapshot_from_db 输出格式一致）。
        rows: List[{"K":float, "c":{...}, "p":{...}, "atm":bool}]  高→低排列
        up:   float  标的价格
        与 old _render_table 完全同口径：直接读 cell dict 的 last/bid/ask/iv/volume/oi/delta/chg。
        """
        if not rows:
            return
        strikes = [r["K"] for r in rows]
        self.setUpdatesEnabled(False)
        try:
            # 行数对齐
            new_n = len(rows)
            old_n = self.rowCount()
            if new_n < old_n:
                for r in range(old_n - 1, new_n - 1, -1):
                    self.removeRow(r)
                    for ck in ("buy_c","sell_c","buy_p","sell_p"):
                        self._cb_refs.pop((r, ck), None)
            elif new_n > old_n:
                for _ in range(new_n - old_n):
                    self.insertRow(self.rowCount())
            if list(self._sr.keys()) != strikes:
                self._sr.clear()
                for i, s in enumerate(strikes):
                    self._sr[s] = i

            col_idx = {k: i for i, k in enumerate(self.all_cols)}
            atm_strike = None

            # 成交量前3名排序（虚值侧独立）
            call_otm_vols = []
            put_otm_vols  = []
            for rw in rows:
                K = rw["K"]
                cd = rw.get("c") or {}
                pd_ = rw.get("p") or {}
                if up > 0 and K > up:
                    vol = cd.get("volume") or 0
                    sym = cd.get("option_sym","")
                    if sym and vol > 5000:
                        call_otm_vols.append((vol, sym))
                if up > 0 and K < up:
                    vol = pd_.get("volume") or 0
                    sym = pd_.get("option_sym","")
                    if sym and vol > 5000:
                        put_otm_vols.append((vol, sym))
            call_top3 = {cid: (i+1) for i,(v,cid) in enumerate(sorted(call_otm_vols, key=lambda x: x[0], reverse=True)[:3])}
            put_top3  = {cid: (i+1) for i,(v,cid) in enumerate(sorted(put_otm_vols,  key=lambda x: x[0], reverse=True)[:3])}
            otm_top3_rank = {**call_top3, **put_top3}

            # ±5/7/9% 波段标记
            band_marks = {}
            if up > 0:
                for p in (0.05, 0.07, 0.09):
                    for tag, tgt in [(f"+{int(p*100)}%", up*(1+p)), (f"-{int(p*100)}%", up*(1-p))]:
                        if strikes:
                            s = min(strikes, key=lambda x: abs(x-tgt))
                            band_marks[s] = band_marks.get(s, tag) if band_marks.get(s, tag) == tag else band_marks.get(s, tag) + "/" + tag

            for rw in rows:
                K   = rw["K"]
                row = self._sr.get(K)
                if row is None:
                    continue
                cd  = rw.get("c") or {}
                pd_ = rw.get("p") or {}
                atm = bool(rw.get("atm"))
                if atm:
                    atm_strike = K
                band_mark = band_marks.get(K, "")

                call_otm = (up > 0 and K > up)
                put_otm  = (up > 0 and K < up)
                if atm:
                    bgc = bgp = C.ATM_ROW_BG
                else:
                    bgc = C.CALL_OTM_BG if call_otm else C.ITM_BG
                    bgp = C.PUT_OTM_BG  if put_otm  else C.ITM_BG
                call_price_color = C.CALL_OTM_FG if call_otm else C.PRICE_DOWN
                put_price_color  = C.PUT_OTM_FG  if put_otm  else C.PRICE_UP
                if atm:
                    call_price_color = put_price_color = C.BRAND

                # buy/sell checkbox
                c_sym = cd.get("option_sym","")
                p_sym = pd_.get("option_sym","")
                for _ck, _ot, _sd in [("buy_c","CALL","BUY"),("sell_c","CALL","SELL")]:
                    self._ensure_leg_cb(row, _ck, col_idx[_ck], c_sym, _ot, K, _sd, bgc)

                def _v(d, key, fmt=None):
                    val = d.get(key, "-")
                    if val is None or val == "-":
                        return "—"
                    if fmt:
                        try:
                            return fmt(val)
                        except Exception:
                            return "—"
                    return str(val)

                # CALL 侧
                if cd:
                    c_iv    = cd.get("iv","-")
                    c_iv_s  = f"{c_iv:.2f}%" if isinstance(c_iv,(int,float)) else str(c_iv)
                    c_iv_chg = cd.get("iv_chg","-")
                    c_chg   = cd.get("chg")
                    rank_c  = otm_top3_rank.get(c_sym, 0)
                    vol_bg_c = bgc if atm else {1:C.VOL_1_BG,2:C.VOL_2_BG,3:C.VOL_3_BG}.get(rank_c, bgc)
                    self._c(row, col_idx["iv_chg_c"], (f"{c_iv_chg:+.4f}" if isinstance(c_iv_chg,(int,float)) else "—"), bg=bgc)
                    self._c(row, col_idx["iv_c_bs"],  c_iv_s, bg=bgc)
                    self._c(row, col_idx["delta_c"],  _v(cd,"delta",lambda v: f"{v:.4f}"), bg=bgc)
                    self._c(row, col_idx["oi_c"],     _v(cd,"oi",   lambda v: str(int(v))), bg=bgc)
                    self._c(row, col_idx["volume_c"], _v(cd,"volume",lambda v: str(int(v))), bg=vol_bg_c, bold=(vol_bg_c!=bgc))
                    self._c(row, col_idx["ask_c"],    _v(cd,"ask",  lambda v: f"{v:.2f}"), bg=bgc)
                    self._c(row, col_idx["bid_c"],    _v(cd,"bid",  lambda v: f"{v:.2f}"), bg=bgc)
                    self._c(row, col_idx["chg_c"],    (f"{c_chg:+.2f}" if isinstance(c_chg,(int,float)) else "—"),
                            fg=C.PRICE_UP if isinstance(c_chg,(int,float)) and c_chg>=0 else C.PRICE_DOWN, bg=bgc)
                    last_c = cd.get("last","-")
                    self._c(row, col_idx["last_c"],   _v(cd,"last", lambda v: f"{v:.2f}"), fg=call_price_color, bg=bgc, bold=atm)
                else:
                    for k in self.call_cols:
                        self._c(row, col_idx[k], "—", bg=bgc)

                # 行权价
                self._c(row, col_idx["strike"], f"{K:.0f}", False, fg=C.STRIKE_TEXT,
                        bg=(C.ATM_BAND_5_BG if band_mark else (C.ATM_ROW_BG if atm else "#FFF4E6")),
                        bold=(atm or bool(band_mark)), udata=(band_mark if band_mark else None))

                # PUT 侧
                if pd_:
                    p_iv    = pd_.get("iv","-")
                    p_iv_s  = f"{p_iv:.2f}%" if isinstance(p_iv,(int,float)) else str(p_iv)
                    p_iv_chg = pd_.get("iv_chg","-")
                    p_chg   = pd_.get("chg")
                    rank_p  = otm_top3_rank.get(p_sym, 0)
                    vol_bg_p = bgp if atm else {1:C.VOL_1_BG,2:C.VOL_2_BG,3:C.VOL_3_BG}.get(rank_p, bgp)
                    last_p = pd_.get("last","-")
                    self._c(row, col_idx["last_p"],   _v(pd_,"last", lambda v: f"{v:.2f}"), fg=put_price_color, bg=bgp, bold=atm)
                    p_chg_v = pd_.get("chg")
                    self._c(row, col_idx["chg_p"],    (f"{p_chg_v:+.2f}" if isinstance(p_chg_v,(int,float)) else "—"),
                            fg=C.PRICE_UP if isinstance(p_chg_v,(int,float)) and p_chg_v>=0 else C.PRICE_DOWN, bg=bgp)
                    self._c(row, col_idx["bid_p"],    _v(pd_,"bid",  lambda v: f"{v:.2f}"), bg=bgp)
                    self._c(row, col_idx["ask_p"],    _v(pd_,"ask",  lambda v: f"{v:.2f}"), bg=bgp)
                    self._c(row, col_idx["volume_p"], _v(pd_,"volume",lambda v: str(int(v))), bg=vol_bg_p, bold=(vol_bg_p!=bgp))
                    self._c(row, col_idx["oi_p"],     _v(pd_,"oi",   lambda v: str(int(v))), bg=bgp)
                    self._c(row, col_idx["delta_p"],  _v(pd_,"delta",lambda v: f"{v:.4f}"), bg=bgp)
                    self._c(row, col_idx["iv_p_bs"],  p_iv_s, bg=bgp)
                    self._c(row, col_idx["iv_chg_p"], (f"{p_iv_chg:+.4f}" if isinstance(p_iv_chg,(int,float)) else "—"), bg=bgp)
                else:
                    for k in self.put_cols:
                        self._c(row, col_idx[k], "—", bg=bgp)

                for _ck, _ot, _sd in [("buy_p","PUT","BUY"),("sell_p","PUT","SELL")]:
                    self._ensure_leg_cb(row, _ck, col_idx[_ck], p_sym, _ot, K, _sd, bgp)

            if atm_strike in self._sr:
                self._center_atm(self._sr[atm_strike], atm_strike)
        finally:
            self.setUpdatesEnabled(True)

    def _on_table_cell_clicked(self, row, col):
        pass  # buy/sell 列已改为 QCheckBox，由复选框自身处理，此处无需额外处理

    def _ensure_leg_cb(self, row, col_key, col, iid, opt_type, strike, side, bg):
        """确保 (row,col_key) 存在 checkbox 控件：不存在则创建一次，存在则只重新绑定数据。
        切换品种时不再销毁/重建 244 个控件，是消除白屏与切换卡顿的关键。"""
        w=self._cb_refs.get((row,col_key))
        if w is None:
            w=self._make_leg_cb_widget(iid,opt_type,strike,side,bg)
            self._cb_refs[(row,col_key)]=w
            self.setCellWidget(row,col,w)
            cb=w._cb
        else:
            cb=w._cb
            if cb._iid!=iid or cb._strike!=float(strike) or cb._side!=side:
                cb._iid=iid; cb._opt_type=opt_type; cb._strike=float(strike); cb._side=side
            if getattr(w,"_bg",None)!=bg:
                w._bg=bg; w.setStyleSheet(f"background:{bg};")
        # 依据最近篮子状态恢复勾选（复用控件时绑定的合约可能已变）
        want=(iid in self._last_basket and self._last_basket[iid].get("side")==side)
        if cb.isChecked()!=want:
            cb.blockSignals(True); cb.setChecked(want); cb.blockSignals(False)

    def _make_leg_cb_widget(self, iid, opt_type, strike, side, bg):
        """创建居中 QCheckBox 容器，用于 buy/sell 列"""
        wrapper=QtWidgets.QWidget()
        wrapper.setStyleSheet(f"background:{bg};")
        wrapper._bg=bg
        lay=QtWidgets.QHBoxLayout(wrapper); lay.setContentsMargins(0,0,0,0); lay.setAlignment(QtCore.Qt.AlignCenter)
        cb=QtWidgets.QCheckBox()
        cb.setStyleSheet(
            "QCheckBox{background:transparent;}"
            "QCheckBox::indicator{width:14px;height:14px;border:2px solid #9E9E9E;border-radius:3px;background:#FAFAFA;}"
            "QCheckBox::indicator:checked{background:#4CAF50;border-color:#388E3C;image:none;}"
            "QCheckBox::indicator:hover{border-color:#1565C0;}"
        )
        cb._iid=iid; cb._side=side; cb._opt_type=opt_type; cb._strike=float(strike)
        # 读取控件当前绑定属性（复用时已重绑），不要捕获创建时的值
        cb.stateChanged.connect(lambda state, c=cb:
            self._on_cb_state(state, c._iid, c._opt_type, c._strike, c._side))
        lay.addWidget(cb)
        wrapper._cb=cb
        return wrapper

    def _on_cb_state(self, state, iid, opt_type, strike, side):
        """QCheckBox 状态变化回调"""
        if state==QtCore.Qt.Checked:
            t=self._current_tc.get(iid)
            ref=float(getattr(t,"last_price",0) or 0)
            self.leg_clicked.emit(iid, opt_type, strike, side, ref)
        else:
            self.leg_removed.emit(iid)

    def sync_basket_checkboxes(self, basket_dict):
        """从 Hub 同步篮子状态到 T型表各 checkbox（basket_dict: {iid: leg}）"""
        self._last_basket = dict(basket_dict or {})
        for _key, wrapper in self._cb_refs.items():
            cb=wrapper._cb
            in_basket=(cb._iid in self._last_basket and self._last_basket[cb._iid].get("side")==cb._side)
            if cb.isChecked()!=in_basket:
                cb.blockSignals(True)
                cb.setChecked(in_basket)
                cb.blockSignals(False)

class MetricChip(QtWidgets.QFrame):
    def __init__(self, caption):
        super().__init__()
        self.setObjectName("metricChip")
        self.setStyleSheet(
            f"QFrame#metricChip{{background:transparent;border-right:1px solid {C.BORDER_COLOR};padding:0 6px;}}"
            f"QLabel{{background:transparent;color:{C.TEXT_LIGHT};font:11px 'SimSun';}}"
        )
        lay=QtWidgets.QVBoxLayout(self);lay.setContentsMargins(0,0,0,0);lay.setSpacing(0)
        self.value_lbl=QtWidgets.QLabel("-")
        self.value_lbl.setStyleSheet(f"color:{C.BRAND};font:bold 16px 'Consolas';")
        self.caption_lbl=QtWidgets.QLabel(caption)
        lay.addWidget(self.value_lbl)
        lay.addWidget(self.caption_lbl)
        self._base_style=f"color:{C.BRAND};font:bold 16px 'Consolas';"

    def set_value(self, value_text="-", caption=None, color=None):
        self.value_lbl.setText(value_text if value_text not in (None, "") else "-")
        if caption:
            self.caption_lbl.setText(caption)
        style=self._base_style
        if color:
            style=f"color:{color};font:bold 16px 'Consolas';"
        self.value_lbl.setStyleSheet(style)

class _CardClickFilter(QtCore.QObject):
    """卡片点击过滤器：在 Qt event() 层拦截，覆盖卡片及所有子控件的鼠标按下事件"""
    def __init__(self, code, signal, parent=None):
        super().__init__(parent)
        self._code=code
        self._signal=signal
    def eventFilter(self, obj, event):
        if event.type()==QtCore.QEvent.MouseButtonPress:
            self._signal.emit(self._code)
        return False

# ========================= 左侧品种栏（变体8：指标大卡 + VIX 胶囊标签）=========================
class ProductSidebar(QtWidgets.QWidget):
    product_clicked = QtCore.pyqtSignal(str)  # product_code
    refresh_clicked = QtCore.pyqtSignal()
    manual_subscribe_requested = QtCore.pyqtSignal(str)  # 手动订阅品种 code

    # 交易所代码 → 中文简称
    _EXCHANGE_CN = {"SHFE":"上期所","DCE":"大商所","CZCE":"郑商所","CFFEX":"中金所","INE":"上期能源","GFEX":"广期所"}

    def __init__(self):
        super().__init__()
        self.setFixedWidth(380)
        self.setStyleSheet(
            f"QWidget{{background:{C.HDR_BG};}}"
            f"QScrollArea{{background:{C.HDR_BG};border:none;}}"
            f"QScrollBar:vertical{{background:{C.HDR_BG};width:6px;border-radius:3px;}}"
            f"QScrollBar::handle:vertical{{background:{C.BORDER_COLOR};border-radius:3px;}}"
            f"QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical{{height:0px;}}"
        )
        # 每个品种保存的 widget 引用
        self._cards={}          # code -> QFrame
        self._lbl_name={}       # code -> QLabel  品种名+代码
        self._lbl_vix={}        # code -> QLabel  VIX 胶囊
        self._lbl_ql={}         # code -> QLabel  钱龙红绿柱（偏多/偏空）
        self._lbl_days={}       # code -> QLabel  剩余天数
        self._mval_price={}     # code -> QLabel  标的价数值
        self._mval_vol={}       # code -> QLabel  成交量数值
        self._mval_cp={}        # code -> QLabel  C/P档位数值
        self._lbl_footer={}     # code -> QLabel  交易所·合约
        self._card_list=[]      # 顺序列表，用于过滤

        root=QtWidgets.QVBoxLayout(self);root.setContentsMargins(0,0,0,0);root.setSpacing(0)

        # ---- 顶栏：标题 + 搜索框 + 刷新按钮 ----
        toolbar=QtWidgets.QWidget()
        toolbar.setStyleSheet(f"background:{C.TOP_BAR_BG};border-bottom:1px solid {C.GRID_LINE};")
        tl=QtWidgets.QHBoxLayout(toolbar);tl.setContentsMargins(8,6,8,6);tl.setSpacing(6)
        title=QtWidgets.QLabel("品种切换")
        title.setStyleSheet(f"color:{C.BRAND};font:bold 13px 'SimSun';")
        self.search=QtWidgets.QLineEdit()
        self.search.setPlaceholderText("搜索品种 / 代码")
        self.search.setFixedHeight(24)
        self.search.setStyleSheet(
            f"background:{C.TABLE_BG};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};"
            f"border-radius:7px;padding:2px 8px;font:12px 'SimSun';"
        )
        self.search.textChanged.connect(self._apply_filter)
        self.btn_refresh=QtWidgets.QPushButton("刷新")
        self.btn_refresh.setFixedHeight(24)
        self.btn_refresh.setCursor(QtCore.Qt.PointingHandCursor)
        self.btn_refresh.setStyleSheet(
            f"QPushButton{{background:{C.TABLE_ALT_ROW};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};border-radius:7px;font:12px 'SimSun';padding:2px 8px;}}"
            f"QPushButton:hover{{background:{C.ATM_HIGHLIGHT};}}"
        )
        self.btn_refresh.clicked.connect(self.refresh_clicked.emit)
        tl.addWidget(title);tl.addWidget(self.search,1);tl.addWidget(self.btn_refresh)
        root.addWidget(toolbar)

        # ---- 手动订阅栏：输入品种代码(如 ag) → 强制订阅期权链 ----
        sub_bar=QtWidgets.QWidget()
        sub_bar.setStyleSheet(f"background:{C.TOP_BAR_BG};border-bottom:1px solid {C.GRID_LINE};")
        sbl=QtWidgets.QHBoxLayout(sub_bar);sbl.setContentsMargins(8,4,8,6);sbl.setSpacing(6)
        sub_lbl=QtWidgets.QLabel("手动订阅")
        sub_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:12px 'SimSun';")
        self.sub_input=QtWidgets.QLineEdit()
        self.sub_input.setPlaceholderText("输入品种代码，如 ag / m / MA")
        self.sub_input.setFixedHeight(24)
        self.sub_input.setStyleSheet(
            f"background:{C.TABLE_BG};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};"
            f"border-radius:7px;padding:2px 8px;font:12px 'SimSun';"
        )
        self.sub_input.returnPressed.connect(self._emit_manual_subscribe)
        self.btn_sub=QtWidgets.QPushButton("订阅")
        self.btn_sub.setFixedHeight(24)
        self.btn_sub.setCursor(QtCore.Qt.PointingHandCursor)
        self.btn_sub.setStyleSheet(
            f"QPushButton{{background:{C.BRAND};color:#FDFBF7;border:none;border-radius:7px;"
            f"font:bold 12px 'SimSun';padding:2px 12px;}}"
            f"QPushButton:hover{{background:#7B5B4C;}}"
        )
        self.btn_sub.clicked.connect(self._emit_manual_subscribe)
        sbl.addWidget(sub_lbl);sbl.addWidget(self.sub_input,1);sbl.addWidget(self.btn_sub)
        root.addWidget(sub_bar)

        self.hint=QtWidgets.QLabel("近月期货对应期权链（ATM±档位）")
        self.hint.setStyleSheet(
            f"color:{C.TEXT_MID};font:11px 'SimSun';background:{C.TABLE_ALT_ROW};"
            f"border-bottom:1px solid {C.GRID_LINE};padding:4px 8px;"
        )
        root.addWidget(self.hint)

        self.scroll=QtWidgets.QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.container=QtWidgets.QWidget()
        self.layout=QtWidgets.QVBoxLayout(self.container)
        self.layout.setContentsMargins(6,6,6,6);self.layout.setSpacing(6)
        self.scroll.setWidget(self.container)
        root.addWidget(self.scroll,1)

        self._hint_default="近月期货对应期权链（ATM±档位）"
        self._hint_timer=QtCore.QTimer(self);self._hint_timer.setSingleShot(True)
        self._hint_timer.timeout.connect(self._restore_hint)

    def _emit_manual_subscribe(self):
        """读取输入框 → 规整品种代码 → 发出手动订阅请求。"""
        raw=(self.sub_input.text() or "").strip()
        code="".join(ch for ch in raw if ch.isalnum())
        if not code:
            return
        self.manual_subscribe_requested.emit(code)
        self.sub_input.clear()
        self.show_message(f"已提交订阅请求：{code.upper()} …")

    def show_message(self, msg, revert_after=5000):
        """在提示条临时显示一条反馈消息，到时自动恢复默认提示。"""
        self.hint.setText(msg)
        if revert_after:
            self._hint_timer.start(revert_after)

    def _restore_hint(self):
        self.hint.setText(self._hint_default)

    # ------------------------------------------------------------------
    # 构建单张大卡（变体8风格）
    # ------------------------------------------------------------------
    def _make_card(self, code, name, underlying, exchange):
        exch_cn=self._EXCHANGE_CN.get(exchange, exchange)

        card=QtWidgets.QFrame()
        card.setObjectName("productCard")
        card.setProperty("filter_text", f"{name} {code} {underlying}")
        card.setProperty("active", False)
        card.setStyleSheet(
            f"QFrame#productCard{{background:#FFFEFB;border:1px solid {C.GRID_LINE};"
            f"border-radius:10px;}} QLabel{{background:transparent;}}"
        )
        cl=QtWidgets.QVBoxLayout(card);cl.setContentsMargins(10,8,10,8);cl.setSpacing(5)

        # ── 首行：品种名·代码  +  VIX胶囊  +  剩余天数 ──
        row1=QtWidgets.QHBoxLayout();row1.setSpacing(6)

        lbl_name=QtWidgets.QLabel(f"{name}期权 · {code.upper()}")
        lbl_name.setStyleSheet(f"color:{C.TEXT_DARK};font:bold 13px 'SimSun';")

        lbl_vix=QtWidgets.QLabel("VIX --")
        lbl_vix.setAlignment(QtCore.Qt.AlignCenter)
        lbl_vix.setFixedHeight(20)
        lbl_vix.setStyleSheet(
            f"color:{C.PRICE_UP};background:#FCE4EC;border-radius:9px;"
            f"padding:0 8px;font:bold 11px 'Consolas','SimSun';"
        )

        # 钱龙红绿柱方向（VIX右侧、剩余天数左侧）：红柱=偏多 / 绿柱=偏空
        lbl_ql=QtWidgets.QLabel("")
        lbl_ql.setAlignment(QtCore.Qt.AlignCenter)
        lbl_ql.setFixedHeight(20)
        lbl_ql.setVisible(False)

        lbl_days=QtWidgets.QLabel("--")
        lbl_days.setAlignment(QtCore.Qt.AlignCenter)
        lbl_days.setFixedHeight(20)
        lbl_days.setStyleSheet(
            f"color:{C.BRAND};background:rgba(93,64,55,0.10);border-radius:9px;"
            f"padding:0 8px;font:11px 'SimSun';"
        )

        row1.addWidget(lbl_name)
        row1.addStretch()
        row1.addWidget(lbl_vix)
        row1.addWidget(lbl_ql)
        row1.addWidget(lbl_days)
        cl.addLayout(row1)

        # ── 三格指标区：标的价 / 成交量 / C-P档位 ──
        metrics_row=QtWidgets.QHBoxLayout();metrics_row.setSpacing(5)
        def _make_metric_box(label_text):
            box=QtWidgets.QFrame()
            box.setStyleSheet(
                f"QFrame{{background:{C.TOOL_BAR_BG};border:1px solid {C.GRID_LINE};border-radius:7px;}}"
                f"QLabel{{background:transparent;}}"
            )
            bl=QtWidgets.QVBoxLayout(box);bl.setContentsMargins(6,5,6,5);bl.setSpacing(1)
            lbl_val=QtWidgets.QLabel("-")
            lbl_val.setAlignment(QtCore.Qt.AlignCenter)
            lbl_val.setStyleSheet(f"color:{C.TEXT_DARK};font:bold 12px 'Consolas','SimSun';")
            lbl_cap=QtWidgets.QLabel(label_text)
            lbl_cap.setAlignment(QtCore.Qt.AlignCenter)
            lbl_cap.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'SimSun';")
            bl.addWidget(lbl_val)
            bl.addWidget(lbl_cap)
            return box, lbl_val

        box_price, lval_price = _make_metric_box("标的价")
        box_vol,   lval_vol   = _make_metric_box("期权成交量")
        box_cp,    lval_cp    = _make_metric_box("C/P 档位")
        for b in (box_price, box_vol, box_cp):
            metrics_row.addWidget(b, 1)
        cl.addLayout(metrics_row)

        # ── 底行：交易所（中文）+ 近月合约 ──
        lbl_footer=QtWidgets.QLabel(f"{exch_cn}  ·  近月合约 {underlying}")
        lbl_footer.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'SimSun';border-top:1px solid {C.GRID_LINE};padding-top:4px;")
        cl.addWidget(lbl_footer)

        # ── 点击整张卡片切换品种（透明按钮覆盖） ──
        btn=QtWidgets.QPushButton("")  # kept for _buttons compat
        btn.setFixedHeight(0)
        btn.setStyleSheet("QPushButton{background:transparent;border:none;}")
        cl.addWidget(btn)
        # 事件过滤器：在 Qt event() 层拦截，覆盖卡片及所有子控件的鼠标点击
        _cf=_CardClickFilter(code, self.product_clicked, parent=card)
        card.installEventFilter(_cf)
        card.setCursor(QtCore.Qt.PointingHandCursor)

        return card, lbl_name, lbl_vix, lbl_ql, lbl_days, lval_price, lval_vol, lval_cp, lbl_footer, btn

    # ------------------------------------------------------------------
    def set_products(self, products):
        while self.layout.count():
            item=self.layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()
        self._cards.clear();self._lbl_name.clear();self._lbl_vix.clear();self._lbl_ql.clear();self._lbl_days.clear()
        self._mval_price.clear();self._mval_vol.clear();self._mval_cp.clear()
        self._lbl_footer.clear();self._card_list.clear()
        # _buttons kept for set_active compat
        self._buttons={}

        for item in products:
            if len(item)>=4:
                code,name,underlying,exchange=item[:4]
            else:
                code,name,underlying=item
                exchange="SHFE"

            card,ln,lv,lq,ld,lp,lvol,lcp,lf,btn=self._make_card(code,name,underlying,exchange)
            self._cards[code]=card
            self._lbl_name[code]=ln
            self._lbl_vix[code]=lv
            self._lbl_ql[code]=lq
            self._lbl_days[code]=ld
            self._mval_price[code]=lp
            self._mval_vol[code]=lvol
            self._mval_cp[code]=lcp
            self._lbl_footer[code]=lf
            self._buttons[code]=btn
            self._card_list.append(card)
            self.layout.addWidget(card)

        self.layout.addStretch()

    def set_active(self, code):
        for k, card in self._cards.items():
            active=(k==code)
            card.setProperty("active", active)
            if active:
                card.setStyleSheet(
                    f"QFrame#productCard{{background:{C.ATM_HIGHLIGHT};"
                    f"border:1px solid {C.BRAND};border-left:4px solid {C.BRAND};"
                    f"border-radius:10px;}} QLabel{{background:transparent;}}"
                )
                # VIX 胶囊激活态：填 PRICE_UP 底色白字
                lv=self._lbl_vix.get(k)
                if lv:
                    cur=lv.text()
                    lv.setStyleSheet(
                        f"color:#FFFFFF;background:{C.PRICE_UP};border-radius:9px;"
                        f"padding:0 8px;font:bold 11px 'Consolas','SimSun';"
                    )
            else:
                card.setStyleSheet(
                    f"QFrame#productCard{{background:#FFFEFB;border:1px solid {C.GRID_LINE};"
                    f"border-radius:10px;}} QLabel{{background:transparent;}}"
                )
                lv=self._lbl_vix.get(k)
                if lv:
                    lv.setStyleSheet(
                        f"color:{C.PRICE_UP};background:#FCE4EC;border-radius:9px;"
                        f"padding:0 8px;font:bold 11px 'Consolas','SimSun';"
                    )

    @staticmethod
    def _calc_days_text(expire_date):
        """根据 expire_date 计算剩余天数文字，兼容 datetime.date / YYYYMMDD / YYYY-MM-DD"""
        if not expire_date:
            return "--"
        try:
            import datetime as _dt
            if isinstance(expire_date, _dt.date):
                ed=expire_date
            else:
                s=str(expire_date).replace("-","").strip()
                if len(s)!=8 or not s.isdigit():
                    return str(expire_date)
                ed=_dt.date(int(s[:4]),int(s[4:6]),int(s[6:8]))
            d=(ed-_dt.date.today()).days
            return f"剩 {d} 天" if d>=0 else f"已到期"
        except Exception:
            return "--"

    def update_product_stats(self, code, price=0.0, call_count=0, put_count=0, vix=None,
                             exchange="SHFE", opt_volume=0.0, expire_date="", underlying=""):
        # ── 标的价 ──
        lp=self._mval_price.get(code)
        if lp:
            lp.setText(f"{price:.2f}" if price else "-")
        # ── 成交量 ──
        lv=self._mval_vol.get(code)
        if lv:
            lv.setText(f"{int(opt_volume):,}" if opt_volume else "-")
        # ── C/P档位 ──
        lcp=self._mval_cp.get(code)
        if lcp:
            lcp.setText(f"{call_count}/{put_count}")
        # ── VIX 胶囊文字 ──
        lbl_vix=self._lbl_vix.get(code)
        if lbl_vix:
            if vix is None:
                lbl_vix.setText("VIX --")
            else:
                pct=vix*100 if vix<2 else vix
                lbl_vix.setText(f"VIX {pct:.2f}%")
        # ── 剩余天数 ──
        lbl_days=self._lbl_days.get(code)
        if lbl_days:
            lbl_days.setText(self._calc_days_text(expire_date))
        # ── 底行交易所（中文）+ 合约 ──
        lf=self._lbl_footer.get(code)
        if lf and underlying:
            exch_cn=self._EXCHANGE_CN.get(exchange, exchange)
            lf.setText(f"{exch_cn}  ·  近月合约 {underlying}")

    def update_qianlong(self, code, bias="", color=""):
        """更新某品种钱龙红绿柱标签：红柱→偏多(红底)，绿柱→偏空(绿底)。
        bias 为空则隐藏标签（数据未就绪/非vix30品种）。"""
        lbl=self._lbl_ql.get(code)
        if not lbl:
            return
        if not bias:
            lbl.setVisible(False)
            lbl.setText("")
            return
        if color == "red":
            fg, bg = C.PRICE_UP, "#FCE4EC"
        else:
            fg, bg = C.PRICE_DOWN, "#E3F2E8"
        lbl.setText(bias)
        lbl.setStyleSheet(
            f"color:{fg};background:{bg};border-radius:9px;"
            f"padding:0 8px;font:bold 11px 'SimSun';"
        )
        lbl.setVisible(True)

    def reorder_by_vix(self, vix_map):
        """按 VIX 降序重排品种卡片；无 VIX 数据的品种排最后"""
        if len(self._cards) < 2:
            return
        sorted_codes=sorted(
            self._cards.keys(),
            key=lambda c: (vix_map.get(c) is None, -(vix_map.get(c) or 0))
        )
        # 移除末尾 stretch spacer
        for i in range(self.layout.count()-1, -1, -1):
            item=self.layout.itemAt(i)
            if item and item.spacerItem():
                self.layout.removeItem(item)
                break
        # 按排序顺序重新插入 widget
        for i, code in enumerate(sorted_codes):
            card=self._cards.get(code)
            if card:
                self.layout.removeWidget(card)
                self.layout.insertWidget(i, card)
        self.layout.addStretch()

    def _apply_filter(self):
        text=(self.search.text() or "").strip().lower()
        for card in self._card_list:
            ft=str(card.property("filter_text") or "").lower()
            card.setVisible((not text) or (text in ft))

# ========================= 主窗口 =========================
class MainWindow(QtWidgets.QMainWindow):
    product_selected = QtCore.pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("期权T型报价")
        self.setMinimumSize(1600, 900)
        self.setStyleSheet(f"QMainWindow{{background:{C.WIN_BG}}}")

        cw = QtWidgets.QWidget(); self.setCentralWidget(cw)
        main = QtWidgets.QVBoxLayout(cw); main.setContentsMargins(0,0,0,0); main.setSpacing(0)

        top_bar=QtWidgets.QWidget();top_bar.setFixedHeight(46)
        top_bar.setStyleSheet(f"background:{C.TOP_BAR_BG};border-bottom:1px solid {C.BORDER_COLOR};")
        th=QtWidgets.QHBoxLayout(top_bar);th.setContentsMargins(20,0,20,0);th.setSpacing(16)
        self.lbl_title=QtWidgets.QLabel("期权T型报价终端")
        self.lbl_title.setStyleSheet(f"color:{C.BRAND};font:bold 16px 'SimSun';")
        self.status_dot=QtWidgets.QLabel("●");self.status_dot.setStyleSheet(f"color:{C.TEXT_LIGHT};font-size:11px;")
        self.trading_lbl=QtWidgets.QLabel("等待行情");self.trading_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:bold 12px 'SimSun';")
        self.count_lbl=QtWidgets.QLabel("-");self.count_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:12px 'SimSun';")
        self.status_msg_lbl=QtWidgets.QLabel("准备就绪");self.status_msg_lbl.setStyleSheet(f"color:{C.BRAND};font:12px 'SimSun';")
        self.clock_lbl=QtWidgets.QLabel("--:--:--");self.clock_lbl.setStyleSheet(f"color:{C.TEXT_DARK};font:bold 13px 'Consolas';")
        th.addWidget(self.lbl_title);th.addWidget(self.status_dot);th.addWidget(self.trading_lbl);th.addStretch();th.addWidget(self.count_lbl);th.addWidget(self.status_msg_lbl);th.addWidget(self.clock_lbl)
        main.addWidget(top_bar)

        body=QtWidgets.QWidget();bl=QtWidgets.QHBoxLayout(body);bl.setContentsMargins(0,0,0,0);bl.setSpacing(0)
        self.sidebar=ProductSidebar()
        self.sidebar.product_clicked.connect(self.product_selected.emit)
        bl.addWidget(self.sidebar)

        right=QtWidgets.QWidget();rl=QtWidgets.QVBoxLayout(right);rl.setContentsMargins(0,0,0,0);rl.setSpacing(0)
        self.ctx_bar=QtWidgets.QFrame()
        self.ctx_bar.setStyleSheet(f"QFrame{{background:{C.TOOL_BAR_BG};border-bottom:1px solid {C.BORDER_COLOR};}} QLabel{{background:transparent;}}")
        cl=QtWidgets.QHBoxLayout(self.ctx_bar);cl.setContentsMargins(12,6,12,6);cl.setSpacing(10)
        self.table_title=QtWidgets.QLabel("请选择品种")
        self.table_title.setMinimumWidth(300)
        self.table_title.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")
        cl.addWidget(self.table_title)
        self.metric={}
        metric_defs=[
            ("price","标的价格"),
            ("vix","代理VIX"),
            ("fresh","行情时间"),
            ("volume","近月期权成交量"),
            ("days","到期剩余"),
            ("cp","C/P档数"),
        ]
        for key,label in metric_defs:
            chip=MetricChip(label)
            chip.setFixedHeight(44)
            chip.setMinimumWidth(90)
            self.metric[key]=chip
            cl.addWidget(chip)
        cl.addStretch()
        rl.addWidget(self.ctx_bar)

        self.table = TOptionTable()
        self.trade_tabs = self._build_trade_tabs()
        self.chart_panel = self._build_chart_panel()

        self.bottom_splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        self.bottom_splitter.setChildrenCollapsible(False)
        self.bottom_splitter.setHandleWidth(6)
        self.bottom_splitter.addWidget(self.trade_tabs)
        self.bottom_splitter.addWidget(self.chart_panel)
        self.bottom_splitter.setSizes([560, 760])

        right_splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        right_splitter.setChildrenCollapsible(False)
        right_splitter.setHandleWidth(6)
        right_splitter.addWidget(self.table)
        right_splitter.addWidget(self.bottom_splitter)
        right_splitter.setSizes([560, 380])

        rl.addWidget(right_splitter,1)
        bl.addWidget(right,1)
        main.addWidget(body,1)

        self._clock_timer=QtCore.QTimer(self);self._clock_timer.timeout.connect(self._update_clock);self._clock_timer.start(1000)
        self._update_clock()

    def _update_clock(self):
        self.clock_lbl.setText(time.strftime("%Y-%m-%d %H:%M:%S"))

    def _make_placeholder_box(self, title, subtitle, detail):
        box=QtWidgets.QFrame()
        box.setObjectName("placeholderBox")
        box.setStyleSheet(
            f"QFrame#placeholderBox{{background:{C.TABLE_BG};border:1px dashed {C.BORDER_COLOR};border-radius:10px;}}"
            f"QLabel{{color:{C.TEXT_MID};font:12px 'SimSun';}}"
        )
        lay=QtWidgets.QVBoxLayout(box)
        lay.setContentsMargins(16,14,16,16)
        lay.setSpacing(6)
        ttl=QtWidgets.QLabel(title)
        ttl.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")
        sub=QtWidgets.QLabel(subtitle)
        sub.setStyleSheet(f"color:{C.TEXT_LIGHT};font:12px 'SimSun';")
        msg=QtWidgets.QLabel(detail)
        msg.setWordWrap(True)
        msg.setAlignment(QtCore.Qt.AlignTop|QtCore.Qt.AlignLeft)
        lay.addWidget(ttl)
        lay.addWidget(sub)
        lay.addWidget(msg,1)
        lay.addStretch()
        return box

    def _build_trade_tabs(self):
        tabs=QtWidgets.QTabWidget()
        tabs.setObjectName("tradeTabs")
        tabs.setStyleSheet(
            f"QTabWidget#tradeTabs{{background:{C.TABLE_BG};border:none;}}"
            f"QTabWidget::pane{{border:1px solid {C.BORDER_COLOR};background:{C.TABLE_BG};top:-1px;}}"
            f"QTabBar::tab{{background:{C.TABLE_ALT_ROW};color:{C.TEXT_MID};border:1px solid {C.BORDER_COLOR};"
            f"border-bottom:none;border-top-left-radius:7px;border-top-right-radius:7px;min-width:72px;padding:6px 12px;margin-right:4px;font:bold 12px 'SimSun';}}"
            f"QTabBar::tab:selected{{background:{C.TABLE_BG};color:{C.BRAND};border-bottom:2px solid {C.TABLE_BG};}}"
            f"QTabBar::tab:hover{{background:{C.ATM_HIGHLIGHT};}}"
        )
        self.basket_panel=BasketPanel()
        tabs.addTab(self.basket_panel, "组合篮子")
        self.position_panel=PositionPanel()
        tabs.addTab(self.position_panel, "组合持仓")
        self.signal_panel=SignalWindow(self)
        tabs.addTab(self.signal_panel, "信号监控")
        self.trade_tabs=tabs
        return tabs

    def focus_signal_panel(self):
        try:
            tw=self.trade_tabs
            for i in range(tw.count()):
                if tw.tabText(i)=="信号监控":
                    tw.setCurrentIndex(i)
                    break
        except Exception:
            pass
        try:
            self.signal_panel.setFocus(QtCore.Qt.OtherFocusReason)
        except Exception:
            pass

    def _build_chart_panel(self):
        panel=QtWidgets.QFrame()
        panel.setObjectName("chartPanel")
        panel.setStyleSheet(
            f"QFrame#chartPanel{{background:{C.TABLE_BG};border:2px solid {C.BRAND};border-radius:10px;}}"
            f"QLabel{{background:transparent;color:{C.TEXT_MID};font:12px 'SimSun';}}"
        )
        lay=QtWidgets.QVBoxLayout(panel)
        lay.setContentsMargins(8,6,8,8)
        lay.setSpacing(6)
        self.payoff_chart=PayoffChart()
        self.pnl_chart=MiniLineChart("实时盈亏曲线")
        lay.addWidget(self.payoff_chart,2)
        lay.addWidget(self.pnl_chart,1)
        return panel

    def _expire_days_text(self, expire_date, base_date=None):
        _date, _days = self._expire_days_pair(expire_date, base_date)
        return _days or _date

    @staticmethod
    def _expire_days_pair(expire_date, base_date=None):
        """返回 (到期日期字符串 YYYY-MM-DD, 剩余天数文字)。
        base_date 可为 date 对象或 'YYYY-MM-DD'/'YYYYMMDD' 字符串（交易日）。"""
        if not expire_date:
            return "-", ""
        s=str(expire_date).replace("-","").replace("/","").strip()
        if len(s) != 8 or (not s.isdigit()):
            return str(expire_date), ""
        try:
            ed=datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            today=None
            if base_date is not None:
                if isinstance(base_date, datetime.date):
                    today=base_date
                else:
                    bs=str(base_date).replace("-","").replace("/","").strip()
                    if len(bs)==8 and bs.isdigit():
                        today=datetime.date(int(bs[:4]), int(bs[4:6]), int(bs[6:8]))
            if today is None:
                today=datetime.date.today()
            d=(ed - today).days
            date_str=f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            days=f"剩{d}天" if d >= 0 else f"已到期({-d}天)"
            return date_str, days
        except Exception:
            return str(expire_date), ""

    def update_product_info(self, code, name, underlying, price, call_c, put_c, tick_total, opt_tick,
                            expire_date="", market_time="-", vix_value="-",
                            opt_volume=0, exchange="SHFE", is_trading=True, vix_raw=None, market_time_live=True,
                            trade_date=None):
        title=f"{exchange or 'SHFE'} · {name or code} ({code.upper()}) · 标的 {underlying}"
        self.table_title.setText(title)
        self.count_lbl.setText(f"总Tick:{tick_total} 期权Tick:{opt_tick}")

        def _fmt_qty(val):
            if not val:
                return "-"
            if abs(val) >= 10000:
                return f"{val/10000:.1f}万"
            return f"{int(val):,}"

        price_txt=f"{price:.2f}" if price else "-"
        self.metric["price"].set_value(price_txt, "标的价格")
        self.metric["vix"].set_value(vix_value or "-", "代理VIX", color=C.PRICE_UP)
        time_label="行情时间" if market_time_live else "最后行情"
        time_value=market_time if (market_time and market_time != "-") else ("休市" if not is_trading else "--:--")
        self.metric["fresh"].set_value(time_value, time_label, color=(C.TRADING_ON if market_time_live else C.TEXT_LIGHT))
        self.metric["volume"].set_value(_fmt_qty(opt_volume), "近月期权成交量")
        self.metric["cp"].set_value(f"{call_c}/{put_c}", "C/P档数")
        _edate, _edays = self._expire_days_pair(expire_date, trade_date)
        self.metric["days"].set_value(_edate, f"到期剩余 · {_edays}" if _edays else "到期剩余")
        if tick_total > 0 and is_trading:
            self.trading_lbl.setText("交易中")
            self.trading_lbl.setStyleSheet(f"color:{C.TRADING_ON};font:bold 12px 'SimSun';")
            self.status_dot.setStyleSheet(f"color:{C.TRADING_ON};font-size:11px;")
        elif not is_trading:
            self.trading_lbl.setText("休市中")
            self.trading_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:bold 12px 'SimSun';")
            self.status_dot.setStyleSheet(f"color:{C.TEXT_LIGHT};font-size:11px;")
        self.sidebar.set_active(code)
        self.sidebar.update_product_stats(code, price, call_c, put_c, vix_raw, exchange, opt_volume,
                                          expire_date, underlying)

    def update_sidebar_bulk_stats(self, stats_map):
        for code, d in (stats_map or {}).items():
            self.sidebar.update_product_stats(
                code,
                d.get("price", 0.0),
                d.get("call_count", 0),
                d.get("put_count", 0),
                d.get("vix"),
                d.get("exchange","SHFE"),
                d.get("opt_volume",0),
                d.get("expire_date",""),
                d.get("underlying",""),
            )
        vix_map={code: d.get("vix") for code, d in (stats_map or {}).items()}
        self.sidebar.reorder_by_vix(vix_map)

    def update_status_banner(self, text):
        if text:
            self.status_msg_lbl.setText(text)
        else:
            self.status_msg_lbl.setText("准备就绪")

# ========================= 组合篮子图表组件 =========================

class MiniLineChart(QtWidgets.QWidget):
    """实时盈亏曲线（QPainter 自绘，零依赖）"""
    def __init__(self, title="实时盈亏曲线", parent=None):
        super().__init__(parent); self.title=title; self.values=[]; self.latest_time=""; self.setMinimumHeight(80)
    def set_values(self, values, latest_time=""):
        self.values=[float(x) for x in (values or []) if isinstance(x,(int,float))]
        self.latest_time=str(latest_time or ""); self.update()
    def paintEvent(self, event):
        p=QtGui.QPainter(self); p.fillRect(self.rect(), QtGui.QColor(C.TABLE_BG))
        p.setRenderHint(QtGui.QPainter.Antialiasing)
        w,h=self.width(),self.height(); m=26
        p.setPen(QtGui.QPen(QtGui.QColor(C.GRID_LINE),1)); p.drawRect(4,4,max(1,w-8),max(1,h-8))
        p.setPen(QtGui.QColor(C.BRAND)); p.drawText(10,18,self.title)
        vals=self.values
        if len(vals)<2:
            p.setPen(QtGui.QColor(C.TEXT_LIGHT)); p.drawText(10,h//2+6,"暂无盈亏数据（添加组合腿后实时计算）"); return
        v_min,v_max=min(vals),max(vals)
        if abs(v_max-v_min)<1e-9: v_max+=1; v_min-=1
        zero_y=h-m-(0-v_min)/(v_max-v_min)*max(1,h-2*m)
        if m<=zero_y<=h-m:
            p.setPen(QtGui.QPen(QtGui.QColor(C.BORDER_COLOR),1,QtCore.Qt.DashLine))
            p.drawLine(m,int(zero_y),w-m,int(zero_y))
        pts=[(int(m+i*max(1,w-2*m)/(len(vals)-1)), int(h-m-(v-v_min)/(v_max-v_min)*max(1,h-2*m))) for i,v in enumerate(vals)]
        last_pnl=vals[-1]
        p.setPen(QtGui.QPen(QtGui.QColor(C.TRADING_ON if last_pnl>=0 else C.PRICE_UP),2))
        for a,b in zip(pts,pts[1:]): p.drawLine(a[0],a[1],b[0],b[1])
        p.setPen(QtGui.QColor(C.TEXT_DARK)); p.drawText(w-130,18,f"浮盈亏 {last_pnl:+.2f}元")

class PayoffChart(QtWidgets.QWidget):
    """到期损益图（方案8-改·双卡片置顶）
    · 顶部两张信息卡：入场数据（价格+品种VIX）/ 实时数据（价格+品种VIX）
    · Y轴左侧：最大盈利 / 0 / 最大亏损 三个关键值（含参考虚线）
    · 行权价拐点：大同心圆（描边+内点）+ 价格数字
    · 盈亏平衡点：小同心圆 + 价格数字（交替上下防重叠）
    · 开仓价紫色实线 / 现价橙色虚线，标签在图外底部错开两行
    """
    def __init__(self, parent=None):
        super().__init__(parent); self.points=[]; self.current_price=None; self.strikes=[]
        self._entry_price=None; self._entry_vix=None; self._cur_vix=None
        self._selected_product=None
        self.setMinimumHeight(140)
    @staticmethod
    def _norm_vix(v):
        """统一归一化为小数形式（与侧栏 update_product_stats 阈值2一致）：
        代理VIX可能是百分比形式(如47.85=47.85%)或小数形式(0.4785)，统一转小数。"""
        if v is None:
            return None
        v=float(v)
        return v/100.0 if v>=2 else v
    def set_entry_overlay(self, entry_price, entry_vix):
        """模拟开仓时记录入场标的价和品种VIX"""
        self._entry_price=float(entry_price) if entry_price else None
        self._entry_vix=self._norm_vix(entry_vix)
        self.update()
    def set_selected_product(self, product_code):
        """记录当前选中组合的品种代码，用于获取正确的实时价/VIX"""
        self._selected_product=product_code or None
    def update_cur_vix(self, vix):
        self._cur_vix=self._norm_vix(vix); self.update()
    def set_basket(self, basket_legs, current_price):
        self.current_price=float(current_price) if current_price else None
        self.strikes=[]; self.points=[]
        if not basket_legs: self.update(); return
        for leg in basket_legs:
            k=leg.get("strike")
            if k: self.strikes.append(float(k))
        if not self.strikes: self.update(); return
        spread=max(self.strikes)-min(self.strikes) if len(self.strikes)>1 else min(self.strikes)*0.05
        padding=max(spread*2.5, min(self.strikes)*0.04, 1)
        lo=min(self.strikes)-padding; hi=max(self.strikes)+padding
        xs=sorted(set(round(x,4) for x in [lo+(hi-lo)*i/200 for i in range(201)]+self.strikes))
        for s in xs:
            pnl=0.0
            for leg in basket_legs:
                k=float(leg.get("strike",0)); mult=float(leg.get("multiplier",1)); qty=int(leg.get("qty",1))
                ep=float(leg.get("entry_price",0) or 0); opt_type=leg.get("opt_type","CALL"); side=leg.get("side","BUY")
                payoff=max(s-k,0) if opt_type=="CALL" else max(k-s,0)
                leg_pnl=(payoff-ep) if side=="BUY" else (ep-payoff)
                pnl+=leg_pnl*mult*qty
            self.points.append((s,pnl))
        self.update()
    def update_price(self, current_price):
        if current_price: self.current_price=float(current_price); self.update()
    # ── 主绘制 ───────────────────────────────────────────
    def paintEvent(self, event):
        p=QtGui.QPainter(self); p.setRenderHint(QtGui.QPainter.Antialiasing)
        w,h=self.width(),self.height()
        # ── 布局参数（无顶部卡片，VIX/价格标签绘于图内虚线顶端）──
        ml,mr=60,12
        mt=8                             # 最小顶边距
        mb=8                             # 最小底边距
        pw=max(1,w-ml-mr); ph=max(1,h-mt-mb); cb=h-mb
        # ── 全局背景 ──
        p.fillRect(self.rect(), QtGui.QColor(C.TABLE_BG))
        # ── 空态 ──
        if len(self.points)<2:
            p.setPen(QtGui.QColor(C.TEXT_LIGHT)); p.setFont(QtGui.QFont("SimSun",10))
            p.drawText(ml+6, mt+ph//2+5, "勾选T表买C/卖C/买P/卖P添加组合腿"); return
        xs=[pt[0] for pt in self.points]; ys=[pt[1] for pt in self.points]
        x_min,x_max=min(xs),max(xs)
        y_min=min(min(ys),0); y_max=max(max(ys),0)
        if abs(y_max-y_min)<1e-9: y_max+=1; y_min-=1
        ypad=(y_max-y_min)*0.08; y_min-=ypad; y_max+=ypad
        def tx(x): return ml+(x-x_min)/max(1e-9,x_max-x_min)*pw
        def ty(y): return h-mb-(y-y_min)/max(1e-9,y_max-y_min)*ph
        zero_y=ty(0)
        # ── 盈/亏填色区 ──
        if mt<zero_y<cb:
            p.fillRect(ml,mt,pw,max(1,int(zero_y-mt)),QtGui.QColor("#E8F5E9"))
            p.fillRect(ml,int(zero_y),pw,max(1,int(cb-zero_y)),QtGui.QColor("#FDECEA"))
        elif zero_y>=cb: p.fillRect(ml,mt,pw,ph,QtGui.QColor("#E8F5E9"))
        else: p.fillRect(ml,mt,pw,ph,QtGui.QColor("#FDECEA"))
        p.setPen(QtGui.QPen(QtGui.QColor(C.BORDER_COLOR),1)); p.drawRect(ml,mt,pw,ph)
        # ── 关键P&L集合（Y轴标签 + 参考线）────────────────────────────
        # · 零轴始终加入
        # · 左侧平坦尾部（净权利金/净收入，始终是有限值）始终加入
        # · 每个行权价处的盈亏值加入
        # · 右侧尾部：仅斜率≈0（有限损益策略，如铁鹰/蝴蝶）时才加入，无限亏损不加入
        key_pnl=set()
        key_pnl.add(0.0)
        key_pnl.add(round(self.points[0][1],4))   # 左侧尾部（必显）
        for k in self.strikes:
            nearest=min(self.points,key=lambda pt:abs(pt[0]-k))
            if abs(nearest[0]-k)<(x_max-x_min)/60:
                key_pnl.add(round(nearest[1],4))
        last_slope=(self.points[-1][1]-self.points[-2][1])/max(abs(self.points[-1][0]-self.points[-2][0]),1e-9)
        if abs(last_slope)<0.5:             # 右侧近似水平→有限损益，加入
            key_pnl.add(round(self.points[-1][1],4))
        disp_ys=list(key_pnl)
        # ── Y轴参考虚线（关键值，非零轴） ──
        for yv in disp_ys:
            py=int(ty(yv))
            if mt<py<cb and abs(yv)>1e-6:
                p.setPen(QtGui.QPen(QtGui.QColor("#D4C5B5"),1,QtCore.Qt.DotLine))
                p.drawLine(ml,py,ml+pw,py)
        # ── 零轴实线 ──
        if mt<=zero_y<=cb:
            p.setPen(QtGui.QPen(QtGui.QColor("#5D4037"),1.5)); p.drawLine(ml,int(zero_y),ml+pw,int(zero_y))
        # ── 虚线 + 顶部标签（VIX上行，价格下行；两线相近时错行防重叠）──
        p.setFont(QtGui.QFont("Consolas",9,QtGui.QFont.Bold)); fm_l=p.fontMetrics()
        has_ep=bool(self._entry_price)
        has_cp=bool(self.current_price)
        # 价格超出X轴时截断到边界，不再消失
        epx=int(tx(max(x_min, min(x_max, self._entry_price)))) if has_ep else 0
        cpx=int(tx(max(x_min, min(x_max, self.current_price)))) if has_cp else 0
        close=(has_ep and has_cp and abs(cpx-epx)<80)  # 两线水平距离<80px则错行
        e_vy=mt+14; e_py=cb-8           # 入场：VIX在虚线顶端 / 价格在虚线底端
        c_vy=mt+14+(16 if close else 0); c_py=cb-8-(16 if close else 0)  # 现价：close时VIX下移/价格上移16px
        # 开仓价紫色实线 + 标签（与现价虚线区分）
        if has_ep:
            p.setPen(QtGui.QPen(QtGui.QColor("#9C27B0"),1.5,QtCore.Qt.SolidLine))
            p.drawLine(epx,mt,epx,cb)
            p.setPen(QtGui.QColor("#7B1FA2"))
            vt=f"VIX {self._entry_vix*100:.1f}%" if self._entry_vix is not None else ""
            if vt: p.drawText(epx-fm_l.horizontalAdvance(vt)//2, e_vy, vt)
            pt=f"{self._entry_price:.0f}"; p.drawText(epx-fm_l.horizontalAdvance(pt)//2, e_py, pt)
        # 现价橙色虚线 + 标签
        if has_cp:
            p.setPen(QtGui.QPen(QtGui.QColor("#E65100"),1,QtCore.Qt.DashLine))
            p.drawLine(cpx,mt,cpx,cb)
            p.setPen(QtGui.QColor("#E65100"))
            vt2=f"VIX {self._cur_vix*100:.1f}%" if self._cur_vix is not None else ""
            if vt2: p.drawText(cpx-fm_l.horizontalAdvance(vt2)//2, c_vy, vt2)
            pt2=f"{self.current_price:.0f}"; p.drawText(cpx-fm_l.horizontalAdvance(pt2)//2, c_py, pt2)
        # ── 盈亏曲线（绿/红，零轴切分） ──
        C_POS="#2E7D32"; C_NEG="#C62828"
        for (x1,y1),(x2,y2) in zip(self.points,self.points[1:]):
            if y1>=0 and y2>=0: col=C_POS
            elif y1<=0 and y2<=0: col=C_NEG
            else:
                zx=x1+(0-y1)*(x2-x1)/(y2-y1)
                p.setPen(QtGui.QPen(QtGui.QColor(C_POS if y1>0 else C_NEG),2))
                p.drawLine(int(tx(x1)),int(ty(y1)),int(tx(zx)),int(ty(0)))
                p.setPen(QtGui.QPen(QtGui.QColor(C_NEG if y2<0 else C_POS),2))
                p.drawLine(int(tx(zx)),int(ty(0)),int(tx(x2)),int(ty(y2))); continue
            p.setPen(QtGui.QPen(QtGui.QColor(col),2))
            p.drawLine(int(tx(x1)),int(ty(y1)),int(tx(x2)),int(ty(y2)))
        # ── Y轴刻度标签（左侧，全显关键值，无间距过滤）──
        p.setFont(QtGui.QFont("Consolas",9,QtGui.QFont.Bold)); fm=p.fontMetrics()
        yticks=sorted([(int(ty(yv)),yv) for yv in disp_ys if mt<=int(ty(yv))<=cb])
        for py2,yv2 in yticks:
            lbl="0" if abs(yv2)<1e-6 else f"{yv2:.0f}"
            lw=fm.horizontalAdvance(lbl)
            col="#2E7D32" if yv2>0 else ("#C62828" if yv2<0 else "#5D4037")
            p.setPen(QtGui.QColor(col)); p.drawText(ml-lw-6,py2+4,lbl)
            p.drawLine(ml-3,py2,ml,py2)
        # ── 统一标注函数：外环圆r=7 + 内实心r=3，全部尺寸一致 ──
        def _draw_kp(cx, cy, fill_hex):    # KP：老和色内心
            p.setBrush(QtCore.Qt.NoBrush)
            p.setPen(QtGui.QPen(QtGui.QColor("#4E342E"),1.5))
            p.drawEllipse(cx-7,cy-7,14,14)
            p.setBrush(QtGui.QColor(fill_hex)); p.setPen(QtCore.Qt.NoPen)
            p.drawEllipse(cx-3,cy-3,6,6)
            p.setBrush(QtCore.Qt.NoBrush)
        def _draw_be(cx, cy, fill_hex):    # BE：同尺寸，琥珀色内心
            p.setBrush(QtCore.Qt.NoBrush)
            p.setPen(QtGui.QPen(QtGui.QColor("#4E342E"),1.5))
            p.drawEllipse(cx-7,cy-7,14,14)
            p.setBrush(QtGui.QColor(fill_hex)); p.setPen(QtCore.Qt.NoPen)
            p.drawEllipse(cx-3,cy-3,6,6)
            p.setBrush(QtCore.Qt.NoBrush)
        # ── 行权价拐点（大同心圆 + 价格数字）──
        p.setFont(QtGui.QFont("Consolas",9,QtGui.QFont.Bold)); fm9=p.fontMetrics()
        for k in self.strikes:
            if x_min<=k<=x_max:
                nearest=min(self.points,key=lambda pt:abs(pt[0]-k))
                if abs(nearest[0]-k)<(x_max-x_min)/60:
                    kpx=int(tx(k)); kpy=int(ty(nearest[1]))
                    if mt-8<=kpy<=cb+8:
                        _draw_kp(kpx, kpy, "#8D6E63")   # 和色和和内心
                        lbl=f"{k:.0f}"; lw=fm9.horizontalAdvance(lbl)
                        above=nearest[1]>0
                        p.setPen(QtGui.QColor("#4E342E"))
                        p.drawText(kpx-lw//2, kpy+(-12 if above else 19), lbl)
        # ── 盈亏平衡点（同尺寸同心圆 + 价格数字，交替上下）──
        _be_prices=[]; be_above=[True,False]
        for i in range(len(self.points)-1):
            s1,y1=self.points[i]; s2,y2=self.points[i+1]
            if (y1<0 and y2>=0) or (y1>0 and y2<=0):
                sz=s1+(0-y1)*(s2-s1)/max(abs(y2-y1),1e-9); _be_prices.append(sz)
                if x_min<=sz<=x_max:
                    bx=int(tx(sz)); bzy=int(ty(0))
                    if mt<=bzy<=cb:
                        idx=len(_be_prices)-1
                        _draw_be(bx, bzy, "#C8A030")     # 琥珀黄内心
                        lbl=f"{sz:.0f}"; lw=fm9.horizontalAdvance(lbl)
                        abv=be_above[idx%2]
                        p.setPen(QtGui.QColor("#7A5C00"))
                        p.drawText(bx-lw//2, bzy+(-12 if abv else 19), lbl)

# ─────────────── 比例价差预估风险（动态，仅实时显示，不落库）───────────────
def _atm_avg_for_product(snapshot, product):
    """从快照中取某品种近月 ATM 的 (Call+Put)/2 现价。无行情返回 None。
    snapshot["products"][i] = {"product","price","rows":[{"K","c":{"last"},"p":{"last"},"atm"}]}"""
    if not snapshot or not product:
        return None
    for p in snapshot.get("products", []):
        if p.get("product") != product:
            continue
        rows = p.get("rows", []) or []
        price = float(p.get("price", 0) or 0)
        atm_row = next((r for r in rows if r.get("atm")), None)
        if atm_row is None and rows and price > 0:
            atm_row = min(rows, key=lambda r: abs(float(r.get("K", 0) or 0) - price))
        if not atm_row:
            return None
        c = (atm_row.get("c") or {}).get("last")
        pp = (atm_row.get("p") or {}).get("last")
        vals = [float(v) for v in (c, pp) if isinstance(v, (int, float)) and v > 0]
        if len(vals) == 2:
            return (vals[0] + vals[1]) / 2.0
        if vals:
            return vals[0]
        return None
    return None


def _estimate_ratio_risk(legs, atm_avg):
    """比例价差预估风险（元，动态实时）。返回 (risk, ok)。
    预估风险 = [ |K卖−K买| − (比例数−1)×((ATM_Call现价+ATM_Put现价)/2) ] × 合约乘数
      · 比例数 = round(卖方总手数 / 买方总手数)（如 买3/卖6 → 2，买1/卖3 → 3）
      · (比例数−1) = 归一化后未对冲的卖方腿数（1:2→1，1:3→2）
      · 按比例归一化的每单位计算，不乘实际手数、不含净权利金
    示例：组合08(买3 C15000/卖6 C16000, ATM均价778.75, 乘数15)
          = (1000 − 1×778.75) × 15 = 3318.75 元
    """
    if not legs:
        return 0.0, False
    mult = float(legs[0].get("multiplier", 1) or 1)
    def _q(l):
        return int(l.get("qty", 1) or 1)
    buy_qty  = sum(_q(l) for l in legs if l.get("side", "BUY") == "BUY")
    sell_qty = sum(_q(l) for l in legs if l.get("side") == "SELL")
    if buy_qty <= 0 or sell_qty <= 0:
        return 0.0, False
    ratio_n = max(1, int(round(sell_qty / buy_qty)))
    def _wk(side):
        ls = [l for l in legs if l.get("side", "BUY") == side]
        q = sum(_q(l) for l in ls)
        if q <= 0:
            return 0.0
        return sum(float(l.get("strike", 0) or 0) * _q(l) for l in ls) / q
    k_buy = _wk("BUY"); k_sell = _wk("SELL")
    width = abs(k_sell - k_buy)
    risk = (width - (ratio_n - 1) * float(atm_avg or 0)) * mult
    return risk, True


# ─────────────── 玻璃拟态组合卡片（护眼暖色）───────────────
class _GlassCard(QtWidgets.QFrame):
    """单个组合的玻璃拟态卡片 — QPainter 圆角 + QGraphicsDropShadowEffect"""
    close_req  = QtCore.pyqtSignal(int)
    delete_req = QtCore.pyqtSignal(int)
    select_req = QtCore.pyqtSignal(int)
    edit_req   = QtCore.pyqtSignal(int)
    _BG_NORM  = QtGui.QColor(255,252,244,235)
    _BG_SEL_G = QtGui.QColor(228,248,232,242)
    _BG_SEL_R = QtGui.QColor(253,238,236,242)
    _BD_NORM  = QtGui.QColor(188,170,164,150)
    _BD_SEL   = QtGui.QColor(93,64,55,210)
    def __init__(self, grp, parent=None):
        super().__init__(parent)
        self._gid=grp["id"]
        self._status=grp.get("status","OPEN")
        self._pnl=float(grp.get("total_pnl",0.0) or 0.0)
        self._selected=False
        self.setCursor(QtCore.Qt.PointingHandCursor)
        sh=QtWidgets.QGraphicsDropShadowEffect(self)
        sh.setBlurRadius(10); sh.setOffset(0,2); sh.setColor(QtGui.QColor(100,60,30,38))
        self.setGraphicsEffect(sh)
        lay=QtWidgets.QVBoxLayout(self); lay.setContentsMargins(10,7,10,7); lay.setSpacing(3)
        hdr=QtWidgets.QHBoxLayout(); hdr.setSpacing(6)
        self._name_lbl=QtWidgets.QLabel(grp["name"])
        self._name_lbl.setStyleSheet(f"color:{C.BRAND};font:bold 12px 'SimSun';background:transparent;")
        if self._status=="CLOSED":
            self._info_lbl=QtWidgets.QLabel(
                f"开仓 {grp['open_time']} · 平仓 {grp.get('closed_time','-')} · 入场 {grp['open_price']:.0f}")
        else:
            self._info_lbl=QtWidgets.QLabel(f"开仓 {grp['open_time']} · 入场 {grp['open_price']:.0f}")
        self._info_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'SimSun';background:transparent;")
        self._pnl_lbl=QtWidgets.QLabel(f"{self._pnl:+.2f}元")
        self._pnl_lbl.setStyleSheet(
            f"color:{'#2E7D32' if self._pnl>=0 else '#C62828'};font:bold 12px 'SimSun';background:transparent;")
        ebtn=None
        if self._status=="OPEN":
            ebtn=QtWidgets.QPushButton("编辑"); ebtn.setFixedSize(40,22)
            ebtn.setToolTip("加腿/减腿/改手数：载入组合篮子编辑，原腿开仓数据保留")
            ebtn.setStyleSheet("QPushButton{background:rgba(21,101,192,210);color:#fff;border:none;"
                               "border-radius:4px;font:10px 'SimSun';}"
                               "QPushButton:hover{background:rgba(13,71,161,230);}")
            ebtn.clicked.connect(lambda: self.edit_req.emit(self._gid))
            cbtn=QtWidgets.QPushButton("平仓"); cbtn.setFixedSize(48,22)
            cbtn.setStyleSheet("QPushButton{background:rgba(198,40,40,210);color:#fff;border:none;"
                               "border-radius:4px;font:10px 'SimSun';}"
                               "QPushButton:hover{background:rgba(183,28,28,230);}")
            cbtn.clicked.connect(lambda: self.close_req.emit(self._gid))
        else:
            # 已平仓：点击按钮后再双击按钮才触发删除
            cbtn=QtWidgets.QPushButton("双击删除"); cbtn.setFixedSize(60,22)
            cbtn.setToolTip("双击此按钮删除该记录")
            cbtn.setStyleSheet("QPushButton{background:rgba(120,120,120,150);color:#eee;border:none;"
                               "border-radius:4px;font:10px 'SimSun';}"
                               "QPushButton:hover{background:rgba(160,50,50,200);}")
            _gid_cap=self._gid; _sig=self.delete_req
            def _make_dblclick(btn, gid, sig):
                def _dbl(ev):
                    if ev.button()==QtCore.Qt.LeftButton:
                        sig.emit(gid)
                    QtWidgets.QPushButton.mouseDoubleClickEvent(btn, ev)
                btn.mouseDoubleClickEvent=_dbl
            _make_dblclick(cbtn, _gid_cap, _sig)
        hdr.addWidget(self._name_lbl); hdr.addWidget(self._info_lbl,1)
        hdr.addWidget(self._pnl_lbl)
        if ebtn is not None:
            hdr.addWidget(ebtn)
        hdr.addWidget(cbtn)
        lay.addLayout(hdr)
        # 预估风险（比例价差·动态实时，不落库）
        self._risk_lbl=QtWidgets.QLabel("预估风险 --")
        self._risk_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'SimSun';background:transparent;")
        lay.addWidget(self._risk_lbl)
        sep=QtWidgets.QFrame(); sep.setFrameShape(QtWidgets.QFrame.HLine)
        sep.setStyleSheet(f"background:{C.GRID_LINE};"); sep.setFixedHeight(1)
        lay.addWidget(sep)
        self._leg_widgets=[]
        for leg in grp["legs"]:
            side=leg.get("side","BUY"); ep=leg.get("entry_price",0) or 0
            cp=leg.get("cur_price",ep) or ep; fpnl=leg.get("float_pnl",0.0)
            oc="C" if leg.get("opt_type","CALL")=="CALL" else "P"
            k=leg.get("strike",0)
            qty=int(leg.get("qty",1) or 1)
            raw_iid=leg.get("iid",""); short=raw_iid.split(".")[-1] if "." in raw_iid else raw_iid
            rw=QtWidgets.QHBoxLayout(); rw.setSpacing(4); rw.setContentsMargins(0,0,0,0)
            sc="#1565C0" if side=="BUY" else "#C62828"
            sw=QtWidgets.QLabel("买" if side=="BUY" else "卖"); sw.setFixedWidth(22)
            sw.setAlignment(QtCore.Qt.AlignCenter)
            sw.setStyleSheet(f"color:{sc};font:bold 11px 'SimSun';background:transparent;")
            qw=QtWidgets.QLabel(f"×{qty}"); qw.setFixedWidth(30)
            qw.setAlignment(QtCore.Qt.AlignCenter)
            qw.setStyleSheet(f"color:{sc};font:bold 10px 'Consolas','SimSun';background:transparent;")
            cw=QtWidgets.QLabel(short); cw.setFixedWidth(88)
            cw.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'SimSun';background:transparent;")
            dw=QtWidgets.QLabel(f"{oc} K{k:.0f}  入:{ep:.2f}")
            dw.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'SimSun';background:transparent;")
            cur_w=QtWidgets.QLabel(f"现:{cp:.2f}"); cur_w.setFixedWidth(72)
            cur_w.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignVCenter)
            cur_w.setStyleSheet(f"color:{C.TEXT_DARK};font:10px 'SimSun';background:transparent;")
            pc="#2E7D32" if fpnl>=0 else "#C62828"
            pnl_w=QtWidgets.QLabel(f"{fpnl:+.2f}"); pnl_w.setFixedWidth(66)
            pnl_w.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignVCenter)
            pnl_w.setStyleSheet(f"color:{pc};font:bold 10px 'SimSun';background:transparent;")
            for ww in (sw,qw,cw,dw,cur_w,pnl_w): rw.addWidget(ww)
            rw.addStretch(0)
            row_w=QtWidgets.QWidget(); row_w.setLayout(rw)
            row_w.setStyleSheet("background:transparent;")
            lay.addWidget(row_w); self._leg_widgets.append((cur_w,pnl_w))
    def set_selected(self,val): self._selected=val; self.update()
    def update_pnl(self,total,leg_data):
        if self._status!="OPEN":
            return
        self._pnl=total; c="#2E7D32" if total>=0 else "#C62828"
        self._pnl_lbl.setText(f"{total:+.2f}元")
        self._pnl_lbl.setStyleSheet(f"color:{c};font:bold 12px 'SimSun';background:transparent;")
        for i,(cur_w,pnl_w) in enumerate(self._leg_widgets):
            if i<len(leg_data):
                cp,fp=leg_data[i]; cur_w.setText(f"现:{cp:.2f}")
                pc="#2E7D32" if fp>=0 else "#C62828"
                pnl_w.setText(f"{fp:+.2f}")
                pnl_w.setStyleSheet(f"color:{pc};font:bold 10px 'SimSun';background:transparent;")
        self.update()
    def update_risk(self, risk, ok, atm_avg=None):
        """更新预估风险显示（动态）。ok=False 或 atm_avg 缺失时显示占位。"""
        if not ok:
            self._risk_lbl.setText("预估风险 计算中…（等待该品种ATM行情）")
            self._risk_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'SimSun';background:transparent;")
            return
        atm_txt = f"  ATM均价{atm_avg:.2f}" if isinstance(atm_avg,(int,float)) and atm_avg>0 else "  (无ATM行情)"
        rc = "#C62828" if risk < 0 else "#2E7D32"
        self._risk_lbl.setText(f"预估风险 {risk:+.2f}元{atm_txt}")
        self._risk_lbl.setStyleSheet(f"color:{rc};font:bold 10px 'SimSun';background:transparent;")
    def mousePressEvent(self,event):
        if event.button()==QtCore.Qt.LeftButton: self.select_req.emit(self._gid)
        super().mousePressEvent(event)
    def mouseDoubleClickEvent(self,event):
        super().mouseDoubleClickEvent(event)
    def paintEvent(self,event):
        p=QtGui.QPainter(self); p.setRenderHint(QtGui.QPainter.Antialiasing)
        p.fillRect(self.rect(),QtGui.QColor(C.TABLE_BG))
        r=self.rect().adjusted(2,2,-2,-2)
        if self._selected:
            bg=self._BG_SEL_G if self._pnl>=0 else self._BG_SEL_R; bd=self._BD_SEL
        else:
            bg=self._BG_NORM; bd=self._BD_NORM
        p.setBrush(QtGui.QBrush(bg)); p.setPen(QtGui.QPen(bd,1.5))
        p.drawRoundedRect(r,8,8)

# ========================= 统一持久化层 (option_sim.db) =========================
# Item4：废弃 GUI 自有 option_snapshots.db，持仓 / 盈亏 / 信号统一落到后端规范库
#        option_sim.db，使用与 core.py 完全一致的 sim_strategy / sim_strategy_leg /
#        sim_pnl_snapshot / signal_log 表，实现「单库单架构」。GUI 自有持仓的
#        strategy_id 统一以 'G' 前缀标识，可与后端策略共存于同一库。
def _sim_ensure_schema(conn):
    """在 option_sim.db 建立后端规范表（幂等），与 core.py _sim_create_tables 对齐。"""
    c=conn
    c.execute("""CREATE TABLE IF NOT EXISTS sim_strategy (
        strategy_id TEXT PRIMARY KEY, strategy_name TEXT, strategy_type TEXT,
        scope_text TEXT, status TEXT, open_time TEXT, close_time TEXT,
        open_premium REAL, close_premium REAL, realized_pnl REAL,
        unrealized_pnl REAL, open_proxy_vix REAL, note TEXT )""")
    strat_cols=[r[1] for r in c.execute("PRAGMA table_info(sim_strategy)").fetchall()]
    if "open_proxy_vix" not in strat_cols:
        c.execute("ALTER TABLE sim_strategy ADD COLUMN open_proxy_vix REAL")
    c.execute("""CREATE TABLE IF NOT EXISTS sim_strategy_leg (
        leg_id TEXT PRIMARY KEY, strategy_id TEXT, exchange TEXT, product TEXT,
        product_name TEXT, underlying_sym TEXT, option_sym TEXT, option_class TEXT,
        strike_price REAL, expire_rest_days REAL, side TEXT, volume INTEGER,
        multiplier REAL, open_price REAL, open_underlying_price REAL, close_price REAL,
        current_price REAL, delta REAL, gamma REAL, theta REAL, vega REAL, iv REAL,
        status TEXT )""")
    leg_cols=[r[1] for r in c.execute("PRAGMA table_info(sim_strategy_leg)").fetchall()]
    if "open_underlying_price" not in leg_cols:
        c.execute("ALTER TABLE sim_strategy_leg ADD COLUMN open_underlying_price REAL")
    c.execute("""CREATE TABLE IF NOT EXISTS sim_pnl_snapshot (
        snapshot_id TEXT PRIMARY KEY, strategy_id TEXT, snapshot_time TEXT, status TEXT,
        unrealized_pnl REAL, realized_pnl REAL, total_pnl REAL,
        open_premium REAL, close_premium REAL )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_strategy_status ON sim_strategy(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_leg_strategy ON sim_strategy_leg(strategy_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_pnl_strategy_time ON sim_pnl_snapshot(strategy_id,snapshot_time)")
    # signal_log：与后端规范一致 + GUI 专用扩展列（完整还原 SignalRecord 需要）
    c.execute("""CREATE TABLE IF NOT EXISTS signal_log (
        signal_id TEXT PRIMARY KEY, trigger_time TEXT NOT NULL, trigger_ts REAL NOT NULL,
        trade_date TEXT NOT NULL, exchange TEXT, product TEXT, name TEXT, underlying_sym TEXT,
        side TEXT NOT NULL, ratio TEXT NOT NULL, ratio_n INTEGER,
        buy_option_sym TEXT, sell_option_sym TEXT, buy_strike REAL, sell_strike REAL,
        buy_price REAL, sell_price REAL, buy_volume INTEGER, sell_volume INTEGER,
        net_premium REAL, atm_avg REAL, threshold REAL, strike_gap REAL,
        underlying_price REAL, proxy_vix REAL, product_volume INTEGER, days INTEGER,
        status TEXT NOT NULL, last_seen_ts REAL, expired_ts REAL,
        last_net_premium REAL, last_buy_price REAL, last_sell_price REAL,
        opened_strategy_id TEXT, opened_ts REAL )""")
    sig_cols=[r[1] for r in c.execute("PRAGMA table_info(signal_log)").fetchall()]
    for _col,_decl in (("opt_type","TEXT"),("vix_rank","INTEGER"),
                       ("near_vol","INTEGER"),("far_vol","INTEGER"),
                       ("near_vol_rank","INTEGER"),("far_vol_rank","INTEGER"),
                       ("multiplier","REAL")):
        if _col not in sig_cols:
            c.execute(f"ALTER TABLE signal_log ADD COLUMN {_col} {_decl}")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_trade_date ON signal_log(trade_date, trigger_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_status ON signal_log(status)")
    c.execute("CREATE TABLE IF NOT EXISTS gui_meta (key TEXT PRIMARY KEY, value TEXT)")
    # 手动订阅品种：GUI 输入强制订阅的品种。added_date=添加交易日；当天强制订阅
    # 期权链且不被重排淘汰。次日是否继续由"是否有未平模拟持仓"决定（见后台 _forced_now）。
    c.execute("""CREATE TABLE IF NOT EXISTS manual_subscribe (
        product TEXT PRIMARY KEY, added_date TEXT, added_ts REAL )""")
    conn.commit()


class _SignalStore:
    """Item3：比例价差信号落库 + 重启恢复。统一落在 option_sim.db 的 signal_log 表。"""
    def __init__(self):
        self._conn=None; self._lock=threading.Lock()
    def _ensure(self):
        if self._conn is None:
            self._conn=sqlite3.connect(DB_PATH_SIM, check_same_thread=False)
            _sim_ensure_schema(self._conn)
        return self._conn
    @staticmethod
    def _trade_date():
        return time.strftime("%Y-%m-%d")
    @staticmethod
    def _ratio_n(ratio):
        try:
            return int(str(ratio).split(":")[-1])
        except Exception:
            return 2
    def persist(self, sigs):
        if not sigs:
            return
        try:
            conn=self._ensure(); td=self._trade_date(); now=time.time()
            with self._lock:
                for sg in sigs:
                    sid=f"{td}_{sg.key}"
                    rn=self._ratio_n(sg.ratio)
                    net=float(sg.near_price or 0)-rn*float(sg.far_price or 0)
                    conn.execute(
                        "INSERT OR IGNORE INTO signal_log("
                        "signal_id,trigger_time,trigger_ts,trade_date,exchange,product,name,"
                        "underlying_sym,side,ratio,ratio_n,buy_option_sym,sell_option_sym,"
                        "buy_strike,sell_strike,buy_price,sell_price,buy_volume,sell_volume,"
                        "net_premium,threshold,strike_gap,underlying_price,proxy_vix,status,last_seen_ts,"
                        "opt_type,vix_rank,near_vol,far_vol,near_vol_rank,far_vol_rank,multiplier"
                        ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (sid, sg.ts, now, td, "", sg.product, sg.product_name, "",
                         sg.opt_type, sg.ratio, rn, sg.near_iid, sg.far_iid,
                         float(sg.near_strike or 0), float(sg.far_strike or 0),
                         float(sg.near_price or 0), float(sg.far_price or 0), 1, rn,
                         net, float(sg.gap_threshold or 0), float(sg.strike_gap or 0),
                         float(sg.underlying_price or 0),
                         (float(sg.vix) if sg.vix is not None else None),
                         "ACTIVE", now, sg.opt_type, int(sg.vix_rank or 0),
                         int(sg.near_vol or 0), int(sg.far_vol or 0),
                         int(sg.near_vol_rank or 0), int(sg.far_vol_rank or 0),
                         float(sg.multiplier or 0)))
                    conn.execute(
                        "UPDATE signal_log SET last_seen_ts=?, last_net_premium=?, "
                        "last_buy_price=?, last_sell_price=? WHERE signal_id=?",
                        (now, net, float(sg.near_price or 0), float(sg.far_price or 0), sid))
                conn.commit()
        except Exception:
            pass
    def load_today(self):
        out=[]
        try:
            conn=self._ensure(); td=self._trade_date()
            with self._lock:
                rows=conn.execute(
                    "SELECT trigger_time,product,name,vix_rank,proxy_vix,opt_type,"
                    "buy_option_sym,sell_option_sym,buy_strike,sell_strike,buy_price,sell_price,"
                    "ratio,strike_gap,threshold,near_vol,far_vol,near_vol_rank,far_vol_rank,"
                    "multiplier,underlying_price FROM signal_log "
                    "WHERE trade_date=? ORDER BY trigger_ts ASC", (td,)).fetchall()
            for r in rows:
                (ts,product,name,vrank,pvix,opt_type,b_iid,s_iid,b_k,s_k,b_px,s_px,
                 ratio,sgap,thr,nv,fv,nvr,fvr,mult,up)=r
                out.append(SignalRecord(
                    ts or "", product or "", name or product or "", int(vrank or 0),
                    pvix, opt_type or "CALL", b_iid or "", s_iid or "",
                    float(b_k or 0), float(s_k or 0), float(b_px or 0), float(s_px or 0),
                    ratio or "1:2", float(sgap or 0), float(thr or 0),
                    int(nv or 0), int(fv or 0), int(nvr or 0), int(fvr or 0),
                    float(mult or 0), float(up or 0)))
        except Exception:
            pass
        return out


_signal_store=_SignalStore()


class _ManualSubStore:
    """手动订阅品种落库：GUI 添加的强制订阅品种。统一落在 option_sim.db 的 manual_subscribe 表。
    当天添加的品种当日强制订阅期权链（重启同日仍生效）；是否跨日由后台按"未平持仓"判定。"""
    def __init__(self):
        self._conn=None; self._lock=threading.Lock()
    def _ensure(self):
        if self._conn is None:
            self._conn=sqlite3.connect(DB_PATH_SIM, check_same_thread=False)
            _sim_ensure_schema(self._conn)
        return self._conn
    @staticmethod
    def _trade_date():
        return time.strftime("%Y-%m-%d")
    def add(self, product):
        if not product:
            return
        try:
            conn=self._ensure()
            with self._lock:
                conn.execute(
                    "INSERT OR REPLACE INTO manual_subscribe(product,added_date,added_ts) "
                    "VALUES(?,?,?)", (product, self._trade_date(), time.time()))
                conn.commit()
        except Exception:
            pass
    def remove(self, product):
        try:
            conn=self._ensure()
            with self._lock:
                conn.execute("DELETE FROM manual_subscribe WHERE product=?", (product,))
                conn.commit()
        except Exception:
            pass
    def load_today(self):
        """返回当日添加的手动订阅品种 code 列表。"""
        out=[]
        try:
            conn=self._ensure(); td=self._trade_date()
            with self._lock:
                rows=conn.execute(
                    "SELECT product FROM manual_subscribe WHERE added_date=?", (td,)).fetchall()
            out=[r[0] for r in rows if r[0]]
        except Exception:
            pass
        return out


_manual_sub_store=_ManualSubStore()


class PositionPanel(QtWidgets.QWidget):
    """组合持仓：玻璃拟态卡片式，按批次分组，对价实时浮亏"""
    group_selected=QtCore.pyqtSignal(list, float, object, str)
    edit_group_requested=QtCore.pyqtSignal(int, list)  # 点击「编辑」→ (gid, 原腿快照) 载入篮子
    def __init__(self, parent=None):
        super().__init__(parent)
        self._groups=[]; self._cards={}; self._selected_gid=None
        self._atm_cache={}  # product -> 最近一次有效 ATM 均价（快照未含该品种时兜底，避免风险显示"--"）
        self._db_conn=None; self._db_lock=threading.Lock()
        self._pnl_history=deque(maxlen=300); self._last_pnl_persist_ts=0.0
        self._group_pnl_history={}
        self.setStyleSheet(f"background:{C.TABLE_BG};")
        lay=QtWidgets.QVBoxLayout(self); lay.setContentsMargins(8,6,8,4); lay.setSpacing(4)
        top=QtWidgets.QHBoxLayout(); top.setSpacing(6)
        title=QtWidgets.QLabel("组合持仓")
        title.setStyleSheet(f"color:{C.BRAND};font:bold 13px 'SimSun';")
        self.pnl_lbl=QtWidgets.QLabel("暂无持仓")
        self.pnl_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:11px 'SimSun';")
        self.close_all_btn=QtWidgets.QPushButton("全部平仓")
        self.close_all_btn.setFixedSize(72,26)
        self.close_all_btn.setStyleSheet(
            "QPushButton{background:#C62828;color:#fff;border:none;border-radius:4px;font:bold 11px 'SimSun';}"
            "QPushButton:hover{background:#B71C1C;} QPushButton:disabled{background:#D6C8C0;color:#888;}"
        )
        self.close_all_btn.setEnabled(False)
        self.close_all_btn.clicked.connect(self._close_all)
        top.addWidget(title); top.addWidget(self.pnl_lbl,1); top.addWidget(self.close_all_btn)
        lay.addLayout(top)
        sep=QtWidgets.QFrame(); sep.setFrameShape(QtWidgets.QFrame.HLine)
        sep.setStyleSheet(f"background:{C.BORDER_COLOR};"); sep.setFixedHeight(1)
        lay.addWidget(sep)
        self._scroll=QtWidgets.QScrollArea(); self._scroll.setWidgetResizable(True)
        self._scroll.setStyleSheet(
            f"QScrollArea{{border:none;background:{C.TABLE_BG};}}"
            f"QScrollBar:vertical{{background:{C.TABLE_ALT_ROW};width:7px;border-radius:3px;}}"
            f"QScrollBar::handle:vertical{{background:{C.BORDER_COLOR};border-radius:3px;min-height:18px;}}"
        )
        self._container=QtWidgets.QWidget()
        self._container.setStyleSheet(f"background:{C.TABLE_BG};")
        self._vlay=QtWidgets.QVBoxLayout(self._container)
        self._vlay.setContentsMargins(2,4,2,4); self._vlay.setSpacing(8)
        self._vlay.addStretch(1)
        self._scroll.setWidget(self._container)
        lay.addWidget(self._scroll,1)

        self._ensure_db()
        self._load_persisted_groups()
        self._load_pnl_history()
        self._rebuild_cards()
        self._update_summary()

    def _db_file_path(self):
        return DB_PATH_SIM

    def _gui_sid(self, gid):
        return f"G{int(gid)}"

    def _next_gid(self, conn):
        rows=conn.execute("SELECT strategy_id FROM sim_strategy WHERE strategy_id LIKE 'G%'").fetchall()
        mx=0
        for (sid,) in rows:
            try:
                n=int(str(sid)[1:])
                if n>mx: mx=n
            except Exception:
                pass
        return mx+1

    def _ensure_db(self):
        if self._db_conn:
            return self._db_conn
        conn=sqlite3.connect(self._db_file_path(), check_same_thread=False)
        _sim_ensure_schema(conn)
        self._db_conn=conn
        self._migrate_legacy_db(conn)
        return conn

    def _migrate_legacy_db(self, conn):
        """一次性迁移：旧 GUI 自有库 option_snapshots.db → 统一库 option_sim.db。幂等。"""
        try:
            with self._db_lock:
                done=conn.execute("SELECT value FROM gui_meta WHERE key='legacy_migrated'").fetchone()
            if done and done[0]=="1":
                return
            old_path=os.path.join(_BASE_DIR, "option_snapshots.db")
            if not os.path.exists(old_path):
                with self._db_lock:
                    conn.execute("INSERT OR REPLACE INTO gui_meta(key,value) VALUES('legacy_migrated','1')")
                    conn.commit()
                return
            old=sqlite3.connect(old_path)
            try:
                has=old.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='position_groups'").fetchone()
                if not has:
                    with self._db_lock:
                        conn.execute("INSERT OR REPLACE INTO gui_meta(key,value) VALUES('legacy_migrated','1')")
                        conn.commit()
                    return
                grows=old.execute(
                    "SELECT id,name,open_time,open_price,open_vix,status,closed_time,closed_pnl "
                    "FROM position_groups").fetchall()
                migrated=0
                with self._db_lock:
                    for gr in grows:
                        gid,name,ot,op,ov,st,ct,cp=gr
                        sid=self._gui_sid(gid)
                        if conn.execute("SELECT 1 FROM sim_strategy WHERE strategy_id=?", (sid,)).fetchone():
                            continue
                        status="CLOSED" if (st or "OPEN")=="CLOSED" else "OPEN"
                        conn.execute(
                            "INSERT INTO sim_strategy(strategy_id,strategy_name,strategy_type,scope_text,"
                            "status,open_time,close_time,open_premium,close_premium,realized_pnl,"
                            "unrealized_pnl,open_proxy_vix,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (sid, name or f"组合{int(gid):02d}", "RATIO_SPREAD", "",
                             status, ot or "-", ct or "", 0.0, 0.0,
                             float(cp or 0.0), 0.0, ov, "migrated"))
                        lrows=old.execute(
                            "SELECT iid,opt_type,strike,side,entry_price,qty,multiplier,product,exchange,sort_idx "
                            "FROM position_legs WHERE group_id=? ORDER BY sort_idx", (gid,)).fetchall()
                        for lr in lrows:
                            iid,opt_type,strike,side,ep,qty,mult,prod,exch,sidx=lr
                            leg_id=f"{sid}#{int(sidx or 0):03d}"
                            conn.execute(
                                "INSERT OR IGNORE INTO sim_strategy_leg(leg_id,strategy_id,exchange,product,"
                                "product_name,underlying_sym,option_sym,option_class,strike_price,"
                                "expire_rest_days,side,volume,multiplier,open_price,open_underlying_price,"
                                "close_price,current_price,delta,gamma,theta,vega,iv,status) "
                                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (leg_id, sid, exch or "", prod or "", "", "", iid or "",
                                 opt_type or "CALL", float(strike or 0), None, side or "BUY",
                                 int(qty or 1), float(mult or 1), float(ep or 0), float(op or 0),
                                 None, float(ep or 0), None, None, None, None, None, status))
                        migrated+=1
                    # 盈亏历史（限量恢复曲线）
                    try:
                        agg=old.execute("SELECT ts,total_open_pnl FROM position_pnl ORDER BY id DESC LIMIT 300").fetchall()
                        seq=0
                        for ts,v in reversed(agg):
                            seq+=1
                            conn.execute(
                                "INSERT OR IGNORE INTO sim_pnl_snapshot(snapshot_id,strategy_id,snapshot_time,"
                                "status,unrealized_pnl,realized_pnl,total_pnl,open_premium,close_premium) "
                                "VALUES(?,?,?,?,?,?,?,?,?)",
                                (f"__ALL__-mig-{seq}", "__ALL__", ts or "", "OPEN",
                                 float(v or 0), 0.0, float(v or 0), 0.0, 0.0))
                    except Exception:
                        pass
                    try:
                        for gr in grows:
                            gid=gr[0]; sid=self._gui_sid(gid)
                            gp=old.execute(
                                "SELECT ts,group_pnl FROM position_pnl_group WHERE group_id=? ORDER BY id DESC LIMIT 300",
                                (gid,)).fetchall()
                            seq=0
                            for ts,v in reversed(gp):
                                seq+=1
                                conn.execute(
                                    "INSERT OR IGNORE INTO sim_pnl_snapshot(snapshot_id,strategy_id,snapshot_time,"
                                    "status,unrealized_pnl,realized_pnl,total_pnl,open_premium,close_premium) "
                                    "VALUES(?,?,?,?,?,?,?,?,?)",
                                    (f"{sid}-mig-{seq}", sid, ts or "", "OPEN",
                                     float(v or 0), 0.0, float(v or 0), 0.0, 0.0))
                    except Exception:
                        pass
                    conn.execute("INSERT OR REPLACE INTO gui_meta(key,value) VALUES('legacy_migrated','1')")
                    conn.commit()
                try:
                    _log(f"[迁移] option_snapshots.db → option_sim.db 完成，迁移 {migrated} 个组合")
                except Exception:
                    pass
            finally:
                old.close()
        except Exception as e:
            try:
                _log(f"[迁移] 失败: {str(e)[:80]}")
            except Exception:
                pass

    def _load_persisted_groups(self):
        conn=self._ensure_db()
        with self._db_lock:
            rows=conn.execute(
                "SELECT strategy_id,strategy_name,open_time,open_proxy_vix,status,close_time,realized_pnl,scope_text "
                "FROM sim_strategy WHERE strategy_id LIKE 'G%' ORDER BY strategy_id DESC"
            ).fetchall()
            for r in rows:
                sid,name,ot,ov,st,ct,cp,scope=r
                try:
                    gid=int(str(sid)[1:])
                except Exception:
                    continue
                leg_rows=conn.execute(
                    "SELECT option_sym,option_class,strike_price,side,open_price,volume,multiplier,"
                    "product,exchange,current_price,open_underlying_price "
                    "FROM sim_strategy_leg WHERE strategy_id=? ORDER BY leg_id", (sid,)
                ).fetchall()
                legs=[]; open_price=0.0
                for lr in leg_rows:
                    iid,opt_type,strike,side,ep,qty,mult,prod,exch,curp,oup=lr
                    leg={
                        "iid":iid or "", "opt_type":opt_type or "CALL", "strike":float(strike or 0),
                        "side":side or "BUY", "entry_price":float(ep or 0), "qty":int(qty or 1),
                        "multiplier":float(mult or 1), "product":prod or "", "exchange":exch or ""
                    }
                    leg["cur_price"]=float(curp or 0) or leg["entry_price"]
                    leg["float_pnl"]=0.0
                    legs.append(leg)
                    if open_price==0.0 and oup:
                        open_price=float(oup or 0)
                prods=sorted(set(l.get("product","") for l in legs if l.get("product")))
                primary=prods[0] if prods else (scope or "")
                grp={
                    "id":int(gid), "name":name or f"组合{int(gid):02d}", "legs":legs,
                    "open_time":ot or "-", "open_price":open_price, "open_vix":ov,
                    "status":(st or "OPEN"), "closed_time":ct or "", "total_pnl":float(cp or 0.0),
                    "product":primary
                }
                self._groups.append(grp)
        self._publish_position_syms()

    def _publish_position_syms(self):
        """把当前 OPEN 持仓的合约代码发布到共享内存，供数据线程补订行情。
        修复：窗口外(非 VIX Top5 ATM±30)历史持仓无行情 → 浮盈亏永远冻结。"""
        syms=set()
        for grp in self._groups:
            if grp.get("status")=="CLOSED":
                continue
            for leg in grp.get("legs",[]):
                iid=leg.get("iid")
                if iid:
                    syms.add(iid)
        try:
            with _data_lock:
                _shm["position_syms"]=list(syms)
        except Exception:
            pass

    def _load_pnl_history(self):
        conn=self._ensure_db()
        with self._db_lock:
            rows=conn.execute(
                "SELECT total_pnl FROM sim_pnl_snapshot WHERE strategy_id='__ALL__' "
                "ORDER BY rowid DESC LIMIT 300").fetchall()
        vals=[float(r[0] or 0.0) for r in reversed(rows)]
        self._pnl_history.clear()
        self._pnl_history.extend(vals)
        self._group_pnl_history.clear()
        with self._db_lock:
            for g in self._groups:
                gid=int(g.get("id",0))
                if gid<=0:
                    continue
                sid=self._gui_sid(gid)
                grows=conn.execute(
                    "SELECT total_pnl FROM sim_pnl_snapshot WHERE strategy_id=? "
                    "ORDER BY rowid DESC LIMIT 300", (sid,)).fetchall()
                gvals=[float(r[0] or 0.0) for r in reversed(grows)]
                self._group_pnl_history[gid]=deque(gvals, maxlen=300)

    def _persist_open_group(self, grp):
        conn=self._ensure_db()
        with self._db_lock:
            gid=self._next_gid(conn)
            sid=self._gui_sid(gid)
            prods=sorted(set(l.get("product","") for l in grp.get("legs",[]) if l.get("product")))
            scope=",".join(prods)
            op=float(grp.get("open_price",0) or 0)
            prem=0.0
            for leg in grp.get("legs",[]):
                sgn=-1 if leg.get("side","BUY")=="BUY" else 1
                prem+=sgn*float(leg.get("entry_price",0) or 0)*float(leg.get("multiplier",1) or 1)*int(leg.get("qty",1) or 1)
            conn.execute(
                "INSERT INTO sim_strategy(strategy_id,strategy_name,strategy_type,scope_text,status,"
                "open_time,close_time,open_premium,close_premium,realized_pnl,unrealized_pnl,open_proxy_vix,note) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sid, grp.get("name",""), "RATIO_SPREAD", scope, "OPEN",
                 grp.get("open_time",""), "", prem, 0.0, 0.0, 0.0, grp.get("open_vix"), ""))
            for idx,leg in enumerate(grp.get("legs",[])):
                leg_id=f"{sid}#{idx:03d}"
                conn.execute(
                    "INSERT OR REPLACE INTO sim_strategy_leg(leg_id,strategy_id,exchange,product,product_name,"
                    "underlying_sym,option_sym,option_class,strike_price,expire_rest_days,side,volume,"
                    "multiplier,open_price,open_underlying_price,close_price,current_price,delta,gamma,"
                    "theta,vega,iv,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (leg_id, sid, leg.get("exchange",""), leg.get("product",""), "", "",
                     leg.get("iid",""), leg.get("opt_type","CALL"), float(leg.get("strike",0) or 0),
                     None, leg.get("side","BUY"), int(leg.get("qty",1) or 1),
                     float(leg.get("multiplier",1) or 1), float(leg.get("entry_price",0) or 0),
                     op, None, float(leg.get("entry_price",0) or 0), None, None, None, None, None, "OPEN"))
            conn.commit()
        return gid

    def _persist_close_group(self, grp):
        conn=self._ensure_db()
        sid=self._gui_sid(grp.get("id",0))
        pnl=float(grp.get("total_pnl",0) or 0)
        with self._db_lock:
            conn.execute(
                "UPDATE sim_strategy SET status='CLOSED', close_time=?, realized_pnl=?, unrealized_pnl=? "
                "WHERE strategy_id=?",
                (grp.get("closed_time",""), pnl, pnl, sid))
            conn.execute(
                "UPDATE sim_strategy_leg SET status='CLOSED', close_price=current_price WHERE strategy_id=?",
                (sid,))
            conn.commit()

    def _append_pnl_point(self, total_open_pnl, group_pnl_map=None):
        self._pnl_history.append(float(total_open_pnl or 0.0))
        for gid,gp in (group_pnl_map or {}).items():
            if gid not in self._group_pnl_history:
                self._group_pnl_history[gid]=deque(maxlen=300)
            self._group_pnl_history[gid].append(float(gp or 0.0))
        now=time.time()
        if (now-self._last_pnl_persist_ts)<1.0:
            return
        self._last_pnl_persist_ts=now
        conn=self._ensure_db()
        ts=time.strftime("%Y-%m-%d %H:%M:%S")
        self._pnl_seq=getattr(self,"_pnl_seq",0)+1
        tag=f"{int(now*1000)}-{self._pnl_seq}"
        with self._db_lock:
            conn.execute(
                "INSERT OR IGNORE INTO sim_pnl_snapshot(snapshot_id,strategy_id,snapshot_time,status,"
                "unrealized_pnl,realized_pnl,total_pnl,open_premium,close_premium) VALUES(?,?,?,?,?,?,?,?,?)",
                (f"__ALL__-{tag}", "__ALL__", ts, "OPEN",
                 float(total_open_pnl or 0.0), 0.0, float(total_open_pnl or 0.0), 0.0, 0.0))
            for gid,gp in (group_pnl_map or {}).items():
                if int(gid)<=0:
                    continue
                sid=self._gui_sid(gid)
                conn.execute(
                    "INSERT OR IGNORE INTO sim_pnl_snapshot(snapshot_id,strategy_id,snapshot_time,status,"
                    "unrealized_pnl,realized_pnl,total_pnl,open_premium,close_premium) VALUES(?,?,?,?,?,?,?,?,?)",
                    (f"{sid}-{tag}", sid, ts, "OPEN", float(gp or 0.0), 0.0, float(gp or 0.0), 0.0, 0.0))
            conn.execute(
                "DELETE FROM sim_pnl_snapshot WHERE strategy_id='__ALL__' AND snapshot_id NOT IN "
                "(SELECT snapshot_id FROM sim_pnl_snapshot WHERE strategy_id='__ALL__' ORDER BY rowid DESC LIMIT 5000)")
            conn.execute(
                "DELETE FROM sim_pnl_snapshot WHERE strategy_id!='__ALL__' AND snapshot_id NOT IN "
                "(SELECT snapshot_id FROM sim_pnl_snapshot WHERE strategy_id!='__ALL__' ORDER BY rowid DESC LIMIT 50000)")
            conn.commit()

    def get_pnl_history(self):
        if self._selected_gid is not None:
            vals=self._group_pnl_history.get(self._selected_gid)
            if vals:
                return list(vals)
        return list(self._pnl_history)

    def get_pnl_curve_label(self):
        if self._selected_gid is None:
            return "实时盈亏曲线（开仓组合汇总）"
        grp=next((g for g in self._groups if g.get("id")==self._selected_gid), None)
        if grp:
            return f"实时盈亏曲线（{grp.get('name','当前组合')}）"
        return "实时盈亏曲线（当前组合）"

    def _rebuild_cards(self):
        while self._vlay.count()>1:
            item=self._vlay.takeAt(0)
            w=item.widget()
            if w:
                w.deleteLater()
        self._cards.clear()
        ordered=sorted(self._groups, key=lambda g: (0 if g.get("status")!="CLOSED" else 1, -int(g.get("id",0))))
        for grp in ordered:
            card=_GlassCard(grp)
            card.close_req.connect(self._close_group)
            card.delete_req.connect(self._delete_group)
            card.select_req.connect(self._on_card_selected)
            card.edit_req.connect(self._on_edit_group)
            self._cards[grp["id"]]=card
            self._vlay.insertWidget(self._vlay.count()-1, card)
            if self._selected_gid is not None:
                card.set_selected(grp["id"]==self._selected_gid)

    def add_position_group(self, legs, open_price=0, open_vix=None):
        import time as _t
        gid=0
        prods=set(leg.get("product","?") for leg in legs)
        gname=f"组合·{'/ '.join(sorted(prods))}"
        primary_product=sorted(prods)[0] if prods else ""
        grp={"id":gid,"name":gname,"legs":[dict(l) for l in legs],
             "open_time":_t.strftime("%H:%M"),"open_price":open_price,
             "open_vix":open_vix,"status":"OPEN","closed_time":"","total_pnl":0.0,
             "product":primary_product}
        for leg in grp["legs"]:
            leg["cur_price"]=leg.get("entry_price",0) or 0; leg["float_pnl"]=0.0
        gid=self._persist_open_group(grp)
        grp["id"]=gid
        grp["name"]=f"组合{gid:02d}·{'/ '.join(sorted(prods))}"
        self._groups.append(grp)
        self._group_pnl_history[gid]=deque(maxlen=300)
        self._publish_position_syms()
        self._rebuild_cards()
        self.close_all_btn.setEnabled(True)
        self._update_summary()

    def _on_card_selected(self, gid):
        self._selected_gid=gid
        for g_id,card in self._cards.items(): card.set_selected(g_id==gid)
        grp=next((g for g in self._groups if g["id"]==gid), None)
        if grp: self.group_selected.emit(list(grp["legs"]), grp["open_price"], grp.get("open_vix"), grp.get("product",""))

    def clear_group_selection(self):
        """清除组合选中态：盈亏曲线回到「开仓组合汇总」，取消卡片高亮。
        构建/编辑新篮子时调用，避免右侧残留上一个已选组合的旧盈亏曲线。"""
        if self._selected_gid is None:
            return
        self._selected_gid=None
        for card in self._cards.values():
            card.set_selected(False)

    def _on_edit_group(self, gid):
        """点击卡片「编辑」→ 把该组合腿快照发给篮子进行加/减腿/改手数编辑。"""
        grp=next((g for g in self._groups if g["id"]==gid), None)
        if not grp or grp.get("status")=="CLOSED":
            return
        legs=[dict(l) for l in grp.get("legs",[])]
        self.edit_group_requested.emit(int(gid), legs)

    def update_group_legs(self, gid, new_legs):
        """保存编辑：用新腿集合替换组合的腿。原腿开仓数据（入场价/开仓时间/入场标的价）
        保留（编辑期间原腿入场价由篮子原样带回），新腿按当前市价入场。组合开仓字段不变。
        减到 0 腿时视为整组撤销（删除该组合）。"""
        grp=next((g for g in self._groups if g["id"]==gid), None)
        if not grp or grp.get("status")=="CLOSED":
            return
        legs=[dict(l) for l in (new_legs or [])]
        if not legs:
            # 全部删除 → 直接撤销该未平仓组合
            self._discard_group(gid)
            return
        for leg in legs:
            leg.setdefault("qty",1); leg.setdefault("multiplier",1)
            leg["cur_price"]=leg.get("cur_price", leg.get("entry_price",0) or 0) or (leg.get("entry_price",0) or 0)
            leg["float_pnl"]=leg.get("float_pnl",0.0)
        grp["legs"]=legs
        prods=sorted(set(l.get("product","") for l in legs if l.get("product")))
        if prods:
            grp["product"]=prods[0]
        self._persist_update_legs(grp)
        self._publish_position_syms()
        self._rebuild_cards()
        self._update_summary()

    def _persist_update_legs(self, grp):
        """重写该组合的 sim_strategy_leg（删旧插新），保持 sim_strategy 开仓字段不变。
        每条腿的 open_underlying_price 统一写组合 open_price，保证重启后 open_price 一致。"""
        conn=self._ensure_db()
        sid=self._gui_sid(grp.get("id",0))
        op=float(grp.get("open_price",0) or 0)
        with self._db_lock:
            conn.execute("DELETE FROM sim_strategy_leg WHERE strategy_id=?", (sid,))
            for idx,leg in enumerate(grp.get("legs",[])):
                leg_id=f"{sid}#{idx:03d}"
                conn.execute(
                    "INSERT OR REPLACE INTO sim_strategy_leg(leg_id,strategy_id,exchange,product,product_name,"
                    "underlying_sym,option_sym,option_class,strike_price,expire_rest_days,side,volume,"
                    "multiplier,open_price,open_underlying_price,close_price,current_price,delta,gamma,"
                    "theta,vega,iv,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (leg_id, sid, leg.get("exchange",""), leg.get("product",""), "", "",
                     leg.get("iid",""), leg.get("opt_type","CALL"), float(leg.get("strike",0) or 0),
                     None, leg.get("side","BUY"), int(leg.get("qty",1) or 1),
                     float(leg.get("multiplier",1) or 1), float(leg.get("entry_price",0) or 0),
                     op, None, float(leg.get("entry_price",0) or 0), None, None, None, None, None, "OPEN"))
            conn.commit()

    def _discard_group(self, gid):
        """编辑时删光所有腿 → 撤销该未平仓组合（内存+数据库彻底移除）。"""
        grp=next((g for g in self._groups if g["id"]==gid), None)
        if not grp:
            return
        self._groups.remove(grp)
        if self._selected_gid==gid:
            self._selected_gid=None
        conn=self._ensure_db()
        sid=self._gui_sid(gid)
        with self._db_lock:
            conn.execute("DELETE FROM sim_strategy_leg WHERE strategy_id=?", (sid,))
            conn.execute("DELETE FROM sim_strategy WHERE strategy_id=?", (sid,))
            conn.execute("DELETE FROM sim_pnl_snapshot WHERE strategy_id=?", (sid,))
            conn.commit()
        self._group_pnl_history.pop(gid, None)
        self._publish_position_syms()
        self._rebuild_cards()
        self._update_summary()

    def update_prices(self, tc, snapshot=None):
        if not self._groups:
            self._append_pnl_point(0.0,{})
            return
        total_open_pnl=0.0
        group_pnl_map={}
        for grp in self._groups:
            if grp.get("status")=="CLOSED":
                continue
            gp=0.0; leg_data=[]
            for leg in grp["legs"]:
                t=tc.get(leg.get("iid",""))
                if t:
                    side=leg.get("side","BUY")
                    cur=float(getattr(t,"bid_price1",None) or getattr(t,"last_price",0) or 0) if side=="BUY" else \
                        float(getattr(t,"ask_price1",None) or getattr(t,"last_price",0) or 0)
                    if cur>0:
                        leg["cur_price"]=cur; sign=1 if side=="BUY" else -1
                        leg["float_pnl"]=sign*(cur-leg.get("entry_price",0))*leg.get("multiplier",1)*leg.get("qty",1)
                gp+=leg.get("float_pnl",0)
                leg_data.append((leg.get("cur_price",0), leg.get("float_pnl",0)))
            grp["total_pnl"]=gp
            total_open_pnl+=gp
            group_pnl_map[int(grp.get("id",0))]=gp
            card=self._cards.get(grp["id"])
            if card:
                card.update_pnl(gp, leg_data)
                # 预估风险（动态实时）：用该组合品种近月 ATM Call/Put 现价
                prod=grp.get("product","") or (grp["legs"][0].get("product","") if grp["legs"] else "")
                atm_avg=_atm_avg_for_product(snapshot, prod)
                if atm_avg is not None and atm_avg>0:
                    self._atm_cache[prod]=atm_avg   # 缓存最近有效值
                elif prod in self._atm_cache:
                    atm_avg=self._atm_cache[prod]    # 快照暂无该品种 → 用缓存兜底
                risk,ok=_estimate_ratio_risk(grp["legs"], atm_avg)
                card.update_risk(risk, ok and (atm_avg is not None), atm_avg)
        self._append_pnl_point(total_open_pnl, group_pnl_map)
        self._update_summary()

    def _close_group(self, gid):
        grp=next((g for g in self._groups if g.get("id")==gid), None)
        if not grp or grp.get("status")=="CLOSED":
            return
        grp["status"]="CLOSED"
        grp["closed_time"]=time.strftime("%H:%M")
        self._persist_close_group(grp)
        if self._selected_gid==gid:
            self._selected_gid=None
        self._publish_position_syms()
        self._rebuild_cards()
        self.close_all_btn.setEnabled(any(g.get("status")!="CLOSED" for g in self._groups))
        self._update_summary()

    def _delete_group(self, gid):
        """删除已平仓记录：双击卡片触发，无确认弹窗，直接从内存列表和数据库中彻底移除"""
        grp=next((g for g in self._groups if g.get("id")==gid), None)
        if not grp or grp.get("status")!="CLOSED":
            return
        self._groups.remove(grp)
        if self._selected_gid==gid:
            self._selected_gid=None
        conn=self._ensure_db()
        sid=self._gui_sid(gid)
        with self._db_lock:
            conn.execute("DELETE FROM sim_strategy_leg WHERE strategy_id=?", (sid,))
            conn.execute("DELETE FROM sim_strategy WHERE strategy_id=?", (sid,))
            conn.execute("DELETE FROM sim_pnl_snapshot WHERE strategy_id=?", (sid,))
            conn.commit()
        self._group_pnl_history.pop(gid, None)
        self._publish_position_syms()
        self._rebuild_cards()
        self._update_summary()

    def _close_all(self):
        changed=False
        for grp in self._groups:
            if grp.get("status")!="CLOSED":
                grp["status"]="CLOSED"
                grp["closed_time"]=time.strftime("%H:%M")
                self._persist_close_group(grp)
                changed=True
        if changed:
            self._publish_position_syms()
            self._rebuild_cards()
        self._selected_gid=None
        self.close_all_btn.setEnabled(False)
        self._update_summary()

    def _update_summary(self):
        open_groups=[g for g in self._groups if g.get("status")!="CLOSED"]
        closed_groups=[g for g in self._groups if g.get("status")=="CLOSED"]
        total=sum(g.get("total_pnl",0) for g in open_groups)
        n_legs=sum(len(g.get("legs",[])) for g in open_groups)
        self.close_all_btn.setEnabled(bool(open_groups))
        if self._groups:
            c="#2E7D32" if total>=0 else "#C62828"
            self.pnl_lbl.setText(
                f"开仓 {len(open_groups)}组/{n_legs}腿 · 已平 {len(closed_groups)}组 · 开仓浮盈亏 {total:+.2f}元")
            self.pnl_lbl.setStyleSheet(f"color:{c};font:bold 11px 'SimSun';")
        else:
            self.pnl_lbl.setText("暂无持仓 · 组合篮子中点击「模拟开仓」")
            self.pnl_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:11px 'SimSun';")

class BasketPanel(QtWidgets.QWidget):
    """组合篮子面板：T型表勾选 → 腿列表，QSpinBox 调整手数，模拟开仓记录损益基准"""
    basket_changed=QtCore.pyqtSignal(list)   # 篮子变化时通知 Hub 更新图表
    open_position=QtCore.pyqtSignal(list)    # 点击模拟开仓时，传出当前腿快照
    save_edit=QtCore.pyqtSignal(int, list)   # 编辑模式「保存修改」：(gid, 新腿集合)
    _OPEN_STYLE=("QPushButton{background:#2E7D32;color:#fff;border:none;border-radius:4px;font:bold 11px 'SimSun';}"
                 "QPushButton:hover{background:#388E3C;} QPushButton:disabled{background:#555;color:#aaa;}")
    _SAVE_STYLE=("QPushButton{background:#1565C0;color:#fff;border:none;border-radius:4px;font:bold 11px 'SimSun';}"
                 "QPushButton:hover{background:#0D47A1;} QPushButton:disabled{background:#555;color:#aaa;}")

    def __init__(self, parent=None):
        super().__init__(parent)
        self._basket={}  # {iid: leg_dict}  — GUI 线程维护
        self._edit_gid=None  # None=新建组合模式；否则为正在编辑的组合 id
        self._last_snapshot=None  # 缓存最近快照，用于加/减腿、改手数时立即重算风险
        lay=QtWidgets.QVBoxLayout(self); lay.setContentsMargins(6,4,6,4); lay.setSpacing(4)
        top=QtWidgets.QHBoxLayout(); top.setSpacing(6)
        title=QtWidgets.QLabel("组合篮子"); title.setStyleSheet(f"color:{C.BRAND};font:bold 13px 'SimSun';")
        self.summary_lbl=QtWidgets.QLabel("篮子为空"); self.summary_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:11px 'SimSun';")
        self.open_btn=QtWidgets.QPushButton("模拟开仓")
        self.open_btn.setFixedHeight(26); self.open_btn.setFixedWidth(78)
        self.open_btn.setStyleSheet(
            f"QPushButton{{background:#2E7D32;color:#fff;border:none;border-radius:4px;font:bold 11px 'SimSun';}}"
            f"QPushButton:hover{{background:#388E3C;}} QPushButton:disabled{{background:#555;color:#aaa;}}"
        )
        self.open_btn.setStyleSheet(self._OPEN_STYLE)
        self.open_btn.setEnabled(False)
        self.open_btn.clicked.connect(self._on_action_btn)
        self.clear_btn=QtWidgets.QPushButton("清空")
        self.clear_btn.setFixedHeight(26); self.clear_btn.setFixedWidth(48)
        self.clear_btn.setStyleSheet(f"QPushButton{{background:{C.TOP_BAR_BG};color:{C.TEXT_MID};border:1px solid {C.BORDER_COLOR};border-radius:3px;font:11px 'SimSun';}}")
        self.clear_btn.clicked.connect(self._clear)
        top.addWidget(title); top.addWidget(self.summary_lbl,1); top.addWidget(self.open_btn); top.addWidget(self.clear_btn)
        lay.addLayout(top)
        # 编辑模式横幅（默认隐藏）：显示正在编辑哪个组合，提供「取消编辑」
        self.edit_bar=QtWidgets.QWidget()
        eb=QtWidgets.QHBoxLayout(self.edit_bar); eb.setContentsMargins(0,0,0,0); eb.setSpacing(6)
        self.edit_lbl=QtWidgets.QLabel("")
        self.edit_lbl.setStyleSheet("color:#1565C0;font:bold 11px 'SimSun';background:transparent;")
        self.cancel_edit_btn=QtWidgets.QPushButton("取消编辑")
        self.cancel_edit_btn.setFixedHeight(22)
        self.cancel_edit_btn.setStyleSheet(f"QPushButton{{background:{C.TOP_BAR_BG};color:{C.TEXT_MID};border:1px solid {C.BORDER_COLOR};border-radius:3px;font:10px 'SimSun';}}")
        self.cancel_edit_btn.clicked.connect(self._exit_edit_mode)
        eb.addWidget(self.edit_lbl,1); eb.addWidget(self.cancel_edit_btn)
        self.edit_bar.setVisible(False)
        lay.addWidget(self.edit_bar)
        # 预估风险（比例价差·动态实时，不落库）
        self.risk_lbl=QtWidgets.QLabel("")
        self.risk_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:11px 'SimSun';")
        lay.addWidget(self.risk_lbl)
        self.table=QtWidgets.QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(["方向","手数","合约","C/P","行权价","入场价","删"])
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(28)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Fixed)
        for i,w in enumerate([54,54,130,36,64,64,36]): self.table.setColumnWidth(i,w)
        self.table.setStyleSheet(
            f"QTableWidget{{background:{C.TABLE_BG};color:{C.TEXT_DARK};gridline-color:{C.GRID_LINE};font:12px 'SimSun';}}"
            f"QHeaderView::section{{background:{C.TOP_BAR_BG};color:{C.TEXT_MID};border:none;border-right:1px solid {C.GRID_LINE};border-bottom:1px solid {C.BORDER_COLOR};font:bold 10px 'SimSun';padding:3px;}}"
            f"QSpinBox{{background:{C.TABLE_BG};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};border-radius:3px;font:11px 'SimSun';}}"
            f"QPushButton{{background:{C.TOP_BAR_BG};color:{C.TEXT_MID};border:1px solid {C.BORDER_COLOR};border-radius:3px;font:11px 'SimSun';}}"
        )
        lay.addWidget(self.table,1)

    def add_leg(self, leg):
        """勾选框选中时加入篮子；iid 已存在则保留手数"""
        iid=leg["iid"]
        if iid in self._basket:
            leg["qty"]=self._basket[iid].get("qty",1)
        self._basket[iid]=leg
        self._render(); self._refresh_risk(); self.basket_changed.emit(self.get_legs())

    def remove_leg(self, iid):
        """勾选框取消时移除篮子对应腿"""
        self._basket.pop(iid, None)
        self._render(); self._refresh_risk(); self.basket_changed.emit(self.get_legs())

    def get_legs(self):
        return list(self._basket.values())

    def _on_action_btn(self):
        """主按钮：新建模式=模拟开仓；编辑模式=保存修改（把新腿集合回写组合）。"""
        legs=self.get_legs()
        if self._edit_gid is not None:
            self.save_edit.emit(int(self._edit_gid), legs)
            self._exit_edit_mode()
        elif legs:
            self.open_position.emit(legs)

    def load_for_edit(self, gid, legs):
        """从组合持仓「编辑」进入：把原组合腿载入篮子。原腿入场价/手数原样带入，
        之后勾选 T 表可加新腿（新腿按当前市价入场），可删腿、改手数，点「保存修改」回写。"""
        self._edit_gid=int(gid)
        self._basket={}
        for leg in (legs or []):
            l=dict(leg); iid=l.get("iid")
            if not iid:
                continue
            l.setdefault("qty",1); l.setdefault("multiplier",1)
            self._basket[iid]=l
        self.open_btn.setText("保存修改")
        self.open_btn.setStyleSheet(self._SAVE_STYLE)
        self.edit_lbl.setText(f"正在编辑 组合#{int(gid):02d}：加/减腿、改手数后点「保存修改」（原腿入场价保留）")
        self.edit_bar.setVisible(True)
        self._render(); self._refresh_risk(); self.basket_changed.emit(self.get_legs())

    def _exit_edit_mode(self):
        """退出编辑模式（保存后或点「取消编辑」）：恢复新建模式并清空篮子。"""
        self._edit_gid=None
        self._basket={}
        self.open_btn.setText("模拟开仓")
        self.open_btn.setStyleSheet(self._OPEN_STYLE)
        self.edit_bar.setVisible(False)
        self.risk_lbl.setText("")
        self._render(); self.basket_changed.emit([])

    def _refresh_risk(self):
        """腿/手数变动时立即用缓存快照重算风险，不等 600ms 定时器。"""
        self.update_risk(self._last_snapshot)

    def update_risk(self, snapshot):
        """比例价差预估风险（动态实时）：用篮子品种近月 ATM Call/Put 现价刷新。"""
        if snapshot:
            self._last_snapshot=snapshot  # 缓存供本地即时重算使用
        legs=self.get_legs()
        if not legs:
            self.risk_lbl.setText("")
            return
        prod=legs[0].get("product","")
        atm_avg=_atm_avg_for_product(snapshot, prod)
        risk,ok=_estimate_ratio_risk(legs, atm_avg)
        if not ok or atm_avg is None:
            self.risk_lbl.setText("预估风险 --（无ATM行情）")
            self.risk_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:11px 'SimSun';")
            return
        rc="#C62828" if risk<0 else "#2E7D32"
        self.risk_lbl.setText(f"预估风险 {risk:+.2f}元   (ATM均价 {atm_avg:.2f})")
        self.risk_lbl.setStyleSheet(f"color:{rc};font:bold 11px 'SimSun';")

    def _clear(self):
        self._basket.clear(); self._render(); self.basket_changed.emit([]); self.risk_lbl.setText("")

    def _set_qty(self, iid, qty):
        if iid in self._basket:
            self._basket[iid]["qty"]=int(qty)
            self._refresh_summary(); self._refresh_risk()  # 改手数→立即刷新净权利金+风险
            self.basket_changed.emit(self.get_legs())

    def _remove(self, iid):
        self._basket.pop(iid,None); self._render(); self._refresh_risk(); self.basket_changed.emit(self.get_legs())

    def _render(self):
        legs=self.get_legs(); self.table.setRowCount(len(legs))
        # 编辑模式下即使 0 腿也允许「保存修改」（保存空=撤销该组合）
        self.open_btn.setEnabled(bool(legs) or self._edit_gid is not None)
        for ri,leg in enumerate(legs):
            iid=leg["iid"]
            # 方向：固定标签（已由 T型表勾选决定，无需下拉）
            side=leg.get("side","BUY")
            side_lbl=QtWidgets.QLabel("买入" if side=="BUY" else "卖出")
            side_lbl.setAlignment(QtCore.Qt.AlignCenter)
            side_lbl.setStyleSheet(f"color:{'#1565C0' if side=='BUY' else '#C62828'};font:bold 12px 'SimSun';background:{C.TABLE_BG};")
            self.table.setCellWidget(ri,0,side_lbl)
            spin=QtWidgets.QSpinBox(); spin.setRange(1,999); spin.setFixedHeight(24)
            spin.setValue(int(leg.get("qty",1)))
            spin.valueChanged.connect(lambda v,k=iid: self._set_qty(k,v))
            self.table.setCellWidget(ri,1,spin)
            short=iid.split(".")[-1] if "." in iid else iid
            opt_c="C" if leg.get("opt_type")=="CALL" else "P"
            ep_val=leg.get("entry_price",0) or 0
            for ci,val in enumerate([short,opt_c,f"{leg.get('strike',0):.0f}",f"{ep_val:.2f}"],2):
                it=QtWidgets.QTableWidgetItem(str(val)); it.setTextAlignment(QtCore.Qt.AlignCenter)
                self.table.setItem(ri,ci,it)
            del_btn=QtWidgets.QPushButton("✕"); del_btn.setFixedHeight(24)
            del_btn.clicked.connect(lambda ch,k=iid: self._remove(k))
            self.table.setCellWidget(ri,6,del_btn)
        self._refresh_summary()

    def _refresh_summary(self):
        """只刷新净权利金汇总标签（改手数时不重建整表，避免打断输入焦点）。"""
        legs=self.get_legs(); net=0.0
        for leg in legs:
            side=leg.get("side","BUY"); ep_val=leg.get("entry_price",0) or 0
            net+=(-1 if side=="BUY" else 1)*ep_val*leg.get("multiplier",1)*leg.get("qty",1)
        if legs:
            self.summary_lbl.setText(f"{len(legs)}腿 | 净权利金 {net:+.2f}元")
        else:
            self.summary_lbl.setText("篮子为空 · 勾选T表买C/卖C/买P/卖P添加")

# ========================= 护眼暖色确认弹窗 =========================
class ConfirmDialog(QtWidgets.QDialog):
    """暖色护眼风格的确认对话框，替代系统默认 QMessageBox.question。
    与 GUI 主题(class C)统一：圆角卡片 + 投影 + 暖色按钮。
    用法: ConfirmDialog.ask(parent, title, message, ok_text, cancel_text, danger) -> bool"""

    def __init__(self, title, message, ok_text="确认", cancel_text="取消", danger=False, parent=None):
        super().__init__(parent, QtCore.Qt.Dialog | QtCore.Qt.FramelessWindowHint)
        self.setModal(True)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground, True)
        accent = "#C62828" if danger else C.BRAND
        ok_hover = "#9A2222" if danger else "#7B5B4C"

        outer = QtWidgets.QVBoxLayout(self)
        outer.setContentsMargins(22, 22, 22, 22)   # 留白给投影

        card = QtWidgets.QFrame(); card.setObjectName("confirmCard")
        card.setStyleSheet(
            f"QFrame#confirmCard{{background:{C.WIN_BG};border:1px solid {C.BORDER_COLOR};border-radius:12px;}}"
            f"QLabel{{background:transparent;}}"
        )
        eff = QtWidgets.QGraphicsDropShadowEffect(self)
        eff.setBlurRadius(30); eff.setOffset(0, 6)
        eff.setColor(QtGui.QColor(62, 39, 35, 100))
        card.setGraphicsEffect(eff)
        outer.addWidget(card)

        v = QtWidgets.QVBoxLayout(card)
        v.setContentsMargins(0, 0, 0, 0); v.setSpacing(0)

        # 顶部标题栏
        header = QtWidgets.QFrame(); header.setObjectName("cdHdr")
        header.setStyleSheet(
            f"QFrame#cdHdr{{background:{C.TOP_BAR_BG};border-top-left-radius:12px;"
            f"border-top-right-radius:12px;border-bottom:1px solid {C.BORDER_COLOR};}}"
        )
        hl = QtWidgets.QHBoxLayout(header); hl.setContentsMargins(16, 12, 16, 12); hl.setSpacing(10)
        icon = QtWidgets.QLabel("⚠" if danger else "❓")
        icon.setStyleSheet(f"color:{accent};font:bold 16px 'SimSun';")
        ttl = QtWidgets.QLabel(title)
        ttl.setStyleSheet(f"color:{accent};font:bold 14px 'SimSun';")
        hl.addWidget(icon); hl.addWidget(ttl); hl.addStretch(1)
        v.addWidget(header)

        # 正文
        body = QtWidgets.QLabel(message)
        body.setWordWrap(True)
        body.setTextFormat(QtCore.Qt.PlainText)
        body.setStyleSheet(f"color:{C.TEXT_DARK};font:13px 'SimSun';padding:18px 20px;")
        v.addWidget(body)

        # 底部按钮栏
        footer = QtWidgets.QFrame(); footer.setObjectName("cdFtr")
        footer.setStyleSheet(
            f"QFrame#cdFtr{{background:{C.TABLE_BG};border-top:1px solid {C.GRID_LINE};"
            f"border-bottom-left-radius:12px;border-bottom-right-radius:12px;}}"
        )
        fl = QtWidgets.QHBoxLayout(footer); fl.setContentsMargins(16, 12, 16, 12); fl.setSpacing(10)
        fl.addStretch(1)
        cancel_btn = QtWidgets.QPushButton(cancel_text)
        cancel_btn.setCursor(QtCore.Qt.PointingHandCursor)
        cancel_btn.setStyleSheet(
            f"QPushButton{{background:{C.TABLE_ALT_ROW};color:{C.TEXT_MID};"
            f"border:1px solid {C.BORDER_COLOR};border-radius:6px;padding:7px 18px;"
            f"font:bold 12px 'SimSun';min-width:72px;}}"
            f"QPushButton:hover{{background:{C.ATM_HIGHLIGHT};}}"
        )
        ok_btn = QtWidgets.QPushButton(ok_text)
        ok_btn.setCursor(QtCore.Qt.PointingHandCursor)
        ok_btn.setStyleSheet(
            f"QPushButton{{background:{accent};color:#FDFBF7;border:none;border-radius:6px;"
            f"padding:7px 18px;font:bold 12px 'SimSun';min-width:72px;}}"
            f"QPushButton:hover{{background:{ok_hover};}}"
        )
        cancel_btn.clicked.connect(self.reject)
        ok_btn.clicked.connect(self.accept)
        ok_btn.setDefault(True)
        fl.addWidget(cancel_btn); fl.addWidget(ok_btn)
        v.addWidget(footer)

        self.setMinimumWidth(380)

    def showEvent(self, e):
        super().showEvent(e)
        par = self.parent()
        if par is not None:
            try:
                pg = par.frameGeometry()
                c = pg.center()
                self.move(c.x() - self.width() // 2, c.y() - self.height() // 2)
            except Exception:
                pass

    @classmethod
    def ask(cls, parent, title, message, ok_text="确认", cancel_text="取消", danger=False):
        dlg = cls(title, message, ok_text, cancel_text, danger, parent)
        return dlg.exec_() == QtWidgets.QDialog.Accepted

class DoubleClickButton(QtWidgets.QPushButton):
    """需双击才触发的按钮（防误触，且不弹确认框）。单击仅视觉按下、不触发动作。"""
    doubleClicked=QtCore.pyqtSignal()
    def mouseDoubleClickEvent(self, e):
        super().mouseDoubleClickEvent(e)
        if e.button()==QtCore.Qt.LeftButton:
            self.doubleClicked.emit()


# ========================= 信号监控面板 =========================
class SignalAlertDialog(QtWidgets.QDialog):
    """非模态弹窗 - 显示一条触发信号的完整详情 + 损益图 + 操作按钮。可堆叠。"""
    open_position = QtCore.pyqtSignal(object)   # SignalRecord
    load_basket   = QtCore.pyqtSignal(object)   # SignalRecord
    _open_dialogs = []

    def __init__(self, sig, on_view_panel=None, parent=None):
        super().__init__(parent,
            QtCore.Qt.Window|QtCore.Qt.WindowStaysOnTopHint|QtCore.Qt.WindowCloseButtonHint)
        self.sig=sig; self._on_view_panel=on_view_panel
        self.setModal(False)
        self.setAttribute(QtCore.Qt.WA_DeleteOnClose, True)
        dir_tag="CALL" if sig.opt_type=="CALL" else "PUT"
        self.setWindowTitle(f"比例价差信号  [{sig.product_name}({sig.product})  {dir_tag}  VIX#{sig.vix_rank}]")
        self.setMinimumSize(720, 480)
        self.resize(820, 520)
        self._build_ui()
        self._apply_position()
        SignalAlertDialog._open_dialogs.append(self)
        try:
            self.destroyed.connect(
                lambda *_: SignalAlertDialog._open_dialogs.remove(self)
                if self in SignalAlertDialog._open_dialogs else None)
        except Exception: pass

    def _build_ui(self):
        """方案7：顶部标题栏 + 中部左信息右图表分栏 + 底部按钮"""
        sig=self.sig
        layout=QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0,0,0,0); layout.setSpacing(0)
        self.setStyleSheet(f"""
            QDialog   {{background:{C.WIN_BG};}}
            QLabel    {{background:transparent;}}
            QFrame#infoPanel {{background:{C.TABLE_BG};border:none;
                border-right:1px solid {C.BORDER_COLOR};}}
            QFrame#chartPanel {{background:{C.TABLE_BG};border:none;}}
            QFrame#headerBar  {{background:{C.TOP_BAR_BG};border-bottom:1px solid {C.BORDER_COLOR};}}
            QFrame#footerBar  {{background:{C.TOP_BAR_BG};border-top:1px solid {C.BORDER_COLOR};}}
            QFrame.secBlock   {{background:{C.TABLE_ALT_ROW};border-radius:5px;}}
            QPushButton {{background:{C.TABLE_ALT_ROW};color:{C.TEXT_DARK};
                border:1px solid {C.BORDER_COLOR};border-radius:5px;
                padding:6px 14px;font:bold 12px 'SimSun';min-width:76px;}}
            QPushButton:hover {{background:{C.ATM_HIGHLIGHT};}}
            QPushButton#primaryBtn {{background:{C.BRAND};color:#FDFBF7;border:none;}}
            QPushButton#primaryBtn:hover {{background:#7B5B4C;}}
        """)

        dir_tag ="CALL" if sig.opt_type=="CALL" else "PUT"
        dir_col ="#1565C0" if sig.opt_type=="CALL" else "#C62828"
        net     = sig.far_price*2 - sig.near_price
        ncol    ="#2E7D32" if net>=0 else "#C62828"
        net_bg  ="#E8F5E9" if net>=0 else "#FFEBEE"
        gap_ok  = sig.strike_gap > sig.gap_threshold
        gap_col ="#2E7D32" if gap_ok else "#C62828"
        near_s  = sig.near_iid.split(".")[-1] if "." in sig.near_iid else sig.near_iid
        far_s   = sig.far_iid.split(".")[-1]  if "." in sig.far_iid  else sig.far_iid

        # ═══════════════════════════════════════
        # ① 顶部标题栏
        # ═══════════════════════════════════════
        header=QtWidgets.QFrame(); header.setObjectName("headerBar")
        hlay=QtWidgets.QHBoxLayout(header)
        hlay.setContentsMargins(14,9,14,9); hlay.setSpacing(10)

        ttl=QtWidgets.QLabel(f"比例价差信号  [{sig.product_name}({sig.product})]")
        ttl.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")

        dir_badge=QtWidgets.QLabel(f"  {dir_tag}  1:2  ")
        dir_badge.setStyleSheet(
            f"color:white;background:{dir_col};border-radius:4px;"
            f"padding:2px 6px;font:bold 12px 'Consolas';"
        )
        vix_badge=QtWidgets.QLabel(f"VIX#{sig.vix_rank}  {sig.vix*100:.1f}%")
        vix_badge.setStyleSheet(
            f"color:{C.BRAND};background:{C.TABLE_ALT_ROW};"
            f"border:1px solid {C.BORDER_COLOR};border-radius:4px;"
            f"padding:2px 8px;font:10px 'Consolas';"
        )
        hlay.addWidget(ttl)
        hlay.addWidget(dir_badge)
        hlay.addStretch(1)
        hlay.addWidget(vix_badge)
        layout.addWidget(header)

        # ═══════════════════════════════════════
        # ② 中部主体：左信息 | 右图表
        # ═══════════════════════════════════════
        body=QtWidgets.QWidget(); body.setStyleSheet(f"background:{C.TABLE_BG};")
        body_lay=QtWidgets.QHBoxLayout(body)
        body_lay.setContentsMargins(0,0,0,0); body_lay.setSpacing(0)

        # ─── 左侧信息面板 ───
        info_panel=QtWidgets.QFrame(); info_panel.setObjectName("infoPanel")
        info_panel.setFixedWidth(310)
        ip=QtWidgets.QVBoxLayout(info_panel)
        ip.setContentsMargins(14,12,14,12); ip.setSpacing(10)

        def _sep():
            f=QtWidgets.QFrame(); f.setFrameShape(QtWidgets.QFrame.HLine)
            f.setStyleSheet(f"background:{C.BORDER_COLOR};"); f.setFixedHeight(1)
            return f

        def _sec_title(txt):
            lb=QtWidgets.QLabel(txt)
            lb.setStyleSheet(
                f"color:{C.BRAND};font:bold 11px 'SimSun';"
                f"border-left:3px solid {C.BRAND};padding-left:6px;"
            )
            return lb

        def _kv_row(k, v, vcol=None):
            row=QtWidgets.QHBoxLayout(); row.setSpacing(6)
            kl=QtWidgets.QLabel(k)
            kl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'SimSun';")
            kl.setFixedWidth(70)
            vl=QtWidgets.QLabel(str(v))
            vl.setStyleSheet(f"color:{vcol or C.TEXT_DARK};font:bold 11px 'Consolas','SimSun';")
            row.addWidget(kl); row.addWidget(vl); row.addStretch(1)
            return row

        # 基本信息组
        ip.addWidget(_sec_title("基本信息"))
        ip.addLayout(_kv_row("触发时间",  sig.ts))
        ip.addLayout(_kv_row("代理 VIX",  f"{sig.vix*100:.2f}%  (第{sig.vix_rank}名)", C.BRAND))
        ip.addLayout(_kv_row("标的价格",  f"{sig.underlying_price:.2f}"))

        # 钱龙红绿柱方向（偏多/偏空）：红柱→偏多(红底) / 绿柱→偏空(绿底)
        try:
            with _data_lock:
                _qd = (_shm.get("qianlong", {}) or {}).get(sig.product) or {}
        except Exception:
            _qd = {}
        ql_bias = _qd.get("bias", ""); ql_color = _qd.get("color", "")
        ql_row = QtWidgets.QHBoxLayout(); ql_row.setSpacing(6)
        ql_k = QtWidgets.QLabel("钱龙方向")
        ql_k.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'SimSun';")
        ql_k.setFixedWidth(70)
        ql_v = QtWidgets.QLabel(ql_bias or "—")
        if ql_bias:
            _qfg, _qbg = (C.PRICE_UP, "#FCE4EC") if ql_color == "red" else (C.PRICE_DOWN, "#E3F2E8")
            ql_v.setStyleSheet(
                f"color:{_qfg};background:{_qbg};border-radius:9px;"
                f"padding:1px 10px;font:bold 11px 'SimSun';")
        else:
            ql_v.setStyleSheet(f"color:{C.TEXT_LIGHT};font:11px 'SimSun';")
        ql_row.addWidget(ql_k); ql_row.addWidget(ql_v); ql_row.addStretch(1)
        ip.addLayout(ql_row)

        ip.addWidget(_sep())

        # 两腿明细组
        ip.addWidget(_sec_title("两腿明细"))
        legs_data=[
            ("买入×1", near_s, sig.near_strike, "ask", sig.near_price, sig.near_vol, sig.near_vol_rank, "#1565C0", "#E3F2FD"),
            ("卖出×2", far_s,  sig.far_strike,  "bid", sig.far_price,  sig.far_vol,  sig.far_vol_rank,  "#C62828", "#FFEBEE"),
        ]
        for side_t,iid_s,k,px_t,px,vol,vrank,scol,sbg in legs_data:
            leg_block=QtWidgets.QFrame()
            leg_block.setStyleSheet(
                f"QFrame{{background:{sbg};border-radius:5px;border:1px solid {scol}22;}}"
            )
            lb=QtWidgets.QVBoxLayout(leg_block)
            lb.setContentsMargins(8,5,8,5); lb.setSpacing(2)

            top_r=QtWidgets.QHBoxLayout(); top_r.setSpacing(6)
            side_lbl=QtWidgets.QLabel(side_t)
            side_lbl.setStyleSheet(
                f"color:{scol};background:rgba(255,255,255,0.6);"
                f"border:1px solid {scol};border-radius:4px;"
                f"padding:1px 6px;font:bold 11px 'SimSun';"
            )
            iid_lbl=QtWidgets.QLabel(iid_s)
            iid_lbl.setStyleSheet(f"color:{C.TEXT_DARK};font:11px 'Consolas';")
            top_r.addWidget(side_lbl); top_r.addWidget(iid_lbl); top_r.addStretch(1)
            lb.addLayout(top_r)

            bot_r=QtWidgets.QHBoxLayout(); bot_r.setSpacing(12)
            k_lbl=QtWidgets.QLabel(f"K = {k:.0f}")
            k_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'Consolas';")
            px_lbl=QtWidgets.QLabel(f"{px_t} = {px:.2f}")
            px_lbl.setStyleSheet(f"color:{scol};font:bold 11px 'Consolas';")
            vol_lbl=QtWidgets.QLabel(f"量 {vol:,}  OTM前{vrank}")
            vol_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'SimSun';")
            bot_r.addWidget(k_lbl); bot_r.addWidget(px_lbl); bot_r.addWidget(vol_lbl)
            bot_r.addStretch(1)
            lb.addLayout(bot_r)
            ip.addWidget(leg_block)

        ip.addWidget(_sep())

        # 过滤条件组
        ip.addWidget(_sec_title("过滤条件"))

        cond_A=QtWidgets.QFrame()
        cond_A.setStyleSheet(f"QFrame{{background:{net_bg};border-radius:4px;border:1px solid {ncol}44;}}")
        ca=QtWidgets.QVBoxLayout(cond_A); ca.setContentsMargins(8,5,8,5); ca.setSpacing(2)
        ca_title=QtWidgets.QLabel("A.  净权利金 ≥ 0")
        ca_title.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'SimSun';")
        ca_formula=QtWidgets.QLabel(
            f"卖×2 − 买 = {sig.far_price:.2f}×2 − {sig.near_price:.2f} = "
        )
        ca_formula.setStyleSheet(f"color:{C.TEXT_DARK};font:10px 'Consolas';")
        ca_result=QtWidgets.QLabel(f"{net:+.2f}  {'✓ 满足' if net>=0 else '✗ 不满足'}")
        ca_result.setStyleSheet(f"color:{ncol};font:bold 11px 'Consolas';")
        ca.addWidget(ca_title); ca.addWidget(ca_formula); ca.addWidget(ca_result)
        ip.addWidget(cond_A)

        cond_B=QtWidgets.QFrame()
        gbg="#E8F5E9" if gap_ok else "#FFEBEE"
        cond_B.setStyleSheet(f"QFrame{{background:{gbg};border-radius:4px;border:1px solid {gap_col}44;}}")
        cb=QtWidgets.QVBoxLayout(cond_B); cb.setContentsMargins(8,5,8,5); cb.setSpacing(2)
        cb_title=QtWidgets.QLabel("B.  行权价间距 > ATM跨式价 ÷ 2")
        cb_title.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'SimSun';")
        cb_formula=QtWidgets.QLabel(
            f"间距 {sig.strike_gap:.0f}  >  {sig.gap_threshold:.1f}    价比 {sig.ratio:.2f}x"
        )
        cb_formula.setStyleSheet(f"color:{C.TEXT_DARK};font:10px 'Consolas';")
        cb_result=QtWidgets.QLabel(f"{'✓ 满足' if gap_ok else '✗ 不满足'}")
        cb_result.setStyleSheet(f"color:{gap_col};font:bold 11px 'Consolas';")
        cb.addWidget(cb_title); cb.addWidget(cb_formula); cb.addWidget(cb_result)
        ip.addWidget(cond_B)

        ip.addStretch(1)

        # ─── 右侧图表面板 ───
        chart_panel=QtWidgets.QFrame(); chart_panel.setObjectName("chartPanel")
        cp=QtWidgets.QVBoxLayout(chart_panel)
        cp.setContentsMargins(12,12,12,12); cp.setSpacing(6)

        ct_lbl=QtWidgets.QLabel("到期损益结构")
        ct_lbl.setStyleSheet(f"color:{C.BRAND};font:bold 12px 'SimSun';")
        cp.addWidget(ct_lbl)

        self.payoff_chart=PayoffChart()
        self.payoff_chart.setMinimumSize(260, 200)
        try:
            cur_px=sig.underlying_price if sig.underlying_price>0 else sig.near_strike
            self.payoff_chart.set_basket(self._build_legs(sig), cur_px)
        except Exception: pass
        cp.addWidget(self.payoff_chart, 1)

        body_lay.addWidget(info_panel)
        body_lay.addWidget(chart_panel, 1)
        layout.addWidget(body, 1)

        # ═══════════════════════════════════════
        # ③ 底部按钮栏
        # ═══════════════════════════════════════
        footer=QtWidgets.QFrame(); footer.setObjectName("footerBar")
        flay=QtWidgets.QHBoxLayout(footer)
        flay.setContentsMargins(14,8,14,8); flay.setSpacing(8)

        self.open_btn=DoubleClickButton("双击开仓 (BUY×1 / SELL×2)")
        self.open_btn.setObjectName("primaryBtn")
        self.open_btn.setToolTip("双击此按钮直接开仓（无确认框）")
        self.open_btn.doubleClicked.connect(self._on_open)
        self.basket_btn=QtWidgets.QPushButton("载入篮子")
        self.basket_btn.clicked.connect(self._on_basket)
        self.panel_btn=QtWidgets.QPushButton("查看面板")
        self.panel_btn.clicked.connect(self._on_panel)
        self.close_btn=QtWidgets.QPushButton("关闭")
        self.close_btn.clicked.connect(self.close)

        flay.addWidget(self.open_btn)
        flay.addWidget(self.basket_btn)
        flay.addWidget(self.panel_btn)
        flay.addStretch(1)
        flay.addWidget(self.close_btn)
        layout.addWidget(footer)

    @staticmethod
    def _build_legs(sig):
        m=sig.multiplier
        return [
            {"iid":sig.near_iid,"opt_type":sig.opt_type,"strike":sig.near_strike,
             "side":"BUY","entry_price":sig.near_price,"qty":1,"multiplier":m,"product":sig.product},
            {"iid":sig.far_iid,"opt_type":sig.opt_type,"strike":sig.far_strike,
             "side":"SELL","entry_price":sig.far_price,"qty":2,"multiplier":m,"product":sig.product},
        ]


    def _on_open(self):
        self.open_position.emit(self.sig); self.close()

    def _on_basket(self):
        self.load_basket.emit(self.sig); self.close()

    def _on_panel(self):
        if self._on_view_panel:
            try: self._on_view_panel()
            except Exception: pass

    def _apply_position(self):
        try:
            screen=QtWidgets.QApplication.primaryScreen().availableGeometry()
            w=self.width() or 700; h=self.height() or 620
            base_x=screen.right()-w-20; base_y=screen.bottom()-h-60
            offset=(len(SignalAlertDialog._open_dialogs)%5)*30
            self.move(base_x-offset, base_y-offset)
        except Exception: pass


def _mk_vline(parent=None):
    """竖向分隔线，用于扫描状态栏"""
    f=QtWidgets.QFrame(parent)
    f.setFrameShape(QtWidgets.QFrame.VLine)
    f.setFixedWidth(1)
    f.setStyleSheet(f"background:{C.BORDER_COLOR};border:none;")
    return f

class _ScanBadge(QtWidgets.QWidget):
    """扫描状态栏单个指标小片：[标签] [数值]"""
    def __init__(self, label, value, text_col, bg_col, parent=None):
        super().__init__(parent)
        self._text_col=text_col; self._bg_col=bg_col
        lay=QtWidgets.QHBoxLayout(self); lay.setContentsMargins(5,2,5,2); lay.setSpacing(3)
        lbl=QtWidgets.QLabel(label)
        lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:9px 'SimSun';background:transparent;")
        self._val=QtWidgets.QLabel(value)
        self._val.setStyleSheet(
            f"color:{text_col};background:{bg_col};border-radius:3px;"
            f"padding:1px 5px;font:bold 10px 'Consolas';"
        )
        lay.addWidget(lbl); lay.addWidget(self._val)
        self.setStyleSheet("background:transparent;")
    def set_value(self, v):
        self._val.setText(v)


class _SignalCard(QtWidgets.QFrame):
    open_req=QtCore.pyqtSignal(object)
    basket_req=QtCore.pyqtSignal(object)
    view_req=QtCore.pyqtSignal(object)

    _BG_CALL_SEL  = QtGui.QLinearGradient(0,0,0,1)
    _BG_PUT_SEL   = QtGui.QLinearGradient(0,0,0,1)

    def __init__(self, sig, parent=None):
        super().__init__(parent)
        self._sig=sig
        self.setCursor(QtCore.Qt.PointingHandCursor)
        self.setStyleSheet(
            f"QFrame{{background:#FFFEFB;border:1px solid {C.GRID_LINE};border-radius:9px;}}"
            f"QFrame:hover{{background:#FFF8F0;border:1px solid {C.BRAND};}}"
            f"QLabel{{background:transparent;}}"
            f"QPushButton#openBtn{{background:{C.BRAND};color:#fff;border:none;border-radius:4px;"
            f"padding:3px 10px;font:bold 11px 'SimSun';}}"
            f"QPushButton#openBtn:hover{{background:#7B5B4C;}}"
            f"QPushButton#auxBtn{{background:{C.TABLE_ALT_ROW};color:{C.TEXT_DARK};"
            f"border:1px solid {C.BORDER_COLOR};border-radius:4px;"
            f"padding:3px 10px;font:11px 'SimSun';}}"
            f"QPushButton#auxBtn:hover{{background:{C.BORDER_COLOR};}}"
        )
        sh=QtWidgets.QGraphicsDropShadowEffect(self)
        sh.setBlurRadius(10); sh.setOffset(0,2); sh.setColor(QtGui.QColor(100,60,30,28))
        self.setGraphicsEffect(sh)

        lay=QtWidgets.QVBoxLayout(self)
        lay.setContentsMargins(12,9,12,9); lay.setSpacing(0)

        dir_tag="CALL" if sig.opt_type=="CALL" else "PUT"
        dir_col="#1565C0" if sig.opt_type=="CALL" else "#C62828"
        net=sig.far_price*2-sig.near_price
        ncol="#2E7D32" if net>=0 else "#C62828"
        net_bg="#E8F5E9" if net>=0 else "#FFEBEE"

        # ── 卡片顶栏 ──
        row1=QtWidgets.QHBoxLayout(); row1.setSpacing(8)
        # 品种+方向标签
        prod_lbl=QtWidgets.QLabel(f"{sig.product_name}({sig.product})")
        prod_lbl.setStyleSheet(f"color:{C.BRAND};font:bold 13px 'SimSun';")
        dir_lbl=QtWidgets.QLabel(f" {dir_tag} 1:2 ")
        dir_lbl.setStyleSheet(
            f"color:white;background:{dir_col};border-radius:3px;"
            f"padding:1px 5px;font:bold 10px 'Consolas';"
        )
        row1.addWidget(prod_lbl)
        row1.addWidget(dir_lbl)
        row1.addStretch(1)
        # VIX 排名气泡
        vix_lbl=QtWidgets.QLabel(f"VIX#{sig.vix_rank}  {sig.vix*100:.1f}%")
        vix_lbl.setStyleSheet(
            f"color:{C.BRAND};background:{C.TABLE_ALT_ROW};"
            f"border:1px solid {C.BORDER_COLOR};border-radius:3px;"
            f"padding:1px 6px;font:10px 'Consolas';"
        )
        # 时间
        ts_lbl=QtWidgets.QLabel(sig.ts)
        ts_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'Consolas';")
        row1.addWidget(vix_lbl); row1.addWidget(ts_lbl)
        lay.addLayout(row1)
        lay.addSpacing(6)

        # ── 分隔线 ──
        sep=QtWidgets.QFrame(); sep.setFrameShape(QtWidgets.QFrame.HLine)
        sep.setStyleSheet(f"background:{C.GRID_LINE};"); sep.setFixedHeight(1)
        lay.addWidget(sep)
        lay.addSpacing(5)

        # ── 两腿详情 ──
        near_s=sig.near_iid.split(".")[-1] if "." in sig.near_iid else sig.near_iid
        far_s =sig.far_iid.split(".")[-1]  if "." in sig.far_iid  else sig.far_iid
        legs=[
            ("买 1手", near_s, sig.near_strike, sig.near_price, sig.near_vol, "#1565C0", "#E3F2FD"),
            ("卖 2手", far_s,  sig.far_strike,  sig.far_price,  sig.far_vol,  "#C62828", "#FFEBEE"),
        ]
        for side_txt,short,k,px,vol,scol,sbg in legs:
            rr=QtWidgets.QHBoxLayout(); rr.setSpacing(6)
            sw=QtWidgets.QLabel(side_txt)
            sw.setFixedWidth(40); sw.setAlignment(QtCore.Qt.AlignCenter)
            sw.setStyleSheet(
                f"color:{scol};background:{sbg};border-radius:3px;"
                f"padding:2px 2px;font:bold 10px 'SimSun';"
            )
            c1=QtWidgets.QLabel(short)
            c1.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'SimSun';")
            c1.setMinimumWidth(100)
            c2=QtWidgets.QLabel(f"K {k:.0f}")
            c2.setStyleSheet(f"color:{C.TEXT_DARK};font:10px 'Consolas';")
            c2.setFixedWidth(60)
            c3=QtWidgets.QLabel(f"价  {px:.2f}")
            c3.setStyleSheet(f"color:{scol};font:bold 10px 'Consolas';")
            c3.setFixedWidth(68)
            c4=QtWidgets.QLabel(f"量 {vol:,}")
            c4.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'Consolas';")
            for w in (sw,c1,c2,c3,c4): rr.addWidget(w)
            rr.addStretch(1)
            rw=QtWidgets.QWidget(); rw.setLayout(rr); rw.setStyleSheet("background:transparent;")
            lay.addWidget(rw)
            lay.addSpacing(2)

        lay.addSpacing(4)
        sep2=QtWidgets.QFrame(); sep2.setFrameShape(QtWidgets.QFrame.HLine)
        sep2.setStyleSheet(f"background:{C.GRID_LINE};"); sep2.setFixedHeight(1)
        lay.addWidget(sep2)
        lay.addSpacing(5)

        # ── 底部：[时间] [净权利金]  →  右侧 [详情] [加入篮子] [开仓] ──
        row_bot=QtWidgets.QHBoxLayout(); row_bot.setSpacing(6)
        # 时间 + 条件摘要（左侧）
        ts_bot=QtWidgets.QLabel(sig.ts)
        ts_bot.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'Consolas';")
        cond_lbl=QtWidgets.QLabel(
            f"价比 {sig.ratio:.2f}x  间距 {sig.strike_gap:.0f}/{sig.gap_threshold:.1f}  标的 {sig.underlying_price:.0f}"
        )
        cond_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:10px 'Consolas';")
        # 净权利金气泡
        net_lbl=QtWidgets.QLabel(f"净权利金 {net:+.2f}")
        net_lbl.setStyleSheet(
            f"color:{ncol};background:{net_bg};border:1px solid {ncol};"
            f"border-radius:4px;padding:2px 7px;font:bold 10px 'Consolas';"
        )
        row_bot.addWidget(cond_lbl)
        row_bot.addWidget(net_lbl)
        row_bot.addStretch(1)
        # 三个操作按钮（右侧，紧凑）
        vb=QtWidgets.QPushButton("详情");   vb.setObjectName("auxBtn"); vb.setFixedHeight(24)
        bb=QtWidgets.QPushButton("加入篮子"); bb.setObjectName("auxBtn"); bb.setFixedHeight(24)
        ob=QtWidgets.QPushButton("开仓");   ob.setObjectName("openBtn"); ob.setFixedHeight(24)
        vb.clicked.connect(lambda: self.view_req.emit(self._sig))
        bb.clicked.connect(lambda: self.basket_req.emit(self._sig))
        ob.clicked.connect(lambda: self.open_req.emit(self._sig))
        row_bot.addWidget(vb); row_bot.addWidget(bb); row_bot.addWidget(ob)
        lay.addLayout(row_bot)

    def mouseDoubleClickEvent(self, event):
        if event.button()==QtCore.Qt.LeftButton:
            self.view_req.emit(self._sig)
        super().mouseDoubleClickEvent(event)


class SignalWindow(QtWidgets.QWidget):
    """信号监控面板（卡片式）：每条信号与组合持仓一致的卡片展示。"""
    load_basket   = QtCore.pyqtSignal(object)
    open_position = QtCore.pyqtSignal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumWidth(480)
        self._history=[]; self._seen_keys=set()
        self._build_ui()
        self.restore_persisted()

    def restore_persisted(self):
        """Item3：重启后从 signal_log 恢复当日已检测信号（仅展示，不再弹窗提醒）。"""
        try:
            sigs=_signal_store.load_today()
        except Exception:
            sigs=[]
        if not sigs:
            return
        for sg in sigs:
            if sg.key in self._seen_keys:
                continue
            self._seen_keys.add(sg.key)
            self._history.insert(0, sg)
        if len(self._history)>200:
            self._history=self._history[:200]
        self._refresh_cards()
        self._summary.setText(f"共 {len(self._history)} 条（已恢复 {len(sigs)}）")

    def _build_ui(self):
        self.setStyleSheet(f"background:{C.TABLE_BG};")
        lay=QtWidgets.QVBoxLayout(self); lay.setContentsMargins(10,8,10,6); lay.setSpacing(0)

        # ── 顶部标题栏 ──
        top=QtWidgets.QHBoxLayout(); top.setSpacing(8)
        title=QtWidgets.QLabel("比例价差信号监控")
        title.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")
        self._summary=QtWidgets.QLabel("等待信号...")
        self._summary.setStyleSheet(f"color:{C.TEXT_MID};font:11px 'SimSun';")
        clr=QtWidgets.QPushButton("清空")
        clr.setFixedSize(52,26)
        clr.setStyleSheet(
            f"QPushButton{{background:{C.TOP_BAR_BG};color:{C.TEXT_MID};"
            f"border:1px solid {C.BORDER_COLOR};border-radius:4px;font:11px 'SimSun';}}"
            f"QPushButton:hover{{background:{C.BORDER_COLOR};}}"
        )
        clr.clicked.connect(self._clear)
        top.addWidget(title); top.addWidget(self._summary,1); top.addWidget(clr)
        lay.addLayout(top)
        lay.addSpacing(8)

        # ── 扫描状态栏（带指标小片）──
        scan_frame=QtWidgets.QFrame()
        scan_frame.setStyleSheet(
            f"QFrame{{background:{C.TOP_BAR_BG};border:1px solid {C.BORDER_COLOR};"
            f"border-radius:6px;padding:0px;}}"
        )
        scan_lay=QtWidgets.QHBoxLayout(scan_frame)
        scan_lay.setContentsMargins(10,5,10,5); scan_lay.setSpacing(6)
        ts_lbl=QtWidgets.QLabel("扫描时间")
        ts_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:9px 'Consolas';")
        self._scan_ts=ts_lbl
        scan_lay.addWidget(ts_lbl)
        scan_lay.addWidget(_mk_vline())
        # 命中数
        self._badge_hit=_ScanBadge("命中","0","#2E7D32","#E8F5E9")
        scan_lay.addWidget(self._badge_hit)
        # 候选数
        self._badge_cand=_ScanBadge("候选C/P","0/0",C.BRAND,C.TABLE_ALT_ROW)
        scan_lay.addWidget(self._badge_cand)
        # 配对数
        self._badge_pair=_ScanBadge("配对C/P","0/0","#5D4037","#EFEBE9")
        scan_lay.addWidget(self._badge_pair)
        # 过滤数
        self._badge_rej=_ScanBadge("负净値过滤","0","#BF360C","#FBE9E7")
        scan_lay.addWidget(self._badge_rej)
        scan_lay.addStretch(1)
        # Top5 品种
        self._top5_lbl=QtWidgets.QLabel("扫描品种 —")
        self._top5_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:9px 'Consolas';")
        scan_lay.addWidget(self._top5_lbl)
        lay.addWidget(scan_frame)
        lay.addSpacing(8)

        sep=QtWidgets.QFrame(); sep.setFrameShape(QtWidgets.QFrame.HLine)
        sep.setStyleSheet(f"background:{C.BORDER_COLOR};"); sep.setFixedHeight(1)
        lay.addWidget(sep)
        lay.addSpacing(4)

        self._scroll=QtWidgets.QScrollArea(); self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QtWidgets.QFrame.NoFrame)
        self._scroll.setStyleSheet(
            f"QScrollArea{{border:none;background:{C.TABLE_BG};}}"
            f"QScrollBar:vertical{{background:{C.TABLE_ALT_ROW};width:7px;border-radius:3px;}}"
            f"QScrollBar::handle:vertical{{background:{C.BORDER_COLOR};border-radius:3px;min-height:18px;}}"
            f"QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical{{height:0px;}}"
        )
        self._container=QtWidgets.QWidget(); self._container.setStyleSheet(f"background:{C.TABLE_BG};")
        self._vlay=QtWidgets.QVBoxLayout(self._container)
        self._vlay.setContentsMargins(2,4,2,4); self._vlay.setSpacing(10)
        self._vlay.addStretch(1)
        self._scroll.setWidget(self._container)
        lay.addWidget(self._scroll,1)

    def update_scan_dbg(self, dbg):
        if not dbg:
            return
        top5=",".join(dbg.get("top5",[])) or "—"
        hits=int(dbg.get("hits",0))
        oc=int(dbg.get("otm_call",0)); op_=int(dbg.get("otm_put",0))
        cp=int(dbg.get("call_pairs",0)); pp=int(dbg.get("put_pairs",0))
        rej=int(dbg.get("net_reject",0))
        ncode=len(dbg.get("top5",[]))
        self._scan_ts.setText(dbg.get("ts","--:--:--"))
        self._badge_hit.set_value(str(hits))
        self._badge_cand.set_value(f"{oc}/{op_}")
        self._badge_pair.set_value(f"{cp}/{pp}")
        self._badge_rej.set_value(str(rej))
        self._top5_lbl.setText(f"扫描{ncode}品种 「{top5}」")

    def push_signals(self, new_signals):
        truly_new=[sg for sg in new_signals if sg.key not in self._seen_keys]
        if not truly_new:
            return []
        for sg in truly_new:
            self._seen_keys.add(sg.key); self._history.insert(0,sg)
        if len(self._history)>200:
            self._history=self._history[:200]
        self._refresh_cards()
        self._summary.setText(f"共 {len(self._history)} 条  最近: {truly_new[0].ts}")
        return truly_new

    def _refresh_cards(self):
        while self._vlay.count()>1:
            item=self._vlay.takeAt(0)
            w=item.widget()
            if w:
                w.deleteLater()
        for sg in self._history:
            card=_SignalCard(sg)
            card.open_req.connect(self.open_position)
            card.basket_req.connect(self.load_basket)
            card.view_req.connect(self._open_detail)
            self._vlay.insertWidget(self._vlay.count()-1, card)

    def _open_detail(self, sg):
        try:
            dlg=SignalAlertDialog(sg, on_view_panel=self.show_and_raise, parent=self)
            dlg.open_position.connect(self.open_position)
            dlg.load_basket.connect(self.load_basket)
            dlg.show()
        except Exception:
            pass

    def _clear(self):
        self._history.clear(); self._seen_keys.clear()
        self._refresh_cards(); self._summary.setText("等待信号...")

    def show_and_raise(self):
        p=self.parent()
        if p and hasattr(p, "focus_signal_panel"):
            try:
                p.focus_signal_panel()
            except Exception:
                pass
        self.setFocus(QtCore.Qt.OtherFocusReason)

# ========================= GUI 桥接 =========================
_hub_lock=threading.Lock();_hub=None;_gui_thread=None
_gui_status_msg=""
_data_lock=threading.Lock()
_shm={"vix30_fe":{},"vix30_tc":{},"vix30_sidebar":{},"vix30_current":"","tc":{},"pc":{},"fe":{},"ci":{},"info":{},"sidebar_stats":{},"iv_map":{},"iv_open_map":{},"iv_sources":{},"status_msg":"","basket":[],"pnl_history":[],"signals":[],"signal_scan_dbg":{},"snapshot":{},"_snapshot_sig":None,"position_syms":[],"qianlong":{}}
_basket_lock=threading.Lock()   # 保护跨线程 basket 读写
_basket_pending=[]              # GUI线程 → 策略线程的篮子快照（整体替换）
_pending_product=""  # 跨线程品种切换信号
_pending_lock=threading.Lock()
_pending_products=[]  # GUI 未就绪时，缓存左侧品种列表
_pending_products_lock=threading.Lock()
_manual_add_pending=[]  # GUI → 后台：待强制订阅的品种 code 队列
_manual_add_lock=threading.Lock()

def _reset_gui_shared_memory(status="等待行情"):
    with _data_lock:
        for key in ("tc","pc","fe","iv_map","iv_open_map","iv_sources","sidebar_stats"):
            _shm[key].clear()
        _shm["info"].clear()
        _shm["signal_scan_dbg"].clear()
        _shm["status_msg"]=status or ""

class Hub(QtCore.QObject):
    sig_add_products = QtCore.pyqtSignal(list)  # [(code, name, underlying), ...]
    sig_manual_result = QtCore.pyqtSignal(str)  # 手动订阅结果反馈文本（后台线程→GUI）

    def __init__(self, window):
        super().__init__()
        self.window = window
        self.window.product_selected.connect(self._on_product_selected)
        self.window.sidebar.refresh_clicked.connect(self._on_refresh_clicked)
        self.window.sidebar.manual_subscribe_requested.connect(self._on_manual_subscribe)
        self.sig_manual_result.connect(self._on_manual_result)
        self.sig_add_products.connect(self._on_add_products)
        self.window.table.leg_clicked.connect(self._on_leg_click)
        self.window.table.leg_removed.connect(self._on_leg_remove)
        self.window.basket_panel.basket_changed.connect(self._on_basket_changed)
        self.window.basket_panel.open_position.connect(self._on_open_position)
        self.window.basket_panel.save_edit.connect(self._on_save_edit)
        self.window.position_panel.group_selected.connect(self._on_group_selected)
        self.window.position_panel.edit_group_requested.connect(self._on_edit_group_requested)
        self.window.signal_panel.load_basket.connect(self._on_signal_load_basket)
        self.window.signal_panel.open_position.connect(self._on_signal_open_position)
        self._render_sig = None  # 上一帧渲染签名，用于跳过无变化帧
        self._last_ver = -1      # 上一帧数据版本号，用于整帧跳过
        self._last_trade_date = None  # 交易日变更检测，变更时清空信号面板
        self._timer=QtCore.QTimer(self);self._timer.timeout.connect(self._render);self._timer.setInterval(600);self._timer.start()
        with _pending_products_lock:
            pending=list(_pending_products)
            _pending_products.clear()
        if pending:
            self._on_add_products(pending)

    def _on_refresh_clicked(self):
        pass

    def _on_manual_subscribe(self, code):
        """GUI 手动订阅请求 → 写入跨线程队列，由后台数据线程处理。"""
        code=(code or "").strip()
        if not code:
            return
        with _manual_add_lock:
            if code not in _manual_add_pending:
                _manual_add_pending.append(code)

    def _on_manual_result(self, msg):
        """后台线程回传的手动订阅结果 → 显示在侧栏提示条。"""
        try:
            self.window.sidebar.show_message(msg)
        except Exception:
            pass

    def _on_product_selected(self, code):
        global _pending_product
        with _pending_lock: _pending_product=code
        # 如果点击的是 VIX30 品种，立即切换 T 型表显示
        with _data_lock:
            if code in _shm.get("vix30_fe", {}):
                _shm["vix30_current"] = code
                _shm["_ver"] = _shm.get("_ver", 0) + 1  # 触发下一帧渲染（切换品种）
        self.window.sidebar.set_active(code)  # 立即高亮卡片，无需等待数据刷新

    def _on_add_products(self, products):
        """在 Qt 线程添加品种列表项"""
        self.window.sidebar.set_products(products)
        if products:
            self.window.sidebar.set_active(products[0][0])

    def _on_leg_click(self, iid, opt_type, strike, side, ref_price):
        """T型表勾选买C/卖C/买P/卖P → 构造 leg → 加入篮子（入场价用对价：买用ask1、卖用bid1）"""
        with _data_lock:
            info=dict(_shm.get("info",{})); iv_map=dict(_shm.get("iv_map",{})); tc=dict(_shm.get("tc",{}))
        exchange=info.get("exchange","SHFE"); product=info.get("product_code","")
        mult=_get_multiplier(exchange, product)
        iv=iv_map.get(iid)
        t=tc.get(iid)
        if side=="BUY":
            entry_price=float(getattr(t,"ask_price1",None) or getattr(t,"last_price",0) or 0) if t else ref_price
        else:
            entry_price=float(getattr(t,"bid_price1",None) or getattr(t,"last_price",0) or 0) if t else ref_price
        if entry_price<=0: entry_price=ref_price
        leg={"iid":iid,"opt_type":opt_type,"strike":strike,"side":side,
             "entry_price":entry_price,"qty":1,"multiplier":mult,"iv":iv,
             "product":product,"exchange":exchange}
        self.window.basket_panel.add_leg(leg)

    def _on_leg_remove(self, iid):
        """T型表取消勾选 → 篮子删除该腔"""
        self.window.basket_panel.remove_leg(iid)

    def _on_group_selected(self, legs, open_price, open_vix, product_code):
        """点击组合持仓中某个组 → 展示该组的到期损益图"""
        with _data_lock:
            info=dict(_shm.get("info",{}))
            sidebar_stats=dict(_shm.get("sidebar_stats",{}))
        # 使用该组合品种的实时价，而非当前主界面品种
        if product_code and product_code in sidebar_stats:
            up=sidebar_stats[product_code].get("price",0) or 0
        else:
            up=info.get("underlying_price",0) or 0
        self.window.payoff_chart.set_basket(legs, up)
        self.window.payoff_chart.set_entry_overlay(open_price, open_vix)
        self.window.payoff_chart.set_selected_product(product_code)

    def _on_basket_changed(self, legs):
        """篮子变化 → 更新到期损益图 + 同步 T型表勾选框状态。
        构建/编辑新篮子属于「未开仓」的理论视图：必须清除上一次已选组合残留的
        开仓叠加线(紫色开仓价/入场VIX)与盈亏曲线，避免右侧显示旧数据。"""
        with _data_lock:
            info=dict(_shm.get("info",{}))
            sidebar_stats=dict(_shm.get("sidebar_stats",{}))
            _shm["basket"]=list(legs)
        # 篮子所属品种（用于取正确的实时标的价/VIX；空篮子回落到主界面品种）
        prod=(legs[0].get("product","") if legs else "") or None
        up=info.get("underlying_price",0) or 0
        if prod and prod in sidebar_stats:
            up=sidebar_stats[prod].get("price",0) or up
        # 清除上一个已选组合的开仓叠加线 + 组合选中态（盈亏曲线回到汇总）
        self.window.payoff_chart.set_entry_overlay(None, None)
        self.window.payoff_chart.set_selected_product(prod)
        self.window.position_panel.clear_group_selection()
        self.window.payoff_chart.set_basket(legs, up)
        basket_dict={leg["iid"]:leg for leg in legs}
        self.window.table.sync_basket_checkboxes(basket_dict)

    def _on_open_position(self, legs):
        """模拟开仓：1. 写入组合持仓面板  2. 更新损益图入场 overlay"""
        with _data_lock:
            info=dict(_shm.get("info",{}))
        entry_price=info.get("underlying_price",0) or 0
        entry_vix=info.get("vix_raw")
        self.window.position_panel.add_position_group(legs, open_price=entry_price, open_vix=entry_vix)
        self.window.payoff_chart.set_entry_overlay(entry_price, entry_vix)
        # 切到组合持仓 tab
        tw=getattr(self.window,"trade_tabs",None)
        if tw:
            for i in range(tw.count()):
                if tw.tabText(i)=="组合持仓":
                    tw.setCurrentIndex(i); break

    def _on_edit_group_requested(self, gid, legs):
        """点击编辑按钮 → 加载该组腿入篮子并切到组合篮子 tab"""
        self.window.basket_panel.load_for_edit(gid, legs)
        tw=getattr(self.window,"trade_tabs",None)
        if tw:
            for i in range(tw.count()):
                if tw.tabText(i) in ("组合篮子","篮子","Basket"):
                    tw.setCurrentIndex(i); break

    def _on_save_edit(self, gid, new_legs):
        """篮子「保存修改」→ 更新持仓面板 → 切回组合持仓 tab"""
        self.window.position_panel.update_group_legs(gid, new_legs)
        tw=getattr(self.window,"trade_tabs",None)
        if tw:
            for i in range(tw.count()):
                if tw.tabText(i)=="组合持仓":
                    tw.setCurrentIndex(i); break

    def _render(self):
        # 顶层异常保护：任何渲染异常都不允许导致进程崩溃
        _t0=time.perf_counter()
        try:
            self._render_impl()
        except Exception as e:
            try:
                _log(f"[GUI] _render 异常(已捕获): {e}\n{traceback.format_exc()[:300]}")
            except Exception:
                pass
        # 慢帧探测：单帧渲染 >120ms 记录一次，用于定位卡顿
        _dt=(time.perf_counter()-_t0)*1000.0
        if _dt>120.0:
            _log(f"[GUI性能] 慢帧 _render 耗时={_dt:.0f}ms")

    def _render_impl(self):
        # 整帧跳过：数据版本号未变化（后台未推送新数据、用户未切换品种）则不渲染
        with _data_lock:
            _ver=_shm.get("_ver", 0)
        if _ver==self._last_ver:
            return
        self._last_ver=_ver
        with _data_lock:
            tc=dict(_shm["tc"]);pc=dict(_shm["pc"]);info=dict(_shm["info"])
            sidebar_stats=dict(_shm["sidebar_stats"])
            iv_map=dict(_shm["iv_map"]);iv_open_map=dict(_shm["iv_open_map"])
            iv_sources={k:dict(v) for k,v in _shm.get("iv_sources",{}).items()}
            status_msg=_shm.get("status_msg","准备就绪")
            cur_signals=list(_shm.get("signals",[]))
            scan_dbg=dict(_shm.get("signal_scan_dbg",{}))
            qianlong={k:dict(v) for k,v in _shm.get("qianlong",{}).items()}
            # 预格式化快照（与 old build_snapshot_from_db 输出一致）
            snapshot     = _shm.get("snapshot") or {}
            vix30_sidebar = dict(_shm.get("vix30_sidebar", {}))
            vix30_current = _shm.get("vix30_current", "")
        live=info.get("market_time_live", False) if info else False
        # 交易日变更检测：日期滚动时清空信号监控（防止昨日信号留存）
        cur_trade_date=info.get("trade_date") or ""
        if cur_trade_date and cur_trade_date!=self._last_trade_date:
            if self._last_trade_date is not None:  # 非首次启动才清空，防止清掉 restore_persisted 数据
                self.window.signal_panel._clear()
                self.window.signal_panel.restore_persisted()
            self._last_trade_date=cur_trade_date

        products = snapshot.get("products", [])

        # ---- snapshot 数据就绪时：用预格式化 products 驱动 T 型表和侧栏 ----
        if products:
            # 侧栏：更新前5品种的统计卡片
            if vix30_sidebar:
                self.window.update_sidebar_bulk_stats(vix30_sidebar)
                # 钱龙红绿柱：仅当前 vix30 品种显示，其余隐藏
                for _qc in vix30_sidebar.keys():
                    _qd = qianlong.get(_qc) or {}
                    self.window.sidebar.update_qianlong(
                        _qc, _qd.get("bias", ""), _qd.get("color", ""))
                _order_key = tuple(sorted(vix30_sidebar.keys()))
                if getattr(self, "_sidebar_order_key", None) != _order_key:
                    self._sidebar_order_key = _order_key
                    vix_order = {code: (info30.get("vix") or 0) for code, info30 in vix30_sidebar.items()}
                    self.window.sidebar.reorder_by_vix(vix_order)
            # T 型表：找当前选中品种的 product 条目
            cur_code = vix30_current or (products[0].get("product","") if products else "")
            cur_prod = next((p for p in products if p.get("product") == cur_code), None)
            if cur_prod is None and products:
                cur_prod = products[0]
                cur_code = cur_prod.get("product", "")
            cur_info30 = vix30_sidebar.get(cur_code, {})
            S30 = float(cur_prod.get("price", 0) or 0) if cur_prod else 0
            rows30 = cur_prod.get("rows", []) if cur_prod else []
            if rows30:
                # 渲染签名：基于预格式化 rows 的 last 价格，无变化跳过重绘
                _sig_parts = [cur_code, round(S30, 4)]
                for rw in rows30:
                    _sig_parts.append((rw.get("c") or {}).get("last"))
                    _sig_parts.append((rw.get("p") or {}).get("last"))
                _new_sig = hash(tuple(_sig_parts))
                if _new_sig != self._render_sig:
                    self._render_sig = _new_sig
                    self.window.table.update_all_from_snapshot(rows30, S30)
                    self.window.payoff_chart.update_price(S30)
            # 顶部信息栏
            vix30_v = cur_prod.get("vix") if cur_prod else None
            if vix30_v is None:
                vix30_v = cur_info30.get("vix")
            vix30_disp = f"{vix30_v:.1f}%" if vix30_v is not None else "-"
            self.window.update_product_info(
                cur_code,
                cur_prod.get("name", cur_code) if cur_prod else cur_code,
                cur_prod.get("sym", "") if cur_prod else "",
                S30,
                cur_info30.get("call_count", len([r for r in rows30 if r.get("c")])),
                cur_info30.get("put_count",  len([r for r in rows30 if r.get("p")])),
                info.get("tick_total", 0), info.get("opt_tick", 0),
                cur_info30.get("expire_date", cur_prod.get("market_time","") if cur_prod else ""),
                info.get("market_time", "-") if live else "-",
                vix30_disp,
                cur_info30.get("opt_volume", 0),
                cur_info30.get("exchange", cur_prod.get("exchange","SHFE") if cur_prod else "SHFE"),
                info.get("is_trading", True),
                vix30_v, live,
                info.get("trade_date"))
        else:
            # 数据未就绪：清空 T 型表
            self.window.table.setRowCount(0)
        self.window.update_status_banner(status_msg)
        if scan_dbg:
            self.window.signal_panel.update_scan_dbg(scan_dbg)
        # 推送实时价和VIX给损益图（使用选中组合的品种，而非当前主界面品种）
        if live and sidebar_stats:
            selected_prod=self.window.payoff_chart._selected_product
            if selected_prod and selected_prod in sidebar_stats:
                prod_stats=sidebar_stats[selected_prod]
                cur_price=prod_stats.get("price",0) or 0
                cur_vix=prod_stats.get("vix")
                if cur_price>0:
                    self.window.payoff_chart.update_price(cur_price)
                if cur_vix is not None:
                    self.window.payoff_chart.update_cur_vix(cur_vix)
            elif info:
                # 无选中组合时，使用当前主界面品种
                self.window.payoff_chart.update_cur_vix(info.get("vix_raw"))
        # 组合持仓面板对价实时更新（含比例价差预估风险，需要 snapshot 取 ATM 现价）
        if tc:
            self.window.position_panel.update_prices(tc, snapshot)
        # 组合篮子预估风险实时刷新
        try:
            self.window.basket_panel.update_risk(snapshot)
        except Exception:
            pass
        # 实时盈亏曲线：与组合持仓(开仓组)联动，并由其持久化恢复
        try:
            pnl_series=self.window.position_panel.get_pnl_history()
            self.window.pnl_chart.title=self.window.position_panel.get_pnl_curve_label()
            self.window.pnl_chart.set_values(pnl_series or [])
        except Exception:
            pass
        # 信号监控推送
        if cur_signals:
            truly_new=self.window.signal_panel.push_signals(cur_signals)
            if truly_new:
                _signal_store.persist(truly_new)   # Item3：新信号落库，重启可恢复
            for sg in truly_new:
                try:
                    dlg=SignalAlertDialog(sg,on_view_panel=self.window.signal_panel.show_and_raise,parent=self.window)
                    dlg.open_position.connect(self._on_signal_open_position)
                    dlg.load_basket.connect(self._on_signal_load_basket)
                    dlg.show()
                except Exception: pass

    def _on_signal_open_position(self, sig):
        """信号卡片「双击开仓」：无确认框，双击触发后直接写入持仓面板"""
        with _data_lock:
            info=dict(_shm.get("info",{})); tc=dict(_shm.get("tc",{}))
        mult=sig.multiplier; product=sig.product; exchange=info.get("exchange","SHFE")
        near_t=tc.get(sig.near_iid); far_t=tc.get(sig.far_iid)
        near_px=float(getattr(near_t,"ask_price1",None) or getattr(near_t,"last_price",0) or 0) if near_t else sig.near_price
        far_px =float(getattr(far_t, "bid_price1",None) or getattr(far_t, "last_price",0) or 0) if far_t  else sig.far_price
        if near_px<=0: near_px=sig.near_price
        if far_px <=0: far_px =sig.far_price
        self._on_open_position([
            {"iid":sig.near_iid,"opt_type":sig.opt_type,"strike":sig.near_strike,
             "side":"BUY", "entry_price":near_px,"qty":1,"multiplier":mult,
             "product":product,"exchange":exchange},
            {"iid":sig.far_iid, "opt_type":sig.opt_type,"strike":sig.far_strike,
             "side":"SELL","entry_price":far_px, "qty":2,"multiplier":mult,
             "product":product,"exchange":exchange},
        ])

    def _on_signal_load_basket(self, sig):
        """信号一键载入篮子：近腿BUY×1 + 远腿SELL×2，使用对价（买ask/卖bid）"""
        with _data_lock:
            info=dict(_shm.get("info",{})); tc=dict(_shm.get("tc",{}))
        mult=sig.multiplier; product=sig.product; exchange=info.get("exchange","SHFE")
        near_t=tc.get(sig.near_iid)
        far_t=tc.get(sig.far_iid)
        near_px=float(getattr(near_t,"ask_price1",None) or getattr(near_t,"last_price",0) or 0) if near_t else sig.near_price
        far_px=float(getattr(far_t,"bid_price1",None) or getattr(far_t,"last_price",0) or 0) if far_t else sig.far_price
        if near_px<=0: near_px=sig.near_price
        if far_px<=0: far_px=sig.far_price
        near_leg={"iid":sig.near_iid,"opt_type":sig.opt_type,"strike":sig.near_strike,
                  "side":"BUY","entry_price":near_px,"qty":1,"multiplier":mult,
                  "product":product,"exchange":exchange}
        far_leg={"iid":sig.far_iid,"opt_type":sig.opt_type,"strike":sig.far_strike,
                 "side":"SELL","entry_price":far_px,"qty":2,"multiplier":mult,
                 "product":product,"exchange":exchange}
        self.window.basket_panel.add_leg(near_leg)
        self.window.basket_panel.add_leg(far_leg)
        tw=getattr(self.window,"trade_tabs",None)
        if tw:
            for i in range(tw.count()):
                if tw.tabText(i)=="组合篮子": tw.setCurrentIndex(i); break

def _run_gui():
    global _hub,_gui_status_msg
    try:
        app=QtWidgets.QApplication([])
        win=MainWindow()
        win.show();win.raise_()
        with _hub_lock:_hub=Hub(win)
        app.exec_()
        _gui_status_msg="GUI 线程运行中"
    except Exception as e:
        _gui_status_msg=f"Qt 事件循环异常: {e}"
        print(f"[GUI] {_gui_status_msg}")

def ensure_gui():
    global _hub,_gui_thread,_gui_status_msg
    if _hub: return _hub
    if not _PYQT_OK:
        _gui_status_msg="PyQt5 未成功导入"
        print("[GUI] PyQt5 未成功导入，界面无法启动。请确保策略运行环境安装 PyQt5。")
        return None
    if not _gui_thread:
        _gui_status_msg="正在启动 PyQt5 线程"
        print("[GUI] 启动 PyQt5 线程...")
        _gui_thread=threading.Thread(target=_run_gui,daemon=True);_gui_thread.start()
    start=time.time()
    for _ in range(80):
        if _hub: break
        time.sleep(0.1)
    if not _hub:
        _gui_status_msg=f"启动超时({time.time()-start:.1f}s)"
        print(f"[GUI] {_gui_status_msg}，请检查是否有显示环境 / PyQt 安装。")
    else:
        _gui_status_msg="GUI 已启动"
    return _hub

# ========================= TQSDK 后端核心函数 =========================
# 以下函数从 tqsdk_old.py 移植，负责连接天勤、订阅期权、计算IV并写入 _shm

def _log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def _disp_w(s):
    """字符串显示宽度：东亚全角字符算2，其余算1"""
    import unicodedata
    return sum(2 if unicodedata.east_asian_width(c) in ("F", "W", "A") else 1 for c in str(s))

def _pad(s, width, align="left"):
    """按显示宽度对齐（兼容中英文混排）"""
    s = str(s)
    gap = width - _disp_w(s)
    if gap <= 0:
        return s
    return (s + " " * gap) if align == "left" else (" " * gap + s)

def _safe_float(v, default=None):
    if v is None: return default
    try:
        f = float(v)
        return f if f == f else default
    except Exception:
        return default

def _create_tq_api():
    from tqsdk import TqApi, TqAuth
    return TqApi(auth=TqAuth(TQ_USER, TQ_PASS))

def _wait_for_tq(api, seconds=2.0):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if not api.wait_update(deadline=deadline):
            break

def _option_class_of(q, product, iid):
    ot = getattr(q, "options_type", None) or getattr(q, "option_class", None)
    if ot in ("CALL", "PUT"):
        return ot
    s = str(iid).upper()
    if "-C-" in s or s.endswith("C"): return "CALL"
    if "-P-" in s or s.endswith("P"): return "PUT"
    return None

def _epoch_to_bj_dt(val):
    """把 tqsdk 的到期时间戳（秒/毫秒/纳秒）转为北京时区 datetime。无法解析返回 None。"""
    try:
        f = float(val)
    except Exception:
        return None
    if f <= 0:
        return None
    # 归一化到秒：2026年 ≈ 1.78e9 秒
    if f > 1e17:        # 纳秒
        f /= 1e9
    elif f > 1e14:      # 微秒
        f /= 1e6
    elif f > 1e11:      # 毫秒
        f /= 1e3
    try:
        return datetime.datetime.fromtimestamp(f, _BJ_TZ)
    except Exception:
        return None

def _quote_epoch_gui(q):
    """期权 Quote 的行情时间戳(秒)。tqsdk Quote.datetime 为北京时间字符串。"""
    dt = getattr(q, "datetime", None)
    if dt is None:
        return None
    if isinstance(dt, (int, float)):
        d = _epoch_to_bj_dt(dt)
        return d.timestamp() if d else None
    s = str(dt).strip()
    if not s:
        return None
    try:
        if s.replace(".", "").isdigit() and "-" not in s:
            d = _epoch_to_bj_dt(s)
            return d.timestamp() if d else None
        parsed = datetime.datetime.fromisoformat(s[:26].replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_BJ_TZ)
        return parsed.timestamp()
    except Exception:
        return None

def _option_expire_ts(q):
    """期权到期时间戳(秒)。
    依据 tqsdk 官方文档：期权到期取期权专有字段 last_exercise_datetime(期权最后行权日)，
    其次 expire_datetime；绝不从合约代码 4 位交割月份推算月末(那是期货交割月末，非期权到期日)。
    """
    for attr in ("last_exercise_datetime", "expire_datetime"):
        v = getattr(q, attr, None)
        if v is None:
            continue
        try:
            if hasattr(v, "timestamp"):              # datetime 对象
                ts = float(v.timestamp())
            else:
                fv = _safe_float(v, None)
                if fv is not None and fv > 0:        # 数字时间戳(秒/ms/ns)
                    d = _epoch_to_bj_dt(fv)
                    ts = d.timestamp() if d else None
                else:                                 # 字符串日期 "2026-07-25 ..."
                    s = str(v).strip()
                    if not s:
                        continue
                    try:
                        d = datetime.datetime.fromisoformat(s[:19])
                    except Exception:
                        d = datetime.datetime.strptime(s[:10], "%Y-%m-%d")
                    if d.tzinfo is None:
                        d = d.replace(tzinfo=_BJ_TZ)
                    ts = d.timestamp()
            if ts and ts > 0:
                return ts
        except Exception:
            continue
    return None

def _option_expire_date_str(q, sym=None):
    """期权到期日 YYYYMMDD（取期权专有 last_exercise_datetime / expire_datetime）。取不到返回 ''。"""
    ts = _option_expire_ts(q)
    if ts and ts > 0:
        try:
            return datetime.datetime.fromtimestamp(ts, _BJ_TZ).strftime("%Y%m%d")
        except Exception:
            pass
    return ""

def _option_expire_days(q, sym=None):
    """期权距到期天数（自然日，期权自身）。
    优先 expire_rest_days(tqsdk 直接给出的期权剩余自然日)；
    否则用期权到期时间戳(last_exercise_datetime/expire_datetime) − 行情时间求差。
    不再用合约代码交割月份推算（那是期货月末，非期权到期日）。
    """
    rest = _safe_float(getattr(q, "expire_rest_days", None), None)
    if rest is not None and rest > 0:
        return float(rest)
    ts = _option_expire_ts(q)
    if ts and ts > 0:
        ref_ts = _quote_epoch_gui(q)
        if ref_ts is None:
            ref_ts = datetime.datetime.now(_BJ_TZ).timestamp()
        days = (ts - ref_ts) / 86400.0
        if days > 0:
            return days
    return None

_tafunc_mod = None

def _get_tafunc():
    global _tafunc_mod
    if _tafunc_mod is None:
        from tqsdk import tafunc
        _tafunc_mod = tafunc
    return _tafunc_mod

def _option_impv(S, K, T, r, market_price, is_call):
    """用 tafunc.get_impv 反算 IV，返回小数形式（如 0.20 表示 20%），失败返回 None"""
    if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
        return None
    try:
        import pandas as _pd
        import numpy as _np
        tafunc = _get_tafunc()
        s_s = _pd.Series([float(S)])
        p_s = _pd.Series([float(market_price)])
        oc_s = _pd.Series(["CALL" if is_call else "PUT"])
        with _np.errstate(divide='ignore', invalid='ignore', over='ignore'):
            result = tafunc.get_impv(s_s, p_s, float(K), r, 0.3, T, oc_s)
        v = float(result.iloc[-1]) if hasattr(result, 'iloc') else float(result)
        if 0 < v < 10.0:
            return v
    except Exception:
        pass
    return None

def _iv_batch_compute(valid, S):
    """向量化批量反算 IV。
    valid: [(iid, K, T_years, price, oc_str), ...]，oc_str ∈ {'CALL','PUT'}。
    返回 {iid: iv_pct}。整组一次 tafunc.get_impv 调用（numpy 在 C 层释放 GIL），
    避免逐合约新建 pandas Series 的纯 Python 开销长时间独占 GIL 饿死 GUI 主线程。
    """
    out = {}
    if not valid or S is None or S <= 0:
        return out
    try:
        import pandas as _pd
        import numpy as _np
        tafunc = _get_tafunc()
        s = _pd.Series([float(S)] * len(valid))
        p = _pd.Series([float(x[3]) for x in valid])
        k = _pd.Series([float(x[1]) for x in valid])
        t = _pd.Series([float(x[2]) for x in valid])
        oc = _pd.Series([x[4] for x in valid])
        with _np.errstate(divide='ignore', invalid='ignore', over='ignore'):
            impv = tafunc.get_impv(s, p, k, RISK_FREE_RATE, 0.3, t, oc)
        for (iid, _K, _T, _price, _oc), v in zip(valid, list(impv)):
            iv = _safe_float(v, None)
            if iv is None or not (0 < iv < 10.0):
                continue
            out[iid] = round(iv * 100.0, 4)
    except Exception:
        # 批量失败时回退逐合约，保证不丢数据
        for (iid, _K, _T, _price, _oc) in valid:
            iv = _option_impv(S, float(_K), float(_T), RISK_FREE_RATE, float(_price), _oc == "CALL")
            if iv and iv > 0:
                out[iid] = round(iv * 100.0, 4)
    return out

def _quote_market_time(q):
    """从 tqsdk Quote 提取行情时间字符串 HH:MM:SS"""
    dt_str = getattr(q, "datetime", None)
    if not dt_str: return None
    try:
        s = str(dt_str)
        if " " in s:
            return s.split(" ")[1][:8]
        return s[:8]
    except Exception:
        return None

# ---- 全局 greeks 缓存 ----
_greeks_cache: dict = {}
_greeks_cache_ts: float = 0.0
_greeks_cache_lock = threading.Lock()
_greeks_columns_logged = False

def _get_quotes_batch(api, syms, batch_size=500):
    """分批 get_quote_list，返回 {sym: Quote}"""
    result = {}
    for i in range(0, len(syms), batch_size):
        batch = syms[i:i+batch_size]
        try:
            qs = api.get_quote_list(batch)
            _wait_for_tq(api, 1.0)
            for q in qs:
                s = getattr(q, "instrument_id", None)
                if s: result[s] = q
        except Exception as e:
            _log(f"[警告] get_quote_list 批次异常: {str(e)[:80]}")
    return result

def _get_multiplier(exchange, product):
    mult = CONTRACT_MULTIPLIERS.get(exchange, {}).get(product)
    if mult: return mult
    for ex_map in CONTRACT_MULTIPLIERS.values():
        if product in ex_map: return ex_map[product]
    return 10

def _calc_proxy_vix(fe, up, iv_map):
    """ATM±ATM3_WINDOW 档全部 (ATM3_WINDOW*2+1)*2 个 IV 均值 → 代理VIX（小数形式）。
    任意一个合约缺失或 IV 缺失则返回 None，不允许输出不完整数据。
    """
    if not fe or not up: return None
    strikes = sorted(fe.keys())
    if not strikes: return None
    atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i]-up))
    lo = max(0, atm_idx - ATM3_WINDOW)
    hi = min(len(strikes), atm_idx + ATM3_WINDOW + 1)
    window = strikes[lo:hi]
    expected = (ATM3_WINDOW * 2 + 1) * 2   # 每档 CALL+PUT
    vals = []
    for k in window:
        pair = fe.get(k, {})
        for leg in (pair.get("CALL"), pair.get("PUT")):
            if not leg:
                return None   # 合约缺失
            iv = iv_map.get(leg)
            if not iv or iv <= 0:
                return None   # IV 缺失
            vals.append(iv)
    if len(vals) != expected:
        return None
    return sum(vals) / len(vals)

def _rerank_select(current_top, vix_map, vol_map, protected, top_n, min_volume, margin):
    """动态重排核心选择（纯函数，便于单测）。

    入参:
      current_top : 当前在位品种 code 列表
      vix_map     : {code: vix 或 None}
      vol_map     : {code: 近月期权总成交量}
      protected   : 持仓/已触发信号品种集合（不被踢出，也不因失格剔除）
      top_n       : 目标 Top-N 数量
      min_volume  : 流动性门槛
      margin      : 防抖迟滞幅度（挑战者需高出在位最弱品种该幅度才替换）

    返回: 新的 Top-N 列表（按 VIX 降序）。
    规则:
      1. 在位品种若失格(无VIX/流动性不足)且未受保护 → 剔除
      2. 合格挑战者先填补空位
      3. 名额已满时，挑战者 VIX 需 > 在位最弱(未受保护)品种 VIX + margin 才替换
      4. 受保护品种永不被踢出
    """
    def _v(c):
        return vix_map.get(c) or 0
    qualified_codes = [c for c, _ in sorted(
        [(c, v) for c, v in vix_map.items()
         if v is not None and vol_map.get(c, 0) >= min_volume],
        key=lambda x: x[1], reverse=True)]
    new_top = list(current_top)
    for c in list(new_top):
        if c in protected:
            continue
        if vix_map.get(c) is None or vol_map.get(c, 0) < min_volume:
            new_top.remove(c)
    challengers = [c for c in qualified_codes if c not in new_top]
    while len(new_top) < top_n and challengers:
        new_top.append(challengers.pop(0))
    while challengers and len(new_top) >= top_n:
        repl = [c for c in new_top if c not in protected]
        if not repl:
            break
        weakest = min(repl, key=_v)
        ch = challengers[0]
        if _v(ch) > _v(weakest) + margin:
            new_top.remove(weakest)
            new_top.append(ch)
            challengers.pop(0)
        else:
            break
    new_top.sort(key=_v, reverse=True)
    return new_top

def _rerank_select_lazy(current_top, vix_map, vol_known, protected, cooldown,
                        top_n, min_volume, margin, warmup):
    """惰性流动性门槛重排（纯函数，便于单测）。

    与 _rerank_select 的区别：VIX 排名阶段**不做成交量过滤**（全品种只订
    ATM±1 算VIX，无法获知近月总量）；只有"已订阅 ATM±30 窗口"的在位品种
    才有近月总成交量(vol_known)，据此**入选后校验**——量不足则退订并由
    下一 VIX 顺位品种补入。被淘汰品种进冷却(cooldown)，过冷却期后按 VIX
    重新探测（成交量为当日累计，午后可能涨过门槛）。

    入参:
      current_top : 当前在位品种 code 列表
      vix_map     : {code: vix 或 None}  全品种代理VIX
      vol_known   : {code: ATM±30窗口期权总成交量}  **仅当前已订阅品种**有值
      protected   : 持仓/已触发信号品种集合（不被踢出、不因失格剔除）
      cooldown    : 近期失格、暂不重探的品种集合（按 VIX 补位时跳过）
      top_n       : 目标 Top-N 数量
      min_volume  : 流动性门槛
      margin      : 防抖迟滞幅度（挑战者需高出在位最弱品种该幅度才替换）
      warmup      : 预热期标志 → True 时**只测量不剔除**（开盘成交量未起来）

    返回: (新 Top-N 列表[按VIX降序], 因低流动性被退订的 code 列表)
    规则:
      1. 预热期内不因流动性剔除；非预热期内，已订阅在位品种 ±30 量<门槛且
         未受保护 → 剔除并记入 evicted_low（进冷却）。
      2. 无VIX的在位品种（无法排名）且未受保护 → 剔除。
      3. 按 VIX 降序的挑战者（排除冷却/本轮失格）先填补空位。
      4. 名额已满时，挑战者 VIX 需 > 在位最弱(未受保护)品种 VIX + margin 才替换。
      5. 仍不足 top_n（合格挑战者枯竭）→ 放宽允许从冷却/失格品种补满，保证持续监控。
    """
    def _v(c):
        return vix_map.get(c) or 0
    new_top = list(current_top)
    evicted_low = []
    # 1) 流动性剔除：仅对"已订阅 ±30 窗口"的在位品种，且非预热期
    if not warmup:
        for c in list(new_top):
            if c in protected:
                continue
            v = vol_known.get(c)
            if v is not None and v < min_volume:
                new_top.remove(c)
                evicted_low.append(c)
    # 2) 无VIX的在位品种（无法排名）且未受保护 → 剔除
    for c in list(new_top):
        if c in protected:
            continue
        if vix_map.get(c) is None:
            new_top.remove(c)
    # 3) 按 VIX 降序挑战者（排除冷却 + 本轮刚失格者，避免退订后立刻又被加回）
    excluded = set(cooldown) | set(evicted_low)
    ranked = [c for c, _ in sorted(
        [(c, v) for c, v in vix_map.items() if v is not None],
        key=lambda x: x[1], reverse=True)]
    challengers = [c for c in ranked if c not in new_top and c not in excluded]
    while len(new_top) < top_n and challengers:
        new_top.append(challengers.pop(0))
    # 4) 名额已满 → VIX 防抖替换末位
    while challengers and len(new_top) >= top_n:
        repl = [c for c in new_top if c not in protected]
        if not repl:
            break
        weakest = min(repl, key=_v)
        ch = challengers[0]
        if _v(ch) > _v(weakest) + margin:
            new_top.remove(weakest)
            new_top.append(ch)
            challengers.pop(0)
        else:
            break
    # 5) 合格挑战者枯竭仍不足 → 放宽用冷却/失格品种补满（持续监控满 top_n）
    if len(new_top) < top_n:
        extra = [c for c in ranked if c not in new_top]
        while len(new_top) < top_n and extra:
            new_top.append(extra.pop(0))
    new_top.sort(key=_v, reverse=True)
    return new_top, evicted_low

def _calc_qianlong_lon(highs, lows, closes, volumes):
    """钱龙 LON 红绿柱（纯函数，便于单测）。

    通达信公式:
      VID  = SUM(VOL,2) / ((HHV(HIGH,2)-LLV(LOW,2))*100)
      RC   = (CLOSE-REF(CLOSE,1))*VID
      LONG = SUM(RC,0)                       # 从第一根累加
      LON  = SMA(LONG,10,1) - SMA(LONG,20,1) # 红绿柱柱高

    入参为等长 OHLCV 序列（list/array），按时间升序，且均为**已收盘**K线。
    返回每根的 LON 值列表（与输入等长）。
    """
    n = len(closes)
    if n == 0:
        return []

    def _f(x):
        try:
            v = float(x)
            return 0.0 if v != v else v   # NaN → 0
        except Exception:
            return 0.0

    rc = [0.0] * n
    for i in range(n):
        if i == 0:
            vol2 = _f(volumes[0])
            hh = _f(highs[0]); ll = _f(lows[0])
        else:
            vol2 = _f(volumes[i]) + _f(volumes[i-1])
            hh = max(_f(highs[i]), _f(highs[i-1]))
            ll = min(_f(lows[i]), _f(lows[i-1]))
        denom = (hh - ll) * 100.0
        vid = (vol2 / denom) if denom > 0 else 0.0
        rc[i] = 0.0 if i == 0 else (_f(closes[i]) - _f(closes[i-1])) * vid

    long_line = [0.0] * n
    acc = 0.0
    for i in range(n):
        acc += rc[i]
        long_line[i] = acc

    def _sma(x, period):
        # 通达信 SMA(X,N,1): Y=(X+(N-1)*Y')/N, 首值 Y0=X0
        out = [0.0] * len(x)
        prev = x[0] if x else 0.0
        for i in range(len(x)):
            prev = (x[i] + (period - 1) * prev) / period
            out[i] = prev
        return out

    diff = _sma(long_line, 10)
    dea  = _sma(long_line, 20)
    return [diff[i] - dea[i] for i in range(n)]

def _qianlong_bias(highs, lows, closes, volumes):
    """对**已收盘**K线序列计算钱龙红绿柱方向（纯函数，便于单测）。

    返回 (bias, color, lon_value):
      color: "red"(LON≥0 在零轴上方→偏多) / "green"(LON<0 在零轴下方→偏空)
      bias : "偏多" / "偏空"
    判定口径：以 LON 相对**零轴**的位置定多空（与官方长庄线读法一致：
    柱子在零轴上方为多头区、下方为空头区），而非柱子相对上一根的斜率。
    数据不足(<2根)返回 ("", "", None)。
    """
    n = len(closes)
    if n < 2:
        return ("", "", None)
    lon = _calc_qianlong_lon(highs, lows, closes, volumes)
    cur = lon[-1]
    if cur >= 0:
        return ("偏多", "red", cur)
    return ("偏空", "green", cur)

def _scan_ratio_spread(fe, up, tick_map, iv_map, product_code, pinfo, vrank, pvix):
    """扫描比例价差信号。返回 (SignalRecord 列表, 诊断统计 dict)。
    诊断统计供扫描状态栏实时展示：otm_call/otm_put 候选数、call_pairs/put_pairs 配对数、net_reject 负净值过滤数。"""
    results = []
    stats = {"otm_call":0,"otm_put":0,"call_pairs":0,"put_pairs":0,"net_reject":0}
    try:
        strikes = sorted(fe.keys())
        if not strikes or up <= 0: return results, stats
        atm_s = min(strikes, key=lambda s: abs(s-up))
        atm_pair = fe.get(atm_s, {})
        atm_ct = tick_map.get(atm_pair.get("CALL",""))
        atm_pt = tick_map.get(atm_pair.get("PUT",""))
        atm_c = _safe_float(getattr(atm_ct,"last_price",0) if atm_ct else 0, 0)
        atm_p = _safe_float(getattr(atm_pt,"last_price",0) if atm_pt else 0, 0)
        if atm_c<=0 or atm_p<=0: return results, stats
        gap_thr = (atm_c+atm_p)/2.0
        mult = _get_multiplier(pinfo.get("exchange","SHFE"), product_code)
        pname = pinfo.get("name", product_code)
        ts = time.strftime("%H:%M:%S")
        def _collect_otm(side):
            out = []
            sk = sorted(strikes, reverse=(side=="PUT"))
            for s in sk:
                if side=="CALL" and s<=up: continue
                if side=="PUT" and s>=up: continue
                iid = fe[s].get(side,"")
                if not iid: continue
                t = tick_map.get(iid)
                if not t: continue
                vol = int(_safe_float(getattr(t,"volume",0),0))
                ask = _safe_float(getattr(t,"ask_price1",0),0)
                bid = _safe_float(getattr(t,"bid_price1",0),0)
                if vol>5000 and ask>0 and bid>0:
                    out.append((vol,s,iid,ask,bid))
            out.sort(reverse=True)
            return out[:3]
        call_top3 = _collect_otm("CALL"); put_top3 = _collect_otm("PUT")
        stats["otm_call"] = len(call_top3); stats["otm_put"] = len(put_top3)
        for opt_type, top3 in [("CALL",call_top3),("PUT",put_top3)]:
            for i in range(len(top3)):
                for j in range(i+1, len(top3)):
                    if opt_type=="CALL": stats["call_pairs"] += 1
                    else:                stats["put_pairs"]  += 1
                    vi,si,ii,iask,ibid = top3[i]; vj,sj,ij,jask,jbid = top3[j]
                    if opt_type=="CALL":
                        if si<sj: ns,ni,np_,nv,nr,fs,fi,fp,fv,fr = si,ii,iask,vi,i+1,sj,ij,jbid,vj,j+1
                        else:      ns,ni,np_,nv,nr,fs,fi,fp,fv,fr = sj,ij,jask,vj,j+1,si,ii,ibid,vi,i+1
                    else:
                        if si>sj: ns,ni,np_,nv,nr,fs,fi,fp,fv,fr = si,ii,iask,vi,i+1,sj,ij,jbid,vj,j+1
                        else:      ns,ni,np_,nv,nr,fs,fi,fp,fv,fr = sj,ij,jask,vj,j+1,si,ii,ibid,vi,i+1
                    if fp<=0: continue
                    ratio=np_/fp; sgap=abs(ns-fs); net=fp*2-np_
                    if 2.0<=ratio<=3.0 and sgap>gap_thr:
                        if net>=0:
                            results.append(SignalRecord(ts,product_code,pname,vrank,pvix,opt_type,ni,fi,ns,fs,np_,fp,ratio,sgap,gap_thr,nv,fv,nr,fr,mult,up))
                        else:
                            stats["net_reject"] += 1
    except Exception:
        pass
    return results, stats

# ---- 预格式化快照（与 old build_snapshot_from_db 输出格式完全一致）----
# 全局稳定快照缓存（稳定性保持：上次数据在 GUI_STALE_SNAPSHOT_SEC 内不清除）
GUI_STALE_SNAPSHOT_SEC = float(os.environ.get("OPTION_GUI_STALE_SNAPSHOT_SEC", "10.0"))
_stale_snapshot_lock = threading.Lock()
_stale_snapshot_by_code: dict = {}   # code -> (snapshot_dict, ts)

def _safe_round(v, n=2):
    try:
        f = float(v)
        if f == f:
            return round(f, n)
    except Exception:
        pass
    return None

def _fmt_cell(q, exchange, product, oc, strike, S, iv_val, delta_val=None):
    """把单个 Quote 对象格式化成与 old fmt() 完全一致的 cell dict。
    delta_val: 来自 query_option_greeks 的 delta（Quote 对象本身不含 delta）。"""
    if q is None:
        return None
    lp       = _safe_round(getattr(q, "last_price",   None), 2)
    pre      = _safe_round(getattr(q, "pre_close",    None), 2)
    bid      = _safe_round(getattr(q, "bid_price1",   None), 2)
    ask      = _safe_round(getattr(q, "ask_price1",   None), 2)
    volume   = int(_safe_float(getattr(q, "volume",        None), 0))
    oi       = int(_safe_float(getattr(q, "open_interest", None), 0))
    delta_r  = _safe_round(delta_val, 4)
    chg = None
    try:
        if isinstance(lp, (int, float)) and isinstance(pre, (int, float)):
            chg = round(lp - pre, 2)
    except Exception:
        pass
    iv_disp   = round(iv_val, 2) if (iv_val is not None and iv_val == iv_val) else "-"
    mult = _get_multiplier(exchange, product)
    # 时价价值
    tv = None
    try:
        if isinstance(lp, (int, float)) and lp > 0 and S > 0:
            intrinsic = max(S - strike, 0) if oc == "CALL" else max(strike - S, 0)
            tv = round(lp - intrinsic, 2)
    except Exception:
        pass
    iid = getattr(q, "instrument_id", None) or ""
    return {
        "option_sym":   iid,
        "option_class": oc,
        "strike":       strike,
        "market_time":  _quote_market_time(q),
        "market_ts":    None,
        "multiplier":   mult,
        "last":  lp if lp else "-",
        "bid":   bid if bid is not None else "-",
        "ask":   ask if ask is not None else "-",
        "volume": volume,
        "oi":     oi,
        "delta":  delta_r if delta_r is not None else "-",
        "iv":     iv_disp,
        "iv_chg": "-",
        "tv":     tv,
        "chg":    chg,
    }

def _build_product_snapshot(code, p_entry, S, fe_window, quote_map, iv_map, delta_map=None):
    """把单个品种的 ATM±30 窗口转成与 old products[i] 完全一致的 dict（含 rows）。
    fe_window: OrderedDict{strike(高→低): {"CALL":sym,"PUT":sym}}
    delta_map: sym -> delta（来自 query_option_greeks）
    """
    delta_map = delta_map or {}
    exchange = p_entry["exchange"]
    strikes  = sorted(fe_window.keys(), reverse=True)   # 高→低，与 old 一致
    md = min(abs(st - S) for st in strikes) if strikes and S > 0 else 999
    rows = []
    for sp in strikes:
        pair = fe_window.get(sp, {})
        c_sym = pair.get("CALL")
        p_sym = pair.get("PUT")
        c_q   = quote_map.get(c_sym) if c_sym else None
        p_q   = quote_map.get(p_sym) if p_sym else None
        c_iv  = iv_map.get(c_sym) if c_sym else None
        p_iv  = iv_map.get(p_sym) if p_sym else None
        c_cell = _fmt_cell(c_q, exchange, code, "CALL", sp, S, c_iv, delta_map.get(c_sym) if c_sym else None)
        p_cell = _fmt_cell(p_q, exchange, code, "PUT",  sp, S, p_iv, delta_map.get(p_sym) if p_sym else None)
        rows.append({
            "K":   sp,
            "c":   c_cell,
            "p":   p_cell,
            "atm": (S > 0 and abs(sp - S) <= md + 0.01),
        })
    return rows

def _snapshot_sig(products_list):
    """与 old _snapshot_signature 同口径：(max_ts, n_products, row_count, iv_count)"""
    row_count = 0; iv_count = 0; ts_vals = []
    for p in products_list:
        for rw in p.get("rows", []):
            row_count += 1
            for side in ("c", "p"):
                cell = rw.get(side)
                if cell and cell.get("iv") != "-":
                    iv_count += 1
        mt = p.get("market_ts")
        if mt is not None:
            try:
                ts_vals.append(float(mt))
            except Exception:
                pass
    return (max(ts_vals) if ts_vals else None, len(products_list), row_count, iv_count)

def _merge_stale_products(products_list, now_ts):
    """稳定性保持：当前缺失的品种从缓存中补入（最长 GUI_STALE_SNAPSHOT_SEC 秒）"""
    if GUI_STALE_SNAPSHOT_SEC <= 0:
        return products_list
    current_codes = {p.get("code") for p in products_list}
    with _stale_snapshot_lock:
        # 更新缓存
        for p in products_list:
            code = p.get("code")
            if code:
                _stale_snapshot_by_code[code] = (p, now_ts)
        # 补入未过期的缓存品种
        merged = list(products_list)
        for code, (cached_p, cached_ts) in list(_stale_snapshot_by_code.items()):
            if now_ts - cached_ts > GUI_STALE_SNAPSHOT_SEC:
                del _stale_snapshot_by_code[code]
                continue
            if code not in current_codes:
                merged.append(cached_p)
    return merged


# ---- TQSDK 数据后台线程主函数 ----
def _tqsdk_data_thread():
    """后台线程：连接天勤 → 订阅期权 → 实时推送数据到 _shm"""
    global running, _pending_product, _gui_status_msg
    _log("[后台] TQSDK 数据线程启动")

    def _set_status(msg):
        global _gui_status_msg
        _gui_status_msg = msg
        with _data_lock:
            _shm["status_msg"] = msg

    _set_status("正在连接天勤...")

    api = None
    try:
        from tqsdk import TqApi, TqAuth
        api = TqApi(auth=TqAuth(TQ_USER, TQ_PASS))
        _log("[后台] 天勤连接成功")
        _set_status("已连接天勤，正在解析期权品种...")
    except Exception as e:
        _log(f"[后台] 天勤连接失败: {e}")
        _set_status(f"天勤连接失败: {str(e)[:60]}")
        return

    try:
        _tqsdk_main_loop(api)
    except Exception as e:
        _log(f"[后台] 主循环异常: {traceback.format_exc()[:2000]}")
        _set_status(f"后台异常: {str(e)[:60]}")
    finally:
        try:
            api.close()
        except Exception:
            pass
        _log("[后台] TQSDK 数据线程退出")

ATM3_WINDOW = 1   # ATM ± 1 档，共 3 档 6 个合约


def _compute_atm3(product_entry, quote_map, iv_map, api, iv_sources_map=None, ATM_WIN=ATM3_WINDOW):
    """计算当前品种 ATM±ATM_WIN 档的行情和IV。
    官方文档依据 (market-data.md):
      - quote_map[sym] 是 get_quote 返回的 live Quote，字段直接读取
      - query_option_greeks(sym_list) 返回含 iv(sigma) 字段的 DataFrame
    返回 dict 写入 _shm['atm3']。
    """
    code        = product_entry["code"]
    exchange    = product_entry["exchange"]
    ul_id       = product_entry["underlying"]
    full_fe     = product_entry["full_fe"]

    ul_q = quote_map.get(ul_id)
    S = _safe_float(getattr(ul_q, "last_price", None) if ul_q else None, 0)
    if S <= 0:
        S = _safe_float(getattr(ul_q, "pre_close", None) if ul_q else None, 0)

    strikes = sorted(full_fe.keys())
    if not strikes:
        return None

    # ATM 定位
    atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - S)) if S > 0 else len(strikes) // 2
    atm_strike = strikes[atm_idx]
    lo = max(0, atm_idx - ATM_WIN)
    hi = min(len(strikes), atm_idx + ATM_WIN + 1)
    window_strikes = strikes[lo:hi]   # 从低到高

    # 对窗口内合约补充查询 Greeks（官方 query_option_greeks，按需调用）
    window_syms = []
    for st in window_strikes:
        pair = full_fe.get(st, {})
        for sym in (pair.get("CALL", ""), pair.get("PUT", "")):
            if sym:
                window_syms.append(sym)

    greeks_iv_pct = {}   # sym → iv (百分比)
    greeks_expire_days = {}  # sym → expire_rest_days（天）
    if window_syms and api is not None:
        try:
            gd = api.query_option_greeks(window_syms)
            if gd is not None and hasattr(gd, "iterrows"):
                _compute_atm3._cols_logged = True
                for _, row in gd.iterrows():
                    iid = row.get("instrument_id") or row.get("InstrumentID") or row.get("symbol")
                    if not iid:
                        continue
                    days_val = row.get("expire_rest_days")
                    if days_val is None:
                        days_val = row.get("remaining_days") or row.get("time_to_expiration")
                    dv = _safe_float(days_val, None)
                    if dv is not None and dv > 0:
                        greeks_expire_days[iid] = dv
        except Exception:
            pass

    def _get_iv(sym):
        """优先 Greeks API，fallback iv_map（含 BS 反算结果）"""
        if not sym:
            return None
        v = greeks_iv_pct.get(sym)
        if v is not None and v > 0:
            return round(v, 4)
        v2 = iv_map.get(sym)
        if v2 is not None and v2 > 0:
            return round(float(v2), 4)
        return None

    def _get_price(sym):
        """从 live Quote 取最优价格（last_price → mid → None）"""
        if not sym:
            return None
        q = quote_map.get(sym)
        if q is None:
            return None
        lp = _safe_float(getattr(q, "last_price", None), 0)
        if lp > 0:
            return lp
        bid = _safe_float(getattr(q, "bid_price1", None), 0)
        ask = _safe_float(getattr(q, "ask_price1", None), 0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2.0, 4)
        stl = _safe_float(getattr(q, "settlement", None), 0)
        if stl > 0:
            return stl
        pc = _safe_float(getattr(q, "pre_close", None), 0)
        if pc > 0:
            return pc
        return None

    official_pct = dict(greeks_iv_pct)
    bs_iv_pct = {}
    bs_iv_debug = {}
    if S > 0:
        for st in window_strikes:
            pair = full_fe.get(st, {}) or {}
            for oc, sym in (("CALL", pair.get("CALL")), ("PUT", pair.get("PUT"))):
                if not sym:
                    continue
                q = quote_map.get(sym)
                if q is None:
                    bs_iv_debug[sym] = "quote_missing"
                    continue
                T_days = _option_expire_days(q, sym=sym)
                if (T_days is None or T_days <= 0) and sym in greeks_expire_days:
                    T_days = greeks_expire_days[sym]
                if T_days is None or T_days <= 0:
                    bs_iv_debug[sym] = f"expire_days={T_days}"
                    continue
                price = _get_price(sym)
                if price is None or price <= 0:
                    bs_iv_debug[sym] = f"price={price}"
                    continue
                iv_bs = _option_impv(S, float(st), T_days / 360.0, RISK_FREE_RATE, float(price), oc == "CALL")
                if iv_bs and iv_bs > 0:
                    bs_iv_pct[sym] = round(iv_bs * 100.0, 4)
                else:
                    bs_iv_debug[sym] = "solver_fail"

    for sym in set(list(official_pct.keys()) + list(bs_iv_pct.keys())):
        existing_official = None
        existing_bs = None
        existing_expire = None
        if iv_sources_map is not None and sym in iv_sources_map:
            existing_official = iv_sources_map[sym].get("official")
            existing_bs = iv_sources_map[sym].get("bs")
            existing_expire = iv_sources_map[sym].get("expire_days")

        preferred = official_pct.get(sym)
        if preferred is None and existing_official is not None:
            preferred = existing_official

        fallback  = bs_iv_pct.get(sym)
        if fallback is None and existing_bs is not None:
            fallback = existing_bs

        value = preferred if preferred is not None else fallback
        if value is not None:
            iv_map[sym] = round(float(value), 4)
        if iv_sources_map is not None:
            entry = iv_sources_map.get(sym, {"official": None, "bs": None, "expire_days": None}).copy()
            if preferred is not None:
                entry["official"] = round(float(preferred), 4)
            if fallback is not None:
                entry["bs"] = round(float(fallback), 4)
            expire_val = greeks_expire_days.get(sym)
            if expire_val is None and existing_expire is not None:
                expire_val = existing_expire
            if expire_val is not None:
                try:
                    entry["expire_days"] = round(float(expire_val), 6)
                except Exception:
                    entry["expire_days"] = expire_val
            iv_sources_map[sym] = entry

    _bs_fail_list = [f"{sym}({reason})" for sym, reason in bs_iv_debug.items() if sym not in bs_iv_pct]

    rows = []
    src_map = iv_sources_map or {}
    for st in sorted(window_strikes, reverse=True):   # 高行权价在上
        pair  = full_fe.get(st, {})
        c_sym = pair.get("CALL", "")
        p_sym = pair.get("PUT", "")
        is_itm_call = (S > 0 and st < S)
        is_itm_put  = (S > 0 and st > S)
        c_src = src_map.get(c_sym, {}) if c_sym else {}
        p_src = src_map.get(p_sym, {}) if p_sym else {}
        c_iv_val = _get_iv(c_sym)
        p_iv_val = _get_iv(p_sym)
        rows.append({
            "strike":   st,
            "is_atm":   (st == atm_strike),
            "call": {
                "sym":   c_sym,
                "price": _get_price(c_sym),
                "iv":    c_iv_val,
                "iv_official": c_src.get("official"),
                "iv_bs": c_src.get("bs"),
                "itm":   is_itm_call,
            },
            "put": {
                "sym":   p_sym,
                "price": _get_price(p_sym),
                "iv":    p_iv_val,
                "iv_official": p_src.get("official"),
                "iv_bs": p_src.get("bs"),
                "itm":   is_itm_put,
            },
        })

    return {
        "product_code": code,
        "exchange":     exchange,
        "underlying":   ul_id,
        "S":            S,
        "atm_strike":   atm_strike,
        "window":       ATM_WIN,
        "rows":         rows,   # 高→低，共 min(7, len(strikes)) 行
        "bs_fail_list": _bs_fail_list,
    }


def _flatten_syms(value):
    """将 query_options 返回值拍平为合约ID字符串列表"""
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    try:
        return [s for s in dict.fromkeys(value) if isinstance(s, str) and s]
    except TypeError:
        return []


def _extract_strike_gui(sym, exchange, product):
    """从合约代码提取行权价，兼容 DCE/CZCE/SHFE 格式。
    另兼容 CFFEX/GFEX 的 `-C-`/`-P-` 破折号格式（与 DCE 同构，行权价在末段）。"""
    try:
        # DCE 及 CFFEX/GFEX 等破折号格式：行权价为最后一个 '-' 之后的数字
        if exchange in ("DCE", "CFFEX", "GFEX") or "-C-" in sym or "-P-" in sym:
            return float(sym.rsplit("-", 1)[1])
        code = sym.split(".", 1)[1]
        rest = code.split(product, 1)[1] if product in code else code
        cpos, ppos = rest.rfind("C"), rest.rfind("P")
        pos = max(cpos, ppos)
        if pos < 0:
            return None
        return float(rest[pos + 1:])
    except Exception:
        return None


def _classify_opt_gui(sym, exchange):
    """从合约代码判断 CALL/PUT。
    先判破折号格式(DCE/CFFEX/GFEX 的 `-C-`/`-P-`)，再回落到 SHFE/CZCE/INE 的紧凑格式。"""
    if "-C-" in sym:
        return "CALL"
    if "-P-" in sym:
        return "PUT"
    if exchange == "DCE":
        return None
    s = sym.split(".")[-1] if "." in sym else sym
    for ch in s:
        if ch == "C":
            return "CALL"
        if ch == "P":
            return "PUT"
    return None


# 支持手动订阅的全部期权交易所（默认3所之外再加 CFFEX 股指/INE 原油/GFEX 工业硅·碳酸锂）
MANUAL_OPTION_EXCHANGES = ("SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX")


def _discover_manual_product(api, raw_code, product_list, valid_codes, sym_to_code,
                             product_info, quote_map, all_underlying_syms):
    """跨交易所动态发现一个手动输入的期权品种（允许非默认3所）。

    成功时：构建 full_fe → 订阅标的 → 注册进 product_list / valid_codes /
    sym_to_code / product_info / all_underlying_syms，返回品种 code；主循环
    既有的「全品种 ATM±1 基础订阅」逻辑会自动订上其期权链。
    失败（任何交易所都无挂牌期权链）时返回 None，调用方按未知品种处理。
    """
    code_raw = (raw_code or "").strip()
    if not code_raw:
        return None
    for exchange in MANUAL_OPTION_EXCHANGES:
        if not running:
            break
        # 查该交易所全部未到期期货，筛出属于本品种的近月候选（大小写不敏感）
        try:
            all_futs = list(api.query_quotes(ins_class="FUTURE", exchange_id=exchange, expired=False))
        except Exception as e:
            _log(f"[手动发现]   {exchange} query_quotes(FUTURE) 异常: {str(e)[:60]}")
            continue
        prefix = f"{exchange}.{code_raw}".upper()
        code = code_raw
        futs_sorted = sorted(s for s in all_futs if s.upper().startswith(prefix))
        if not futs_sorted:
            continue
        # 用实际合约还原品种大小写（如输入 IO → CFFEX.IO2508）
        try:
            _tail = futs_sorted[0].split(".", 1)[1]
            for _n in range(len(_tail), 0, -1):
                if _tail[:_n].upper() == code_raw.upper() and not _tail[:_n].isdigit():
                    code = _tail[:_n]
                    break
        except Exception:
            pass
        futs_sorted = futs_sorted[:8]

        found_calls = found_puts = []
        underlying_id = ""
        for fut_sym in futs_sorted:
            calls_try = puts_try = []
            try:
                calls_try = _flatten_syms(api.query_options(fut_sym, option_class="CALL", expired=False))
            except Exception:
                pass
            try:
                puts_try = _flatten_syms(api.query_options(fut_sym, option_class="PUT", expired=False))
            except Exception:
                pass
            if calls_try and puts_try:
                found_calls, found_puts, underlying_id = calls_try, puts_try, fut_sym
                break
        if not (found_calls and found_puts):
            continue

        full_fe = OrderedDict()
        for sym in list(found_calls) + list(found_puts):
            strike = _extract_strike_gui(sym, exchange, code)
            if strike is None:
                continue
            oc = _classify_opt_gui(sym, exchange)
            if oc not in ("CALL", "PUT"):
                continue
            full_fe.setdefault(strike, {"CALL": "", "PUT": ""})[oc] = sym
        if not full_fe:
            continue
        full_fe = OrderedDict(sorted(full_fe.items(), reverse=True))

        entry = {
            "code": code, "name": code.upper(), "exchange": exchange,
            "underlying": underlying_id, "full_fe": full_fe, "expire_date": "",
        }
        product_list.append(entry)
        valid_codes.add(code)
        product_info[code] = {"name": entry["name"], "exchange": exchange, "underlying": underlying_id}
        for _pair in full_fe.values():
            for _s in (_pair.get("CALL"), _pair.get("PUT")):
                if _s:
                    sym_to_code[_s] = code
        # 订阅标的行情（期权链由主循环 ATM±1 基础订阅逻辑自动补订）
        if underlying_id:
            if underlying_id not in all_underlying_syms:
                all_underlying_syms.append(underlying_id)
            try:
                for q in api.get_quote_list([underlying_id]):
                    iid = getattr(q, "instrument_id", None) or getattr(q, "symbol", None)
                    if iid:
                        quote_map[iid] = q
            except Exception as e:
                _log(f"[手动发现] 订阅标的 {underlying_id} 失败: {str(e)[:60]}")
        n_c = sum(1 for v in full_fe.values() if v.get("CALL"))
        n_p = sum(1 for v in full_fe.values() if v.get("PUT"))
        _log(f"[手动发现] 新品种 {exchange}.{code} 标的={underlying_id} "
             f"{len(full_fe)}档 {n_c}C+{n_p}P")
        return code
    return None


def _tqsdk_main_loop(api):
    """TQSDK 主数据循环：解析品种 → 订阅 → 实时更新 _shm
    API 依据 (已阅读官方文档 references/market-data.md + 参考 期权t数据获取_tqsdk_old.py):
      步骤1: query_quotes(ins_class="FUTURE", exchange_id, product_id, expired=False)
             → 获取各品种期货候选合约列表
      步骤2: api.get_quote(fut_sym) 订阅期货行情，取标的价格
      步骤3: api.query_options(underlying_sym, option_class, expired=False)
             → 以标的合约为参数发现期权链（与 old 文件完全一致）
      步骤4: api.get_quote(sym) 订阅期权行情（live reference，wait_update 刷新）
    """
    global _pending_product, running

    _log("[后台] 等待行情服务器初始化...")
    try:
        api.wait_update(deadline=time.time() + 8)
    except Exception:
        pass

    _log("[后台] 开始解析期权品种...")

    all_products = [
        ("DCE",  p, n) for p, n in DCE_PRODUCTS
    ] + [
        ("CZCE", p, n) for p, n in CZCE_PRODUCTS
    ] + [
        ("SHFE", p, n) for p, n in SHFE_PRODUCTS
    ]

    product_list = []

    # ---- 步骤1: 批量查各交易所全部未到期期货合约（一次性，不按品种循环查）----
    # 与 old 文件 _query_product_futures_fast 一致：一次 query_quotes 拿全交易所期货
    _log("[后台] 批量查询期货候选合约...")
    fut_by_exchange = {}
    for ex in ("DCE", "CZCE", "SHFE"):
        try:
            all_futs = list(api.query_quotes(ins_class="FUTURE", exchange_id=ex, expired=False))
            fut_by_exchange[ex] = all_futs
            _log(f"[后台]   {ex} 期货候选: {len(all_futs)} 个")
        except Exception as e:
            _log(f"[后台]   {ex} query_quotes(FUTURE) 异常: {str(e)[:60]}")
            fut_by_exchange[ex] = []

    for exchange, code, name in all_products:
        if not running:
            break

        # 从批量结果中筛选该品种期货候选，按月份排序取前8个近月
        all_futs = fut_by_exchange.get(exchange, [])
        futs_sorted = sorted(
            [s for s in all_futs if s.upper().startswith(f"{exchange}.{code}".upper())],
        )[:8]

        if not futs_sorted:
            continue

        # ---- 步骤2(快速): 直接调 query_options，不订阅期货行情 ----
        # 与 old 文件完全一致: query_options(underlying_sym, option_class, expired=False)
        found_calls = []
        found_puts  = []
        underlying_id = ""

        # 遍历候选期货月份，必须找到 CALL/PUT 同一标的月份
        same_month_found = False
        fallback_calls = []
        fallback_puts = []
        fallback_call_underlying = ""
        fallback_put_underlying = ""

        for fut_sym in futs_sorted:
            if not running:
                break

            calls_raw_try = []
            puts_raw_try  = []
            try:
                calls_raw_try = _flatten_syms(api.query_options(fut_sym, option_class="CALL", expired=False))
            except Exception:
                pass
            try:
                puts_raw_try = _flatten_syms(api.query_options(fut_sym, option_class="PUT", expired=False))
            except Exception:
                pass

            _log(f"[发现]   候选 {fut_sym} CALL={len(calls_raw_try)} PUT={len(puts_raw_try)}")

            if calls_raw_try and puts_raw_try:
                found_calls = calls_raw_try
                found_puts  = puts_raw_try
                underlying_id = fut_sym
                same_month_found = True
                break

            if calls_raw_try and not fallback_calls:
                fallback_calls = calls_raw_try
                fallback_call_underlying = fut_sym
            if puts_raw_try and not fallback_puts:
                fallback_puts = puts_raw_try
                fallback_put_underlying = fut_sym

        if not same_month_found:
            if fallback_calls and fallback_puts:
                _log(f"[警告] {exchange}.{code} 未找到 CALL/PUT 均挂牌的共同标的月份 (CALL:{fallback_call_underlying}, PUT:{fallback_put_underlying})，跳过该品种")
            else:
                _log(f"[警告] {exchange}.{code} 未找到可用的期权链，跳过该品种")
            continue

        _log(f"[发现]   {exchange}.{code} 标的={underlying_id}  CALL={len(found_calls)} PUT={len(found_puts)}")

        # ---- 步骤3: 纯字符串解析行权价 + CALL/PUT，无需订阅行情 ----
        # 与 old 文件 extract_strike / classify_opts 完全一致，零等待
        full_fe = OrderedDict()
        for sym in found_calls + found_puts:
            strike = _extract_strike_gui(sym, exchange, code)
            if strike is None:
                continue
            oc = _classify_opt_gui(sym, exchange)
            if oc not in ("CALL", "PUT"):
                continue
            full_fe.setdefault(strike, {"CALL": "", "PUT": ""})[oc] = sym

        if not full_fe:
            continue

        full_fe = OrderedDict(sorted(full_fe.items(), reverse=True))
        n_c = sum(1 for v in full_fe.values() if v.get("CALL"))
        n_p = sum(1 for v in full_fe.values() if v.get("PUT"))
        _log(f"[发现] {exchange}.{code} {name}  {len(full_fe)} 个行权价  {n_c}C+{n_p}P  标的={underlying_id}")

        product_list.append({
            "code": code, "name": name, "exchange": exchange,
            "underlying": underlying_id, "full_fe": full_fe,
            "expire_date": "",
        })

    # product_info: code → {name, exchange, underlying} 供信号扫描使用
    product_info = {p["code"]: {"name": p["name"], "exchange": p["exchange"], "underlying": p["underlying"]} for p in product_list}

    if not product_list:
        _log("[后台] 未发现任何期权品种，退出")
        with _data_lock:
            _shm["status_msg"] = "未发现期权品种，请检查天勤账号权限"
        return

    _log(f"[后台] 共发现 {len(product_list)} 个期权品种")
    # 注意：侧栏品种列表在 VIX 排名锁定后才推送（只推 Top5），此处不再推全部品种

    current_product = product_list[0]["code"]
    with _data_lock:
        _shm["status_msg"] = f"已识别 {len(product_list)} 个品种，订阅行情中..."

    # ---- 步骤4: 只订阅标的 + 各品种 ATM±3 档期权（轻量模式）----
    # 官方文档: get_quote(sym) 返回 live Quote 对象，随 wait_update 持续刷新
    # 不订阅全量期权；ATM 窗口漂移时在主循环动态补订
    all_underlying_syms = list(dict.fromkeys(
        p["underlying"] for p in product_list if p["underlying"]
    ))

    def _atm3_pairs_for_product(p_entry):
        """返回该品种 ATM±ATM3_WINDOW 档的成对期权

        返回 (pairs, missing, window_strikes):
          - pairs:       List[Tuple[strike, call_sym, put_sym]]
          - missing:     List[Tuple[strike, missing_side]]  （missing_side 可能为 "CALL", "PUT" 或 "CALL/PUT"）
          - window_strikes: 当前窗口内的全部行权价列表
        """
        ul_id   = p_entry["underlying"]
        full_fe = p_entry["full_fe"]
        strikes = sorted(full_fe.keys())
        if not strikes:
            return [], [], []
        ul_q = quote_map.get(ul_id)
        S = _safe_float(getattr(ul_q, "last_price", None) if ul_q else None, 0)
        if S <= 0:
            S = _safe_float(getattr(ul_q, "settlement", None) if ul_q else None, 0)
        if S <= 0:
            S = _safe_float(getattr(ul_q, "pre_close", None) if ul_q else None, 0)
        mid = min(range(len(strikes)), key=lambda i: abs(strikes[i] - S)) if S > 0 else len(strikes) // 2
        lo = max(0, mid - ATM3_WINDOW)
        hi = min(len(strikes), mid + ATM3_WINDOW + 1)
        window = strikes[lo:hi]
        pairs = []
        missing = []
        for st in window:
            pair = full_fe.get(st, {}) or {}
            call_sym = pair.get("CALL") or ""
            put_sym  = pair.get("PUT")  or ""
            if call_sym and put_sym:
                pairs.append((st, call_sym, put_sym))
            else:
                missing_side = []
                if not call_sym:
                    missing_side.append("CALL")
                if not put_sym:
                    missing_side.append("PUT")
                missing.append((st, "/".join(missing_side) if missing_side else "UNKNOWN"))
        return pairs, missing, window

    def _bulk_subscribe(symbols, label, batch_size=None):
        """使用官方 get_quote_list 一次性注册行情订阅。
        [测试结论 2026-06-25] test_tqsdk_quote_limit.py 实测:
          N=50/100/200/300/500/674 均 OK，无硬性批次上限。
          get_quote_list(384) 一次性订阅 5 品种 ATM±30 全部合约耗时 3.02s 正常。
          => 分批逻辑是多余的，直接一次性调用即可。
        """
        unique = [s for s in dict.fromkeys(symbols) if s]
        result = {}
        if not unique:
            return result
        _log(f"[后台] 一次性注册 {label}: {len(unique)} 个合约")
        try:
            quotes = list(api.get_quote_list(unique))
            for q in quotes:
                iid = getattr(q, "instrument_id", None) or getattr(q, "symbol", None)
                if not iid:
                    continue
                result[iid] = q
        except Exception as e:
            _log(f"[警告] 批量订阅失败 {label}: {len(unique)} 个合约 ({str(e)[:80]})")
        return result

    # 官方文档 wait-update-and-update-loop.md Mental Model:
    # 每次 wait_update() 只处理"一个"业务数据包（发送挂起订阅 + 接收一个更新）。
    # 45个合约需要至少 45 次 wait_update 才能收齐，必须用循环驱动，不能只调一次。
    quote_map = {}

    # 第一批：全部标的（批量注册，再循环 wait_update 直到全部有价格）
    quote_map.update(_bulk_subscribe(all_underlying_syms, "标的行情"))

    _log("[后台] 循环等待标的行情到位...")
    deadline_ul = time.time() + 20
    last_ready = -1
    while running and time.time() < deadline_ul:
        try:
            api.wait_update(deadline=time.time() + 1)
        except Exception:
            break
        ready = sum(
            1 for sym in all_underlying_syms
            if _safe_float(getattr(quote_map.get(sym), "last_price", None), 0) > 0
            or _safe_float(getattr(quote_map.get(sym), "settlement", None), 0) > 0
            or _safe_float(getattr(quote_map.get(sym), "pre_close", None), 0) > 0
        )
        if ready != last_ready:
            _log(f"[后台]   标的行情: {ready}/{len(all_underlying_syms)} 个已就绪")
            last_ready = ready
        if ready >= len(all_underlying_syms):
            break

    # 第二批：基于标的价计算各品种 ATM±3 档，注册期权订阅
    expected_pairs = ATM3_WINDOW * 2 + 1
    init_opt_entries = []  # [(sym, strike, oc, product_dict)]
    opt_meta = {}
    for p in product_list:
        p.setdefault("atm_skip", False)
        pairs, missing, window = _atm3_pairs_for_product(p)
        pair_count = len(pairs)
        contract_count = pair_count * 2
        if pair_count < expected_pairs:
            missing_desc = ", ".join(f"K={st} 缺{ms}" for st, ms in missing[:5]) if missing else ""
            _log(
                f"[警告] {p['exchange']}.{p['code']} {p['name']} ATM±{ATM3_WINDOW} 仅 {contract_count}/6 个合约"
                + (f" ({missing_desc})" if missing_desc else "")
                + "，跳过该品种订阅"
            )
            p["atm_skip"] = True
            continue

        _log(
            f"[订阅]   {p['exchange']}.{p['code']} {p['name']} ATM±{ATM3_WINDOW}: {contract_count} 个合约"
        )

        for strike, call_sym, put_sym in pairs:
            for oc, sym in (("CALL", call_sym), ("PUT", put_sym)):
                if sym not in opt_meta:
                    opt_meta[sym] = {
                        "exchange": p["exchange"],
                        "product":  p["code"],
                        "name":     p["name"],
                        "strike":   strike,
                        "option_class": oc,
                    }
                    init_opt_entries.append((sym, strike, oc, p))

    init_opt_syms = [sym for sym, *_ in init_opt_entries]

    # 过滤掉未通过 ATM14 校验的品种
    skipped_products = [p for p in product_list if p.get("atm_skip")]
    product_list = [p for p in product_list if not p.get("atm_skip")]
    if skipped_products:
        _log(f"[后台] 已跳过 {len(skipped_products)} 个品种 (ATM±{ATM3_WINDOW} 档缺失)")

    if not product_list:
        _log("[错误] 无可订阅的品种（ATM±3 档不完整），终止数据线程")
        return

    if current_product not in {p["code"] for p in product_list}:
        current_product = product_list[0]["code"]
        _log(f"[后台] 当前品种重置为 {current_product}")

    all_underlying_syms = list(dict.fromkeys(
        p["underlying"] for p in product_list if p["underlying"]
    ))

    all_option_syms = list(init_opt_syms)

    with _data_lock:
        _shm["status_msg"] = f"已识别 {len(product_list)} 个品种，订阅行情中..."

    quote_map.update(_bulk_subscribe(init_opt_syms, f"ATM±{ATM3_WINDOW} 档期权"))
    _subscribed_ok  = [s for s in init_opt_syms if s in quote_map]
    _subscribed_fail = [s for s in init_opt_syms if s not in quote_map]
    _log(f"[订阅汇总] 成功 {len(_subscribed_ok)}/{len(init_opt_syms)} 个" +
         (f"  失败: {' '.join(_subscribed_fail[:10])}" if _subscribed_fail else ""))

    # 循环 wait_update 直到期权行情数据到位
    _log(f"[后台] 循环等待期权行情到位 (共 {len(quote_map)} 个合约)...")
    deadline_opt = time.time() + 30
    last_opt_ready = -1
    while running and time.time() < deadline_opt:
        try:
            api.wait_update(deadline=time.time() + 1)
        except Exception:
            break
        ready = sum(
            1 for sym in init_opt_syms
            if _safe_float(getattr(quote_map.get(sym), "last_price", None), 0) > 0
            or _safe_float(getattr(quote_map.get(sym), "bid_price1", None), 0) > 0
            or _safe_float(getattr(quote_map.get(sym), "ask_price1", None), 0) > 0
            or _safe_float(getattr(quote_map.get(sym), "settlement", None), 0) > 0
            or _safe_float(getattr(quote_map.get(sym), "pre_close", None), 0) > 0
        )
        if ready != last_opt_ready:
            _log(f"[后台]   期权行情: {ready}/{len(init_opt_syms)} 个已就绪")
            last_opt_ready = ready
        if ready >= len(init_opt_syms) * 0.9:   # 90%到位即可进入主循环
            break
    _log(f"[后台] 订阅完成: {len(all_underlying_syms)} 标的 + {last_opt_ready}/{len(init_opt_syms)} ATM±{ATM3_WINDOW}档期权已就绪")

    # ---- 订阅后立即执行一次全品种 ATM3 快照（盘中用实时价，休盘用 settlement/pre_close）----
    # 官方 Quote 字段优先级: last_price → bid_ask_mid → settlement → pre_close
    # query_option_greeks 休盘时同样返回基于 settlement/pre_close 的 IV
    _log("[后台] 执行初始 ATM±3 档快照（价格+IV）...")
    init_iv_map = {}
    init_iv_sources = {}
    # 先批量查 Greeks 获取 IV
    if init_opt_syms:
        try:
            gd = api.query_option_greeks(init_opt_syms)
            if gd is not None and hasattr(gd, "iterrows"):
                for _, row in gd.iterrows():
                    iid = row.get("instrument_id") or row.get("InstrumentID") or row.get("symbol")
                    if not iid:
                        continue
                    days_val = row.get("expire_rest_days")
                    if days_val is None:
                        days_val = row.get("remaining_days") or row.get("time_to_expiration")
                    dv = _safe_float(days_val, None)
                    if dv is not None and dv > 0:
                        entry = init_iv_sources.get(iid, {"official": None, "bs": None, "expire_days": None})
                        entry["expire_days"] = round(float(dv), 6)
                        init_iv_sources[iid] = entry
            _log(f"[后台]   初始 Greeks expire_days: {len(init_iv_sources)} 个合约")
        except Exception as e:
            _log(f"[后台]   初始 Greeks 查询异常: {str(e)[:80]}")

    # ---- 补订漂移合约：标的价可能在等待期间变化，窗口需重新对齐 ----
    _补订新合约 = []
    for p in product_list:
        pairs, _, _ = _atm3_pairs_for_product(p)
        for strike, call_sym, put_sym in pairs:
            for sym in (call_sym, put_sym):
                if sym and sym not in quote_map:
                    _补订新合约.append(sym)
    if _补订新合约:
        _log(f"[后台] 补订漂移合约 {len(_补订新合约)} 个...")
        try:
            got = list(api.get_quote_list(_补订新合约))
            for q in got:
                sym = getattr(q, "instrument_id", None)
                if sym:
                    quote_map[sym] = q
                    if sym not in all_option_syms:
                        all_option_syms.append(sym)
            api.wait_update(deadline=time.time() + 2)
        except Exception as e:
            _log(f"[后台] 补订漂移合约异常: {str(e)[:60]}")
        # 重新批量查 Greeks 覆盖补订合约
        _all_syms_now = [s for s in all_option_syms if s in quote_map]
        try:
            gd2 = api.query_option_greeks(_all_syms_now)
            if gd2 is not None and hasattr(gd2, "iterrows"):
                for _, row in gd2.iterrows():
                    iid = row.get("instrument_id") or row.get("InstrumentID") or row.get("symbol")
                    if not iid:
                        continue
                    iv_col = next((c for c in ("sigma", "iv", "implied_volatility") if row.get(c) is not None), None)
                    if iv_col:
                        v = _safe_float(row.get(iv_col), None)
                        if v and 0 < v < 5:
                            init_iv_map[iid] = round(v * 100.0, 4)
                            entry = init_iv_sources.get(iid, {"official": None, "bs": None, "expire_days": None})
                            entry["official"] = round(v * 100.0, 4)
                            init_iv_sources[iid] = entry
        except Exception:
            pass

    # 对每个品种计算并打印 ATM3 快照
    def _fmt_iv(val):
        return f"{val:.1f}%" if (val is not None and val == val) else "—"

    _log("─" * 72)
    _log(f"[初始快照] 共 {len(product_list)} 个品种  ATM±{ATM3_WINDOW} 档")
    _log("─" * 72)
    all_bs_fails = []
    for p in product_list:
        snap = _compute_atm3(p, quote_map, init_iv_map, api=None, iv_sources_map=init_iv_sources)
        if snap and snap.get("rows"):
            rows = snap["rows"]
            S = snap.get("S", 0)
            atm = snap.get("atm_strike", 0)
            iv_vals = [r["call"].get("iv_bs") for r in rows if r["call"].get("iv_bs")] + \
                      [r["put"].get("iv_bs")  for r in rows if r["put"].get("iv_bs")]
            atm_iv = None
            for r in rows:
                if r["is_atm"]:
                    c_iv = r["call"].get("iv_bs")
                    p_iv = r["put"].get("iv_bs")
                    if c_iv and p_iv: atm_iv = (c_iv + p_iv) / 2
                    elif c_iv: atm_iv = c_iv
                    elif p_iv: atm_iv = p_iv
            iv_range = f"{min(iv_vals):.1f}%~{max(iv_vals):.1f}%" if iv_vals else "—"
            atm_iv_str = f"{atm_iv:.1f}%" if atm_iv else "—"
            _log(f"┌─ {p['exchange']}.{p['code']} {p['name']}  S={S}  ATM={atm}  ATM-IV={atm_iv_str}  IV范围={iv_range}")
            for r in rows:
                c = r["call"]; pt = r["put"]
                atm_mark = "←" if r["is_atm"] else "  "
                c_px = f"{c['price']:>8.2f}" if c.get("price") else "       -"
                p_px = f"{pt['price']:>8.2f}" if pt.get("price") else "       -"
                c_iv = _fmt_iv(c.get("iv_bs")); p_iv = _fmt_iv(pt.get("iv_bs"))
                _log(f"│  K={r['strike']:>8.0f}{atm_mark}  C:{c_px} IV:{c_iv:>6}  │  P:{p_px} IV:{p_iv:>6}")
            fails = snap.get("bs_fail_list", [])
            _log(f"└─ {len(rows)} 档  IV计算={len(iv_vals)}/{len(rows)*2}" + (f"  BS缺失:{len(fails)}" if fails else ""))
            all_bs_fails.extend(fails)

    # VIX 排名汇总
    _vix_rank_list = []
    for p in product_list:
        snap = _compute_atm3(p, quote_map, init_iv_map, api=None, iv_sources_map=init_iv_sources)
        if snap:
            vix = _calc_proxy_vix(p["full_fe"], snap.get("S", 0), init_iv_map)
            if vix is not None:
                _vix_rank_list.append((p["code"], p["name"], vix))
    _vix_rank_list.sort(key=lambda x: x[2], reverse=True)
    _log("\n[代理VIX排名] 全部档严格匹配才计算")
    for _rank, (_code, _name, _vix) in enumerate(_vix_rank_list, 1):
        _star = " ★" if _rank <= 5 else "  "
        _log(f"  {_rank:>2}.{_star} {_code} {_name:<6}  代理VIX={_vix:.1f}%")
    if not _vix_rank_list:
        _log("  （暂无品种具备完整 6 个 IV）")
    _log("─" * 72)

    # ---- 3. 主推送循环 ----
    iv_map   = dict(init_iv_map)  # 用初始 IV 作为基底，主循环继续更新
    iv_sources = {sym: dict(src) for sym, src in init_iv_sources.items()}
    delta_map = {}    # {iid: delta}  来自 query_option_greeks（Quote 不含 delta），随窗口刷新更新
    tick_map = {}     # {iid: Quote}  (复用 quote_map 引用)
    last_iv_refresh  = 0.0
    last_gui_push    = 0.0
    last_signal_scan = 0.0
    last_heartbeat    = 0.0   # 心跳日志：每30秒打印一次，确认主循环存活
    HEARTBEAT_INTERVAL = 30.0
    # 代理VIX 只在启动阶段算一次（来自上方 _vix_rank_list），主循环不再重算
    product_vix_map  = {code: vix for code, name, vix in _vix_rank_list}
    atm1_unsubscribed = False  # ATM±1 合约是否已在锁定Top5后释放

    # VIX前5品种 ATM±30 持续订阅状态
    # [测试结论 2026-06-25] query_option_greeks(384合约) 一次性OK，无上限。
    # Greeks 列: [instrument_id, instrument_name, option_class, expire_rest_days,
    #             expire_datetime, underlying_symbol, strike_price,
    #             delta, gamma, theta, vega, rho]  —— 无 sigma/iv 列！
    # => IV 必须用 BS 反算 (tafunc.get_impv)，不能从 Greeks API 直接取。
    VIX30_WIN         = 30   # ATM±30 档窗口
    VIX30_TOP_N       = 5    # 取VIX排名前N品种
    vix30_subscribed  = {}   # sym -> Quote  当前已订阅的 ATM±30 合约
    vix30_top_codes   = []   # 当前入选的VIX前N品种code列表（动态重排，见下方重排逻辑）
    vix30_window_syms = set() # 当前窗口内所有合约sym集合
    # {code: set(syms)} 当前持有 ATM±30 订阅窗口的品种 → 信号扫描的真实范围。
    # 为后续"动态品种订阅"预留：订阅集变化时同步更新本字典，扫描自动跟随。
    vix30_code_window = {}
    vix30_initialized = False # 是否已锁定前5品种
    last_vix30_update = 0.0   # 上次窗口刷新时间
    VIX30_UPDATE_INTERVAL = 10.0  # 固定前5品种后，每10秒维护一次窗口

    # ---- 动态重排（解决"VIX只排一次"）----
    # 锁定 Top-N 后，全品种 ATM±1 订阅持续保留（不释放），每 RERANK_INTERVAL 秒
    # 重算全部品种代理VIX并重排；带流动性门槛 + 防抖迟滞 + 持仓/已触发信号保护。
    RERANK_INTERVAL  = 60.0   # 重排周期（秒）
    RERANK_MARGIN    = 1.0    # 防抖：挑战者VIX需高出在位末位品种该幅度(百分点)才替换
    MIN_PROD_VOLUME  = 10000  # 流动性门槛：品种近月(ATM±30窗口≈近月全链)期权总成交量下限
    # 惰性流动性门槛：VIX排名阶段不过滤成交量（无法获知未订阅品种近月总量）；入选订
    # 阅 ATM±30 后才用其窗口总成交量校验，<门槛则退订并进冷却、按VIX补入下一品种。
    RERANK_WARMUP_SEC = 600.0   # 预热宽限：VIX锁定后前10分钟只测量不剔除（开盘量未起来）
    LIQ_RECHECK_SEC   = 300.0   # 失格品种冷却：5分钟后才按VIX重新探测（成交量午后或涨过门槛）
    last_rerank      = 0.0    # 上次重排时间
    vix30_init_ts    = 0.0    # VIX首次锁定时间（用于预热宽限计时）
    liq_cooldown     = {}     # {code: 冷却截止时间} 因低流动性退订的品种，到期前不重探
    all_atm1_syms    = set(all_option_syms)  # 全品种 ATM±1 基础订阅（永久保留，供持续算VIX）
    triggered_products = set()  # 当日已触发过信号的品种（保护不被重排踢出）
    triggered_date   = time.strftime("%Y-%m-%d")  # triggered_products 所属交易日，跨日清空

    # ---- 手动订阅 / 持仓强制订阅（受保护，不被重排淘汰，且强制纳入订阅期权链）----
    # valid_codes : 已发现且通过校验的全部品种 code（手动订阅只接受其中的品种）
    # sym_to_code : 期权合约 sym → 品种 code（用于由持仓合约反推所属品种）
    # forced_products : 手动添加的强制订阅品种（当日有效，重启同日由DB恢复）
    valid_codes = {p["code"] for p in product_list}
    sym_to_code = {}
    for _p in product_list:
        for _pair in _p["full_fe"].values():
            for _s in (_pair.get("CALL"), _pair.get("PUT")):
                if _s:
                    sym_to_code[_s] = _p["code"]
    forced_products = set(c for c in _manual_sub_store.load_today() if c in valid_codes)
    if forced_products:
        _log(f"[手动订阅] 从DB恢复当日手动订阅品种: {sorted(forced_products)}")

    def _forced_now():
        """当前强制订阅品种 = 手动订阅(当日) ∪ 有未平模拟持仓的品种（由持仓合约反推）。
        持仓部分使 '有持仓的品种重启/次日自动订阅期权链' 自然成立。"""
        pcodes = set()
        try:
            with _data_lock:
                _psyms = list(_shm.get("position_syms") or [])
            for _s in _psyms:
                _c = sym_to_code.get(_s)
                if _c:
                    pcodes.add(_c)
        except Exception:
            pass
        return (forced_products | pcodes) & valid_codes

    def _push_topn_gui():
        """把当前 vix30_top_codes 推送到 GUI 侧栏（含强制订阅品种）。"""
        _gui = []
        for _c in vix30_top_codes:
            _pp = next((p for p in product_list if p["code"] == _c), None)
            if _pp:
                _gui.append((_pp["code"], _pp["name"], _pp["underlying"], _pp["exchange"]))
        with _pending_products_lock:
            _pending_products.clear()
            _pending_products.extend(_gui)
        if _hub:
            try:
                _hub.sig_add_products.emit(_gui)
            except Exception:
                pass

    # ---- 钱龙 LON 红绿柱（15min K线，仅对 vix30 品种）----
    # 订阅各品种标的的 15min K线，**仅在该 15min K线走完后**才更新红绿柱方向。
    # 天勤 K线按交易所交易时段分桶（非代码启动时间）；最后一根为正在走的未收盘 bar，
    # 永远排除在计算之外 → 满足"开盘5min时显示昨天最后一根已收盘15min的钱龙"。
    QL_DURATION   = 15 * 60   # 15min K线
    QL_KLEN       = 400       # 历史根数（LONG 累加 + SMA10/20 暖机收敛）
    QL_INTERVAL   = 3.0       # 检查周期（秒）；仅在已收盘 bar 变化时才真正重算
    ql_kline_map  = {}        # code -> 标的 15min KlineSerial（订阅一次，随 wait_update 刷新）
    ql_last_dt    = {}        # code -> 最近已纳入计算的"已收盘 bar"的 datetime(ns)
    ql_result     = {}        # code -> {"bias","color","lon","dt"}（仅 bar 走完才更新）
    last_ql_check = 0.0

    # 持仓合约补订（修复 A 对齐）：窗口外历史持仓也要拿实时行情，否则浮盈亏冻结
    pos_subscribed = {}       # iid -> Quote  已为持仓补订的合约
    want_pos_syms = []        # 最近一次读到的 OPEN 持仓合约清单
    last_pos_sub = 0.0
    POS_SUB_INTERVAL = 5.0    # 每5秒检查一次持仓合约订阅

    with _data_lock:
        _shm["status_msg"] = f"实时行情推送中，{len(product_list)} 个品种"

    while running:
        try:
            deadline = time.time() + EVENT_DEADLINE_SEC
            api.wait_update(deadline=deadline)
        except Exception as e:
            _log(f"[后台] wait_update 异常: {str(e)[:80]}")
            time.sleep(1)
            continue

        now = time.time()

        # ---- 处理 GUI 切换品种请求 ----
        with _pending_lock:
            req = _pending_product
            _pending_product = ""
        if req and req != current_product:
            current_product = req
            _log(f"[后台] 切换品种 → {current_product}")

        # ---- 处理 GUI 手动订阅请求（强制订阅期权链，受当日保护）----
        manual_force_refresh = False
        with _manual_add_lock:
            _new_manual = list(_manual_add_pending)
            _manual_add_pending.clear()
        for _mc in _new_manual:
            _mc = (_mc or "").strip()
            if not _mc:
                continue
            # 大小写不敏感匹配已发现品种 code（如输入 AG → ag, MA → MA）
            _match = _mc if _mc in valid_codes else next(
                (c for c in valid_codes if c.lower() == _mc.lower()), None)
            if not _match:
                # 已发现列表(默认3所)之外：跨交易所动态发现（含 CFFEX/INE/GFEX）
                _log(f"[手动订阅] {_mc} 不在已发现品种中，尝试跨交易所动态发现…")
                try:
                    _match = _discover_manual_product(
                        api, _mc, product_list, valid_codes, sym_to_code,
                        product_info, quote_map, all_underlying_syms)
                except Exception as _de:
                    _log(f"[手动订阅] 动态发现异常: {str(_de)[:80]}")
                    _match = None
            if not _match:
                _log(f"[手动订阅] 忽略未知/未发现品种: {_mc}")
                if _hub:
                    try: _hub.sig_manual_result.emit(f"订阅失败：未发现品种 {_mc.upper()}")
                    except Exception: pass
                continue
            if _match in forced_products:
                if _hub:
                    try: _hub.sig_manual_result.emit(f"{_match.upper()} 已在手动订阅列表")
                    except Exception: pass
                continue
            forced_products.add(_match)
            _manual_sub_store.add(_match)
            _log(f"[手动订阅] 新增强制订阅品种: {_match}（当日保护，不被重排淘汰）")
            if vix30_initialized and _match not in vix30_top_codes:
                vix30_top_codes.append(_match)
                manual_force_refresh = True
            if _hub:
                try: _hub.sig_manual_result.emit(f"已订阅 {_match.upper()} 期权链（强制）")
                except Exception: pass
        if manual_force_refresh:
            _push_topn_gui()

        # ---- 持仓合约自动补订（修复 A）----
        # GUI 持仓面板把 OPEN 持仓合约写入 _shm["position_syms"]；这里把不在
        # 当前订阅范围内的合约补订进来，保证窗口外历史持仓也有实时对价行情。
        pos_force_refresh = False
        if api is not None and now - last_pos_sub >= POS_SUB_INTERVAL:
            last_pos_sub = now
            with _data_lock:
                want_pos_syms = list(_shm.get("position_syms") or [])
            need_pos = [s for s in want_pos_syms if s and s not in quote_map]
            if need_pos:
                try:
                    pos_qs = list(api.get_quote_list(need_pos))
                    n_ok = 0
                    for q in pos_qs:
                        iid = getattr(q, "instrument_id", None)
                        if iid:
                            quote_map[iid] = q
                            pos_subscribed[iid] = q
                            n_ok += 1
                    if n_ok:
                        _log(f"[持仓补订] 新增 {n_ok} 个窗口外持仓合约至订阅 (持仓估值需要, 总计={len(pos_subscribed)})")
                except Exception as e:
                    _log(f"[持仓补订] 失败: {str(e)[:60]}")
            # 强制订阅品种（有未平持仓 / 手动订阅）立即纳入 VIX30 窗口，使其 ATM±30
            # 行情进入快照 → 组合持仓「预估风险」可即时计算，不必等 60s 重排周期。
            if vix30_initialized:
                _missing_forced = [c for c in _forced_now() if c not in vix30_top_codes]
                if _missing_forced:
                    vix30_top_codes.extend(_missing_forced)
                    pos_force_refresh = True
                    _log(f"[持仓强制] 纳入持仓品种至VIX30窗口(即时算风险): {_missing_forced}  Top={vix30_top_codes}")

        # ---- 心跳日志（每30秒）：确认主循环存活，显示行情状态和订阅数 ----
        # ATM±1 档只在启动时订阅一次（见上方初始化阶段），主循环不再补订
        if now - last_heartbeat >= HEARTBEAT_INTERVAL:
            last_heartbeat = now
            # 判断是否有行情（取任意一个标的的 datetime）
            mkt_active = False
            sample_time = "-"
            for ul_sym_hb in all_underlying_syms[:3]:
                q_hb = quote_map.get(ul_sym_hb)
                if q_hb:
                    t_hb = _quote_market_time(q_hb)
                    if t_hb:
                        sample_time = t_hb
                        mkt_active = True
                        break
            mkt_status = "交易中" if mkt_active else "休盘/无行情"
            _log(
                f"[心跳] 主循环正常  行情={mkt_status}  最新时间={sample_time}  "
                f"VIX30订阅={len(vix30_subscribed)}合约({len(vix30_top_codes)}品种)  "
                f"Top5={vix30_top_codes}"
            )
            # ---- VIX前5品种逐个订阅情况汇总 ----
            if vix30_initialized and vix30_top_codes:
                _log("─" * 92)
                _hdr = (_pad("排名", 5) + _pad("品种", 8) + _pad("代理VIX", 9, "right") + "  "
                        + _pad("标的价", 12, "right") + "  " + _pad("C/P档", 9) + _pad("有数据", 10)
                        + _pad("成交量", 12, "right") + "  " + _pad("到期日", 10) + _pad("行情时间", 10))
                _log("[VIX30汇总] " + _hdr)
                for _rk, _code in enumerate(vix30_top_codes, 1):
                    _p = next((p for p in product_list if p["code"] == _code), None)
                    if not _p:
                        continue
                    _ulq = quote_map.get(_p["underlying"])
                    _S = _safe_float(getattr(_ulq, "last_price", None) if _ulq else None, 0)
                    _full = _p["full_fe"]
                    _strks = sorted(_full.keys())
                    if _strks and _S > 0:
                        _ai = min(range(len(_strks)), key=lambda i: abs(_strks[i] - _S))
                        _win = _strks[max(0, _ai - VIX30_WIN): _ai + VIX30_WIN + 1]
                    else:
                        _win = _strks
                    _ccnt = _pcnt = _cdata = _pdata = 0
                    _vol = 0.0
                    _ptime = "-"
                    for _st in _win:
                        _pair = _full.get(_st, {})
                        _csym = _pair.get("CALL"); _psym = _pair.get("PUT")
                        if _csym:
                            _ccnt += 1
                            _cq = quote_map.get(_csym)
                            if _cq is not None:
                                _cdata += 1
                                _vol += _safe_float(getattr(_cq, "volume", None), 0)
                                if _ptime == "-":
                                    _mt = _quote_market_time(_cq)
                                    if _mt: _ptime = _mt
                        if _psym:
                            _pcnt += 1
                            _pq = quote_map.get(_psym)
                            if _pq is not None:
                                _pdata += 1
                                _vol += _safe_float(getattr(_pq, "volume", None), 0)
                    _vix = product_vix_map.get(_code)
                    _vix_s = f"{_vix:.1f}%" if _vix is not None else "—"
                    _exp = _p.get("expire_date", "") or "—"
                    _row = (_pad(f"{_rk}.", 5) + _pad(_code, 8) + _pad(_vix_s, 9, "right") + "  "
                            + _pad(f"{_S:.2f}", 12, "right") + "  " + _pad(f"{_ccnt}/{_pcnt}", 9)
                            + _pad(f"{_cdata+_pdata}/{_ccnt+_pcnt}", 10)
                            + _pad(f"{int(_vol):,}", 12, "right") + "  " + _pad(_exp, 10) + _pad(_ptime, 10))
                    _log("            " + _row)
                _log("─" * 92)

        # ---- VIX前5品种 ATM±30 持续订阅与IV计算 ----
        # 启动后首次获取到完整 VIX 排名时锁定前5品种，此后只维护这些品种的窗口与IV
        # 手动订阅新增品种时（manual_force_refresh）也强制刷新窗口以立即订阅其期权链
        force_vix30_refresh = manual_force_refresh or pos_force_refresh
        if product_vix_map:
            if not vix30_initialized:
                ranked = sorted(
                    [(code, vix) for code, vix in product_vix_map.items() if vix is not None],
                    key=lambda x: x[1], reverse=True
                )
                if len(ranked) >= VIX30_TOP_N:
                    vix30_top_codes = [code for code, _ in ranked[:VIX30_TOP_N]]
                    # 强制订阅品种（手动订阅当日 + 持仓品种）无条件纳入，即便未进VIX前N
                    for _fc in _forced_now():
                        if _fc not in vix30_top_codes:
                            vix30_top_codes.append(_fc)
                    vix30_initialized = True
                    last_vix30_update = now
                    vix30_init_ts = now      # 预热宽限计时起点
                    last_rerank = now        # 首次重排延后到60s后（此时±30已订阅、量已流动）
                    force_vix30_refresh = True
                    current_product = vix30_top_codes[0]
                    _log(f"[VIX30] 固定前{VIX30_TOP_N}品种: {vix30_top_codes}")
                    # 推送 Top5 到 GUI 侧栏（按 VIX 降序）
                    top5_gui = []
                    for code in vix30_top_codes:
                        p_t5 = next((p for p in product_list if p["code"] == code), None)
                        if p_t5:
                            top5_gui.append((p_t5["code"], p_t5["name"], p_t5["underlying"], p_t5["exchange"]))
                    with _pending_products_lock:
                        _pending_products.clear()
                        _pending_products.extend(top5_gui)
                    if _hub:
                        try:
                            _hub.sig_add_products.emit(top5_gui)
                        except Exception:
                            pass
            elif now - last_vix30_update >= VIX30_UPDATE_INTERVAL:
                last_vix30_update = now
                force_vix30_refresh = True

        # ---- 动态重排：每 RERANK_INTERVAL 秒对全部品种重算VIX并按流动性+VIX重排 ----
        # 解决"VIX只排一次"：保留全品种 ATM±1 订阅持续算VIX；带流动性门槛、防抖迟滞、
        # 以及"持仓/当日已触发信号品种不被踢出"的保护。重排后增量换 ATM±30 窗口。
        if vix30_initialized and product_vix_map and now - last_rerank >= RERANK_INTERVAL:
            last_rerank = now
            _today_str = time.strftime("%Y-%m-%d")
            if _today_str != triggered_date:
                triggered_date = _today_str
                triggered_products.clear()  # 跨交易日：清空"已触发信号"保护名单
            # 0) 价格随盘漂移 → 各品种当前 ATM±1 可能已不在启动订阅集；批量补订缺失合约，
            #    保证非Top-N品种也能持续算VIX（开销 ≈ 品种数×6 合约）。
            need_atm1 = []
            for p in product_list:
                pairs_a, _, _ = _atm3_pairs_for_product(p)
                for _st, c_sym, p_sym in pairs_a:
                    for s_a in (c_sym, p_sym):
                        if s_a and s_a not in quote_map:
                            need_atm1.append(s_a)
            if need_atm1 and api is not None:
                try:
                    for q_a in api.get_quote_list(need_atm1):
                        iid_a = getattr(q_a, "instrument_id", None)
                        if iid_a:
                            quote_map[iid_a] = q_a
                            all_atm1_syms.add(iid_a)
                    _log(f"[VIX30·重排] 补订 {len(need_atm1)} 个漂移 ATM±1 合约供全品种算VIX")
                except Exception as e:
                    _log(f"[VIX30·重排] ATM±1 补订失败: {str(e)[:60]}")
            # 1) 用当前行情对全部品种重算 ATM±1 IV 与代理VIX（api=None → BS反算写入 iv_map）
            #    注意：VIX 排名阶段**不做任何成交量过滤**——未订阅品种只有 ATM±1，
            #    无法获知近月总量；成交量校验后移到"入选订阅 ATM±30 之后"。
            for p in product_list:
                code_r = p["code"]
                try:
                    snap_r = _compute_atm3(p, quote_map, iv_map, api=None, iv_sources_map=iv_sources)
                except Exception:
                    snap_r = None
                S_r = snap_r.get("S", 0) if snap_r else 0
                vix_r = _calc_proxy_vix(p["full_fe"], S_r, iv_map)
                if vix_r is not None:
                    product_vix_map[code_r] = vix_r
            # 2) 流动性指标：**仅对当前已订阅 ATM±30 窗口的品种**计算近月期权总成交量
            #    （vix30_code_window 为上一周期已订阅的逐品种窗口，≈近月全链；未订阅
            #    品种无此数据 → 入选订阅后下一周期才可校验）。
            vol_known = {}
            for code_w, syms_w in vix30_code_window.items():
                vsum = 0.0
                for s_w in syms_w:
                    q_w = quote_map.get(s_w)
                    if q_w is not None:
                        vsum += _safe_float(getattr(q_w, "volume", None), 0)
                vol_known[code_w] = vsum

            # 3) 保护集合：持有OPEN仓位的品种 + 当日已触发信号的品种 → 不被踢出
            with _data_lock:
                _pos_syms_r = set(_shm.get("position_syms") or [])
            protected = set()
            for code_r in vix30_top_codes:
                p_r = next((p for p in product_list if p["code"] == code_r), None)
                if not p_r:
                    continue
                syms_r = set()
                for _st, pair_r in p_r["full_fe"].items():
                    for s_r in (pair_r.get("CALL"), pair_r.get("PUT")):
                        if s_r:
                            syms_r.add(s_r)
                if _pos_syms_r & syms_r:
                    protected.add(code_r)
            protected |= (triggered_products & set(vix30_top_codes))
            # 强制订阅品种（手动订阅当日 + 有未平持仓）始终受保护，不被重排淘汰
            protected |= _forced_now()

            # 4) 惰性流动性门槛 + 防抖迟滞 + 保护 + 预热宽限/冷却，计算新 Top-N（纯函数，已单测）
            #    预热期：VIX锁定后前 RERANK_WARMUP_SEC 秒只测量不剔除（开盘量未起来）。
            warmup = (now - vix30_init_ts) < RERANK_WARMUP_SEC
            # 过期冷却清理 → 到期品种可重新被探测
            liq_cooldown = {c: t for c, t in liq_cooldown.items() if now < t}
            cooldown_set = set(liq_cooldown)
            new_top, evicted_low = _rerank_select_lazy(
                vix30_top_codes, product_vix_map, vol_known, protected, cooldown_set,
                VIX30_TOP_N, MIN_PROD_VOLUME, RERANK_MARGIN, warmup
            )
            # 因低流动性退订的品种进冷却（LIQ_RECHECK_SEC 后才按VIX重探，避免反复进出）
            # 仅对最终确实被剔出的品种设冷却（极端候选枯竭时可能被规则5补回，则不冷却）
            for c in evicted_low:
                if c not in new_top:
                    liq_cooldown[c] = now + LIQ_RECHECK_SEC
            # 强制订阅品种无条件纳入（含新开仓/跨日持仓品种），即便未进VIX前N
            for _fc in _forced_now():
                if _fc not in new_top:
                    new_top.append(_fc)

            if set(new_top) != set(vix30_top_codes):
                added_r   = [c for c in new_top if c not in vix30_top_codes]
                dropped_r = [c for c in vix30_top_codes if c not in new_top]
                vix30_top_codes = new_top
                force_vix30_refresh = True
                last_vix30_update = now
                if current_product not in vix30_top_codes:
                    current_product = vix30_top_codes[0]
                _vol_note = ""
                if evicted_low:
                    _vol_note = "  ✗低流动性退订" + str(
                        [f"{c}({int(vol_known.get(c, 0)):,}<{MIN_PROD_VOLUME:,})" for c in evicted_low])
                _log(f"[VIX30·重排] Top{VIX30_TOP_N}={vix30_top_codes}  新增{added_r} 淘汰{dropped_r} "
                     f"保护{sorted(protected)}{_vol_note}")
                # 推送新的 Top-N 到 GUI 侧栏（按VIX降序）
                topn_gui = []
                for code_r in vix30_top_codes:
                    p_t = next((p for p in product_list if p["code"] == code_r), None)
                    if p_t:
                        topn_gui.append((p_t["code"], p_t["name"], p_t["underlying"], p_t["exchange"]))
                with _pending_products_lock:
                    _pending_products.clear()
                    _pending_products.extend(topn_gui)
                if _hub:
                    try:
                        _hub.sig_add_products.emit(topn_gui)
                    except Exception:
                        pass
            else:
                # 集合未变，仅按最新VIX刷新展示顺序
                vix30_top_codes = new_top

        if vix30_initialized and force_vix30_refresh:
            # 计算固定品种的 ATM±30 窗口
            new_window = set()
            new_code_window = {}   # {code: set(syms)} 逐品种窗口 → 扫描真实范围
            for code in vix30_top_codes:
                p_entry = next((p for p in product_list if p["code"] == code), None)
                if not p_entry:
                    continue
                ul_q_30 = quote_map.get(p_entry["underlying"])
                S_30 = _safe_float(getattr(ul_q_30, "last_price", None) if ul_q_30 else None, 0)
                if S_30 <= 0:
                    S_30 = _safe_float(getattr(ul_q_30, "pre_close", None) if ul_q_30 else None, 0)
                full_fe_30 = p_entry["full_fe"]
                strikes_30 = sorted(full_fe_30.keys())
                if not strikes_30:
                    continue
                atm_i = min(range(len(strikes_30)), key=lambda i: abs(strikes_30[i] - S_30)) if S_30 > 0 else len(strikes_30)//2
                lo_30 = max(0, atm_i - VIX30_WIN)
                hi_30 = min(len(strikes_30), atm_i + VIX30_WIN + 1)
                code_syms = set()
                for st in strikes_30[lo_30:hi_30]:
                    pair_30 = full_fe_30.get(st, {})
                    for sym_30 in (pair_30.get("CALL"), pair_30.get("PUT")):
                        if sym_30:
                            new_window.add(sym_30)
                            code_syms.add(sym_30)
                if code_syms:
                    new_code_window[code] = code_syms

            # 新增合约 → 一次性订阅（测试证明无上限，直接调用）
            to_subscribe = [s for s in new_window if s not in vix30_subscribed]
            if to_subscribe:
                try:
                    new_qs = list(api.get_quote_list(to_subscribe))
                    for q in new_qs:
                        iid = getattr(q, "instrument_id", None)
                        if iid:
                            vix30_subscribed[iid] = q
                            quote_map[iid] = q
                            # 注意：VIX30合约只加入 quote_map 和 vix30_subscribed，
                            # 不加入 all_option_syms（那是 ATM±1 档的计数）
                    _log(f"[VIX30] 订阅固定Top{VIX30_TOP_N}新增 {len(new_qs)} 个 ATM±{VIX30_WIN} 合约 (总计={len(vix30_subscribed)})")
                except Exception as e:
                    _log(f"[VIX30] 订阅失败: {str(e)[:60]}")

            vix30_window_syms = new_window
            vix30_code_window = new_code_window

            # ---- 动态重排支持：全品种 ATM±1 基础订阅永久保留（供持续算VIX重排）----
            # 不再像原先那样锁定后释放非Top-N的 ATM±1 合约；改为只释放"被淘汰品种"
            # 残留的 ATM±30 专属合约（即不在新窗口、且不属于 ATM±1 基础集的合约），
            # 这样新晋品种能立刻算VIX、淘汰品种回落到仅 ATM±1 订阅，开销可控。
            stale_syms = [s for s in list(vix30_subscribed)
                          if s not in new_window and s not in all_atm1_syms]
            if stale_syms:
                for _sym in stale_syms:
                    vix30_subscribed.pop(_sym, None)
                    quote_map.pop(_sym, None)
                    iv_map.pop(_sym, None)
                    iv_sources.pop(_sym, None)
                    tick_map.pop(_sym, None)
                _log(f"[VIX30] 释放 {len(stale_syms)} 个被淘汰品种的 ATM±{VIX30_WIN} 残留合约（ATM±1基础订阅保留）")

            # 为 ATM±30 窗口内合约 BS 反算 IV（写入公共 iv_map）
            # Greeks API 列中无 sigma/iv，必须用 tafunc.get_impv BS 反算
            for code in vix30_top_codes:
                p_entry = next((p for p in product_list if p["code"] == code), None)
                if not p_entry:
                    continue
                ul_q_30 = quote_map.get(p_entry["underlying"])
                S_30 = _safe_float(getattr(ul_q_30, "last_price", None) if ul_q_30 else None, 0)
                if S_30 <= 0:
                    continue
                full_fe_30 = p_entry["full_fe"]
                strikes_30 = sorted(full_fe_30.keys())
                if not strikes_30:
                    continue
                atm_i = min(range(len(strikes_30)), key=lambda i: abs(strikes_30[i] - S_30))
                lo_30 = max(0, atm_i - VIX30_WIN)
                hi_30 = min(len(strikes_30), atm_i + VIX30_WIN + 1)
                # 收集本品种窗口内全部有效合约 → 一次性向量化 get_impv（替代逐合约调用）
                valid_30 = []
                for st in strikes_30[lo_30:hi_30]:
                    pair_30 = full_fe_30.get(st, {})
                    for oc_30, iid_30 in (("CALL", pair_30.get("CALL")), ("PUT", pair_30.get("PUT"))):
                        if not iid_30:
                            continue
                        q_30 = quote_map.get(iid_30)
                        if q_30 is None:
                            continue
                        lp_30 = _safe_float(getattr(q_30, "last_price", None), 0)
                        if lp_30 <= 0:
                            bid_30 = _safe_float(getattr(q_30, "bid_price1", None), 0)
                            ask_30 = _safe_float(getattr(q_30, "ask_price1", None), 0)
                            if bid_30 > 0 and ask_30 > 0:
                                lp_30 = (bid_30 + ask_30) / 2.0
                        if lp_30 <= 0:
                            continue
                        T_days_30 = _option_expire_days(q_30, sym=iid_30)
                        if not T_days_30 or T_days_30 <= 0:
                            continue
                        valid_30.append((iid_30, float(st), T_days_30 / 360.0, lp_30, oc_30))
                # 整组一次反算（一个品种一次调用，而非 ATM±30×2 次）
                iv_batch_30 = _iv_batch_compute(valid_30, S_30)
                for iid_30, pct_30 in iv_batch_30.items():
                    iv_map[iid_30] = pct_30
                    entry_30 = iv_sources.get(iid_30, {"official": None, "bs": None}).copy()
                    entry_30.setdefault("official", None)
                    entry_30["bs"] = pct_30
                    iv_sources[iid_30] = entry_30

            # ---- 整窗一次性查询 Greeks 取 Delta（Quote 不含 delta，须经 query_option_greeks）----
            # 与后端 天勤-期权.py 同源：query_option_greeks 返回 delta/gamma/theta/vega/rho（无 iv）。
            # 节流：仅在窗口刷新周期（VIX30_UPDATE_INTERVAL）调用一次，整窗批量，不逐合约。
            if vix30_window_syms and api is not None:
                try:
                    gd = api.query_option_greeks(list(vix30_window_syms))
                    if gd is not None and hasattr(gd, "iterrows"):
                        for _, _grow in gd.iterrows():
                            _iid = (_grow.get("instrument_id") or _grow.get("InstrumentID")
                                    or _grow.get("symbol"))
                            if not _iid:
                                continue
                            _dv = _safe_float(_grow.get("delta"), None)
                            if _dv is not None:
                                delta_map[_iid] = _dv
                except Exception as _e:
                    _log(f"[VIX30] query_option_greeks 取Delta失败: {str(_e)[:60]}")

            # ---- 填充到期日 + 实时重算 Top5 代理VIX（用刚算出的 live IV）----
            for code in vix30_top_codes:
                p_entry = next((p for p in product_list if p["code"] == code), None)
                if not p_entry:
                    continue
                # 到期日：从窗口内任一期权合约的 expire_datetime 提取一次
                if not p_entry.get("expire_date"):
                    for st_e, pair_e in p_entry["full_fe"].items():
                        sym_e = pair_e.get("CALL") or pair_e.get("PUT")
                        q_e = quote_map.get(sym_e) if sym_e else None
                        if q_e is not None:
                            eds = _option_expire_date_str(q_e, sym=sym_e)
                            if eds:
                                p_entry["expire_date"] = eds
                                break
                # 代理VIX：用 ATM±1 的 6 个 live IV 重算（订阅品种允许实时计算）
                ul_q_v = quote_map.get(p_entry["underlying"])
                S_v = _safe_float(getattr(ul_q_v, "last_price", None) if ul_q_v else None, 0)
                if S_v <= 0:
                    S_v = _safe_float(getattr(ul_q_v, "pre_close", None) if ul_q_v else None, 0)
                vix_live = _calc_proxy_vix(p_entry["full_fe"], S_v, iv_map)
                if vix_live is not None:
                    product_vix_map[code] = vix_live

        # ---- 更新 tick_map ----
        for sym, q in quote_map.items():
            try:
                if api.is_changing(q):
                    tick_map[sym] = q
            except Exception:
                pass

        # ---- 钱龙 LON 红绿柱：仅对 vix30 品种、仅在 15min K线走完后更新 ----
        if vix30_initialized and vix30_top_codes and now - last_ql_check >= QL_INTERVAL:
            last_ql_check = now
            for code in vix30_top_codes:
                p_ql = product_info.get(code) or {}
                ul_ql = p_ql.get("underlying", "")
                if not ul_ql:
                    continue
                # 首次遇到该品种：订阅其标的 15min K线（一次性，随 wait_update 刷新）
                kl = ql_kline_map.get(code)
                if kl is None:
                    try:
                        kl = api.get_kline_serial(ul_ql, QL_DURATION, QL_KLEN)
                        ql_kline_map[code] = kl
                    except Exception as e:
                        _log(f"[钱龙] {code} 订阅15min K线失败: {str(e)[:60]}")
                    continue  # 本轮数据尚未到位，下轮再算
                try:
                    dt_arr = list(kl["datetime"]); hi_arr = list(kl["high"])
                    lo_arr = list(kl["low"]);      cl_arr = list(kl["close"])
                    vol_arr = list(kl["volume"])
                except Exception:
                    continue
                # 过滤暖机期 NaN / 空 bar
                valid = [j for j in range(len(cl_arr))
                         if dt_arr[j] and not (math.isnan(cl_arr[j]) or math.isnan(hi_arr[j]) or math.isnan(lo_arr[j]))]
                if len(valid) < 3:
                    continue
                # 排除最后一根（正在走的未收盘 bar）→ 只用已收盘 K线
                completed = valid[:-1]
                if len(completed) < 2:
                    continue
                last_done_dt = dt_arr[completed[-1]]
                if ql_last_dt.get(code) == last_done_dt:
                    continue  # 该已收盘 bar 未变 → 不更新（必须 15min 走完才更新）
                hs = [hi_arr[j] for j in completed]; ls = [lo_arr[j] for j in completed]
                cs = [cl_arr[j] for j in completed]; vs = [vol_arr[j] for j in completed]
                bias, color, lonv = _qianlong_bias(hs, ls, cs, vs)
                if bias:
                    ql_last_dt[code] = last_done_dt
                    ql_result[code] = {"bias": bias, "color": color,
                                       "lon": lonv, "dt": int(last_done_dt)}

        # ---- 定期推送 GUI（只推 VIX 前5品种的 ATM±30 数据）----
        if now - last_gui_push >= GUI_REFRESH_INTERVAL_SEC:
            last_gui_push = now

            # VIX30 未锁定（启动等待阶段）：GUI 不显示任何期权数据
            if not vix30_initialized or not vix30_top_codes:
                with _data_lock:
                    _shm["status_msg"] = "正在计算代理VIX排名，等待锁定前5品种..."
                continue

            # 当前显示品种必须是 Top5 之一
            if current_product not in vix30_top_codes:
                current_product = vix30_top_codes[0]

            # ---- 构造 VIX30 专用数据（5品种 × ATM±30 档）----
            vix30_fe_all   = {}   # code -> OrderedDict{strike: {CALL,PUT}}
            vix30_tc_new   = {}   # sym -> Quote（只含VIX30窗口合约）
            vix30_sidebar_new = {}  # code -> {name,price,vix,...}
            market_time = "-"
            for code in vix30_top_codes:
                p30 = next((p for p in product_list if p["code"] == code), None)
                if not p30:
                    continue
                ul_q30 = quote_map.get(p30["underlying"])
                S30    = _safe_float(getattr(ul_q30,"last_price",None) if ul_q30 else None, 0)
                if S30 <= 0:
                    S30 = _safe_float(getattr(ul_q30,"pre_close",None) if ul_q30 else None, 0)
                # 行情时间：取标的的市场时间
                if ul_q30:
                    _mt = _quote_market_time(ul_q30)
                    if _mt:
                        market_time = _mt
                full30   = p30["full_fe"]
                strikes30 = sorted(full30.keys())
                if not strikes30:
                    continue
                atm_i30 = min(range(len(strikes30)), key=lambda i: abs(strikes30[i]-S30)) if S30>0 else len(strikes30)//2
                lo30 = max(0, atm_i30 - VIX30_WIN)
                hi30 = min(len(strikes30), atm_i30 + VIX30_WIN + 1)
                fe30 = OrderedDict()
                opt_vol30 = 0.0
                for st30 in sorted(strikes30[lo30:hi30], reverse=True):
                    pair30 = full30.get(st30, {})
                    fe30[st30] = {"CALL": pair30.get("CALL"), "PUT": pair30.get("PUT")}
                    for sym30 in (pair30.get("CALL"), pair30.get("PUT")):
                        if sym30 and sym30 in quote_map:
                            vix30_tc_new[sym30] = quote_map[sym30]
                            opt_vol30 += _safe_float(getattr(quote_map[sym30],"volume",None), 0)
                vix30_fe_all[code] = fe30
                vix30_sidebar_new[code] = {
                    "name":       p30["name"],
                    "price":      S30,
                    "vix":        product_vix_map.get(code),
                    "exchange":   p30["exchange"],
                    "call_count": sum(1 for v in fe30.values() if v.get("CALL")),
                    "put_count":  sum(1 for v in fe30.values() if v.get("PUT")),
                    "expire_date": p30.get("expire_date",""),
                    "underlying":  p30["underlying"],
                    "opt_volume":  opt_vol30,
                }

            # ---- 信号扫描（扫描全部持有 ATM±30 订阅窗口的品种）----
            # 范围 = vix30_code_window 的全部品种（当前实际订阅的 VIX ATM±30 品种）。
            # 后续接入动态品种订阅时，只要订阅集变化同步更新 vix30_code_window，
            # 扫描范围即自动跟随，无需改本处逻辑。
            new_sigs = None
            scan_dbg = None
            if now - last_signal_scan >= 2.0:
                last_signal_scan = now
                new_sigs = []
                scan_codes = [c for c in vix30_top_codes if c in vix30_code_window]
                scan_codes += [c for c in vix30_code_window if c not in scan_codes]
                agg = {"otm_call":0,"otm_put":0,"call_pairs":0,"put_pairs":0,"net_reject":0}
                for rank, code in enumerate(scan_codes, 1):
                    p_entry = next((p for p in product_list if p["code"]==code), None)
                    if not p_entry: continue
                    pinfo_entry = product_info.get(code, {})
                    p_ul_q2 = quote_map.get(p_entry["underlying"])
                    p_up2 = _safe_float(getattr(p_ul_q2,"last_price",None) if p_ul_q2 else None, 0)
                    sigs, st = _scan_ratio_spread(p_entry["full_fe"], p_up2, tick_map, iv_map, code, pinfo_entry, rank, product_vix_map.get(code))
                    new_sigs.extend(sigs)
                    if sigs:
                        triggered_products.add(code)  # 当日已触发信号 → 重排时锁定不被踢出
                    for _k in agg: agg[_k] += st.get(_k, 0)
                scan_dbg = {
                    "ts":         time.strftime("%H:%M:%S"),
                    "top5":       scan_codes,
                    "hits":       len(new_sigs),
                    "otm_call":   agg["otm_call"],
                    "otm_put":    agg["otm_put"],
                    "call_pairs": agg["call_pairs"],
                    "put_pairs":  agg["put_pairs"],
                    "net_reject": agg["net_reject"],
                }

            # 当前品种信息
            cur_info30 = vix30_sidebar_new.get(current_product, {})
            cur_vix    = product_vix_map.get(current_product)

            # ---- 预格式化 snapshot（与 old build_snapshot_from_db 输出完全一致）----
            products_snapshot = []
            for code30 in vix30_top_codes:
                p30 = next((p for p in product_list if p["code"] == code30), None)
                if not p30:
                    continue
                fe30 = vix30_fe_all.get(code30, {})
                if not fe30:
                    continue
                sbar30 = vix30_sidebar_new.get(code30, {})
                S30_snap = sbar30.get("price", 0) or 0
                rows30 = _build_product_snapshot(
                    code30, p30, S30_snap, fe30, quote_map, iv_map, delta_map
                )
                # market_ts / market_time: 从窗口内任一期权取
                mts30 = None; mtime30 = None
                for pair30 in fe30.values():
                    for sym30_chk in (pair30.get("CALL"), pair30.get("PUT")):
                        if sym30_chk and sym30_chk in quote_map:
                            q30_chk = quote_map[sym30_chk]
                            mt30_chk = _quote_market_time(q30_chk)
                            if mt30_chk:
                                mtime30 = mt30_chk
                                break
                    if mtime30:
                        break
                ed30 = sbar30.get("expire_date", "")
                try:
                    days30 = float(p30.get("expire_date_days", 0) or 0)
                except Exception:
                    days30 = 0
                products_snapshot.append({
                    "exchange":   p30["exchange"],
                    "product":    code30,
                    "name":       p30["name"],
                    "sym":        p30["underlying"],
                    "price":      S30_snap,
                    "days":       days30,
                    "days_key":   int(days30) if days30 > 0 else 0,
                    "multiplier": _get_multiplier(p30["exchange"], code30),
                    "ctype":      "near",
                    "rows":       rows30,
                    "market_time": mtime30,
                    "market_ts":  mts30,
                    "vix":        product_vix_map.get(code30),
                })

            # 稳定性保持：补入未过期的缓存品种
            products_snapshot = _merge_stale_products(products_snapshot, now)

            new_snapshot = {
                "products":   products_snapshot,
                "is_trading": _is_trading_time(),
            }

            # ---- 签名比对：只在内容真正变化时递增 _ver ----
            new_sig = _snapshot_sig(products_snapshot)

            with _data_lock:
                _shm["vix30_fe"]      = vix30_fe_all
                _shm["vix30_tc"]      = vix30_tc_new
                _shm["vix30_sidebar"] = vix30_sidebar_new
                # 钱龙红绿柱：只暴露当前 vix30 品种（动态跟随重排）
                _shm["qianlong"] = {c: dict(v) for c, v in ql_result.items() if c in vix30_top_codes}
                if _shm["vix30_current"] not in vix30_fe_all:
                    _shm["vix30_current"] = current_product
                # tc：VIX30 窗口合约 + 窗口外持仓补订合约（修复 A：保证持仓浮盈亏不冻结）
                # vix30_tc 仍只含窗口合约（T 表渲染用），tc 额外并入持仓合约供持仓估值
                tc_out = dict(vix30_tc_new)
                for _psym in want_pos_syms:
                    if _psym not in tc_out:
                        _pq = pos_subscribed.get(_psym) or quote_map.get(_psym)
                        if _pq is not None:
                            tc_out[_psym] = _pq
                _shm["tc"]     = tc_out
                _shm["pc"]     = {}
                _shm["iv_map"] = {k: v for k,v in iv_map.items() if k in vix30_tc_new}
                _shm["iv_open_map"] = {}
                _shm["iv_sources"]  = {k: dict(v) for k,v in iv_sources.items() if k in vix30_tc_new}
                _shm["sidebar_stats"] = vix30_sidebar_new
                if new_sigs is not None:
                    _shm["signals"] = new_sigs
                if scan_dbg is not None:
                    _shm["signal_scan_dbg"] = scan_dbg
                # 预格式化快照：GUI 渲染层直接消费，不再需要解析 Quote 对象
                _shm["snapshot"] = new_snapshot
                _shm["info"] = {
                    "product_code":    current_product,
                    "product_name":    cur_info30.get("name", current_product),
                    "underlying_id":   cur_info30.get("underlying", ""),
                    "underlying_price": cur_info30.get("price", 0),
                    "market_time":     market_time,
                    "market_time_live": market_time != "-",
                    "vix_display":     f"{cur_vix:.1f}%" if cur_vix is not None else "-",
                    "vix_raw":         cur_vix,
                    "expire_date":     cur_info30.get("expire_date", ""),
                    "call_count":      cur_info30.get("call_count", 0),
                    "put_count":       cur_info30.get("put_count", 0),
                    "tick_total":      len(tick_map),
                    "opt_tick":        len(vix30_tc_new),
                    "opt_volume":      cur_info30.get("opt_volume", 0),
                    "exchange":        cur_info30.get("exchange", "SHFE"),
                    "is_trading":      _is_trading_time(),
                    "trade_date":      datetime.date.today().strftime("%Y%m%d"),
                }
                _shm["status_msg"] = f"VIX前{VIX30_TOP_N}品种 · ATM±{VIX30_WIN}档 · {market_time}"
                # 签名比对：只在 snapshot 内容真正变化时才递增 _ver
                old_sig = _shm.get("_snapshot_sig")
                if new_sig != old_sig:
                    _shm["_snapshot_sig"] = new_sig
                    _shm["_ver"] = _shm.get("_ver", 0) + 1

    _log("[后台] 主推送循环退出")


# ========================= 入口 =========================
def main():
    global running, _hub, _gui_status_msg
    running = True
    _reset_gui_shared_memory("启动中...")

    if not _PYQT_OK:
        print("[错误] PyQt5 未安装，无法启动 GUI。请运行: pip install PyQt5")
        return

    # TQSDK 数据采集在后台线程
    data_thread = threading.Thread(target=_tqsdk_data_thread, name="TQSDKDataThread", daemon=True)
    data_thread.start()

    # GUI 必须在主线程运行（PyQt5 要求）
    try:
        app = QtWidgets.QApplication(sys.argv)
        win = MainWindow()
        win.show()
        win.raise_()
        with _hub_lock:
            _hub = Hub(win)
        _gui_status_msg = "GUI 已启动"
        rc = app.exec_()
        _log(f"[主线程] GUI 关闭，退出码 {rc}")
    except Exception as e:
        _log(f"[主线程] GUI 异常: {e}\n{traceback.format_exc()[:200]}")
    finally:
        running = False
        _log("[主线程] 程序退出")


if __name__ == "__main__":
    main()


# ---- 兼容无限易运行环境（若仍在无限易中运行则忽略） ----
class _期权数据单个订阅_vix筛选_dummy:
    """空壳类：兼容无限易按类名查找的机制，实际逻辑已迁移到 main()"""
    pass


_DUMMY_PLACEHOLDER = True  # end of file
