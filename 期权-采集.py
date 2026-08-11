"""
天勤-期权 (TQSDK 数据采集后端)
=============================================
- 常驻 TqApi 连接，实时订阅期权/标的行情
- 自动解析目标合约、补订缺失行情、计算 IV/Greeks
- 将行情写入 SQLite option_chain / option_chain_latest
- 可作为独立数据采集服务供 GUI 读取
"""
import warnings; warnings.filterwarnings("ignore")
import time, logging, sqlite3, os, sys, signal, threading
from datetime import date, datetime, timedelta, timezone

# 官方约定: tqsdk Quote.datetime 是北京时间字符串 (如 "2017-07-26 23:04:21.000001")。
# 显式按 Asia/Shanghai 解析，避免脚本跑在非 UTC+8 机器上时 timestamp 偏移。
try:
    from zoneinfo import ZoneInfo
    _BJ_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    _BJ_TZ = timezone(timedelta(hours=8))

logging.disable(logging.CRITICAL)
for _m in ["tqsdk","tqsdk.objs","tqsdk.api","tqsdk.sim","tqsdk.ta"]:
    logging.getLogger(_m).setLevel(logging.ERROR)

TQ_USER = "18365470981"
TQ_PASS = "Woaini1314@"
# 物理双库分离:
#   - DB_PATH_QUOTES: T 报实时行情 (option_chain / option_chain_latest), 启动时清空
#   - DB_PATH_SIM:    模拟交易 (sim_strategy / sim_strategy_leg / sim_trade / sim_pnl_snapshot), 持久化
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH_QUOTES = os.path.join(_BASE_DIR, "option_quotes.db")
DB_PATH_SIM = os.path.join(_BASE_DIR, "option_sim.db")
DB_PATH = DB_PATH_QUOTES  # 兼容旧引用 (默认指向行情库)      
# === v7 事件驱动模式参数 ===
EVENT_DEADLINE_SEC = float(os.environ.get("OPTION_EVENT_DEADLINE_SEC", "1.0"))
DB_FLUSH_INTERVAL_SEC = float(os.environ.get("OPTION_DB_FLUSH_INTERVAL_SEC", "1.0"))
DB_FLUSH_INTERVAL_OFF_SEC = float(os.environ.get("OPTION_DB_FLUSH_INTERVAL_OFF_SEC", "0"))
QUOTE_LATEST_FLUSH_INTERVAL_SEC = float(os.environ.get("OPTION_QUOTE_LATEST_FLUSH_INTERVAL_SEC", str(DB_FLUSH_INTERVAL_SEC)))
GREEKS_DB_FLUSH_INTERVAL_SEC = float(os.environ.get("OPTION_GREEKS_DB_FLUSH_INTERVAL_SEC", "20.0"))
GREEKS_MAX_GROUPS_PER_FLUSH = int(os.environ.get("OPTION_GREEKS_MAX_GROUPS_PER_FLUSH", "6"))
GUI_REFRESH_INTERVAL_SEC = float(os.environ.get("OPTION_GUI_REFRESH_INTERVAL_SEC", "1.0"))
GUI_STALE_PRODUCT_KEEP_SEC = float(os.environ.get("OPTION_GUI_STALE_PRODUCT_KEEP_SEC", "0"))
PNL_SNAPSHOT_SEC = float(os.environ.get("OPTION_PNL_SNAPSHOT_SEC", "60"))
RECONNECT_AFTER_IDLE_SEC = float(os.environ.get("OPTION_RECONNECT_AFTER_IDLE_SEC", "300"))
# 行情字段静默触发重连阈值: 调短为 90s, 用户体感 1 分钟无更新即可自愈
QUOTE_STALE_RECONNECT_SEC = float(os.environ.get("OPTION_QUOTE_STALE_RECONNECT_SEC", "90"))
FETCH_SCOPE = os.environ.get("OPTION_FETCH_SCOPE", "all").lower()
FULL_REFRESH_EVERY_N_ROUNDS = int(os.environ.get("OPTION_FULL_REFRESH_EVERY", "20"))
# ATM 行权价窗口: 仅取 ATM±N (默认30档, 已覆盖常见交易区间; 不再支持全量模式以避免订阅压力)
OPTION_ATM_WINDOW = max(1, int(os.environ.get("OPTION_ATM_WINDOW", "30")))
OPTION_NEAR_CANDIDATE_LIMIT = max(1, int(os.environ.get("OPTION_NEAR_CANDIDATE_LIMIT", "8")))
OPTION_MIN_EXPIRE_DAYS = max(1.0, float(os.environ.get("OPTION_MIN_EXPIRE_DAYS", "1")))
# 月份范围: near=仅近月(默认,最快), main=仅主力, near_main=近月+主力(全量)
OPTION_MONTH_SCOPE = os.environ.get("OPTION_MONTH_SCOPE", "near").lower()
SIM_OPTION_MULTIPLIER = float(os.environ.get("OPTION_SIM_MULTIPLIER", "1"))
OPTION_LOG_GREEKS_FIELDS = os.environ.get("OPTION_LOG_GREEKS_FIELDS", "0").lower() in ("1", "true", "yes", "on")
CONNECT_RETRY_MAX = int(os.environ.get("OPTION_CONNECT_RETRY_MAX", "2"))
CONNECT_RETRY_SLEEP_SEC = float(os.environ.get("OPTION_CONNECT_RETRY_SLEEP_SEC", "15"))
QUOTE_RETRY_MIN_BATCH = int(os.environ.get("OPTION_QUOTE_RETRY_MIN_BATCH", "1"))

# --- 交易时间配置 ---
# 日盘: 09:00-11:30, 13:30-15:00
# 夜盘: 21:00-02:30(次日) (部分品种有夜盘)
# 收盘期间暂停获取, 避免获取重复的收盘数据
TRADING_SESSIONS = [
    ("09:00", "11:30"),   # 日盘上午
    ("13:30", "15:00"),   # 日盘下午  
    ("21:00", "23:59"),   # 夜盘前段
    ("00:00", "02:30"),   # 夜盘后段(跨日)
]
NON_TRADING_INTERVAL = 300   # 非交易时段检查间隔(秒, 5分钟检查一次是否开盘)

DCE_PRODUCTS = {
    "m":"豆粕","i":"铁矿石","c":"玉米","y":"豆油","p":"棕榈油",
    "pp":"聚丙烯","pg":"LPG","l":"塑料","v":"PVC","eg":"乙二醇",
    "cs":"玉米淀粉","a":"豆一","b":"豆二","j":"焦炭","jm":"焦煤",
}
CZCE_PRODUCTS = {
    "SR":"白糖","CF":"棉花","MA":"甲醇","TA":"PTA","SA":"纯碱",
    "FG":"玻璃","RM":"菜粕","OI":"菜油","SF":"硅铁","SM":"锰硅",
    "PF":"短纤","PK":"花生","AP":"苹果","CJ":"红枣","UR":"尿素","SH":"烧碱",
}
SHFE_PRODUCTS = {
    "cu":"铜","al":"铝","zn":"锌","pb":"铅","ni":"镍","sn":"锡",
    "au":"黄金","ag":"白银","rb":"螺纹钢","hc":"热轧卷板","ss":"不锈钢",
    "ru":"天然橡胶","bu":"沥青","sp":"纸浆","fu":"燃料油","ao":"氧化铝","br":"合成橡胶",
}

def _env_csv(name, default):
    raw = os.environ.get(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

FOCUS_EXCHANGES = _env_csv("OPTION_FOCUS_EXCHANGES", "DCE")
FOCUS_PRODUCTS = {
    "DCE": _env_csv("OPTION_FOCUS_DCE", "m"),
    "CZCE": _env_csv("OPTION_FOCUS_CZCE", ""),
    "SHFE": _env_csv("OPTION_FOCUS_SHFE", ""),
}

EXCHANGE_PRODUCT_CONFIG = [
    ("DCE", "大商所", DCE_PRODUCTS),
    ("CZCE", "郑商所", CZCE_PRODUCTS),
    ("SHFE", "上期所", SHFE_PRODUCTS),
]

def _select_products_for_round(exchange, products_dict, round_num):
    if FETCH_SCOPE == "all":
        return products_dict, "全市场"
    if FETCH_SCOPE == "hybrid" and FULL_REFRESH_EVERY_N_ROUNDS > 0 and round_num % FULL_REFRESH_EVERY_N_ROUNDS == 0:
        return products_dict, f"全市场周期刷新/{FULL_REFRESH_EVERY_N_ROUNDS}轮"
    if exchange not in FOCUS_EXCHANGES:
        return {}, "跳过非关注交易所"
    focus = FOCUS_PRODUCTS.get(exchange) or []
    selected = {k: products_dict[k] for k in focus if k in products_dict}
    return selected, f"关注品种:{','.join(selected) if selected else '-'}"

# ============================================================
#  交易时间检测 — 非交易时段暂停获取
# ============================================================
def is_trading_time():
    """判断当前是否在交易时间内"""
    now = datetime.now()
    # 周末不交易
    if now.weekday() >= 5:  # 周六=5, 周日=6
        return False
    t = now.strftime("%H:%M")
    for start, end in TRADING_SESSIONS:
        if start <= t <= end:
            return True
    return False

def next_trading_time():
    """返回下一个开盘时间(用于显示等待信息)"""
    now = datetime.now()
    today = now.date()
    
    sessions_today = []
    for start, end in TRADING_SESSIONS:
        s = datetime.strptime(f"{today} {start}", "%Y-%m-%d %H:%M")
        e = datetime.strptime(f"{today} {end}", "%Y-%m-%d %H:%M")
        if end < start:  # 跨夜 (21:00-02:30)
            e += timedelta(days=1)
        if now < e:
            sessions_today.append(s)
    
    # 找今天还没过的最早开盘时间
    for st in sorted(sessions_today):
        if now < st:
            return st.strftime("%H:%M")
    
    # 今天都没了, 找明天第一个
    tomorrow = today + timedelta(days=1)
    while tomorrow.weekday() >= 5:  # 跳过周末
        tomorrow += timedelta(days=1)
    return f"明天 {TRADING_SESSIONS[0][0]}"

def _current_session_id():
    """返回当前交易段唯一标识；非交易段返回 None。
    日盘 09:00-15:00 (含午休) -> YYYY-MM-DD-day
    夜盘 21:00-23:59 -> YYYY-MM-DD-night
    夜盘后段 00:00-02:30 -> 前一日 YYYY-MM-DD-night
    周末/节假日 -> None
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return None
    t = now.strftime("%H:%M")
    if "09:00" <= t <= "15:00":
        return f"{now.strftime('%Y-%m-%d')}-day"
    if "21:00" <= t <= "23:59":
        return f"{now.strftime('%Y-%m-%d')}-night"
    if "00:00" <= t <= "02:30":
        prev = (now - timedelta(hours=4)).strftime("%Y-%m-%d")
        return f"{prev}-night"
    return None

# ============================================================
#  带时间戳的日志输出 — 所有输出统一走这里
# ============================================================
def log(msg):
    """统一日志: HH:MM:SS  文本
    推荐在 msg 起头加 [前缀] 标注分类，可用前缀:
    [启动] [订阅] [事件] [落库] [图形] [重连] [警告] [错误] [心跳] [退出]
    """
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)

def log_banner(msg):
    """阶段横幅: 用 ═══ 包裹的单行标题，上方留空行突出阶段切换"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n{ts}  {'═' * 16} {msg} {'═' * 16}", flush=True)

def log_gap():
    print("", flush=True)

def _wait_for(api, seconds):
    """官方 wait_update 的 deadline 参数是绝对 Unix 时间戳 (time.time() 秒数)。
    这里封装成"相对秒数"，避免传错。
    - seconds <= 0 时退化为非阻塞探针 (deadline=now)，仅驱动一次事件处理。
    - 抛出的异常按官方语义向上抛，由调用方决定如何处理。
    """
    try:
        return api.wait_update(deadline=time.time() + max(0.0, float(seconds)))
    except TypeError:
        # 极旧 tqsdk 不支持 deadline 关键字时退化到无超时一次驱动
        return api.wait_update()

def _clean_err_msg(e, max_len=120):
    """剥离 tqsdk/SQLite/Socket 错误的英文堆栈，返回中文摘要。
    避免日志中出现 query { multi_symbol_info... 这种 GraphQL 乱码。
    """
    s = str(e) if e is not None else ""
    if not s:
        return "未知错误"
    low = s.lower()
    if "query {" in s or "mutation {" in s:
        if "multi_symbol_info" in s:
            return "天勤合约元信息查询失败 (合约可能不存在或已下市)"
        if "查询合约服务" in s:
            return "天勤合约查询服务失败"
        first = s.split("\n")[0].strip()
        return f"天勤 GraphQL 请求失败: {first[:max_len]}"
    if "connectionreset" in low or "10054" in s:
        return "连接被远端强制关闭 (ConnectionResetError 10054)"
    if "connectionaborted" in low or "10053" in s:
        return "连接被本地中止 (ConnectionAborted 10053)"
    if "timeout" in low or "timed out" in low:
        return "请求超时"
    if "broken pipe" in low or "epipe" in low:
        return "连接管道破裂 (BrokenPipe)"
    if "database is locked" in low or "database table is locked" in low:
        return "数据库被锁占用 (其他实例正在写入)"
    if "no such table" in low:
        return "数据库表不存在 (行情表 option_chain 尚未初始化)"
    if "websocket" in low:
        return "WebSocket 连接异常"
    return s.replace("\n", " ").strip()[:max_len]

def _fmt_days(days):
    try:
        v = float(days)
        return f"{v:g}"
    except:
        return "-"

def get_mem_mb():
    """获取当前进程内存占用(MB)"""
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    except:
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            c_ulong = ctypes.c_ulong
            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ('cb', c_ulong),('PageFaultCount', c_ulong),
                    ('PeakWorkingSetSize', c_ulong),('WorkingSetSize', c_ulong),
                    ('QuotaPeakPagedPoolUsage', c_ulong),
                    ('QuotaPagedPoolUsage', c_ulong),
                    ('QuotaPeakNonPagedPoolUsage', c_ulong),
                    ('PagefileUsage', c_ulong),('PeakPagefileUsage', c_ulong),
                ]
            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(counters)
            kernel32.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb)
            return counters.WorkingSetSize / 1024 / 1024
        except:
            return 0

def fmt_time(seconds):
    """格式化秒数为可读字符串"""
    if seconds >= 3600:
        return f"{seconds/3600:.1f}h"
    elif seconds >= 60:
        return f"{seconds/60:.1f}min"
    else:
        return f"{seconds:.1f}s"

def safe_float(v, default=0.0):
    if v is None: return default
    try:
        f = float(v)
        return f if f == f else default
    except:
        return default

CONTRACT_MULTIPLIERS = {
    "DCE": {"m": 10, "i": 100, "c": 10, "y": 10, "p": 10, "pp": 5, "pg": 20, "l": 5, "v": 5, "eg": 10, "cs": 10, "a": 10, "b": 10, "j": 100, "jm": 60},
    "CZCE": {"SR": 10, "CF": 5, "MA": 10, "TA": 5, "SA": 20, "FG": 20, "RM": 10, "OI": 10, "SF": 5, "SM": 5, "PF": 5, "PK": 5, "AP": 10, "CJ": 5, "UR": 20, "SH": 30},
    "SHFE": {"cu": 5, "al": 5, "zn": 5, "pb": 5, "ni": 1, "sn": 1, "au": 1000, "ag": 15, "rb": 10, "hc": 10, "ss": 5, "ru": 10, "bu": 10, "sp": 10, "fu": 10, "ao": 20, "br": 5},
}

def _product_multiplier(exchange=None, product=None, default=None, explicit=None):
    v = safe_float(explicit, None)
    if v is not None and v > 0 and abs(v - 1) > 1e-9:
        return float(v)
    ex = str(exchange or "").upper()
    pd = str(product or "")
    m = CONTRACT_MULTIPLIERS.get(ex, {}).get(pd)
    if m is None:
        m = CONTRACT_MULTIPLIERS.get(ex, {}).get(pd.upper())
    if m is None:
        m = default if default is not None else SIM_OPTION_MULTIPLIER
    return float(m or 1)

def _quote_epoch(q):
    _raw, ts = _quote_market_time_values(q)
    return ts

def _parse_quote_market_time_value(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float)):
        v = float(dt)
        if v > 1e17:
            return v / 1e9
        if v > 1e14:
            return v / 1e6
        if v > 1e11:
            return v / 1e3
        if v > 0:
            return v
        return None
    s = str(dt).strip()
    if not s:
        return None
    try:
        return _parse_quote_market_time_value(float(s))
    except:
        pass
    try:
        # tqsdk Quote.datetime 是北京时间字符串 (官方约定)。
        # 显式按 Asia/Shanghai 解析，避免运行环境时区不同导致 timestamp 偏移。
        parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_BJ_TZ)
        return parsed.timestamp()
    except:
        return None

def _quote_market_time_values(q):
    dt = getattr(q, "datetime", None)
    raw = None if dt is None else str(dt)
    return raw, _parse_quote_market_time_value(dt)

def _option_expire_ts(q):
    """期权到期时间戳(秒)。
    依据 tqsdk 官方文档：期权到期取期权专有字段 last_exercise_datetime(期权最后行权日)，
    其次才是通用的 expire_datetime；delivery_year/month 只对期货有效，不可用于期权。
    """
    for attr in ("last_exercise_datetime", "expire_datetime"):
        ts = safe_float(getattr(q, attr, None), None)
        if ts and ts > 0:
            return ts
    return None

def _option_time_to_expiry(q):
    expire_ts = _option_expire_ts(q)
    market_ts = _quote_epoch(q)
    if expire_ts and expire_ts > 0 and market_ts is not None:
        t = (expire_ts - market_ts) / (360.0 * 86400.0)
        if t > 0:
            return t
    days = safe_float(getattr(q, "expire_rest_days", None), None)
    if days and days > 0:
        return float(days) / 360.0
    return None

def _option_impv_price(q):
    bid = safe_float(getattr(q, "bid_price1", None), None)
    ask = safe_float(getattr(q, "ask_price1", None), None)
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2.0
    for attr in ("last_price", "settlement", "pre_close"):
        v = safe_float(getattr(q, attr, None), None)
        if v is not None and v > 0:
            return v
    return None

def _tq_option_impv(q, S, oc):
    """单合约 IV 计算, 失败返回 None。包 np.errstate 抑制 BS 计算的除零/无效警告。"""
    K = safe_float(getattr(q, "strike_price", None), None)
    t = _option_time_to_expiry(q)
    price = _option_impv_price(q)
    if not (S and K and t and price) or S <= 0 or K <= 0 or t <= 0 or price <= 0:
        return None
    try:
        pd_, np_, tafunc_ = _ensure_iv_libs()
        s = pd_.Series([float(S)])
        p = pd_.Series([float(price)])
        with np_.errstate(divide='ignore', invalid='ignore', over='ignore'):
            impv = tafunc_.get_impv(s, p, float(K), RISK_FREE_RATE, 0.3, t, oc)
        v = safe_float(impv.iloc[-1], None)
        if v is not None and _IV_VALUE_LOWER < v < _IV_VALUE_UPPER:
            return round(v * 100.0, 4)
    except Exception:
        pass
    return None

def _option_class_from_symbol(sym, product):
    if '-C-' in sym:
        return "CALL"
    if '-P-' in sym:
        return "PUT"
    rest = sym.split(f"{product}", 1)[1] if product in sym else ""
    for ch in rest:
        if ch == 'C':
            return "CALL"
        if ch == 'P':
            return "PUT"
    return None

def _option_class_of(q, product=None, symbol=None):
    """官方 Quote.option_class 字段是字符串 'CALL'/'PUT'，优先使用。
    取不到再降级到符号解析（兜底极端命名）。
    """
    if q is not None:
        oc = getattr(q, "option_class", None)
        if isinstance(oc, str):
            oc_up = oc.strip().upper()
            if oc_up in ("CALL", "PUT"):
                return oc_up
    sym = symbol or (getattr(q, "instrument_id", None) if q is not None else None)
    if sym and product:
        return _option_class_from_symbol(sym, product)
    return None

def _tq_option_impv_batch(quotes, S, product):
    """批量计算 IV, 返回 (iv_map, reasons)。
    reasons 是诊断计数字典: no_oc/no_K/no_t/no_price/days_short/below_bound/out_of_range/batch_fail
    """
    reasons = {"no_oc": 0, "no_K": 0, "no_t": 0, "no_price": 0,
               "days_short": 0, "below_bound": 0, "out_of_range": 0, "batch_fail": 0}
    if not quotes:
        return {}, reasons
    if not S or S <= 0:
        reasons["no_price"] = len(quotes)  # 标的价缺失等同所有合约无价
        return {}, reasons
    try:
        pd_, np_, tafunc_ = _ensure_iv_libs()
    except Exception as e:
        log(f"[IV] 依赖加载失败: {_clean_err_msg(e, 80)}")
        reasons["batch_fail"] = len(quotes)
        return {}, reasons

    valid = []
    for q in quotes:
        oc = _option_class_of(q, product)
        K = safe_float(getattr(q, "strike_price", None), None)
        t = _option_time_to_expiry(q)
        days = _option_expire_days(q)
        price = _option_impv_price(q)
        if not oc:
            reasons["no_oc"] += 1; continue
        if not K or K <= 0:
            reasons["no_K"] += 1; continue
        if not t or t <= 0:
            reasons["no_t"] += 1; continue
        if not price or price <= 0:
            reasons["no_price"] += 1; continue
        if days is None or days <= OPTION_MIN_EXPIRE_DAYS:
            reasons["days_short"] += 1; continue
        valid.append((q.instrument_id, float(K), float(t), float(price), oc))

    if not valid:
        return {}, reasons

    out = {}
    try:
        s = pd_.Series([float(S)] * len(valid))
        p = pd_.Series([x[3] for x in valid])
        k = pd_.Series([x[1] for x in valid])
        t = pd_.Series([x[2] for x in valid])
        oc = pd_.Series([x[4] for x in valid])
        with np_.errstate(divide='ignore', invalid='ignore', over='ignore'):
            impv = tafunc_.get_impv(s, p, k, RISK_FREE_RATE, 0.3, t, oc)
        for (sym, _K, _t, _price, _oc), v in zip(valid, impv):
            iv = safe_float(v, None)
            if iv is None:
                # NaN: 期权价低于内在价值现值下界, 或牛顿迭代发散
                reasons["below_bound"] += 1
                continue
            if not (_IV_VALUE_LOWER < iv < _IV_VALUE_UPPER):
                reasons["out_of_range"] += 1
                continue
            out[sym] = round(iv * 100.0, 4)
        return out, reasons
    except Exception as e:
        # 整批失败: 切单合约兜底
        log(f"[IV] {product} 批量计算异常, 切单合约模式: {_clean_err_msg(e, 80)} (valid={len(valid)})")
        single_fail = 0
        for sym, K, t_v, price, oc_ in valid:
            try:
                s1 = pd_.Series([float(S)])
                p1 = pd_.Series([float(price)])
                with np_.errstate(divide='ignore', invalid='ignore', over='ignore'):
                    impv_one = tafunc_.get_impv(s1, p1, float(K), RISK_FREE_RATE, 0.3, float(t_v), oc_)
                iv1 = safe_float(impv_one.iloc[-1], None)
                if iv1 is None:
                    reasons["below_bound"] += 1
                    continue
                if not (_IV_VALUE_LOWER < iv1 < _IV_VALUE_UPPER):
                    reasons["out_of_range"] += 1
                    continue
                out[sym] = round(iv1 * 100.0, 4)
            except Exception:
                single_fail += 1
        if single_fail:
            reasons["batch_fail"] = single_fail
            log(f"[IV] {product} 单合约兜底仍失败 {single_fail} 个")
        return out, reasons

def _get_quote_list_resilient(api, symbols, min_batch=QUOTE_RETRY_MIN_BATCH):
    if not symbols:
        return []
    if len(symbols) <= min_batch:
        try:
            return list(api.get_quote_list(symbols))
        except Exception as e:
            sample = ",".join(symbols[:4])
            log(f"[警告] 行情小批次跳过: {len(symbols)} 个合约 sample={sample} ({_clean_err_msg(e, 80)})")
            return []
    try:
        return list(api.get_quote_list(symbols))
    except Exception as e:
        mid = len(symbols) // 2
        log(f"[警告] 行情批次失败: {len(symbols)} 个合约, 拆分重试 ({_clean_err_msg(e, 80)})")
        return _get_quote_list_resilient(api, symbols[:mid], min_batch) + _get_quote_list_resilient(api, symbols[mid:], min_batch)

def _subscribe_quote_list_fast(api, symbols):
    try:
        return list(api.get_quote_list(symbols))
    except Exception as e:
        sample = ",".join(symbols[:4])
        log(f"[警告] 行情缺失小批次跳过: {len(symbols)} 个合约 sample={sample} ({_clean_err_msg(e, 80)})")
        return []

def _get_quote_list_limited(api, symbols, retries=1):
    if not symbols:
        return []
    last_err = None
    for i in range(retries + 1):
        try:
            return list(api.get_quote_list(symbols))
        except Exception as e:
            last_err = e
            if i < retries:
                try:
                    _wait_for(api, 1)
                except:
                    pass
    log(f"[警告] 行情批次放弃: {len(symbols)} 个合约 ({_clean_err_msg(last_err, 80)})")
    return []

def _get_quotes_cached(api, symbols):
    if not symbols:
        return [], 0, 0
    unique = list(dict.fromkeys(symbols))
    cached = {}
    missing = []
    with _quote_cache_lock:
        for sym in unique:
            q = _quote_cache.get(sym)
            if q is None:
                missing.append(sym)
            else:
                cached[sym] = q
    subscribed = []
    failed_batches = 0
    for i in range(0, len(missing), QUOTE_BATCH_SIZE):
        batch = missing[i:i + QUOTE_BATCH_SIZE]
        try:
            got = list(api.get_quote_list(batch))
        except Exception as e:
            failed_batches += 1
            sample = ",".join(batch[:4])
            log(f"[警告] 行情批次跳过: {len(batch)} 个合约 sample={sample} ({_clean_err_msg(e, 80)})")
            got = []
        subscribed.extend(got)
        got_syms = {getattr(q, "instrument_id", None) for q in got}
        lost = [s for s in batch if s not in got_syms]
        if lost and len(lost) <= QUOTE_RETRY_MIN_BATCH:
            subscribed.extend(_subscribe_quote_list_fast(api, lost))
        elif lost:
            log(f"[警告] 行情批次缺失跳过: {len(lost)} 个合约 sample={','.join(lost[:4])}")
    if subscribed:
        with _quote_cache_lock:
            for q in subscribed:
                sym = getattr(q, "instrument_id", None)
                if sym:
                    _quote_cache[sym] = q
                    cached[sym] = q
    else:
        try:
            _wait_for(api, QUOTE_CACHE_WAIT_SEC)
        except:
            pass
    out = []
    with _quote_cache_lock:
        for sym in unique:
            q = cached.get(sym) or _quote_cache.get(sym)
            if q is not None:
                out.append(q)
    return out, len(missing), failed_batches

def _clear_quote_cache():
    with _quote_cache_lock:
        _quote_cache.clear()

def _clear_api_bound_caches():
    global _greeks_cache_ts
    _clear_quote_cache()
    with _greeks_cache_lock:
        _greeks_cache.clear()
        _greeks_cache_ts = 0

DB_BUSY_TIMEOUT_MS = int(os.environ.get("OPTION_DB_BUSY_TIMEOUT_MS", "5000"))
DB_WRITE_RETRY_MAX = int(os.environ.get("OPTION_DB_WRITE_RETRY_MAX", "4"))
DB_WRITE_RETRY_SLEEP_SEC = float(os.environ.get("OPTION_DB_WRITE_RETRY_SLEEP_SEC", "1.5"))
_db_write_lock = threading.RLock()
_db_init_lock = threading.RLock()
_db_initialized = False
_db_app_lock_fd = None
_db_app_lock_path = DB_PATH_QUOTES + ".app.lock"

def _resolve_db_path(kind):
    if kind == "sim":
        return DB_PATH_SIM
    return DB_PATH_QUOTES

def _db_connect(row_factory=False, busy_timeout_ms=None, kind="quotes"):
    timeout_ms = DB_BUSY_TIMEOUT_MS if busy_timeout_ms is None else int(busy_timeout_ms)
    path = _resolve_db_path(kind)
    conn = sqlite3.connect(path, timeout=timeout_ms / 1000)
    conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    conn.execute("PRAGMA synchronous=NORMAL")
    if row_factory:
        conn.row_factory = sqlite3.Row
    return conn

def _is_db_locked_error(e):
    return isinstance(e, sqlite3.OperationalError) and "database is locked" in str(e).lower()

def _db_write_retry(label, fn, attempts=None, kind="quotes"):
    attempts = int(attempts or DB_WRITE_RETRY_MAX)
    last_err = None
    for i in range(1, attempts + 1):
        conn = None
        try:
            with _db_write_lock:
                conn = _db_connect(kind=kind)
                cur = conn.cursor()
                result = fn(conn, cur)
                conn.commit()
                return result
        except Exception as e:
            last_err = e
            if conn is not None:
                try:
                    conn.rollback()
                except:
                    pass
            if not _is_db_locked_error(e) or i >= attempts:
                raise
            log(f"数据库写入繁忙: {label} 第{i}/{attempts}次失败，{DB_WRITE_RETRY_SLEEP_SEC * i:.1f}s后重试")
            time.sleep(DB_WRITE_RETRY_SLEEP_SEC * i)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except:
                    pass
    raise last_err

def _pid_exists(pid):
    try:
        pid = int(pid)
    except:
        return False
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except:
            return False
    try:
        os.kill(pid, 0)
        return True
    except:
        return False

def _acquire_app_instance_lock():
    global _db_app_lock_fd
    try:
        fd = os.open(_db_app_lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, f"{os.getpid()} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".encode("utf-8"))
        _db_app_lock_fd = fd
        return True
    except FileExistsError:
        old_pid = None
        try:
            with open(_db_app_lock_path, "r", encoding="utf-8") as f:
                old_pid = (f.read().strip().split() or [None])[0]
        except:
            pass
        if old_pid and _pid_exists(old_pid):
            log(f"[警告] 检测到已有实例正在使用数据库: PID {old_pid}, 为避免数据库锁冲突, 本实例不启动")
            return False
        try:
            os.remove(_db_app_lock_path)
        except:
            log(f"[错误] 数据库实例锁文件无法清理: {_db_app_lock_path}")
            return False
        return _acquire_app_instance_lock()
    except Exception as e:
        log(f"[错误] 数据库实例锁创建失败: {_clean_err_msg(e, 120)}")
        return False

def _release_app_instance_lock():
    global _db_app_lock_fd
    if _db_app_lock_fd is not None:
        try:
            os.close(_db_app_lock_fd)
        except:
            pass
        _db_app_lock_fd = None
    try:
        if os.path.exists(_db_app_lock_path):
            os.remove(_db_app_lock_path)
    except:
        pass

OPTION_CHAIN_COLUMNS = [
    "id", "market_time", "market_ts", "exchange", "product", "product_name", "underlying_sym",
    "option_sym", "contract_role", "option_class", "strike_price", "last_price", "pre_close",
    "settlement", "bid_price1", "ask_price1", "bid_volume1", "ask_volume1", "volume",
    "open_interest", "underlying_price", "volume_multiple", "expire_rest_days", "expire_datetime",
    "exercise_type", "delta", "gamma", "theta", "vega", "rho", "iv", "iv_open", "iv_chg"
]

OPTION_CHAIN_LATEST_COLUMNS = OPTION_CHAIN_COLUMNS[1:]

def _create_option_chain_table(c, table_name, latest=False):
    id_sql = "option_sym TEXT PRIMARY KEY," if latest else "id INTEGER PRIMARY KEY AUTOINCREMENT,"
    option_sym_sql = "" if latest else "option_sym TEXT NOT NULL,"
    c.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
        {id_sql}
        market_time TEXT NOT NULL,
        market_ts REAL NOT NULL,
        exchange TEXT, product TEXT, product_name TEXT,
        underlying_sym TEXT, {option_sym_sql}
        contract_role TEXT,
        option_class TEXT, strike_price REAL,
        last_price REAL, pre_close REAL, settlement REAL,
        bid_price1 REAL, ask_price1 REAL,
        bid_volume1 INTEGER, ask_volume1 INTEGER,
        volume INTEGER, open_interest INTEGER,
        underlying_price REAL,
        volume_multiple REAL,
        expire_rest_days REAL, expire_datetime REAL,
        exercise_type TEXT,
        delta REAL, gamma REAL, theta REAL, vega REAL, rho REAL,
        iv REAL, iv_open REAL, iv_chg REAL
    )""")

def _market_time_expr(cols):
    if "market_time" in cols:
        return "market_time"
    if "quote_datetime" in cols:
        return "quote_datetime"
    return "NULL"

def _market_ts_expr(cols):
    if "market_ts" in cols:
        return "market_ts"
    if "quote_ts" in cols:
        return "quote_ts"
    return "NULL"

def _migrate_option_table_to_market_time(c, table_name):
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table_name})").fetchall()]
    if not cols:
        _create_option_chain_table(c, table_name, table_name == "option_chain_latest")
        return
    target_cols = OPTION_CHAIN_COLUMNS if table_name == "option_chain" else OPTION_CHAIN_LATEST_COLUMNS
    if "fetch_time" not in cols and "quote_datetime" not in cols and "quote_ts" not in cols and all(x in cols for x in target_cols):
        return
    tmp = f"{table_name}_market_migrate"
    c.execute(f"DROP TABLE IF EXISTS {tmp}")
    _create_option_chain_table(c, tmp, table_name == "option_chain_latest")
    select_exprs = []
    insert_cols = []
    for col in target_cols:
        if col == "id":
            if col in cols:
                select_exprs.append(col)
                insert_cols.append(col)
            continue
        if col == "market_time":
            select_exprs.append(_market_time_expr(cols))
        elif col == "market_ts":
            select_exprs.append(_market_ts_expr(cols))
        elif col in cols:
            select_exprs.append(col)
        else:
            select_exprs.append("NULL")
        insert_cols.append(col)
    market_time_expr = _market_time_expr(cols)
    market_ts_expr = _market_ts_expr(cols)
    c.execute(f"""INSERT INTO {tmp} ({",".join(insert_cols)})
        SELECT {",".join(select_exprs)} FROM {table_name}
        WHERE {market_time_expr} IS NOT NULL AND {market_ts_expr} IS NOT NULL AND {market_ts_expr}>0""")
    c.execute(f"DROP TABLE {table_name}")
    c.execute(f"ALTER TABLE {tmp} RENAME TO {table_name}")

_SIM_TABLES = ("sim_strategy", "sim_strategy_leg", "sim_trade", "sim_pnl_snapshot", "signal_log")

def _sim_create_tables(c):
    """模拟交易库 schema, 跨启动持久化"""
    c.execute("""CREATE TABLE IF NOT EXISTS sim_strategy (
        strategy_id TEXT PRIMARY KEY,
        strategy_name TEXT,
        strategy_type TEXT,
        scope_text TEXT,
        status TEXT,
        open_time TEXT,
        close_time TEXT,
        open_premium REAL,
        close_premium REAL,
        realized_pnl REAL,
        unrealized_pnl REAL,
        open_proxy_vix REAL,
        note TEXT
    )""")
    strat_cols = [r[1] for r in c.execute("PRAGMA table_info(sim_strategy)").fetchall()]
    if "open_proxy_vix" not in strat_cols:
        c.execute("ALTER TABLE sim_strategy ADD COLUMN open_proxy_vix REAL")
    c.execute("""CREATE TABLE IF NOT EXISTS sim_strategy_leg (
        leg_id TEXT PRIMARY KEY,
        strategy_id TEXT,
        exchange TEXT,
        product TEXT,
        product_name TEXT,
        underlying_sym TEXT,
        option_sym TEXT,
        option_class TEXT,
        strike_price REAL,
        expire_rest_days REAL,
        side TEXT,
        volume INTEGER,
        multiplier REAL,
        open_price REAL,
        open_underlying_price REAL,
        close_price REAL,
        current_price REAL,
        delta REAL,
        gamma REAL,
        theta REAL,
        vega REAL,
        iv REAL,
        status TEXT
    )""")
    leg_cols = [r[1] for r in c.execute("PRAGMA table_info(sim_strategy_leg)").fetchall()]
    if "open_underlying_price" not in leg_cols:
        c.execute("ALTER TABLE sim_strategy_leg ADD COLUMN open_underlying_price REAL")
    c.execute("""CREATE TABLE IF NOT EXISTS sim_trade (
        trade_id TEXT PRIMARY KEY,
        strategy_id TEXT,
        leg_id TEXT,
        option_sym TEXT,
        action TEXT,
        side TEXT,
        volume INTEGER,
        price REAL,
        trade_time TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS sim_pnl_snapshot (
        snapshot_id TEXT PRIMARY KEY,
        strategy_id TEXT,
        snapshot_time TEXT,
        status TEXT,
        unrealized_pnl REAL,
        realized_pnl REAL,
        total_pnl REAL,
        open_premium REAL,
        close_premium REAL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_strategy_status ON sim_strategy(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_leg_strategy ON sim_strategy_leg(strategy_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_pnl_strategy_time ON sim_pnl_snapshot(strategy_id,snapshot_time)")
    # === 比例价差信号监控 持久化表 ===
    c.execute("""CREATE TABLE IF NOT EXISTS signal_log (
        signal_id TEXT PRIMARY KEY,
        trigger_time TEXT NOT NULL,
        trigger_ts REAL NOT NULL,
        trade_date TEXT NOT NULL,
        exchange TEXT, product TEXT, name TEXT,
        underlying_sym TEXT,
        side TEXT NOT NULL,
        ratio TEXT NOT NULL,
        ratio_n INTEGER,
        buy_option_sym TEXT, sell_option_sym TEXT,
        buy_strike REAL, sell_strike REAL,
        buy_price REAL, sell_price REAL,
        buy_volume INTEGER, sell_volume INTEGER,
        net_premium REAL,
        atm_avg REAL,
        threshold REAL,
        strike_gap REAL,
        underlying_price REAL,
        proxy_vix REAL,
        product_volume INTEGER,
        days INTEGER,
        status TEXT NOT NULL,
        last_seen_ts REAL,
        expired_ts REAL,
        last_net_premium REAL,
        last_buy_price REAL,
        last_sell_price REAL,
        opened_strategy_id TEXT,
        opened_ts REAL
    )""")
    # ALTER 兼容: 旧版本 signal_log 缺新增列时补
    sig_cols = [r[1] for r in c.execute("PRAGMA table_info(signal_log)").fetchall()]
    if "opened_strategy_id" not in sig_cols:
        c.execute("ALTER TABLE signal_log ADD COLUMN opened_strategy_id TEXT")
    if "opened_ts" not in sig_cols:
        c.execute("ALTER TABLE signal_log ADD COLUMN opened_ts REAL")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_trade_date ON signal_log(trade_date, trigger_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_status ON signal_log(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_opened ON signal_log(opened_strategy_id)")


def _migrate_sim_tables_from_quotes_to_sim(quotes_conn, sim_conn):
    """一次性迁移: 旧版本 sim_* 表写在 quotes 库, 把数据搬到独立 sim 库, 然后从 quotes 库 DROP。
    幂等: 只迁移 quotes 库还存在的表; 已经迁移过的不会重复。
    """
    qc = quotes_conn.cursor()
    sc = sim_conn.cursor()
    moved_total = 0
    for tbl in _SIM_TABLES:
        row = qc.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
        ).fetchone()
        if not row:
            continue
        try:
            cols = [r[1] for r in qc.execute(f"PRAGMA table_info({tbl})").fetchall()]
            if not cols:
                qc.execute(f"DROP TABLE IF EXISTS {tbl}")
                continue
            sim_cols = [r[1] for r in sc.execute(f"PRAGMA table_info({tbl})").fetchall()]
            common = [c for c in cols if c in sim_cols]
            if not common:
                qc.execute(f"DROP TABLE IF EXISTS {tbl}")
                continue
            col_list = ",".join(common)
            rows = qc.execute(f"SELECT {col_list} FROM {tbl}").fetchall()
            if rows:
                qmarks = ",".join(["?"] * len(common))
                sc.executemany(
                    f"INSERT OR IGNORE INTO {tbl} ({col_list}) VALUES ({qmarks})",
                    rows,
                )
                moved_total += len(rows)
            qc.execute(f"DROP TABLE IF EXISTS {tbl}")
        except Exception as _e:
            log(f"[警告] 迁移 {tbl} 失败: {_clean_err_msg(_e, 80)}")
    if moved_total:
        log(f"[启动] 已把 {moved_total} 行 sim_* 数据从行情库迁移到独立模拟库")
    quotes_conn.commit()
    sim_conn.commit()


def _init_quotes_db(c):
    """T 报行情库 schema (option_chain / option_chain_latest); 启动时表内容会被清空"""
    c.execute("PRAGMA journal_mode=WAL")
    _migrate_option_table_to_market_time(c, "option_chain")
    _migrate_option_table_to_market_time(c, "option_chain_latest")
    c.execute("CREATE INDEX IF NOT EXISTS idx_market_ts ON option_chain(market_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ep ON option_chain(exchange,product)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_market_epu ON option_chain(market_ts,exchange,product,underlying_sym)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_option_market ON option_chain(option_sym,market_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_latest_market_ts ON option_chain_latest(market_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_latest_epu ON option_chain_latest(exchange,product,underlying_sym)")


def _init_sim_db(c):
    c.execute("PRAGMA journal_mode=WAL")
    _sim_create_tables(c)


def init_db():
    global _db_initialized
    with _db_init_lock:
        if _db_initialized:
            return
        with _db_write_lock:
            qconn = sconn = None
            try:
                qconn = _db_connect(kind="quotes")
                qc = qconn.cursor()
                _init_quotes_db(qc)
                qconn.commit()

                sconn = _db_connect(kind="sim")
                sc = sconn.cursor()
                _init_sim_db(sc)
                sconn.commit()

                # 一次性迁移: 旧 quotes 库里残留的 sim_* 表 -> 独立 sim 库
                try:
                    _migrate_sim_tables_from_quotes_to_sim(qconn, sconn)
                except Exception as _e:
                    log(f"[警告] sim 表迁移异常: {_clean_err_msg(_e, 80)}")

                _db_initialized = True
            finally:
                for c_ in (qconn, sconn):
                    if c_ is not None:
                        try:
                            c_.close()
                        except:
                            pass


# ============================================================
#  比例价差信号监控 (SignalMonitor)
#
#  数据流: get_memory_snapshot() -> SignalMonitor.scan_once() -> 状态机
#  -> (新触发) 弹窗 + 提示音 + 写 signal_log + 加入 active
#  -> (持续)   仅更新 last_seen_ts / last_*_price
#  -> (失效)   标记 EXPIRED, 写 expired_ts
#  -> (重现)   视为新信号, 生成新 signal_id
# ============================================================

def _play_alert_sound():
    """Windows 提示音 (非阻塞)。失败静默, 不影响主流程。"""
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_ICONASTERISK)
    except Exception:
        try:
            sys.stdout.write("\a"); sys.stdout.flush()
        except Exception:
            pass


def _signal_key(sig):
    return (sig.get("exchange") or "", sig.get("product") or "",
            sig.get("side") or "", sig.get("ratio") or "",
            float(sig.get("buy_strike") or 0), float(sig.get("sell_strike") or 0))


def _current_trade_date():
    """中国期货交易日: 按 16:00 切日 (夜盘归到下一交易日)。
    跨周末/节假日处理粗略, 只用于"今日信号"分组, 历史 DB 仍按 trigger_ts 完整保留。"""
    now = datetime.now()
    base = now.date()
    if now.hour >= 16:
        base = base + timedelta(days=1)
    # 跨周末: 周六/周日 推到下周一
    wd = base.weekday()  # 0=Mon ... 6=Sun
    if wd == 5:
        base = base + timedelta(days=2)
    elif wd == 6:
        base = base + timedelta(days=1)
    return base.strftime("%Y-%m-%d")


class SignalMonitor:
    """正比例价差信号监控器 - 5 阶段筛选 + 状态机 + DB 持久化。

    阶段1: 代理 VIX Top 5 品种 (近月)
    阶段2: 近月合约组成交量 > 20000
    阶段3: CALL/PUT 虚值合约 Top3 by volume, 最小 volume > 2000
    阶段4: 3 合约枚举 (3 组合 × 比例 1:2/1:3) 共 6 种价差候选
    阶段5: 净权利金>0 且 行权价间距>ATM均值×倍数
    """

    PRODUCT_VOLUME_THRESHOLD = 20000
    LEG_VOLUME_MIN = 2000

    def __init__(self, snapshot_getter=None):
        """snapshot_getter: 注入 get_memory_snapshot, 默认用模块级函数。"""
        self._get_snapshot = snapshot_getter or get_memory_snapshot
        self._active = {}              # key -> sig dict (含 signal_id, status=ACTIVE/OPENED)
        self._opened_keys = {}         # key -> {"signal_id":..., "strategy_id":...} (已开仓, 不再弹窗)
        # 弹窗去重: 同一 signal_key 在同一交易日只弹一次
        # entry: {"trade_date": "2026-05-28", "signal_id": "SIG..."}
        # 释放条件 (_maybe_reset_alerted): 跨交易日 + key 不在 _opened_keys (没有 OPEN 仓位)
        self._alerted_keys = {}
        self._lock = threading.RLock()
        self._alert_callback = None
        try:
            init_db()
            self._load_opened_keys()
            self._load_alerted_keys_today()
        except Exception as _e:
            log(f"[信号监控] 初始化 DB 失败: {_clean_err_msg(_e, 80)}")

    def _load_opened_keys(self):
        """从 signal_log 读取所有 status='OPENED' 且 opened_strategy_id 关联的 sim_strategy 仍 OPEN 的信号,
        恢复 _opened_keys 映射 (跨启动不重弹窗)。"""
        try:
            conn = _db_connect(row_factory=True, kind="sim")
            cur = conn.cursor()
            cur.execute("""SELECT s.* FROM signal_log s
                           LEFT JOIN sim_strategy st ON st.strategy_id = s.opened_strategy_id
                           WHERE s.status='OPENED' AND st.status='OPEN'""")
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            for r in rows:
                k = self._key_from_row(r)
                self._opened_keys[k] = {
                    "signal_id": r.get("signal_id"),
                    "strategy_id": r.get("opened_strategy_id"),
                }
            if rows:
                log(f"[信号监控] 启动恢复 {len(rows)} 个已开仓信号 (不再弹窗)")
        except Exception as e:
            log(f"[信号监控] 恢复已开仓信号失败: {_clean_err_msg(e, 80)}")

    def _load_alerted_keys_today(self):
        """启动时从 signal_log 读取「当日」所有触发记录, 按 key 去重后灬入 _alerted_keys,
        避免重启后对同日事件重复弹窗。"""
        try:
            today = _current_trade_date()
            conn = _db_connect(row_factory=True, kind="sim")
            cur = conn.cursor()
            cur.execute("""SELECT signal_id, exchange, product, side, ratio, buy_strike, sell_strike, trade_date
                           FROM signal_log WHERE trade_date=? ORDER BY trigger_ts""", (today,))
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            for r in rows:
                k = self._key_from_row(r)
                # 后者覆盖前者 (ORDER BY trigger_ts ASC 保证最后保留的是最新 signal_id)
                self._alerted_keys[k] = {
                    "trade_date": r.get("trade_date") or today,
                    "signal_id": r.get("signal_id"),
                }
            if rows:
                log(f"[信号监控] 启动恢复 今日弹过信号 {len(self._alerted_keys)} 个 (不再重弹)")
        except Exception as e:
            log(f"[信号监控] 恢复当日弹窗记录失败: {_clean_err_msg(e, 80)}")

    def _maybe_reset_alerted(self):
        """跨交易日 + 无关联 OPEN 仓位 → 从 _alerted_keys 移除, 允许下次重弹。
        规则:
          - 同一 trade_date: 不重弹
          - 跨日且 key 不在 _opened_keys: 解锁
          - 跨日但 key 仍在 _opened_keys: 仓位未平, 不解锁 (等平仓+再跨日)
        """
        today = _current_trade_date()
        with self._lock:
            for k in list(self._alerted_keys.keys()):
                entry = self._alerted_keys[k]
                if entry.get("trade_date") != today and k not in self._opened_keys:
                    self._alerted_keys.pop(k, None)

    @staticmethod
    def _key_from_row(row):
        return (row.get("exchange") or "", row.get("product") or "",
                row.get("side") or "", row.get("ratio") or "",
                float(row.get("buy_strike") or 0), float(row.get("sell_strike") or 0))

    def set_alert_callback(self, fn):
        """fn(sig_dict) -> None  (新边沿触发时调用, 用于弹窗/提示音)"""
        self._alert_callback = fn

    # ---------- 公开接口 ----------
    def scan_once(self):
        """单轮扫描. 非交易段直接返回. 返回 (new_triggered, expired_keys)。"""
        try:
            snap = self._get_snapshot()
        except Exception:
            return [], []
        if not snap or not snap.get("is_trading"):
            return [], []
        # 扫描前先试图跨日解锁已弹过的信号 (跨日且无仓 → 可重弹)
        try:
            self._maybe_reset_alerted()
        except Exception as _e:
            log(f"[信号监控] 跨日重置弹窗记录失败: {_clean_err_msg(_e, 80)}")
        triggered = self._scan(snap.get("products") or [])
        return self._update_state(triggered)

    def get_active_signals(self):
        with self._lock:
            return [dict(s) for s in self._active.values()]

    def get_today_history(self, trade_date=None):
        """从 DB 读取当天所有信号 (按触发时间倒序)。trade_date 为 None 时用当前交易日。"""
        if trade_date is None:
            trade_date = _current_trade_date()
        try:
            init_db()
            conn = _db_connect(row_factory=True, kind="sim")
            cur = conn.cursor()
            # 排序: (交易所, 品种, CALL→PUT, 行权价高→低, 触发时间新→旧)
            cur.execute("""SELECT * FROM signal_log WHERE trade_date=?
                           ORDER BY exchange, product, side,
                                    COALESCE(sell_strike, buy_strike) DESC,
                                    trigger_ts DESC""", (trade_date,))
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            log(f"[信号监控] 读取历史失败: {_clean_err_msg(e, 80)}")
            return []

    # ---------- 5 阶段扫描 ----------
    def _scan(self, products):
        if not products:
            return []
        # 阶段 1: 代理 VIX Top 5
        vix_rank = self._product_vix_rank(products)
        if not vix_rank:
            return []
        top5 = vix_rank[:5]
        # 映射到 pi (近月)
        near_lookup = {}
        for pi in products:
            key = (pi.get("exchange") or "", pi.get("product") or "", pi.get("name") or "")
            if (pi.get("ctype") or "") not in ("near", "near_main"):
                continue
            # 同 key 多个 pi (理论不会发生), 取到期最近
            if key in near_lookup:
                prev = near_lookup[key]
                if (pi.get("days") or 0) < (prev.get("days") or 0):
                    near_lookup[key] = pi
            else:
                near_lookup[key] = pi

        triggered = []
        for rank_row in top5:
            key = (rank_row.get("exchange"), rank_row.get("product"), rank_row.get("name"))
            pi = near_lookup.get(key)
            if not pi:
                # 没有近月数据, 跳过 (用户确认)
                continue
            S = pi.get("price")
            try:
                S = float(S) if S not in (None, "", "-") else 0
            except (TypeError, ValueError):
                S = 0
            if S <= 0:
                continue
            # 阶段 2: 合约组成交量
            product_volume = 0
            for rw in pi.get("rows", []):
                for sk in ("c", "p"):
                    d = rw.get(sk)
                    if d:
                        product_volume += int(d.get("volume") or 0)
            if product_volume <= self.PRODUCT_VOLUME_THRESHOLD:
                continue
            atm_avg = self._compute_atm_avg(pi)
            if atm_avg is None or atm_avg <= 0:
                continue
            proxy_vix = rank_row.get("vix")
            # 阶段 3-5: CALL/PUT 独立判定
            for side_key, side_label in (("c", "CALL"), ("p", "PUT")):
                otm_top3 = self._collect_otm_top3(pi, side_key, S)
                if len(otm_top3) < 3:
                    continue
                if min(it["volume"] for it in otm_top3) <= self.LEG_VOLUME_MIN:
                    continue
                # 按 K 升序
                otm_sorted = sorted(otm_top3, key=lambda x: x["K"])
                # 3 种两两组合
                for i_pair in ((0, 1), (0, 2), (1, 2)):
                    if side_key == "c":
                        # CALL: 买低 K (贵 OTM 接近 ATM) + 卖高 K (更虚, 便宜) × N
                        buy_leg = otm_sorted[i_pair[0]]
                        sell_leg = otm_sorted[i_pair[1]]
                    else:
                        # PUT: 买高 K (贵 OTM 接近 ATM) + 卖低 K (更虚, 便宜) × N
                        buy_leg = otm_sorted[2 - i_pair[0]]
                        sell_leg = otm_sorted[2 - i_pair[1]]
                    if buy_leg["K"] == sell_leg["K"]:
                        continue
                    buy_ask = self._safe_num(buy_leg.get("ask"))
                    sell_bid = self._safe_num(sell_leg.get("bid"))
                    if not buy_ask or buy_ask <= 0:
                        continue
                    if not sell_bid or sell_bid <= 0:
                        continue
                    strike_gap = abs(sell_leg["K"] - buy_leg["K"])
                    for ratio_n in (2, 3):
                        net_premium = sell_bid * ratio_n - buy_ask * 1
                        if net_premium <= 0:
                            continue
                        threshold = atm_avg * (1 if ratio_n == 2 else 2)
                        if strike_gap <= threshold:
                            continue
                        triggered.append({
                            "exchange": pi.get("exchange"),
                            "product": pi.get("product"),
                            "name": pi.get("name"),
                            "underlying_sym": pi.get("sym"),
                            "side": side_label,
                            "ratio": f"1:{ratio_n}",
                            "ratio_n": ratio_n,
                            "buy_option_sym": buy_leg["sym"],
                            "sell_option_sym": sell_leg["sym"],
                            "buy_strike": buy_leg["K"],
                            "sell_strike": sell_leg["K"],
                            "buy_price": buy_ask,
                            "sell_price": sell_bid,
                            "buy_volume": buy_leg["volume"],
                            "sell_volume": sell_leg["volume"],
                            "net_premium": net_premium,
                            "atm_avg": atm_avg,
                            "threshold": threshold,
                            "strike_gap": strike_gap,
                            "underlying_price": S,
                            "proxy_vix": proxy_vix,
                            "product_volume": product_volume,
                            "days": pi.get("days"),
                        })
        return triggered

    def _collect_otm_top3(self, pi, side_key, S):
        items = []
        for rw in pi.get("rows", []):
            d = rw.get(side_key)
            if not d:
                continue
            K = rw.get("K")
            if K is None:
                continue
            # ATM 那一档已用于计算 atm_avg, 不重复算入虚值
            if rw.get("atm"):
                continue
            # 虚值: CALL K>S, PUT K<S
            if side_key == "c" and K <= S:
                continue
            if side_key == "p" and K >= S:
                continue
            vol = int(d.get("volume") or 0)
            if vol <= 0:
                continue
            items.append({
                "K": K, "sym": d.get("option_sym"),
                "ask": d.get("ask"), "bid": d.get("bid"),
                "last": d.get("last"), "volume": vol,
            })
        items.sort(key=lambda x: x["volume"], reverse=True)
        return items[:3]

    def _compute_atm_avg(self, pi):
        atm_rows = [rw for rw in pi.get("rows", []) if rw.get("atm")]
        if not atm_rows:
            return None
        rw = atm_rows[0]
        c_last = self._safe_num((rw.get("c") or {}).get("last"))
        p_last = self._safe_num((rw.get("p") or {}).get("last"))
        if c_last and c_last > 0 and p_last and p_last > 0:
            return (c_last + p_last) / 2.0
        # 退化: 只有一边
        if c_last and c_last > 0:
            return c_last
        if p_last and p_last > 0:
            return p_last
        return None

    def _product_vix_rank(self, products):
        """代理 VIX = 近月 ATM 行 (CALL IV + PUT IV) 均值。与 GUI _product_vix_rank 同口径。"""
        groups = {}
        for pi in products:
            ex = pi.get("exchange") or ""
            code = pi.get("product") or ""
            name = pi.get("name") or code
            groups.setdefault((ex, code, name), []).append(pi)
        out = []
        for (ex, code, name), items in groups.items():
            near_pi = next((p for p in items if (p.get("ctype") or "") in ("near", "near_main")), None)
            if near_pi is None:
                near_pi = min(items, key=lambda p: p.get("days") or 999999)
            v = self._contract_atm_vix(near_pi)
            if v is not None:
                out.append({"exchange": ex, "product": code, "name": name, "vix": v})
        out.sort(key=lambda x: x["vix"], reverse=True)
        return out

    def _contract_atm_vix(self, pi):
        rows = pi.get("rows", [])
        atm_rows = [rw for rw in rows if rw.get("atm")]
        if not atm_rows and rows:
            S = pi.get("price", 0) or 0
            atm_rows = sorted(rows, key=lambda rw: abs((rw.get("K") or 0) - S))[:1]
        vals = []
        for rw in atm_rows:
            for side in ("c", "p"):
                d = rw.get(side)
                if not d:
                    continue
                iv = self._safe_num(d.get("iv"))
                if iv is not None:
                    vals.append(iv)
        return sum(vals) / len(vals) if vals else None

    @staticmethod
    def _safe_num(v):
        if v is None or v == "-" or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    # ---------- 状态机 ----------
    def _update_state(self, triggered):
        now_ts = time.time()
        new_signals = []
        with self._lock:
            current_keys = set()
            for sig in triggered:
                key = _signal_key(sig)
                current_keys.add(key)
                # 已开仓的 key: 持续追踪 last_*, 但不弹窗不算新触发
                if key in self._opened_keys:
                    opened_sig_id = self._opened_keys[key].get("signal_id")
                    if opened_sig_id:
                        try:
                            self._update_last_seen(opened_sig_id, now_ts,
                                                   sig["net_premium"], sig["buy_price"], sig["sell_price"])
                        except Exception as _e:
                            log(f"[信号监控] 更新已开仓信号 last_seen 失败: {_clean_err_msg(_e, 80)}")
                    continue
                if key in self._active:
                    existing = self._active[key]
                    existing["last_seen_ts"] = now_ts
                    existing["last_net_premium"] = sig["net_premium"]
                    existing["last_buy_price"] = sig["buy_price"]
                    existing["last_sell_price"] = sig["sell_price"]
                    try:
                        self._update_last_seen(existing["signal_id"], now_ts,
                                               sig["net_premium"], sig["buy_price"], sig["sell_price"])
                    except Exception as _e:
                        log(f"[信号监控] 更新 last_seen 失败: {_clean_err_msg(_e, 80)}")
                else:
                    sig["signal_id"] = _sim_id("SIG")
                    sig["trigger_ts"] = now_ts
                    sig["trigger_time"] = datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
                    sig["trade_date"] = _current_trade_date()
                    sig["status"] = "ACTIVE"
                    sig["last_seen_ts"] = now_ts
                    sig["last_net_premium"] = sig["net_premium"]
                    sig["last_buy_price"] = sig["buy_price"]
                    sig["last_sell_price"] = sig["sell_price"]
                    self._active[key] = sig
                    try:
                        self._persist_new(sig)
                    except Exception as _e:
                        log(f"[信号监控] 写入 signal_log 失败: {_clean_err_msg(_e, 80)}")
                    # 去重: 仅当日首次触发 (key 不在 _alerted_keys) 才走弹窗回调
                    if key not in self._alerted_keys:
                        self._alerted_keys[key] = {
                            "trade_date": sig["trade_date"],
                            "signal_id": sig["signal_id"],
                        }
                        new_signals.append(sig)
                    else:
                        # 同日重现 (之前 EXPIRED 过), 不弹窗但仍记录进 _active+DB 保证「信号监控」Tab 可见
                        pass
            expired_keys = [k for k in list(self._active.keys()) if k not in current_keys]
            for k in expired_keys:
                sig = self._active.pop(k)
                sig["status"] = "EXPIRED"
                sig["expired_ts"] = now_ts
                try:
                    self._persist_expire(sig["signal_id"], now_ts)
                except Exception as _e:
                    log(f"[信号监控] 标记 EXPIRED 失败: {_clean_err_msg(_e, 80)}")
        if new_signals and self._alert_callback:
            for s in new_signals:
                try:
                    self._alert_callback(s)
                except Exception as _e:
                    log(f"[信号监控] 回调异常: {_clean_err_msg(_e, 80)}")
        return new_signals, expired_keys

    # ---------- 已开仓信号 ----------
    def is_opened(self, sig):
        """判断该信号 (按 key) 是否已开仓"""
        try:
            return _signal_key(sig) in self._opened_keys
        except Exception:
            return False

    def get_opened_strategy_id(self, sig):
        info = self._opened_keys.get(_signal_key(sig))
        return info.get("strategy_id") if info else None

    def mark_opened(self, sig, strategy_id):
        """把信号标记为已开仓: 写库 + 加入 _opened_keys + 从 _active 移除"""
        if not sig or not strategy_id:
            return
        now_ts = time.time()
        sig_id = sig.get("signal_id")
        key = _signal_key(sig)
        with self._lock:
            self._opened_keys[key] = {"signal_id": sig_id, "strategy_id": strategy_id}
            self._active.pop(key, None)
        try:
            def _op(conn, cur):
                cur.execute("""UPDATE signal_log SET status='OPENED', opened_strategy_id=?, opened_ts=?
                               WHERE signal_id=?""",
                            (strategy_id, now_ts, sig_id))
                return True
            _db_write_retry("标记信号 OPENED", _op, kind="sim")
        except Exception as e:
            log(f"[信号监控] 标记 OPENED 失败: {_clean_err_msg(e, 80)}")

    def sync_opened_from_strategies(self):
        """同步: 已开仓信号对应的 sim_strategy 若已关闭, 把信号从 _opened_keys 移除 (允许再次弹窗)"""
        if not self._opened_keys:
            return
        try:
            conn = _db_connect(row_factory=True, kind="sim")
            cur = conn.cursor()
            sids = [v.get("strategy_id") for v in self._opened_keys.values() if v.get("strategy_id")]
            if not sids:
                conn.close(); return
            qmarks = ",".join(["?"] * len(sids))
            cur.execute(f"SELECT strategy_id, status FROM sim_strategy WHERE strategy_id IN ({qmarks})", sids)
            statuses = {r["strategy_id"]: r["status"] for r in cur.fetchall()}
            conn.close()
            with self._lock:
                for k in list(self._opened_keys.keys()):
                    sid = self._opened_keys[k].get("strategy_id")
                    if not sid or statuses.get(sid) != "OPEN":
                        self._opened_keys.pop(k, None)
        except Exception as e:
            log(f"[信号监控] 同步已开仓状态失败: {_clean_err_msg(e, 80)}")

    # ---------- 持久化 ----------
    def _persist_new(self, sig):
        def _op(conn, cur):
            cur.execute("""INSERT INTO signal_log (
                signal_id, trigger_time, trigger_ts, trade_date,
                exchange, product, name, underlying_sym,
                side, ratio, ratio_n,
                buy_option_sym, sell_option_sym,
                buy_strike, sell_strike,
                buy_price, sell_price,
                buy_volume, sell_volume,
                net_premium, atm_avg, threshold, strike_gap,
                underlying_price, proxy_vix, product_volume, days,
                status, last_seen_ts, expired_ts,
                last_net_premium, last_buy_price, last_sell_price
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                sig["signal_id"], sig["trigger_time"], sig["trigger_ts"], sig["trade_date"],
                sig.get("exchange"), sig.get("product"), sig.get("name"), sig.get("underlying_sym"),
                sig["side"], sig["ratio"], sig["ratio_n"],
                sig["buy_option_sym"], sig["sell_option_sym"],
                sig["buy_strike"], sig["sell_strike"],
                sig["buy_price"], sig["sell_price"],
                sig["buy_volume"], sig["sell_volume"],
                sig["net_premium"], sig["atm_avg"], sig["threshold"], sig["strike_gap"],
                sig["underlying_price"], sig.get("proxy_vix"), sig.get("product_volume"), sig.get("days"),
                sig["status"], sig["last_seen_ts"], None,
                sig["last_net_premium"], sig["last_buy_price"], sig["last_sell_price"],
            ))
            return True
        return _db_write_retry("写入比例价差信号", _op, kind="sim")

    def _update_last_seen(self, signal_id, last_seen_ts, net_premium, buy_price, sell_price):
        def _op(conn, cur):
            cur.execute("""UPDATE signal_log SET last_seen_ts=?, last_net_premium=?, last_buy_price=?, last_sell_price=?
                WHERE signal_id=?""",
                (last_seen_ts, net_premium, buy_price, sell_price, signal_id))
            return True
        return _db_write_retry("更新信号 last_seen", _op, kind="sim")

    def _persist_expire(self, signal_id, expired_ts):
        def _op(conn, cur):
            cur.execute("""UPDATE signal_log SET status='EXPIRED', expired_ts=? WHERE signal_id=?""",
                        (expired_ts, signal_id))
            return True
        return _db_write_retry("标记信号 EXPIRED", _op, kind="sim")


_memory_snapshot = None
_memory_snapshot_ts = 0
_memory_snapshot_lock = threading.RLock()

_product_last_update = {}
_product_last_update_lock = threading.Lock()

def _record_product_update(exchange, product, ts=None):
    if not exchange or not product or ts is None:
        return
    with _product_last_update_lock:
        _product_last_update[(exchange, product)] = ts

def get_product_last_update_map():
    with _product_last_update_lock:
        return dict(_product_last_update)
_snapshot_loading = False
_snapshot_loading_lock = threading.Lock()
_greeks_cache = {}
_greeks_cache_ts = 0
_greeks_cache_lock = threading.RLock()
_greeks_columns_logged = False
_quote_cache = {}
_quote_cache_lock = threading.RLock()
_pd_mod = None
_tq_tafunc_mod = None
_np_mod = None
_IV_VALUE_UPPER = 10.0  # IV 上限 (小数, 1000%): 覆盖临到期深度 OTM 极端情况
_IV_VALUE_LOWER = 0.0   # IV 下限 (小数, 严格 > 0)


def _ensure_iv_libs():
    """惰性加载 IV 计算依赖, 返回 (pd, np, tafunc)"""
    global _pd_mod, _tq_tafunc_mod, _np_mod
    if _pd_mod is None:
        import pandas as pd
        _pd_mod = pd
    if _np_mod is None:
        import numpy as np
        _np_mod = np
    if _tq_tafunc_mod is None:
        from tqsdk import tafunc
        _tq_tafunc_mod = tafunc
    return _pd_mod, _np_mod, _tq_tafunc_mod

def get_memory_snapshot():
    with _memory_snapshot_lock:
        return _memory_snapshot

def set_memory_snapshot(snapshot):
    global _memory_snapshot, _memory_snapshot_ts
    with _memory_snapshot_lock:
        _memory_snapshot = snapshot
        _memory_snapshot_ts = time.time()

def build_snapshot_from_db(market_ts=None):
    """从 option_chain_latest 构建 GUI 快照。
    严格只读最新快照表; 启动后该表为空时返回空快照, 让 GUI 显示空白等待实时数据。
    不再回退到 option_chain 历史表 (启动时已被清空, 也不应使用历史数据)。
    """
    from collections import OrderedDict
    init_db()
    conn = _db_connect(row_factory=True, kind="quotes"); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM option_chain_latest WHERE market_ts IS NOT NULL AND market_ts>0")
    latest_count = cur.fetchone()[0]
    if not latest_count:
        conn.close()
        return {"products": [], "is_trading": is_trading_time()}
    source_sql = "option_chain_latest oc"
    base_where = "oc.market_ts IS NOT NULL AND oc.market_ts>0"
    base_params = [] if market_ts is None else [market_ts]
    if market_ts is not None:
        base_where += " AND oc.market_ts<=?"

    cur.execute("""SELECT exchange,product,underlying_sym,expire_rest_days,
           MAX(contract_role) AS contract_role,
           SUM(volume) AS tvol FROM """ + source_sql + """ WHERE """ + base_where + """
           GROUP BY exchange,product,underlying_sym,expire_rest_days
           ORDER BY exchange,product,tvol DESC""", base_params)
    ms_all = [dict(r) for r in cur.fetchall()]

    ct_map = {}
    tmp = OrderedDict()
    for m in ms_all:
        k = (m["exchange"], m["product"])
        if k not in tmp: tmp[k] = []
        tmp[k].append(m)
    for k, months in tmp.items():
        m2 = {}
        if months:
            role_rows = [x for x in months if x.get("contract_role")]
            if role_rows:
                for x in role_rows:
                    m2[x.get("underlying_sym")] = x.get("contract_role")
            else:
                main_m = max(months, key=lambda x: safe_float(x.get("tvol"), 0))
                near_m = min(months, key=lambda x: safe_float(x.get("expire_rest_days"), 999999))
                main_sym = main_m.get("underlying_sym")
                near_sym = near_m.get("underlying_sym")
                if main_sym == near_sym:
                    m2[main_sym] = "near_main"
                else:
                    m2[main_sym] = "main"
                    m2[near_sym] = "near"
        ct_map[k] = m2

    cur.execute("""SELECT exchange,product,product_name,underlying_sym,
           MAX(underlying_price) AS up,MAX(expire_rest_days) AS ed,MAX(volume_multiple) AS mult,
           MAX(market_ts) AS market_ts
           FROM """ + source_sql + """ WHERE """ + base_where + """
           GROUP BY exchange,product,underlying_sym ORDER BY exchange,product""", base_params)
    prods = [dict(r) for r in cur.fetchall()]
    result = {"products": [], "is_trading": is_trading_time()}

    for p in prods:
        ex, pd, us = p["exchange"], p["product"], p["underlying_sym"]
        cur.execute("""SELECT option_sym,option_class,strike_price,last_price,pre_close,
               bid_price1,ask_price1,bid_volume1,ask_volume1,volume,open_interest,
               volume_multiple,delta,gamma,theta,vega,rho,iv,iv_chg,market_time,market_ts FROM """ + source_sql + """
               WHERE """ + base_where + """ AND exchange=? AND product=? AND underlying_sym=?
               ORDER BY strike_price,option_class""", base_params + [ex, pd, us])
        opts = [dict(r) for r in cur.fetchall()]
        ct = ct_map.get((ex, pd), {})
        ed = safe_float(p["ed"], 0)
        product_market_ts = safe_float(p.get("market_ts"), None)
        product_market_time = None
        if opts:
            latest_opt = max(opts, key=lambda x: safe_float(x.get("market_ts"), -1) or -1)
            product_market_time = latest_opt.get("market_time")
        td = {"exchange": ex, "product": pd, "name": p["product_name"], "sym": us,
              "price": p["up"] or 0, "days": ed,
              "days_key": int(ed) if ed and ed > 0 else 0,
              "multiplier": _product_multiplier(ex, pd, SIM_OPTION_MULTIPLIER, p.get("mult")),
              "ctype": ct.get(us, ""), "rows": [],
              "market_time": product_market_time,
              "market_ts": product_market_ts}
        strikes = sorted({o["strike_price"] for o in opts if o["strike_price"] is not None}, reverse=True)
        S = td["price"]
        md = min([abs(s-S) for s in strikes]) if strikes else 999
        call_map = {o["strike_price"]: o for o in opts if o["option_class"] == "CALL"}
        put_map = {o["strike_price"]: o for o in opts if o["option_class"] == "PUT"}
        for sp in strikes:
            cr = call_map.get(sp)
            pr = put_map.get(sp)
            def fmt(o):
                if not o: return None
                lp, pre = o["last_price"], o["pre_close"]
                chg = None
                try:
                    if isinstance(lp,(int,float)) and isinstance(pre,(int,float)):
                        chg = round(lp-pre, 2)
                except: pass
                iv_val = round(o["iv"],2) if o.get("iv") is not None else "-"
                iv_chg = round(o["iv_chg"],2) if o.get("iv_chg") is not None else "-"
                tv = None
                return {"option_sym": o.get("option_sym"), "option_class": o.get("option_class"),
                        "strike": o.get("strike_price"),
                        "market_time": o.get("market_time"), "market_ts": o.get("market_ts"),
                        "multiplier": _product_multiplier(ex, pd, SIM_OPTION_MULTIPLIER, o.get("volume_multiple")),
                        "last": round(lp,2) if lp else "-",
                        "bid": round(o["bid_price1"],2) if o["bid_price1"] is not None else "-",
                        "ask": round(o["ask_price1"],2) if o["ask_price1"] is not None else "-",
                        "volume": o.get("volume") or 0,"oi": o.get("open_interest") or 0,
                        "delta": round(o["delta"],4) if o["delta"] is not None else "-",
                        "iv": iv_val, "iv_chg": iv_chg, "tv": tv, "chg": chg}
            td["rows"].append({"K": sp, "c": fmt(cr), "p": fmt(pr), "atm": abs(sp-S)<=md+0.01})
        for row in td["rows"]:
            K_val = row["K"]
            for side in ("c", "p"):
                d = row[side]
                if not d or not isinstance(d.get("last"), (int,float)): continue
                opt_price = float(d["last"])
                intrinsic = max(S - K_val, 0) if side == "c" else max(K_val - S, 0)
                d["tv"] = round(opt_price - intrinsic, 2)
        result["products"].append(td)
    conn.close()
    return result

def refresh_memory_snapshot(market_ts=None):
    try:
        t0 = time.time()
        snapshot = build_snapshot_from_db(market_ts)
        if snapshot:
            set_memory_snapshot(snapshot)
            dur = time.time() - t0
            if dur > 1.0:
                log(f"[警告] 内存快照刷新耗时 {dur:.1f}s market_ts={market_ts}")
            return True
    except Exception as e:
        log(f"[警告] 内存快照刷新失败: {_clean_err_msg(e, 80)}")
    return False


def _sim_id(prefix):
    return f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{time.time_ns() % 1000000:06d}"

def _sim_direction(side):
    side = str(side or "BUY").upper()
    return 1 if side == "BUY" else -1

def _sim_premium_cashflow(side, price, volume=1, multiplier=1):
    return -_sim_direction(side) * (_sim_num(price, 0) or 0) * int(volume or 0) * float(multiplier or 1)

def _sim_leg_multiplier(leg):
    live_mult = _sim_num(leg.get("volume_multiple"), None)
    stored_mult = _sim_num(leg.get("multiplier"), None)
    explicit = live_mult if live_mult is not None and live_mult > 0 and abs(live_mult - 1) > 1e-9 else stored_mult
    return _product_multiplier(leg.get("exchange"), leg.get("product"), SIM_OPTION_MULTIPLIER, explicit)

def _expiry_status_text(days):
    d = safe_float(days, None)
    if d is None:
        return "到期 -", "unknown"
    if d <= 0:
        return "已到期", "expired"
    if d < 1:
        return "到期<1天", "urgent"
    return f"到期 {int(d)}天", "normal"

def _sim_num(v, default=None):
    if isinstance(v, (int, float)):
        return float(v)
    try:
        if v in (None, "", "-"):
            return default
        return float(v)
    except:
        return default

def _sim_leg_price(leg, action):
    last = _sim_num(leg.get("last"))
    bid = _sim_num(leg.get("bid"))
    ask = _sim_num(leg.get("ask"))
    last = last if last is not None and last > 0 else 0
    bid = bid if bid is not None and bid > 0 else None
    ask = ask if ask is not None and ask > 0 else None
    side = str(leg.get("side", "BUY")).upper()
    if action == "OPEN":
        return (ask if side == "BUY" and ask is not None else
                bid if side == "SELL" and bid is not None else last or 0)
    close_side = "SELL" if side == "BUY" else "BUY"
    return (bid if close_side == "SELL" and bid is not None else
            ask if close_side == "BUY" and ask is not None else last or 0)

def _sim_opposite_side(side):
    return "SELL" if str(side or "BUY").upper() == "BUY" else "BUY"

def _sim_leg_floating_pnl(leg):
    side = str(leg.get("side") or "BUY").upper()
    vol = int(_sim_num(leg.get("volume"), 0) or 0)
    mult = _sim_leg_multiplier(leg)
    open_price = _sim_num(leg.get("open_price"), None)
    if open_price is None:
        open_price = _sim_leg_price(leg, "OPEN")
    close_price = _sim_leg_price(leg, "CLOSE")
    open_cash = _sim_premium_cashflow(side, open_price, vol, mult)
    close_cash = _sim_premium_cashflow(_sim_opposite_side(side), close_price, vol, mult)
    return close_price, open_cash + close_cash

def _sim_strategy_open_premium(legs):
    total = 0.0
    for leg in legs or []:
        side = str(leg.get("side") or "BUY").upper()
        vol = int(_sim_num(leg.get("volume"), 0) or 0)
        mult = _sim_leg_multiplier(leg)
        open_price = _sim_num(leg.get("open_price"), None)
        if open_price is None:
            open_price = _sim_leg_price(leg, "OPEN")
        total += _sim_premium_cashflow(side, open_price, vol, mult)
    return total

def _sim_backfill_open_underlying(leg, open_time=None):
    """开仓标的价回填: 优先 sim 库 sim_strategy_leg.open_underlying_price (开仓时已落库),
    其次 leg 上即时挂的 underlying_price; 都缺则返回 None。
    不再查 T 报库 option_chain 历史 (启动时该表已被清空, 也不应跨启动复用历史标的价)。
    """
    val = _sim_num(leg.get("open_underlying_price"), None)
    if val is not None and val > 0:
        return val
    val = _sim_num(leg.get("underlying_price"), None)
    if val is not None and val > 0:
        return val
    return None

def _sim_scope(legs):
    scopes = sorted({f"{x.get('exchange')}.{x.get('product')}" for x in legs})
    return ("SINGLE_PRODUCT" if len(scopes) == 1 else "CROSS_PRODUCT"), " + ".join(scopes)

def _sim_save_pnl_snapshot(strategy):
    if not strategy:
        return
    status = strategy.get("status")
    snapshot_time = strategy.get("snapshot_time") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    unrealized = _sim_num(strategy.get("unrealized_pnl"), 0) or 0
    realized = _sim_num(strategy.get("realized_pnl"), 0) or 0
    total = realized if status == "CLOSED" else unrealized + realized
    def _op(conn, cur):
        cur.execute("""SELECT total_pnl,unrealized_pnl,realized_pnl,status
                       FROM sim_pnl_snapshot WHERE strategy_id=? AND snapshot_time=?
                       ORDER BY rowid DESC LIMIT 1""", (strategy.get("strategy_id"), snapshot_time))
        prev = cur.fetchone()
        same_snapshot = bool(prev and abs((_sim_num(prev[0], 0) or 0) - total) < 1e-9
                             and abs((_sim_num(prev[1], 0) or 0) - unrealized) < 1e-9
                             and abs((_sim_num(prev[2], 0) or 0) - realized) < 1e-9
                             and prev[3] == status)
        if same_snapshot:
            cur.execute("SELECT unrealized_pnl FROM sim_strategy WHERE strategy_id=?", (strategy.get("strategy_id"),))
            st_prev = cur.fetchone()
            if st_prev and abs((_sim_num(st_prev[0], 0) or 0) - unrealized) < 1e-9:
                return False
        if not same_snapshot:
            cur.execute("""INSERT INTO sim_pnl_snapshot
                (snapshot_id,strategy_id,snapshot_time,status,unrealized_pnl,realized_pnl,total_pnl,open_premium,close_premium)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (_sim_id("PNL"), strategy.get("strategy_id"), snapshot_time,
                 status, unrealized, realized, total, strategy.get("open_premium"), strategy.get("close_premium")))
        cur.execute("UPDATE sim_strategy SET unrealized_pnl=? WHERE strategy_id=?", (unrealized, strategy.get("strategy_id")))
        return True
    return _db_write_retry("保存PnL快照", _op, kind="sim")

def sim_get_pnl_history(strategy_id, limit=240):
    init_db()
    conn = _db_connect(row_factory=True, kind="sim")
    cur = conn.cursor()
    cur.execute("""SELECT snapshot_time,total_pnl,unrealized_pnl,realized_pnl,status
                   FROM sim_pnl_snapshot WHERE strategy_id=?
                   ORDER BY snapshot_time DESC LIMIT ?""", (strategy_id, int(limit)))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    rows.reverse()
    return rows

def sim_delete_strategy(strategy_id):
    init_db()
    def _op(conn, cur):
        cur.execute("DELETE FROM sim_pnl_snapshot WHERE strategy_id=?", (strategy_id,))
        cur.execute("DELETE FROM sim_trade WHERE strategy_id=?", (strategy_id,))
        cur.execute("DELETE FROM sim_strategy_leg WHERE strategy_id=?", (strategy_id,))
        cur.execute("DELETE FROM sim_strategy WHERE strategy_id=?", (strategy_id,))
        return True
    return _db_write_retry("删除模拟组合", _op, kind="sim")

def sim_open_strategy(strategy_name, legs):
    init_db()
    if not legs:
        raise ValueError("组合篮子为空")
    strategy_type, scope_text = _sim_scope(legs)
    strategy_id = _sim_id("SIM")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not strategy_name:
        strategy_name = f"{scope_text} {'单品种组合' if strategy_type == 'SINGLE_PRODUCT' else '跨品种组合'}"
    open_vix_vals = []
    for leg in legs:
        v = _sim_num(leg.get("open_proxy_vix"), None)
        if v is None:
            v = _sim_num(leg.get("proxy_vix"), None)
        if v is not None and v > 0:
            open_vix_vals.append(v)
    open_proxy_vix = (sum(open_vix_vals) / len(open_vix_vals)) if open_vix_vals else None
    def _op(conn, cur):
        open_value = 0.0
        cur.execute("""INSERT INTO sim_strategy
            (strategy_id,strategy_name,strategy_type,scope_text,status,open_time,close_time,
             open_premium,close_premium,realized_pnl,unrealized_pnl,open_proxy_vix,note)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (strategy_id, strategy_name, strategy_type, scope_text, "OPEN", now, None, 0, None, 0, 0, open_proxy_vix, ""))
        for leg in legs:
            leg_id = _sim_id("LEG")
            side = leg.get("side", "BUY")
            vol = int(leg.get("volume") or 1)
            mult = _sim_leg_multiplier(leg)
            price = _sim_leg_price(leg, "OPEN")
            open_underlying = _sim_num(leg.get("open_underlying_price"), None)
            if open_underlying is None:
                open_underlying = _sim_num(leg.get("underlying_price"), None)
            open_value += _sim_premium_cashflow(side, price, vol, mult)
            cur.execute("""INSERT INTO sim_strategy_leg
                (leg_id,strategy_id,exchange,product,product_name,underlying_sym,option_sym,
                 option_class,strike_price,expire_rest_days,side,volume,multiplier,open_price,
                 open_underlying_price,close_price,current_price,delta,gamma,theta,vega,iv,status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (leg_id, strategy_id, leg.get("exchange"), leg.get("product"), leg.get("product_name"),
                 leg.get("underlying_sym"), leg.get("option_sym"), leg.get("option_class"),
                 _sim_num(leg.get("strike")), _sim_num(leg.get("days")), side, vol, mult, price,
                 open_underlying, None, price, _sim_num(leg.get("delta")), None, None, None, _sim_num(leg.get("iv")), "OPEN"))
            cur.execute("""INSERT INTO sim_trade
                (trade_id,strategy_id,leg_id,option_sym,action,side,volume,price,trade_time)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (_sim_id("TRD"), strategy_id, leg_id, leg.get("option_sym"), "OPEN", side, vol, price, now))
        cur.execute("UPDATE sim_strategy SET open_premium=? WHERE strategy_id=?", (open_value, strategy_id))
        return open_value
    open_value = _db_write_retry("模拟组合开仓", _op, kind="sim")
    _sim_save_pnl_snapshot({
        "strategy_id": strategy_id, "status": "OPEN", "unrealized_pnl": 0,
        "realized_pnl": 0, "open_premium": open_value, "close_premium": None
    })
    return strategy_id

def _sim_latest_option_map(option_syms):
    """读取模拟持仓各 leg 对应期权的最新实时行情; 走 quotes 库 (option_chain_latest 在那里)"""
    if not option_syms:
        return {}
    conn = _db_connect(row_factory=True, kind="quotes")
    cur = conn.cursor()
    result = {}
    for sym in option_syms:
        cur.execute("""SELECT market_time,market_ts,underlying_price,last_price,bid_price1,ask_price1,volume_multiple,delta,gamma,theta,vega,iv
                       FROM option_chain_latest WHERE option_sym=?""", (sym,))
        row = cur.fetchone()
        if row:
            result[sym] = dict(row)
    conn.close()
    return result

_sim_missing_log_state = {"last": {}, "interval": 300.0}  # sym -> 上次 log 时间; 5 分钟内同 sym 不重复

def _log_sim_missing_quotes(option_syms):
    """节流版: 同一 option_sym 5 分钟内只 log 一次, 避免 1Hz 调用刷屏。
    修复 A 后历史持仓合约会被自动订阅, 该函数只在真异常时触发。"""
    missing = [s for s in option_syms if s]
    if not missing:
        return
    now = time.time()
    state = _sim_missing_log_state
    new_missing = [s for s in missing if (now - state["last"].get(s, 0)) > state["interval"]]
    if not new_missing:
        return
    for s in new_missing:
        state["last"][s] = now
    log(f"[模拟持仓] 缺少实时行情，无法估值: {','.join(new_missing[:6])}{'...' if len(new_missing) > 6 else ''}")


def _collect_sim_position_targets():
    """读取所有 OPEN 状态的模拟持仓 leg, 按 (exchange, product, underlying_sym) 分组,
    返回兼容 targets_by_exchange 的结构 (role='sim_position', 仅用于订阅, 不参与 ATM 扫描)。
    这样历史持仓的远 OTM 合约 (本次启动 ATM 窗口外) 也能拿到实时行情, 浮盈亏正常更新。
    """
    try:
        init_db()
        conn = _db_connect(row_factory=True, kind="sim")
        cur = conn.cursor()
        cur.execute("""SELECT exchange, product, product_name, underlying_sym, option_sym, option_class
                       FROM sim_strategy_leg WHERE status='OPEN'""")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        log(f"[启动] 读取模拟持仓订阅清单失败: {_clean_err_msg(e, 80)}")
        return {}
    groups = {}  # (ex, prod, und) -> {"name":..., "calls":set(), "puts":set()}
    for r in rows:
        ex = r.get("exchange"); prod = r.get("product"); und = r.get("underlying_sym")
        osym = r.get("option_sym")
        if not (ex and prod and und and osym):
            continue
        key = (ex, prod, und)
        g = groups.setdefault(key, {"name": r.get("product_name") or prod, "calls": set(), "puts": set()})
        cls = str(r.get("option_class") or "").upper()
        if cls.startswith("C"):
            g["calls"].add(osym)
        else:
            g["puts"].add(osym)
    out = {}
    for (ex, prod, und), g in groups.items():
        out.setdefault(ex, []).append((prod, g["name"], und, "sim_position", sorted(g["calls"]), sorted(g["puts"])))
    return out


def _merge_sim_position_targets(targets_by_exchange, sim_targets):
    """把 sim_position 目标 (历史持仓) 合并进 targets_by_exchange:
    - 同 (exchange, product, underlying) 已有 entry → 把 sim 持仓的 calls/puts 追加 (去重)
    - 不存在 → 整条 entry 追加 (role='sim_position')
    注意: 返回的 targets_by_exchange 是原对象被原地修改, 同时也作为返回值。
    """
    if not sim_targets:
        return targets_by_exchange
    for ex, entries in sim_targets.items():
        existing = targets_by_exchange.setdefault(ex, [])
        for sim_entry in entries:
            sim_prod, sim_name, sim_und, sim_role, sim_calls, sim_puts = sim_entry
            merged = False
            for i, ent in enumerate(existing):
                prod, name, und, role, calls, puts = ent
                if prod == sim_prod and und == sim_und:
                    new_calls = list(dict.fromkeys(list(calls or []) + list(sim_calls)))
                    new_puts = list(dict.fromkeys(list(puts or []) + list(sim_puts)))
                    existing[i] = (prod, name, und, role, new_calls, new_puts)
                    merged = True
                    break
            if not merged:
                existing.append(sim_entry)
    return targets_by_exchange

def sim_list_strategies(save_snapshots=True):
    init_db()
    conn = _db_connect(row_factory=True, kind="sim")
    cur = conn.cursor()
    cur.execute("SELECT * FROM sim_strategy ORDER BY open_time DESC")
    strategies = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT * FROM sim_strategy_leg ORDER BY strategy_id, leg_id")
    legs = [dict(r) for r in cur.fetchall()]
    conn.close()
    qmap = _sim_latest_option_map([x["option_sym"] for x in legs])
    _log_sim_missing_quotes([x["option_sym"] for x in legs if x.get("status") == "OPEN" and x.get("option_sym") not in qmap])
    by_strategy = {}
    for leg in legs:
        open_time = next((s.get("open_time") for s in strategies if s.get("strategy_id") == leg.get("strategy_id")), None)
        quote = qmap.get(leg["option_sym"], {})
        leg["quote_status"] = "OK" if quote else "MISSING"
        leg["quote_error"] = "" if quote else "缺少实时行情，无法估值"
        last = _sim_num(quote.get("last_price"), _sim_num(leg.get("current_price"), _sim_num(leg.get("open_price"), 0)))
        leg["open_underlying_price"] = _sim_backfill_open_underlying(leg, open_time)
        underlying_now = _sim_num(quote.get("underlying_price"), None)
        if underlying_now is None:
            underlying_now = _underlying_last_price(None, leg.get("underlying_sym"))
        if underlying_now is None:
            underlying_now = _sim_num(leg.get("open_underlying_price"), None)
        leg["underlying_price"] = underlying_now
        leg["last"] = last
        leg["bid"] = _sim_num(quote.get("bid_price1"), last)
        leg["ask"] = _sim_num(quote.get("ask_price1"), last)
        leg["volume_multiple"] = _sim_num(quote.get("volume_multiple"), leg.get("multiplier"))
        leg["multiplier"] = _sim_leg_multiplier(leg)
        leg["quote_time"] = quote.get("market_time")
        current, current_pnl = (None, 0) if not quote and leg.get("status") == "OPEN" else _sim_leg_floating_pnl(leg)
        leg["current_price"] = current
        leg["current_pnl"] = current_pnl
        by_strategy.setdefault(leg["strategy_id"], []).append(leg)
    for st in strategies:
        st["legs"] = by_strategy.get(st["strategy_id"], [])
        if st["legs"]:
            st["open_premium"] = _sim_strategy_open_premium(st["legs"])
            current_underlyings = [_sim_num(x.get("underlying_price"), None) for x in st["legs"]]
            current_underlyings = [x for x in current_underlyings if x is not None and x > 0]
            if current_underlyings:
                st["current_price"] = current_underlyings[0]
        st["unrealized_pnl"] = sum(x.get("current_pnl", 0) for x in st["legs"] if x.get("status") == "OPEN")
        st["leg_count"] = len(st["legs"])
        quote_times = [x.get("quote_time") for x in st["legs"] if x.get("quote_time")]
        if quote_times:
            st["snapshot_time"] = max(quote_times)
        if save_snapshots and st.get("status") == "OPEN":
            _sim_save_pnl_snapshot(st)
    return strategies

def sim_close_strategy(strategy_id):
    init_db()
    strategies = {st["strategy_id"]: st for st in sim_list_strategies(save_snapshots=False)}
    strategy = strategies.get(strategy_id)
    if not strategy or strategy.get("status") != "OPEN":
        raise ValueError("组合不存在或不是持仓中")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def _op(conn, cur):
        close_value = 0.0
        realized = 0.0
        for leg in strategy.get("legs", []):
            if leg.get("status") != "OPEN":
                continue
            side = leg.get("side")
            vol = leg.get("volume") or 0
            mult = _sim_leg_multiplier(leg)
            close_price, pnl = _sim_leg_floating_pnl(leg)
            realized += pnl
            close_value += _sim_premium_cashflow("SELL" if side == "BUY" else "BUY", close_price, vol, mult)
            cur.execute("UPDATE sim_strategy_leg SET close_price=?, current_price=?, status=? WHERE leg_id=?", (close_price, close_price, "CLOSED", leg.get("leg_id")))
            cur.execute("""INSERT INTO sim_trade
                (trade_id,strategy_id,leg_id,option_sym,action,side,volume,price,trade_time)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (_sim_id("TRD"), strategy_id, leg.get("leg_id"), leg.get("option_sym"), "CLOSE", "SELL" if side == "BUY" else "BUY", vol, close_price, now))
        cur.execute("""UPDATE sim_strategy SET status=?, close_time=?, close_premium=?,
                       realized_pnl=?, unrealized_pnl=? WHERE strategy_id=?""",
                    ("CLOSED", now, close_value, realized, 0, strategy_id))
        return close_value, realized
    close_value, realized = _db_write_retry("模拟组合平仓", _op, kind="sim")
    _sim_save_pnl_snapshot({
        "strategy_id": strategy_id, "status": "CLOSED", "unrealized_pnl": 0,
        "realized_pnl": realized, "open_premium": strategy.get("open_premium"), "close_premium": close_value
    })
    return realized

# IV 开盘值进程内缓存: 启动后第一次见到的 IV 即为该合约当日 IV 开盘值。
# 不再依赖 option_chain 历史表; 跨启动 iv_chg 不再连续 (符合"启动后只用实时数据"的语义)。
# Key: (yyyy-mm-dd, option_sym) -> first_iv
_iv_open_cache_lock = threading.RLock()
_iv_open_cache = {}

def load_daily_iv_open(option_syms, market_time):
    """返回当日各合约首次出现的 IV (用于计算 iv_chg)。
    本实现完全使用进程内缓存, 不读历史 DB。
    调用方拿到的 dict 是当前已知的开盘 IV 视图; 当合约本次 IV 第一次出现时, 调用方应主动写入缓存。
    """
    if not option_syms or not market_time:
        return {}
    day = str(market_time)[:10]
    result = {}
    with _iv_open_cache_lock:
        for sym in option_syms:
            v = _iv_open_cache.get((day, sym))
            if v is not None:
                result[sym] = v
    return result

def _record_iv_open(market_time, option_sym, iv):
    """合约 IV 首次出现时由调用方写入, 后续不会被覆盖。"""
    if not market_time or not option_sym or iv is None:
        return
    day = str(market_time)[:10]
    key = (day, option_sym)
    with _iv_open_cache_lock:
        if key not in _iv_open_cache:
            _iv_open_cache[key] = iv

def _purge_iv_open_cache_old_days(keep_days=("",)):
    """可选: 启动或日切时清理过期缓存条目, 避免长期运行内存累积。"""
    with _iv_open_cache_lock:
        if not keep_days or keep_days == ("",):
            return
        keys = list(_iv_open_cache.keys())
        for k in keys:
            if k[0] not in keep_days:
                _iv_open_cache.pop(k, None)

def classify_opts(sym_list, exchange, product):
    """分类CALL/PUT, 返回 (calls, puts)"""
    calls, puts = [], []
    if exchange == "DCE":
        for s in sym_list:
            if '-C-' in s: calls.append(s)
            elif '-P-' in s: puts.append(s)
    else:
        for s in sym_list:
            rest = s.split(f"{product}")[1] if product in s else ""
            if not rest: continue
            found = False
            for ch in rest:
                if ch == 'C': calls.append(s); found = True; break
                elif ch == 'P': puts.append(s); found = True; break
    return sorted(calls), sorted(puts)

def extract_month(sym, exchange, product):
    """提取标的月份代码"""
    if exchange == "DCE":
        return sym.split(".")[1].split("-")[0]  # m2607
    code = sym.split(".")[1]
    i = 0
    while i < len(code) and code[i].isalpha(): i += 1
    j = i
    while j < len(code) and code[j].isdigit(): j += 1
    digits = code[i:j]
    if exchange == "CZCE" and len(digits) >= 3:
        digits = digits[:3]
    elif len(digits) >= 4:
        digits = digits[:4]
    return code[:i] + digits

def extract_strike(sym, exchange, product):
    try:
        if exchange == "DCE":
            return float(sym.rsplit("-", 1)[1])
        code = sym.split(".", 1)[1]
        rest = code.split(product, 1)[1] if product in code else code
        cpos, ppos = rest.rfind("C"), rest.rfind("P")
        pos = max(cpos, ppos)
        if pos < 0:
            return None
        return float(rest[pos + 1:])
    except:
        return None

def _underlying_last_price(api, underlying_sym):
    with _quote_cache_lock:
        q = _quote_cache.get(underlying_sym)
    if q is not None:
        for attr in ("last_price", "pre_close"):
            v = safe_float(getattr(q, attr, None), None)
            if v is not None and v > 0:
                return v
    return None

def _filter_atm_window(api, exchange, product, underlying_sym, calls, puts, underlying_price_map=None):
    all_syms = list(dict.fromkeys(calls + puts))
    strikes = sorted({extract_strike(s, exchange, product) for s in all_syms if extract_strike(s, exchange, product) is not None})
    if not strikes:
        return calls, puts, None, None
    S = (underlying_price_map or {}).get(underlying_sym)
    if S is None:
        S = _underlying_last_price(api, underlying_sym)
    if S is None:
        mid = len(strikes) // 2
    else:
        mid = min(range(len(strikes)), key=lambda i: abs(strikes[i] - S))
    lo = max(0, mid - OPTION_ATM_WINDOW)
    hi = min(len(strikes), mid + OPTION_ATM_WINDOW + 1)
    keep = set(strikes[lo:hi])
    return [s for s in calls if extract_strike(s, exchange, product) in keep], [s for s in puts if extract_strike(s, exchange, product) in keep], S, strikes[mid]

def _quote_price_for_atm(q):
    for attr in ("last_price", "pre_close", "open", "settlement"):
        v = safe_float(getattr(q, attr, None), None)
        if v is not None and v > 0:
            return v
    bid = safe_float(getattr(q, "bid_price1", None), None)
    ask = safe_float(getattr(q, "ask_price1", None), None)
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2.0
    return None

def _flatten_option_symbols(value):
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError:
            values = [value]
    return [s for s in dict.fromkeys(values) if isinstance(s, str) and s]

def _query_atm_window_by_official_api(api, exchange, product, underlying_sym, S):
    """按官方 TqSdk API 解析当月 ATM±OPTION_ATM_WINDOW 期权链。
    - 主路径: query_options(underlying, option_class, expired=False)
        服务端只返回未下市期权，响应体小，避免 query_atm_options 内部拉全部历史期权
        (含已下市) 导致的 30s 超时。本地用 extract_strike 选 ATM 窗口。
    - 兜底: 主路径异常 (含重试 1 次) 时，回退到官方 query_atm_options 按档位查询。
    - 不传 has_MS / has_A 参数，保持官方默认。
    - 区分"异常"与"全空"：异常单次不放弃候选 (内部已重试一次)；全空才换下一候选。
    """
    if S is None or S <= 0:
        return [], [], None

    def _safe_query_options(option_class):
        last_err = None
        for _ in range(2):
            try:
                raw = api.query_options(underlying_sym, option_class=option_class, expired=False)
                return _flatten_option_symbols(raw), None
            except Exception as e:
                last_err = e
                try:
                    _wait_for(api, 0.3)
                except:
                    pass
        return None, last_err

    def _pick_window(syms):
        if not syms:
            return []
        scored = []
        for s in syms:
            k = extract_strike(s, exchange, product)
            if k is None:
                continue
            scored.append((abs(k - S), s))
        if not scored:
            return []
        scored.sort(key=lambda x: x[0])
        return [s for _, s in scored[: OPTION_ATM_WINDOW * 2 + 1]]

    calls_all, call_err = _safe_query_options("CALL")
    if calls_all is None:
        raw_tail = str(call_err).replace("\n", " ").strip()[-200:]
        log(f"[启动]   候选 {underlying_sym} query_options(CALL,expired=False) 异常(已重试1次), 回退 query_atm_options | 摘要={_clean_err_msg(call_err, 60)} | 原文尾={raw_tail}")
        return _query_atm_window_via_atm_options(api, exchange, product, underlying_sym, S)
    if not calls_all:
        log(f"[启动]   候选 {underlying_sym} 官方 query_options(CALL,expired=False) 返回空, 当月无挂牌 CALL 期权, 换下一候选")
        return [], [], None

    puts_all, put_err = _safe_query_options("PUT")
    if puts_all is None:
        raw_tail = str(put_err).replace("\n", " ").strip()[-200:]
        log(f"[启动]   候选 {underlying_sym} query_options(PUT,expired=False) 异常(已重试1次), 仅保留 CALL 侧 | 摘要={_clean_err_msg(put_err, 60)} | 原文尾={raw_tail}")
        puts_all = []
    elif not puts_all:
        log(f"[启动]   候选 {underlying_sym} 官方 query_options(PUT,expired=False) 返回空, 仅保留 CALL 侧")

    calls = _pick_window(calls_all)
    puts = _pick_window(puts_all)
    strikes = [extract_strike(s, exchange, product) for s in calls + puts]
    strikes = [x for x in strikes if x is not None]
    atm = min(strikes, key=lambda x: abs(x - S)) if strikes else None
    return calls, puts, atm


def _query_atm_window_via_atm_options(api, exchange, product, underlying_sym, S):
    """兜底路径: 官方 query_atm_options 按档位查询 (与官方文档参数完全一致)。"""
    if S is None or S <= 0:
        return [], [], None
    levels = list(range(OPTION_ATM_WINDOW, -OPTION_ATM_WINDOW - 1, -1))

    def _safe_query(option_class):
        last_err = None
        for _ in range(2):
            try:
                raw = api.query_atm_options(underlying_sym, S, levels, option_class)
                return (list(raw) if raw is not None else []), None
            except Exception as e:
                last_err = e
                try:
                    _wait_for(api, 0.3)
                except:
                    pass
        return None, last_err

    raw_calls, call_err = _safe_query("CALL")
    if raw_calls is None:
        raw_tail = str(call_err).replace("\n", " ").strip()[-200:]
        log(f"[启动]   候选 {underlying_sym} 兜底 query_atm_options(CALL) 仍异常(已重试1次), 跳过该候选 | 摘要={_clean_err_msg(call_err, 60)} | 原文尾={raw_tail}")
        return [], [], None
    calls = _flatten_option_symbols(raw_calls)
    if not calls:
        log(f"[启动]   候选 {underlying_sym} 兜底 query_atm_options 返回 CALL 档位 {len(raw_calls)} 个均无期权, 换下一候选")
        return [], [], None

    raw_puts, put_err = _safe_query("PUT")
    if raw_puts is None:
        raw_tail = str(put_err).replace("\n", " ").strip()[-200:]
        log(f"[启动]   候选 {underlying_sym} 兜底 query_atm_options(PUT) 仍异常(已重试1次), 仅保留 CALL 侧 | 摘要={_clean_err_msg(put_err, 60)} | 原文尾={raw_tail}")
        puts = []
    else:
        puts = _flatten_option_symbols(raw_puts)

    strikes = [extract_strike(s, exchange, product) for s in calls + puts]
    strikes = [x for x in strikes if x is not None]
    atm = min(strikes, key=lambda x: abs(x - S)) if strikes else None
    return calls, puts, atm

def _option_expire_days(q):
    days = safe_float(getattr(q, "expire_rest_days", None), None)
    if days is not None:
        return days
    t = _option_time_to_expiry(q)
    return t * 360.0 if t is not None else None

def _expiry_probe_symbols(symbols, calls=None, puts=None):
    out = []
    groups = [list(calls or []), list(puts or [])] if calls is not None or puts is not None else [list(symbols or [])]
    for group in groups:
        if not group:
            continue
        sym = group[len(group) // 2]
        if sym not in out:
            out.append(sym)
    return out[:2]

def _expiry_probe_quotes(api, symbols):
    symbols = list(dict.fromkeys(symbols or []))
    if not symbols:
        return []
    cached = []
    missing = []
    with _quote_cache_lock:
        for sym in symbols:
            q = _quote_cache.get(sym)
            if q is None:
                missing.append(sym)
            else:
                cached.append(q)
    got = []
    if missing:
        try:
            got = list(api.get_quote_list(missing))
        except Exception as e:
            log(f"[启动]   到期探针行情失败: {len(missing)}个合约, 保留候选 ({_clean_err_msg(e, 80)})")
            got = []
    if got:
        with _quote_cache_lock:
            for q in got:
                sym = getattr(q, "instrument_id", None)
                if sym:
                    _quote_cache[sym] = q
    try:
        _wait_for(api, 0.2)
    except:
        pass
    return cached + got

def _expiry_days_from_greeks(api, symbols):
    try:
        gd = api.query_option_greeks(list(dict.fromkeys(symbols or [])))
        if gd is None or not hasattr(gd, "iterrows"):
            return None
        min_days = None
        for _, row in gd.iterrows():
            days = safe_float(row.get("expire_rest_days"), None)
            if days is None:
                continue
            min_days = days if min_days is None else min(min_days, days)
        return min_days
    except:
        return None

def _filter_option_symbols_by_expiry(api, symbols, calls=None, puts=None):
    symbols = list(dict.fromkeys(symbols or []))
    if not symbols:
        return [], None
    probe_symbols = _expiry_probe_symbols(symbols, calls, puts)
    min_days = _expiry_days_from_greeks(api, probe_symbols)
    if min_days is None:
        quotes = _expiry_probe_quotes(api, probe_symbols)
        for q in quotes:
            days = _option_expire_days(q)
            if days is None:
                continue
            min_days = days if min_days is None else min(min_days, days)
    if min_days is None:
        return symbols, None
    if min_days <= OPTION_MIN_EXPIRE_DAYS:
        return [], min_days
    return symbols, min_days

def _query_product_futures_fast(api, exchange, products_dict):
    products = list(products_dict.keys())
    grouped = {p: [] for p in products}
    try:
        all_futs = list(api.query_quotes(ins_class="FUTURE", exchange_id=exchange, product_id=products, expired=False))
    except TypeError:
        all_futs = list(api.query_quotes(ins_class="FUTURE", exchange_id=exchange, expired=False))
    except Exception as e:
        log(f"[启动] 官方目标发现: 批量查询期货候选失败 ({_clean_err_msg(e, 80)})")
        all_futs = []
    for sym in all_futs:
        for product in products:
            if option_matches_product(sym, exchange, product):
                grouped[product].append(sym)
                break
    for product in products:
        grouped[product] = sorted(dict.fromkeys(grouped[product]), key=lambda s: extract_month(s, exchange, product))
    return grouped

def _select_underlying_candidates(exchange, product, futs, dom_month):
    by_month = {extract_month(s, exchange, product): s for s in futs}
    near_candidates = futs[:OPTION_NEAR_CANDIDATE_LIMIT]
    dom_sym = by_month.get(dom_month) if dom_month else None
    selected = []
    if OPTION_MONTH_SCOPE == "main":
        selected = [dom_sym] if dom_sym else near_candidates
    else:
        selected = list(near_candidates)
        if OPTION_MONTH_SCOPE == "near_main" and dom_sym:
            selected.append(dom_sym)
    return [s for s in dict.fromkeys(selected) if s]

def _resolve_atm_candidate(api, exchange, product, candidates, underlying_price_map):
    for idx, underlying_sym in enumerate(candidates, 1):
        month = extract_month(underlying_sym, exchange, product)
        S = underlying_price_map.get(underlying_sym) or _underlying_last_price(api, underlying_sym)
        calls, puts, atm0 = _query_atm_window_by_official_api(api, exchange, product, underlying_sym, S)
        if calls or puts:
            valid_syms, min_days = _filter_option_symbols_by_expiry(api, calls + puts, calls, puts)
            if not valid_syms:
                log(f"[启动]   跳过 {underlying_sym}: 到期 {_fmt_days(min_days)} 天 <= {_fmt_days(OPTION_MIN_EXPIRE_DAYS)} 天, 继续下一候选")
                continue
            valid_set = set(valid_syms)
            calls = [s for s in calls if s in valid_set]
            puts = [s for s in puts if s in valid_set]
            if calls or puts:
                return {"month": month, "underlying": underlying_sym, "calls": calls, "puts": puts, "S": S, "atm": atm0, "idx": idx, "expire_days": min_days}
    return None

def option_matches_product(sym, exchange, product):
    """精确匹配期权品种，避免 p 匹配 pp、j 匹配 jm/jd 这类前缀串品种。"""
    try:
        if exchange    == "DCE" and "-MS-" in sym:
            return False
        code = sym.split(".", 1)[1]
        root = code.split("-", 1)[0] if exchange == "DCE" else code
        i = 0
        while i < len(root) and root[i].isalpha():
            i += 1
        return root[:i] == product and i < len(root) and root[i].isdigit()
    except Exception:
        return False

def find_dominant_months(api, exchange, products_dict):
    """查找每个品种的主力合约月份(持仓量最大的未到期期货合约)"""
    dominant = {}
    try:
        t0 = time.time()
        for product in products_dict:
            try:
                conts = list(api.query_cont_quotes(exchange_id=exchange, product_id=product))
            except:
                conts = []
            for sym in conts:
                if option_matches_product(sym, exchange, product):
                    dominant[product] = extract_month(sym, exchange, product)
                    break
        log(f"[启动] 期货主力识别: {len(dominant)}/{len(products_dict)} 个品种 ({time.time()-t0:.1f}s)")
    except Exception as e:
        log(f"[警告] 获取主力合约失败: {_clean_err_msg(e, 80)}")
    return dominant

# --- 目标合约缓存 (跨轮次共享) ---
_cached_targets = {}
_last_resolve_time = 0
_resolved_underlying_hint = {}
_target_resolve_sources = {}

def _build_target_set(api, exchange, products_dict):
    """首轮/刷新时调用: 按官方 query_atm_options 识别 ATM±N 目标合约"""
    t1 = time.time()
    product_futures = _query_product_futures_fast(api, exchange, products_dict)
    log(f"[启动] 官方目标发现: 批量查询期货候选 + 平值期权链, 期货候选 {sum(len(v) for v in product_futures.values())} 个, 窗口 ATM±{OPTION_ATM_WINDOW} ({time.time()-t1:.1f}s)")
    dominant = find_dominant_months(api, exchange, products_dict)

    product_plan = []
    for product, name in products_dict.items():
        futs = product_futures.get(product, [])
        if not futs:
            continue
        dom_month = dominant.get(product)
        by_month = {extract_month(s, exchange, product): s for s in futs}
        hint = _resolved_underlying_hint.get((exchange, product))
        near_candidates = list(futs[:OPTION_NEAR_CANDIDATE_LIMIT])
        if hint in futs:
            near_candidates = [hint] + [s for s in near_candidates if s != hint]
        dom_sym = by_month.get(dom_month) if dom_month else None
        selected = _select_underlying_candidates(exchange, product, futs, dom_month)
        if hint in futs:
            selected = [hint] + [s for s in selected if s != hint]
        product_plan.append((product, name, near_candidates, dom_sym, dom_month, selected))

    underlying_syms = list(dict.fromkeys(s for x in product_plan for s in x[5]))
    underlying_price_map = {}
    if underlying_syms:
        t_under = time.time()
        underlying_quotes, quote_miss, failed_batches = _get_quotes_cached(api, underlying_syms)
        try:
            _wait_for(api, 1)
        except:
            pass
        for q in underlying_quotes:
            v = _quote_price_for_atm(q)
            if v is not None and v > 0:
                underlying_price_map[q.instrument_id] = v
        log(f"[启动] 标的期货候选订阅: 有效价格 {len(underlying_price_map)}/{len(underlying_syms)}, 新订阅 {quote_miss}, 失败兜底 {failed_batches} 批 ({time.time()-t_under:.1f}s)")

    target_list = []
    for product, name, near_candidates, dom_sym, dom_month, selected in product_plan:
        entries = []
        if OPTION_MONTH_SCOPE == "main":
            res = _resolve_atm_candidate(api, exchange, product, selected, underlying_price_map)
            if res:
                role = "main" if dom_month and res["month"] == dom_month else "near"
                label = "主力" if role == "main" else "近月(无主力)"
                entries.append((label, role, res, len(selected)))
        else:
            near_res = _resolve_atm_candidate(api, exchange, product, near_candidates, underlying_price_map)
            if near_res:
                role = "near_main" if dom_month and near_res["month"] == dom_month else "near"
                label = "近月=主力" if role == "near_main" else "近月"
                entries.append((label, role, near_res, len(near_candidates)))
            if OPTION_MONTH_SCOPE == "near_main" and dom_sym and (not near_res or dom_sym != near_res["underlying"]):
                main_res = _resolve_atm_candidate(api, exchange, product, [dom_sym], underlying_price_map)
                if main_res:
                    entries.append(("主力", "main", main_res, 1))
        if not entries:
            log(f"[启动]   {product} {name} 未识别到平值期权链: 候选 {len(selected)} 个")
            continue
        labels = [f"{label}{res['month']}" for label, _, res, _ in entries]
        log(f"[启动]   {product} {name} {' + '.join(labels)}")
        for label, role, res, total_candidates in entries:
            calls, puts = res["calls"], res["puts"]
            _resolved_underlying_hint[(exchange, product)] = res["underlying"]
            target_list.append((product, name, res["underlying"], role, calls, puts))
            log(f"[启动]     {res['month']}: {len(calls)}C + {len(puts)}P, 官方平值链 ATM±{OPTION_ATM_WINDOW}, 候选 {res['idx']}/{total_candidates}, 标的 {res['S']}, 平值 {res['atm']}, 到期 {_fmt_days(res.get('expire_days'))} 天")
    return target_list

def _ensure_resolved(api, exchange, products_dict, force=False):
    """检查缓存是否有效, 无效则重新识别目标合约"""
    global _last_resolve_time

    now = time.time()
    cache_key = (exchange, tuple(sorted(products_dict.keys())))
    cache = _cached_targets.get(cache_key)

    # 缓存有效
    if not force and cache and cache["targets"] and (now - cache["ts"]) < TARGET_REFRESH_SEC:
        _target_resolve_sources[exchange] = f"内存缓存命中: {len(cache['targets'])}组"
        return cache["targets"]

    # 需要刷新
    action = "强制刷新目标合约" if force else "刷新目标合约" if cache else "首次识别目标合约"
    _target_resolve_sources[exchange] = action
    targets = _build_target_set(api, exchange, products_dict)

    opt_count = sum(len(c)+len(p) for _,_,_,_,c,p in targets)
    _target_resolve_sources[exchange] = f"{action}: {len(targets)}组/{opt_count}期权"
    if not targets:
        _target_resolve_sources[exchange] = f"{action}失败: 官方未识别到目标合约，未订阅"
        log(f"[错误] {exchange} 官方目标解析失败: 未识别到平值期权链，未订阅；不使用历史库，不使用模拟持仓兜底")
        _cached_targets.pop(cache_key, None)
        return []

    _cached_targets[cache_key] = {"targets": targets, "ts": now}
    _last_resolve_time = now
    return targets

def fetch_exchange(api, exchange, products_dict, market_time=None):
    """获取一个交易所的行情数据并立即入库, 返回记录数"""
    t_start = time.time()

    # 阶段1: 获取/复用目标合约列表
    targets = _ensure_resolved(api, exchange, products_dict)
    if not targets:
        log(f"[启动] {exchange} 无目标合约, 跳过")
        return []

    # 收集所有目标期权符号
    all_target_syms = []
    for product, name, underlying_sym, contract_role, calls, puts in targets:
        all_target_syms.extend(calls + puts)

    cache_key = (exchange, tuple(sorted(products_dict.keys())))
    is_cached = _cached_targets.get(cache_key) and (time.time()-_cached_targets[cache_key]['ts']) < TARGET_REFRESH_SEC
    cache_tag = "缓存命中" if is_cached else "重新识别"
    log(f"[订阅] 目标期权: {len(all_target_syms)} 个 ({cache_tag})")

    t2 = time.time()
    all_quotes, quote_miss, failed_batches = _get_quotes_cached(api, all_target_syms)
    total_batches = (quote_miss + QUOTE_BATCH_SIZE - 1) // QUOTE_BATCH_SIZE if quote_miss else 0
    fail_tag = f" 失败兜底{failed_batches}批" if failed_batches else ""
    log(f"[订阅] 行情订阅缓存: {len(all_quotes)}/{len(all_target_syms)} 条, 新订阅 {quote_miss} ({total_batches} 批{fail_tag}, {time.time()-t2:.1f}s)")

    if not all_quotes:
        return []

    # 分批批量获取Greeks
    global _greeks_cache_ts, _greeks_columns_logged
    greeks_map = {}
    active_all = [
        q.instrument_id for q in all_quotes
        if q.open_interest and q.open_interest > 0
        and (_option_expire_days(q) is not None and _option_expire_days(q) > OPTION_MIN_EXPIRE_DAYS)
    ]
    if active_all:
        tg = time.time()
        now_ts = time.time()
        with _greeks_cache_lock:
            cache_fresh = _greeks_cache and (now_ts - _greeks_cache_ts) < GREEKS_REFRESH_SEC
            if cache_fresh:
                greeks_map = {s: _greeks_cache[s] for s in active_all if s in _greeks_cache}
                need_query = [s for s in active_all if s not in greeks_map]
            else:
                need_query = active_all
                greeks_map = {s: _greeks_cache[s] for s in active_all if s in _greeks_cache}
        greeks_batches = (len(need_query) + GREEKS_BATCH_SIZE - 1) // GREEKS_BATCH_SIZE if need_query else 0
        new_greeks = {}
        for gi in range(greeks_batches):
            gs = gi * GREEKS_BATCH_SIZE
            ge = min(gs + GREEKS_BATCH_SIZE, len(need_query))
            batch_active = need_query[gs:ge]
            try:
                gd = api.query_option_greeks(batch_active)
                if gd is not None and hasattr(gd, 'iterrows'):
                    if not _greeks_columns_logged:
                        if OPTION_LOG_GREEKS_FIELDS:
                            log(f"[订阅] Greeks 字段明细: {','.join(str(c) for c in gd.columns)}")
                        else:
                            log("[订阅] Greeks 字段已确认, 默认隐藏明细")
                        _greeks_columns_logged = True
                    for _, row in gd.iterrows():
                        new_greeks[row['instrument_id']] = row
            except:
                pass
        if new_greeks:
            with _greeks_cache_lock:
                _greeks_cache.update(new_greeks)
                _greeks_cache_ts = time.time()
                greeks_map = {s: _greeks_cache[s] for s in active_all if s in _greeks_cache}
        log(f"[订阅] Greeks: 活跃 {len(greeks_map)}/{len(active_all)}, 缓存 {len(active_all)-len(need_query)}, 新取 {len(new_greeks)} ({greeks_batches} 批, {time.time()-tg:.1f}s)")

    # 解析 + 立即入库
    quote_map = {q.instrument_id: q for q in all_quotes}
    if market_time is None:
        for q in all_quotes:
            mt, _mts = _quote_market_time_values(q)
            if mt is not None:
                market_time = mt
                break

    def _gv(gr, key):
        if gr is None: return None
        v = gr.get(key)
        if v is None: return None
        try:
            f = float(v)
            return f if f == f else None
        except: return None

    def _iv_value(q, month_iv_map):
        iv = month_iv_map.get(q.instrument_id)
        return iv, "tq_impv" if iv is not None else "none"

    total_saved = 0
    iv_open_map = load_daily_iv_open(all_target_syms, market_time)
    iv_from_tq_impv = 0
    iv_missing = 0
    iv_reason_total = {"no_oc": 0, "no_K": 0, "no_t": 0, "no_price": 0,
                       "days_short": 0, "below_bound": 0, "out_of_range": 0, "batch_fail": 0}

    for product, name, underlying_sym, contract_role, calls, puts in targets:
        month_syms = set(calls + puts)
        month_quotes = [quote_map[s] for s in month_syms if s in quote_map]
        if not month_quotes:
            continue

        # 标的价格
        S = None
        underlying_mult = None
        try:
            uq = api.get_quote(underlying_sym)
            underlying_mult = safe_float(getattr(uq, "volume_multiple", None), None)
            S = safe_float(uq.last_price, None)
            if not S or S <= 0:
                bid = safe_float(getattr(uq, "bid_price1", None), None)
                ask = safe_float(getattr(uq, "ask_price1", None), None)
                if bid and ask and bid > 0 and ask > 0 and ask >= bid:
                    S = (bid + ask) / 2.0
            if not S or S <= 0:
                S = safe_float(uq.pre_close, None)
        except:
            pass
        if not S:
            for q in month_quotes:
                up = getattr(q, 'underlying_price', None)
                sv = safe_float(up, None)
                if sv and sv > 0:
                    S = sv; break

        t_impv = time.time()
        month_iv_map, month_iv_reasons = _tq_option_impv_batch(month_quotes, S, product)
        for k_, v_ in month_iv_reasons.items():
            iv_reason_total[k_] = iv_reason_total.get(k_, 0) + v_
        impv_ms = (time.time() - t_impv) * 1000
        records = []
        for q in month_quotes:
            exp_days = _option_expire_days(q)
            if exp_days is None or exp_days <= OPTION_MIN_EXPIRE_DAYS:
                continue
            oi = safe_float(q.open_interest, 0)
            vol = safe_float(q.volume, 0)
            lp = safe_float(q.last_price, 0)
            pc = safe_float(q.pre_close, 0)
            bid = safe_float(getattr(q, "bid_price1", None), 0)
            ask = safe_float(getattr(q, "ask_price1", None), 0)
            # 有效行情判定: 任一价格/成交/挂单信号存在即认为有效
            if oi <= 0 and vol <= 0 and lp <= 0 and pc <= 0 and bid <= 0 and ask <= 0:
                continue

            iid = q.instrument_id
            # B3: 期权方向优先用官方 Quote.option_class
            oc = _option_class_of(q, product, iid)
            if not oc:
                continue
            # B4: 用官方 Quote.underlying_symbol 交叉校验, 防止月份解析错配
            q_underlying = getattr(q, "underlying_symbol", None)
            if q_underlying and underlying_sym and q_underlying != underlying_sym:
                log(f"[警告] underlying 不一致: {iid} 期望 {underlying_sym}, 官方返回 {q_underlying}, 跳过")
                continue

            gr = greeks_map.get(iid)
            iv, iv_src = _iv_value(q, month_iv_map)
            if iv_src == "tq_impv":
                iv_from_tq_impv += 1
            else:
                iv_missing += 1
            mult = _product_multiplier(exchange, product, SIM_OPTION_MULTIPLIER, safe_float(getattr(q, "volume_multiple", None), underlying_mult))
            market_time, market_ts = _quote_market_time_values(q)
            if market_time is None or market_ts is None:
                continue
            iv_open = iv_open_map.get(iid)
            if iv is not None and iv_open is None:
                iv_open = iv
                iv_open_map[iid] = iv_open
                _record_iv_open(market_time, iid, iv_open)
            iv_chg = round(iv - iv_open, 4) if iv is not None and iv_open is not None else None
            records.append((
                market_time, market_ts, exchange, product, name, underlying_sym,
                iid, contract_role, oc,
                safe_float(q.strike_price),
                safe_float(q.last_price, None), safe_float(q.pre_close, None),
                safe_float(q.settlement, None),
                safe_float(q.bid_price1, None), safe_float(q.ask_price1, None),
                safe_float(q.bid_volume1, 0), safe_float(q.ask_volume1, 0),
                int(vol), int(oi), S, mult,
                exp_days, _option_expire_ts(q),
                getattr(q, "exercise_type", None),
                _gv(gr, "delta"), _gv(gr, "gamma"), _gv(gr, "theta"), _gv(gr, "vega"), _gv(gr, "rho"),
                iv, iv_open, iv_chg,
            ))

        # === 每个品种解析完立刻写入数据库 ===
        if records:
            cnt = _save_records(records)
            total_saved += cnt
            _record_product_update(exchange, product, max(r[1] for r in records if r[1] is not None))

        month_display = underlying_sym.split(".")[-1] if "." in underlying_sym else underlying_sym
        log(f"  {month_display}: {len(calls)}C+{len(puts)}P S={S} IV{len(month_iv_map)}({impv_ms:.0f}ms) -> {len(records)}条已入库")

    elapsed = time.time() - t_start
    mem = get_mem_mb()
    speed = total_saved / elapsed if elapsed > 0 else 0
    log(f"[{exchange}] IV口径: TqSdk get_impv 同query_option_greeks(last_price/underlying_last/expire_datetime)")
    log(f"[{exchange}] IV: 天勤IMPv{iv_from_tq_impv} 缺失{iv_missing}")
    if iv_missing > 0 and any(iv_reason_total.values()):
        nonzero = {k: v for k, v in iv_reason_total.items() if v > 0}
        reason_desc = " ".join(f"{k}={v}" for k, v in sorted(nonzero.items(), key=lambda x: -x[1]))
        log(f"[{exchange}] IV缺失原因: {reason_desc}")
    log(f"[{exchange}] 完成: {total_saved}条  耗时{elapsed:.1f}s  {speed:.0f}条/s  内存{mem:.0f}MB")

    return total_saved

def _save_records(records):
    """追加写入一组记录到数据库(不删除), 返回入库条数"""
    if not records: return 0
    def _op(conn, c):
        qmarks = ",".join(["?"] * len(records[0]))
        c.executemany(f"""INSERT OR REPLACE INTO option_chain (
            market_time,market_ts,exchange,product,product_name,underlying_sym,
            option_sym,contract_role,option_class,strike_price,last_price,pre_close,
            settlement,bid_price1,ask_price1,bid_volume1,ask_volume1,
            volume,open_interest,underlying_price,volume_multiple,
            expire_rest_days,expire_datetime,exercise_type,
            delta,gamma,theta,vega,rho,iv,iv_open,iv_chg
        ) VALUES ({qmarks})""", records)
        latest_records = [(r[6],) + r[:6] + r[7:] for r in records]
        latest_qmarks = ",".join(["?"] * len(latest_records[0]))
        c.executemany(f"""INSERT OR REPLACE INTO option_chain_latest (
            option_sym,market_time,market_ts,exchange,product,product_name,underlying_sym,
            contract_role,option_class,strike_price,last_price,pre_close,
            settlement,bid_price1,ask_price1,bid_volume1,ask_volume1,
            volume,open_interest,underlying_price,volume_multiple,
            expire_rest_days,expire_datetime,exercise_type,
            delta,gamma,theta,vega,rho,iv,iv_open,iv_chg
        ) VALUES ({latest_qmarks})""", latest_records)
        return len(records)
    return _db_write_retry("写入行情记录", _op)

def _save_latest_quote_records(records):
    if not records:
        return 0
    def _op(conn, c):
        latest_records = [(r[6],) + r[:6] + r[7:] for r in records]
        latest_qmarks = ",".join(["?"] * len(latest_records[0]))
        c.executemany(f"""INSERT INTO option_chain_latest (
            option_sym,market_time,market_ts,exchange,product,product_name,underlying_sym,
            contract_role,option_class,strike_price,last_price,pre_close,
            settlement,bid_price1,ask_price1,bid_volume1,ask_volume1,
            volume,open_interest,underlying_price,volume_multiple,
            expire_rest_days,expire_datetime,exercise_type,
            delta,gamma,theta,vega,rho,iv,iv_open,iv_chg
        ) VALUES ({latest_qmarks})
        ON CONFLICT(option_sym) DO UPDATE SET
            market_time=excluded.market_time,
            market_ts=excluded.market_ts,
            exchange=excluded.exchange,
            product=excluded.product,
            product_name=excluded.product_name,
            underlying_sym=excluded.underlying_sym,
            contract_role=excluded.contract_role,
            option_class=excluded.option_class,
            strike_price=excluded.strike_price,
            last_price=excluded.last_price,
            pre_close=excluded.pre_close,
            settlement=excluded.settlement,
            bid_price1=excluded.bid_price1,
            ask_price1=excluded.ask_price1,
            bid_volume1=excluded.bid_volume1,
            ask_volume1=excluded.ask_volume1,
            volume=excluded.volume,
            open_interest=excluded.open_interest,
            underlying_price=excluded.underlying_price,
            volume_multiple=excluded.volume_multiple,
            expire_rest_days=excluded.expire_rest_days,
            expire_datetime=excluded.expire_datetime,
            exercise_type=excluded.exercise_type
        WHERE excluded.market_ts >= option_chain_latest.market_ts""", latest_records)
        return len(records)
    return _db_write_retry("覆盖最新行情", _op)

def _quote_underlying_level(underlying_sym, month_quotes, quote_map):
    S = None
    underlying_mult = None
    uq = quote_map.get(underlying_sym) if underlying_sym else None
    if uq is not None:
        underlying_mult = safe_float(getattr(uq, "volume_multiple", None), None)
        S = safe_float(getattr(uq, "last_price", None), None)
        if not S or S <= 0:
            bid = safe_float(getattr(uq, "bid_price1", None), None)
            ask = safe_float(getattr(uq, "ask_price1", None), None)
            if bid and ask and bid > 0 and ask > 0 and ask >= bid:
                S = (bid + ask) / 2.0
        if not S or S <= 0:
            S = safe_float(getattr(uq, "pre_close", None), None)
    if not S:
        for q in month_quotes:
            sv = safe_float(getattr(q, 'underlying_price', None), None)
            if sv and sv > 0:
                S = sv
                break
    return S, underlying_mult

def _build_quote_only_record(ex, product, name, underlying_sym, contract_role, q, S, underlying_mult):
    exp_days = _option_expire_days(q)
    if exp_days is None or exp_days <= OPTION_MIN_EXPIRE_DAYS:
        return None
    oi = safe_float(getattr(q, "open_interest", None), 0)
    vol = safe_float(getattr(q, "volume", None), 0)
    lp = safe_float(getattr(q, "last_price", None), 0)
    pc = safe_float(getattr(q, "pre_close", None), 0)
    bid = safe_float(getattr(q, "bid_price1", None), 0)
    ask = safe_float(getattr(q, "ask_price1", None), 0)
    if oi <= 0 and vol <= 0 and lp <= 0 and pc <= 0 and bid <= 0 and ask <= 0:
        return None
    iid = getattr(q, "instrument_id", None)
    if not iid:
        return None
    oc = _option_class_of(q, product, iid)
    if not oc:
        return None
    mult = _product_multiplier(ex, product, SIM_OPTION_MULTIPLIER, safe_float(getattr(q, "volume_multiple", None), underlying_mult))
    market_time, market_ts = _quote_market_time_values(q)
    if market_time is None or market_ts is None:
        return None
    return (
        market_time, market_ts, ex, product, name, underlying_sym,
        iid, contract_role, oc,
        safe_float(getattr(q, "strike_price", None)),
        safe_float(getattr(q, "last_price", None), None), safe_float(getattr(q, "pre_close", None), None),
        safe_float(getattr(q, "settlement", None), None),
        safe_float(getattr(q, "bid_price1", None), None), safe_float(getattr(q, "ask_price1", None), None),
        safe_float(getattr(q, "bid_volume1", None), 0), safe_float(getattr(q, "ask_volume1", None), 0),
        int(vol), int(oi), S, mult,
        exp_days, _option_expire_ts(q),
        getattr(q, "exercise_type", None),
        None, None, None, None, None, None, None, None,
    )

def _update_latest_quotes(dirty_syms, targets_by_exchange, quote_map):
    if not dirty_syms:
        return 0, 0
    records = []
    touched_products = {}
    for ex, targets in targets_by_exchange.items():
        for product, name, underlying, role, calls, puts in targets:
            option_syms = list(dict.fromkeys((calls or []) + (puts or [])))
            group_syms = set(option_syms) | ({underlying} if underlying else set())
            if not (group_syms & dirty_syms):
                continue
            underlying_dirty = underlying in dirty_syms if underlying else False
            update_syms = option_syms if underlying_dirty else [s for s in option_syms if s in dirty_syms]
            month_quotes = [quote_map[s] for s in option_syms if s in quote_map]
            S, underlying_mult = _quote_underlying_level(underlying, month_quotes, quote_map)
            before = len(records)
            for sym in update_syms:
                q = quote_map.get(sym)
                if q is None:
                    continue
                rec = _build_quote_only_record(ex, product, name, underlying, role, q, S, underlying_mult)
                if rec is not None:
                    records.append(rec)
                    cur_ts = touched_products.get((ex, product))
                    if cur_ts is None or rec[1] > cur_ts:
                        touched_products[(ex, product)] = rec[1]
    saved = _save_latest_quote_records(records)
    for (ex, product), market_ts in touched_products.items():
        _record_product_update(ex, product, market_ts)
    return saved, len(touched_products)

# ============================================================
#  T型报价打印(控制台预览)
# ============================================================

def print_t(records, exchange, product, name, underlying_sym, label=""):
    sub = [r for r in records if r["product"] == product and r["underlying_sym"] == underlying_sym]
    if not sub: return
    S = sub[0]["underlying_price"]
    try: S = float(S)
    except: S = None
    strikes = sorted(set(r["strike_price"] for r in sub))
    cm, pm = {}, {}
    for r in sub:
        k = r["strike_price"]
        if r["option_class"] == "CALL": cm[k]=r
        else: pm[k]=r
    tag = f"[{label}] " if label else ""
    log(f"\n{tag}{exchange}.{product}({name}) {underlying_sym} S={S}")
    header = f"{'K':>8} | {'C_Bid':>7} {'C_Last':>7} {'C_OI':>7} | {'P_Last':>7} {'P_Ask':>7} {'P_OI':>7} | {'Delta':>7}"
    print(f"       {header}", flush=True)
    for k in strikes:
        cr, pr = cm.get(k), pm.get(k)
        try:
            cb = f"{float(cr['bid_price1']):.1f}" if cr and cr["bid_price1"] else "-"
            cl = f"{float(cr['last_price']):.1f}" if cr and cr["last_price"] else "-"
            co = f"{int(cr['open_interest']):>5}" if cr else "    -"
            pl = f"{float(pr['last_price']):.1f}" if pr and pr["last_price"] else "-"
            pa = f"{float(pr['ask_price1']):.1f}" if pr and pr["ask_price1"] else "-"
            po = f"{int(pr['open_interest']):>5}" if pr else "    -"
            d = f"{float(cr['delta']):.4f}" if cr and cr["delta"] else "-"
        except: cb=cl="-"; co=po="    -"; pl=pa="-"; d="-"
        atm = ""
        try:
            if S and abs(float(k)-float(S)) < max(float(S)*0.015, 5): atm=" *"
        except: pass
        row = f"{float(k):>8.0f} | {cb:>7} {cl:>7} {co:>7} | {pl:>7} {pa:>7} {po:>7} | {d:>7}{atm}"
        print(f"       {row}", flush=True)

def _print_round_summary(market_time_str):
    """打印本轮入库的汇总统计(从数据库读取)"""
    conn = _db_connect()
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM option_chain WHERE market_time=?", (market_time_str,))
    cnt = c.fetchone()[0]

    if cnt > 0:
        c.execute("SELECT exchange,product,product_name,COUNT(*),SUM(open_interest) FROM option_chain WHERE market_time=? GROUP BY exchange,product ORDER BY exchange,product", (market_time_str,))
        log(f"本轮入库 {cnt} 条:")
        log(f"  {'交易所':<6} {'品种':<6} {'名称':<8} {'合约':>5} {'总持仓':>10}")
        for r in c.fetchall():
            log(f"  {r[0]:<6} {r[1]:<6} {r[2] or '':<8} {r[3]:>5} {r[4] or 0:>10.0f}")

    c.execute("SELECT COUNT(*), MIN(market_time), MAX(market_time) FROM option_chain")
    total, earliest, latest = c.fetchone()
    log(f"DB总计: {total}条  范围: {earliest or '无'} ~ {latest or '无'}")

    conn.commit(); conn.close()

def _print_t_preview(market_time_str):
    """从数据库读取本轮数据, 打印T型报价预览"""
    from collections import defaultdict

    preview_items = []
    for ex, _, products in EXCHANGE_PRODUCT_CONFIG:
        for pd, nm in list(products.items())[:2]:
            preview_items.append((ex, pd, nm))
    for ex, pd, nm in preview_items:
        conn = _db_connect(row_factory=True)
        c = conn.cursor()
        c.execute("""
            SELECT * FROM option_chain
            WHERE market_time=? AND exchange=? AND product=?
            ORDER BY option_class, strike_price
        """, (market_time_str, ex, pd))
        rows = c.fetchall()
        conn.close()

        if not rows:
            continue

        by_underlying = defaultdict(list)
        for r in rows:
            by_underlying[r["underlying_sym"]].append(r)

        for us in sorted(by_underlying.keys()):
            sub = by_underlying[us]
            label = "近月" if us == min(by_underlying.keys()) else "主力"
            print_t(sub, ex, pd, nm, us, label)

# ============================================================
#  天勤长连接管理
# ============================================================
def _create_api():
    """创建天勤API连接"""
    from tqsdk import TqApi, TqAuth
    return TqApi(auth=TqAuth(TQ_USER, TQ_PASS), disable_print=True)

def _check_api_alive(api):
    """检查连接是否存活。
    官方 get_quote 只返回对象引用，不会因为合约不存在抛异常，也不会触发网络请求；
    必须通过一次真正的官方查询 + wait_update 驱动事件循环来确认连接状态。
    - query_quotes 是官方查询接口，返回合约代码列表；超时/断连会抛异常。
    - _wait_for 用 time.time()+秒数 的绝对截止符合官方约定。
    """
    try:
        _ = list(api.query_quotes(ins_class="FUTURE", exchange_id="DCE", expired=False))
        _wait_for(api, CONN_CHECK_TIMEOUT)
        return True
    except Exception:
        return False

# ============================================================
#  事件驱动模式 — 收到行情包即处理 + 节流落库 (v7)
# ============================================================
_event_subscribed_syms = set()


def _event_selected_products(exchange, all_products):
    if FETCH_SCOPE == "all":
        return all_products
    if exchange in FOCUS_EXCHANGES:
        focus = FOCUS_PRODUCTS.get(exchange, []) or []
        return {k: all_products[k] for k in focus if k in all_products}
    return None


def _collect_all_targets(api):
    """对 EXCHANGE_PRODUCT_CONFIG 中所有交易所/品种调用 _ensure_resolved，
    返回 targets_by_exchange = {exchange: [(product, name, underlying, role, calls, puts), ...]}
    """
    out = {}
    for ex, ex_name, all_products in EXCHANGE_PRODUCT_CONFIG:
        selected = _event_selected_products(ex, all_products)
        if selected is None:
            log(f"[事件] [{ex}] {ex_name} 跳过 (非 FOCUS_EXCHANGES)")
            out[ex] = []
            continue
        if not selected:
            log(f"[事件] [{ex}] {ex_name} 无关注品种")
            out[ex] = []
            continue
        log_gap()
        product_codes = ",".join(selected.keys())
        log(f"[目标解析] {ex} {ex_name}")
        log(f"├─ 配置品种: {len(selected)} ({product_codes})")
        log("├─ 来源: 内置配置 -> TqSdk期货候选 -> 标的价格 -> 平值期权链")
        log(f"├─ 月份策略: {_month_scope_name()}；近月按未到期期货月份从近到远尝试，主力由 query_cont_quotes 识别")
        try:
            targets = _ensure_resolved(api, ex, selected)
        except Exception as e:
            log(f"[事件] [{ex}] 目标合约解析失败: {str(e)[:120]}")
            targets = []
        out[ex] = targets or []
        opt_count = sum(len(c) + len(p) for _, _, _, _, c, p in (targets or []))
        target_products = len({p for p, _, _, _, _, _ in out[ex]})
        log(f"├─ 目标来源: {_target_resolve_sources.get(ex, '-')}")
        log(f"├─ 结果: {target_products}品种 / {len(out[ex])}组 / {opt_count}期权")
        _log_target_details(ex, out[ex])
    return out


def _products_for_exchange(exchange):
    for ex, _ex_name, all_products in EXCHANGE_PRODUCT_CONFIG:
        if ex == exchange:
            selected = _event_selected_products(ex, all_products)
            return selected or {}
    return {}


def _target_role_name(role):
    return {"near": "近月", "main": "主力", "near_main": "近月=主力"}.get(role, role or "-")


def _month_scope_name():
    return {"near": "仅近月", "main": "仅主力", "near_main": "近月+主力"}.get(OPTION_MONTH_SCOPE, OPTION_MONTH_SCOPE)


def _log_target_details(exchange, targets):
    targets = list(targets or [])
    if not targets:
        log("└─ 明细: 无")
        return
    log("└─ 明细:")
    for idx, (product, name, underlying, role, calls, puts) in enumerate(targets):
        branch = "   └─" if idx == len(targets) - 1 else "   ├─"
        log(f"{branch} {exchange}.{product:<4} {name:<8} {_target_role_name(role):<8} {underlying or '-':<18} {len(calls or []):>3}C + {len(puts or []):>3}P")


def _invalid_target_rows(targets_by_exchange, quote_map):
    rows, _ex_stats, _total, _got = _subscription_coverage_rows(targets_by_exchange, quote_map)
    bad = []
    for r in rows:
        if r.get("opt_total", 0) > 0 and r.get("opt_got", 0) == r.get("opt_total", 0) and r.get("opt_valid", 0) == 0:
            bad.append(r)
    return bad


def _refresh_invalid_target_groups(api, targets_by_exchange, quote_map):
    bad_rows = _invalid_target_rows(targets_by_exchange, quote_map)
    if not bad_rows:
        return targets_by_exchange, quote_map
    bad_desc = ", ".join(f"{r['exchange']}.{r['product']}({r['underlying']})" for r in bad_rows[:8])
    if len(bad_rows) > 8:
        bad_desc += f" 等{len(bad_rows)}组"
    log(f"[订阅] 检测到无效目标合约组: {bad_desc}，强制重新识别目标")
    refreshed = dict(targets_by_exchange)
    for r in bad_rows:
        _resolved_underlying_hint.pop((r.get("exchange"), r.get("product")), None)
    for ex in sorted({r.get("exchange") for r in bad_rows if r.get("exchange")}):
        products = _products_for_exchange(ex)
        if not products:
            continue
        try:
            targets = _ensure_resolved(api, ex, products, force=True)
            refreshed[ex] = targets or []
        except Exception as e:
            log(f"[警告] {ex} 无效目标强制刷新失败: {_clean_err_msg(e, 120)}")
    if refreshed == targets_by_exchange:
        return targets_by_exchange, quote_map
    quote_map = _subscribe_all_targets(api, refreshed)
    return refreshed, quote_map


def _flatten_target_syms(targets_by_exchange):
    """从 targets_by_exchange 收集所有需订阅的 instrument_id (含 underlying)。"""
    syms = []
    for ex, targets in targets_by_exchange.items():
        for product, name, underlying, role, calls, puts in targets:
            if underlying:
                syms.append(underlying)
            syms.extend(calls)
            syms.extend(puts)
    return list(dict.fromkeys(syms))


def _flatten_target_option_syms(targets_by_exchange):
    syms = []
    for ex, targets in targets_by_exchange.items():
        for product, name, underlying, role, calls, puts in targets:
            syms.extend(calls or [])
            syms.extend(puts or [])
    return list(dict.fromkeys(syms))


def _prune_latest_to_targets(targets_by_exchange):
    syms = _flatten_target_option_syms(targets_by_exchange)
    if not syms:
        return 0
    def _op(conn, c):
        c.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_current_target_options(option_sym TEXT PRIMARY KEY)")
        c.execute("DELETE FROM tmp_current_target_options")
        c.executemany("INSERT OR IGNORE INTO tmp_current_target_options(option_sym) VALUES (?)", [(s,) for s in syms])
        c.execute("""DELETE FROM option_chain_latest
                     WHERE option_sym NOT IN (SELECT option_sym FROM tmp_current_target_options)""")
        deleted = c.rowcount
        c.execute("DROP TABLE tmp_current_target_options")
        return deleted
    deleted = _db_write_retry("清理非当前目标快照", _op)
    if deleted:
        log(f"[订阅] 已清理非当前目标快照 {deleted} 条, 防止 GUI 显示旧合约")
    return deleted


def _truncate_quotes_db(label="启动清空 T 报数据库"):
    """启动时无条件清空行情库 option_chain / option_chain_latest。
    确保 T 报数据全部来自启动后的实时订阅, 不沿用任何历史数据。
    sim_* 表已迁移到独立 sim 库, 不受影响。
    """
    def _op(conn, c):
        c.execute("DELETE FROM option_chain")
        chain_n = c.rowcount
        c.execute("DELETE FROM option_chain_latest")
        latest_n = c.rowcount
        return chain_n, latest_n
    try:
        chain_n, latest_n = _db_write_retry(label, _op)
    except Exception as _e:
        log(f"[警告] {label}失败: {_clean_err_msg(_e, 120)}")
        return 0, 0
    log(f"[启动] T 报库已清空: option_chain {chain_n or 0} 条, option_chain_latest {latest_n or 0} 条; "
        f"行情数据将完全来自实时订阅")
    return chain_n or 0, latest_n or 0


def _quote_recordable(q, product):
    if q is None:
        return False
    exp_days = _option_expire_days(q)
    if exp_days is None or exp_days <= OPTION_MIN_EXPIRE_DAYS:
        return False
    oi = safe_float(getattr(q, "open_interest", None), 0)
    vol = safe_float(getattr(q, "volume", None), 0)
    lp = safe_float(getattr(q, "last_price", None), 0)
    pc = safe_float(getattr(q, "pre_close", None), 0)
    bid = safe_float(getattr(q, "bid_price1", None), 0)
    ask = safe_float(getattr(q, "ask_price1", None), 0)
    # 有效行情判定: 历史/当前任一价格信号存在即认为有效, 避免刚挂牌仅有挂单的合约被丢弃
    if oi <= 0 and vol <= 0 and lp <= 0 and pc <= 0 and bid <= 0 and ask <= 0:
        return False
    iid = getattr(q, "instrument_id", None)
    return bool(iid and _option_class_of(q, product, iid))


def _subscription_coverage_rows(targets_by_exchange, quote_map):
    subscribed = set(quote_map.keys())
    rows = []
    ex_stats = {}
    total = 0
    got = 0
    for ex, targets in targets_by_exchange.items():
        stat = ex_stats.setdefault(ex, {"total": 0, "got": 0, "valid": 0, "opt_total": 0, "groups": 0, "missing_groups": 0})
        for product, name, underlying, role, calls, puts in targets:
            syms = []
            if underlying:
                syms.append(underlying)
            syms.extend(calls)
            syms.extend(puts)
            syms = list(dict.fromkeys(syms))
            opt_syms = list(dict.fromkeys((calls or []) + (puts or [])))
            row_total = len(syms)
            row_got = sum(1 for s in syms if s in subscribed)
            opt_total = len(opt_syms)
            opt_got = sum(1 for s in opt_syms if s in subscribed)
            opt_valid = sum(1 for s in opt_syms if s in subscribed and _quote_recordable(quote_map.get(s), product))
            under_total = 1 if underlying else 0
            under_got = 1 if underlying and underlying in subscribed else 0
            missing = [s for s in syms if s not in subscribed]
            total += row_total
            got += row_got
            stat["total"] += row_total
            stat["got"] += row_got
            stat["valid"] += opt_valid
            stat["opt_total"] += opt_total
            stat["groups"] += 1
            if missing:
                stat["missing_groups"] += 1
            rows.append({
                "exchange": ex,
                "product": product,
                "name": name,
                "underlying": underlying,
                "total": row_total,
                "got": row_got,
                "missing": len(missing),
                "opt_total": opt_total,
                "opt_got": opt_got,
                "opt_valid": opt_valid,
                "under_total": under_total,
                "under_got": under_got,
                "sample": missing[:4],
            })
    return rows, ex_stats, total, got


def _missing_subscription_syms(targets_by_exchange, quote_map):
    subscribed = set(quote_map.keys())
    return [s for s in _flatten_target_syms(targets_by_exchange) if s not in subscribed]


def _merge_pending_subscriptions(pending_queue, missing_syms):
    seen = set(pending_queue)
    added = 0
    for sym in missing_syms:
        if sym and sym not in seen:
            pending_queue.append(sym)
            seen.add(sym)
            added += 1
    return added


def _log_subscription_coverage(label, targets_by_exchange, quote_map, full=False, max_missing_groups=12):
    rows, ex_stats, total, got = _subscription_coverage_rows(targets_by_exchange, quote_map)
    miss = max(0, total - got)
    pct = (got / total * 100.0) if total else 100.0
    valid_total = sum(st.get("valid", 0) for st in ex_stats.values())
    opt_total = sum(st.get("opt_total", 0) for st in ex_stats.values())
    valid_pct = (valid_total / opt_total * 100.0) if opt_total else 100.0
    if full:
        show_rows = rows
    else:
        show_rows = [r for r in rows if r["missing"] or r.get("opt_valid", 0) < r.get("opt_total", 0)][:max_missing_groups]
    log(f"[{label}]")
    log(f"├─ 总计: {got}/{total} 缺{miss} 覆盖率 {pct:.1f}%")
    log(f"├─ 有效行情: {valid_total}/{opt_total} ({valid_pct:.1f}%)")
    ex_items = sorted(ex_stats.items())
    for idx, (ex, st) in enumerate(ex_items):
        branch = "└─" if idx == len(ex_items) - 1 and not show_rows else "├─"
        ex_miss = st["total"] - st["got"]
        log(f"{branch} {ex}: {st['got']}/{st['total']} 缺{ex_miss} 有效{st.get('valid', 0)}/{st.get('opt_total', 0)} 组缺{st['missing_groups']}/{st['groups']}")
    if show_rows:
        log("└─ 明细:")
        for idx, r in enumerate(show_rows):
            branch = "   └─" if idx == len(show_rows) - 1 else "   ├─"
            miss_tag = ""
            if r["missing"]:
                sample = ",".join(r["sample"])
                miss_tag = f" 缺{r['missing']} sample={sample}"
            log(f"{branch} {r['exchange']}.{r['product']} {r['name']} {r['underlying']}: {r['got']}/{r['total']} (期权{r['opt_got']}/{r['opt_total']} 有效{r.get('opt_valid', 0)}/{r['opt_total']} 标的{r['under_got']}/{r['under_total']}){miss_tag}")
    if not full:
        hidden = sum(1 for r in rows if r["missing"] or r.get("opt_valid", 0) < r.get("opt_total", 0)) - len(show_rows)
        if hidden > 0:
            log(f"└─ 仍有 {hidden} 个缺失/无效组未展开显示")


def _backfill_missing_quotes(api, targets_by_exchange, quote_map, quote_signatures, pending_queue):
    global _event_subscribed_syms
    _merge_pending_subscriptions(pending_queue, _missing_subscription_syms(targets_by_exchange, quote_map))
    if not pending_queue:
        return set(), 0, 0, 0, 0
    pending_queue[:] = [s for s in pending_queue if s not in quote_map]
    if not pending_queue:
        return set(), 0, 0, 0, 0
    batch_size = min(QUOTE_BACKFILL_BATCH_SIZE, len(pending_queue))
    batch = pending_queue[:batch_size]
    del pending_queue[:batch_size]
    t0 = time.time()
    quotes, miss, failed = _get_quotes_cached(api, batch)
    try:
        _wait_for(api, 1)
    except:
        pass
    added = set()
    for q in quotes:
        sym = getattr(q, "instrument_id", None)
        if sym and sym in batch:
            quote_map[sym] = q
            _event_subscribed_syms.add(sym)
            quote_signatures[sym] = _quote_change_signature(q)
            added.add(sym)
    still_missing = [s for s in batch if s not in quote_map]
    if still_missing:
        pending_queue.extend(still_missing)
    log(f"[补订] 本批 {len(batch)} 成功 {len(added)} 失败 {len(still_missing)}  待补 {len(pending_queue)}  失败批 {failed}  ({(time.time()-t0)*1000:.0f}ms)")
    if still_missing:
        log(f"[补订] 失败样本: {','.join(still_missing[:6])}")
    return added, len(batch), len(still_missing), len(pending_queue), failed


def _subscribe_all_targets(api, targets_by_exchange):
    """订阅所有目标合约 (期权 + underlying)。复用 _get_quotes_cached 的分批+二分容错。
    返回 quote_map = {sym: Quote}。
    """
    global _event_subscribed_syms
    all_syms = _flatten_target_syms(targets_by_exchange)
    if not all_syms:
        log("[警告] 没有任何目标合约可订阅")
        return {}
    log(f"[事件] 一次性订阅: {len(all_syms)} 个合约 (含 underlying)")
    t0 = time.time()
    quotes, miss, failed = _get_quotes_cached(api, all_syms)
    try:
        _wait_for(api, 5)
    except:
        pass
    quote_map = {q.instrument_id: q for q in quotes if getattr(q, "instrument_id", None)}
    _event_subscribed_syms = set(quote_map.keys())
    log(f"[事件] 订阅完成: {len(quote_map)}/{len(all_syms)} 新订阅{miss} 失败兜底{failed}批 ({time.time()-t0:.1f}s)")
    _log_subscription_coverage("订阅覆盖", targets_by_exchange, quote_map)
    return quote_map


def _diff_refresh_targets(api, old_targets_by_ex, new_targets_by_ex, quote_map):
    """目标合约换月: 增量订阅新出现的 sym。天勤无显式 unsubscribe, 旧 sym 自然不再写入 records。
    返回更新后的 quote_map。
    """
    global _event_subscribed_syms
    old_syms = set(_flatten_target_syms(old_targets_by_ex))
    new_syms = set(_flatten_target_syms(new_targets_by_ex))
    added = new_syms - old_syms
    removed = old_syms - new_syms
    if not added and not removed:
        log("[事件] 目标合约无变化, 跳过增量订阅")
        return quote_map
    log(f"[事件] 目标合约换月: 新增 {len(added)} 个, 不再使用 {len(removed)} 个")
    if added:
        new_quotes, miss, failed = _get_quotes_cached(api, list(added))
        try:
            _wait_for(api, 2)
        except:
            pass
        for q in new_quotes:
            sym = getattr(q, "instrument_id", None)
            if sym:
                quote_map[sym] = q
                _event_subscribed_syms.add(sym)
        log(f"[事件] 增量订阅完成: {len(new_quotes)}/{len(added)}")
    return quote_map


def _quote_change_signature(q):
    """Quote 变更签名: 行情时间 + 一档报价 + 量 + 持仓; 加标的最新价 (来自 q.underlying_quote, 期权 Quote 才有)。
    注意: 官方 Quote 字段不存在 "underlying_price", 标的现价要从 q.underlying_quote.last_price 取。
    """
    if q is None:
        return None
    vals = []
    for attr in ("datetime", "last_price", "bid_price1", "ask_price1", "volume", "open_interest"):
        try:
            v = getattr(q, attr, None)
            if isinstance(v, float) and v != v:
                v = None
            vals.append(v)
        except:
            vals.append(None)
    # 标的价格变化 (只有期权 Quote 上 underlying_quote 才非 None)
    try:
        uq = getattr(q, "underlying_quote", None)
        u_lp = safe_float(getattr(uq, "last_price", None), None) if uq is not None else None
        if isinstance(u_lp, float) and u_lp != u_lp:
            u_lp = None
        vals.append(u_lp)
    except:
        vals.append(None)
    return tuple(vals)


def _scan_changed_quotes(api, quote_map, quote_signatures):
    changed = set()
    for sym, q in list(quote_map.items()):
        is_changed = False
        try:
            is_changed = bool(api.is_changing(q))
        except:
            is_changed = False
        sig = _quote_change_signature(q)
        old_sig = quote_signatures.get(sym)
        if old_sig is None:
            quote_signatures[sym] = sig
            continue
        if is_changed or sig != old_sig:
            quote_signatures[sym] = sig
            changed.add(sym)
    return changed


def _flush_dirty_groups(api, dirty_syms, targets_by_exchange, quote_map, max_groups=None):
    """找到 dirty_syms 涉及的 (exchange, product, underlying) 组, 全组重算 IV/Greeks 入库。
    返回 (saved_records, affected_groups)。
    """
    if not dirty_syms:
        return 0, 0, set()
    affected_groups = []
    for ex, targets in targets_by_exchange.items():
        for product, name, underlying, role, calls, puts in targets:
            group_syms = set(calls) | set(puts) | ({underlying} if underlying else set())
            if group_syms & dirty_syms:
                affected_groups.append((ex, product, name, underlying, role, calls, puts))
    if not affected_groups:
        return 0, 0, set()
    deferred_syms = set()
    if max_groups and max_groups > 0 and len(affected_groups) > max_groups:
        pending_groups = affected_groups[max_groups:]
        affected_groups = affected_groups[:max_groups]
        for _ex, _product, _name, underlying, _role, calls, puts in pending_groups:
            if underlying:
                deferred_syms.add(underlying)
            deferred_syms.update(calls or [])
            deferred_syms.update(puts or [])
    by_ex = {}
    for g in affected_groups:
        by_ex.setdefault(g[0], []).append(g)
    total_saved = 0
    iv_open_cache_by_ex = {}
    global _greeks_cache_ts, _greeks_columns_logged
    for ex, groups in by_ex.items():
        iv_reason_total = {"no_oc": 0, "no_K": 0, "no_t": 0, "no_price": 0,
                           "days_short": 0, "below_bound": 0, "out_of_range": 0, "batch_fail": 0}
        iv_count_ex = 0
        all_syms_this_ex = []
        for _g0, _g1, _g2, _g3, _g4, calls, puts in groups:
            all_syms_this_ex.extend(calls + puts)
        all_syms_this_ex = list(dict.fromkeys(all_syms_this_ex))
        market_time_for_iv_open = None
        for s in all_syms_this_ex:
            q = quote_map.get(s)
            if q is None:
                continue
            mt, _mts = _quote_market_time_values(q)
            if mt is not None:
                market_time_for_iv_open = mt
                break
        active_all = []
        for s in all_syms_this_ex:
            q = quote_map.get(s)
            if q is None:
                continue
            oi = safe_float(getattr(q, "open_interest", None), 0)
            days = _option_expire_days(q)
            if oi and oi > 0 and days is not None and days > OPTION_MIN_EXPIRE_DAYS:
                active_all.append(s)
        greeks_map = {}
        if active_all:
            now_ts = time.time()
            with _greeks_cache_lock:
                cache_fresh = _greeks_cache and (now_ts - _greeks_cache_ts) < GREEKS_REFRESH_SEC
                if cache_fresh:
                    greeks_map = {s: _greeks_cache[s] for s in active_all if s in _greeks_cache}
                    need_query = [s for s in active_all if s not in greeks_map]
                else:
                    need_query = active_all
                    greeks_map = {s: _greeks_cache[s] for s in active_all if s in _greeks_cache}
            new_greeks = {}
            for gi in range(0, len(need_query), GREEKS_BATCH_SIZE):
                batch_active = need_query[gi:gi + GREEKS_BATCH_SIZE]
                try:
                    gd = api.query_option_greeks(batch_active)
                    if gd is not None and hasattr(gd, 'iterrows'):
                        if not _greeks_columns_logged:
                            if OPTION_LOG_GREEKS_FIELDS:
                                log(f"[订阅] Greeks 字段明细: {','.join(str(c) for c in gd.columns)}")
                            else:
                                log("[订阅] Greeks 字段已确认, 默认隐藏明细")
                            _greeks_columns_logged = True
                        for _, row in gd.iterrows():
                            new_greeks[row['instrument_id']] = row
                except:
                    pass
            if new_greeks:
                with _greeks_cache_lock:
                    _greeks_cache.update(new_greeks)
                    _greeks_cache_ts = time.time()
                    greeks_map = {s: _greeks_cache[s] for s in active_all if s in _greeks_cache}
        if ex not in iv_open_cache_by_ex:
            iv_open_cache_by_ex[ex] = load_daily_iv_open(all_syms_this_ex, market_time_for_iv_open)
        iv_open_map = iv_open_cache_by_ex[ex]

        def _gv(gr, key):
            if gr is None:
                return None
            v = gr.get(key)
            if v is None:
                return None
            try:
                f = float(v)
                return f if f == f else None
            except:
                return None

        for _g_ex, product, name, underlying_sym, contract_role, calls, puts in groups:
            month_syms = set(calls + puts)
            month_quotes = [quote_map[s] for s in month_syms if s in quote_map]
            if not month_quotes:
                continue
            S = None
            underlying_mult = None
            uq = quote_map.get(underlying_sym) if underlying_sym else None
            if uq is not None:
                underlying_mult = safe_float(getattr(uq, "volume_multiple", None), None)
                S = safe_float(getattr(uq, "last_price", None), None)
                if not S or S <= 0:
                    bid = safe_float(getattr(uq, "bid_price1", None), None)
                    ask = safe_float(getattr(uq, "ask_price1", None), None)
                    if bid and ask and bid > 0 and ask > 0 and ask >= bid:
                        S = (bid + ask) / 2.0
                if not S or S <= 0:
                    S = safe_float(getattr(uq, "pre_close", None), None)
            if not S:
                for q in month_quotes:
                    up = getattr(q, 'underlying_price', None)
                    sv = safe_float(up, None)
                    if sv and sv > 0:
                        S = sv
                        break
            month_iv_map, month_iv_reasons = _tq_option_impv_batch(month_quotes, S, product)
            for k_, v_ in month_iv_reasons.items():
                iv_reason_total[k_] = iv_reason_total.get(k_, 0) + v_
            iv_count_ex += len(month_iv_map)
            records = []
            for q in month_quotes:
                exp_days = _option_expire_days(q)
                if exp_days is None or exp_days <= OPTION_MIN_EXPIRE_DAYS:
                    continue
                oi = safe_float(q.open_interest, 0)
                vol = safe_float(q.volume, 0)
                lp = safe_float(q.last_price, 0)
                pc = safe_float(q.pre_close, 0)
                bid = safe_float(getattr(q, "bid_price1", None), 0)
                ask = safe_float(getattr(q, "ask_price1", None), 0)
                if oi <= 0 and vol <= 0 and lp <= 0 and pc <= 0 and bid <= 0 and ask <= 0:
                    continue
                iid = q.instrument_id
                oc = _option_class_of(q, product, iid)
                if not oc:
                    continue
                q_underlying = getattr(q, "underlying_symbol", None)
                if q_underlying and underlying_sym and q_underlying != underlying_sym:
                    log(f"[警告] underlying 不一致: {iid} 期望 {underlying_sym}, 官方返回 {q_underlying}, 跳过")
                    continue
                gr = greeks_map.get(iid)
                iv = month_iv_map.get(iid)
                mult = _product_multiplier(ex, product, SIM_OPTION_MULTIPLIER, safe_float(getattr(q, "volume_multiple", None), underlying_mult))
                market_time, market_ts = _quote_market_time_values(q)
                if market_time is None or market_ts is None:
                    continue
                iv_open = iv_open_map.get(iid)
                if iv is not None and iv_open is None:
                    iv_open = iv
                    iv_open_map[iid] = iv_open
                    _record_iv_open(market_time, iid, iv_open)
                iv_chg = round(iv - iv_open, 4) if iv is not None and iv_open is not None else None
                records.append((
                    market_time, market_ts, ex, product, name, underlying_sym,
                    iid, contract_role, oc,
                    safe_float(q.strike_price),
                    safe_float(q.last_price, None), safe_float(q.pre_close, None),
                    safe_float(q.settlement, None),
                    safe_float(q.bid_price1, None), safe_float(q.ask_price1, None),
                    safe_float(q.bid_volume1, 0), safe_float(q.ask_volume1, 0),
                    int(vol), int(oi), S, mult,
                    exp_days, _option_expire_ts(q),
                    getattr(q, "exercise_type", None),
                    _gv(gr, "delta"), _gv(gr, "gamma"), _gv(gr, "theta"), _gv(gr, "vega"), _gv(gr, "rho"),
                    iv, iv_open, iv_chg,
                ))
            if records:
                cnt = _save_records(records)
                total_saved += cnt
                _record_product_update(ex, product, max(r[1] for r in records if r[1] is not None))
        # 每个交易所处理完后, 汇报 IV 缺失诊断
        nonzero = {k: v for k, v in iv_reason_total.items() if v > 0}
        if nonzero:
            reason_desc = " ".join(f"{k}={v}" for k, v in sorted(nonzero.items(), key=lambda x: -x[1]))
            log(f"[事件][{ex}] IV: 成功{iv_count_ex} 缺失原因: {reason_desc}")
    return total_saved, len(affected_groups), deferred_syms


def _do_reconnect_and_resubscribe(targets_by_exchange):
    """断线重连 + 重订阅。返回 (new_api, new_quote_map)。"""
    log("[事件] 触发重连...")
    _clear_api_bound_caches()
    api = _create_api()
    quote_map = _subscribe_all_targets(api, targets_by_exchange)
    log("[事件] 重连完成")
    return api, quote_map


def _event_loop(api):
    """事件驱动主循环。
    - wait_update(deadline=EVENT_DEADLINE_SEC) 节拍源
    - is_changing 维护 dirty_syms
    - DB_FLUSH_INTERVAL_SEC 节流落库 (交易段)
    - DB_FLUSH_INTERVAL_OFF_SEC=0 非交易段不落库 (但仍 wait_update 保活)
    - GUI_REFRESH_INTERVAL_SEC 节流刷快照
    - 目标合约: 启动时 + 每个新交易段开盘时刷新
    """
    global running
    # 启动时无条件清空 T 报库, 保证行情数据全部来自启动后的实时订阅
    try:
        _truncate_quotes_db()
    except Exception as e:
        log(f"[警告] 启动清空 T 报库异常: {_clean_err_msg(e, 120)}")
    targets_by_exchange = _collect_all_targets(api)
    # 修复 A: 追加 sim_strategy_leg 中 status='OPEN' 的历史持仓合约到订阅清单
    # 避免远 OTM 历史持仓 (不在本次 ATM 窗口) 无行情 → 浮盈亏永为 0
    try:
        sim_extra = _collect_sim_position_targets()
        if sim_extra:
            before_cnt = len(_flatten_target_syms(targets_by_exchange))
            targets_by_exchange = _merge_sim_position_targets(targets_by_exchange, sim_extra)
            after_cnt = len(_flatten_target_syms(targets_by_exchange))
            log(f"[启动] 追加模拟持仓历史合约 {after_cnt - before_cnt} 个至订阅清单 (持仓估值需要)")
    except Exception as e:
        log(f"[启动] 合并模拟持仓订阅失败: {_clean_err_msg(e, 80)}")
    quote_map = _subscribe_all_targets(api, targets_by_exchange)
    try:
        targets_by_exchange, quote_map = _refresh_invalid_target_groups(api, targets_by_exchange, quote_map)
    except Exception as e:
        log(f"[警告] 无效目标自动修复失败: {_clean_err_msg(e, 120)}")

    last_quote_flush = 0.0
    last_greeks_flush = 0.0
    last_gui_refresh = 0.0
    last_heartbeat = 0.0
    last_packet_ts = time.time()
    # PnL 快照后台线程: 从主循环中拆出, 避免 sim_list_strategies 的 DB I/O 冻结 GUI
    def _pnl_snapshot_worker():
        while running:
            try:
                if is_trading_time():
                    t0 = time.time()
                    sim_list_strategies(save_snapshots=True)
                    dur = time.time() - t0
                    if dur > 5.0:
                        log(f"[警告] PnL 快照线程耗时 {dur:.1f}s (DB I/O 或模拟持仓过多)")
            except Exception as _e:
                if not _is_db_locked_error(_e):
                    log(f"[错误] PnL 快照线程异常: {_clean_err_msg(_e, 80)}")
            # 可中断睡眠, 便于退出时快速收尾
            slept = 0.0
            while slept < PNL_SNAPSHOT_SEC and running:
                time.sleep(1.0)
                slept += 1.0
    threading.Thread(target=_pnl_snapshot_worker, name="PnLSnapshotWorker", daemon=True).start()
    last_session_id = _current_session_id()
    quote_dirty_syms = set()
    greeks_dirty_syms = set()
    quote_signatures = {}
    pending_subscription_syms = []
    flush_count = 0
    quote_flush_count = 0
    backfill_success_total = 0
    backfill_fail_total = 0
    saved_total = 0
    error_count = 0
    packets_since_heartbeat = 0
    quote_changes_since_heartbeat = 0
    reconnect_pending = False
    startup_ts = time.time()
    HEARTBEAT_TRADING_SEC = 60.0      # 交易段心跳间隔
    HEARTBEAT_OFF_SEC = 300.0         # 非交易段心跳间隔 (5分钟)
    last_backfill_try = 0.0
    last_coverage_log = 0.0
    last_quote_flush_log = 0.0
    last_greeks_flush_log = 0.0
    missing_initial = _missing_subscription_syms(targets_by_exchange, quote_map)
    if missing_initial:
        added_pending = _merge_pending_subscriptions(pending_subscription_syms, missing_initial)
        log(f"[补订] 初始化待补队列: 缺失 {len(missing_initial)} 个, 入队 {added_pending} 个")

    sess_now = _current_session_id() or "非交易段"
    log_banner(f"事件循环启动  当前交易段: {sess_now}")
    log(f"[事件] 节拍 wait_update={EVENT_DEADLINE_SEC}s  最新行情 {QUOTE_LATEST_FLUSH_INTERVAL_SEC}s  IV/Greeks {GREEKS_DB_FLUSH_INTERVAL_SEC}s  GUI {GUI_REFRESH_INTERVAL_SEC}s  PnL {PNL_SNAPSHOT_SEC}s  心跳 60s/300s")

    # 启动后做一次初始 flush, 确保 GUI 有首张快照 (无视交易段)
    try:
        init_dirty = set(quote_map.keys())
        quote_signatures = {sym: _quote_change_signature(q) for sym, q in quote_map.items()}
        if init_dirty:
            latest_saved, latest_products = _update_latest_quotes(init_dirty, targets_by_exchange, quote_map)
            quote_flush_count += 1
            log(f"[落库] 初始最新行情: 覆盖 {latest_saved} 条, 命中 {latest_products} 品种")
            try:
                refresh_memory_snapshot()
            except Exception as _e:
                log(f"[警告] 初始 GUI 快照失败: {_clean_err_msg(_e, 80)}")
            # 启动初轮: 不限组数, 一次性把全部品种 IV/Greeks 算完, 避免 GUI 首屏大片 IV 缺失。
            # 后续增量 flush 仍然走 GREEKS_MAX_GROUPS_PER_FLUSH 节流。
            t_init_flush = time.time()
            saved, groups, deferred = _flush_dirty_groups(api, init_dirty, targets_by_exchange, quote_map, max_groups=None)
            if deferred:
                greeks_dirty_syms |= deferred
            log(f"[落库] 初始 IV/Greeks (全量): 入库 {saved} 条, 命中 {groups} 组, 延后 {len(deferred)} 合约, 耗时 {time.time()-t_init_flush:.1f}s")
            saved_total += saved
            last_quote_flush = time.time()
            last_greeks_flush = last_quote_flush
            last_gui_refresh = last_quote_flush
    except Exception as e:
        log(f"[错误] 初始 flush 异常: {_clean_err_msg(e, 120)}")
        error_count += 1

    last_heartbeat = time.time()

    while running:
        loop_start_ts = time.time()
        # ---- 1. 接收 ----
        try:
            updated = _wait_for(api, EVENT_DEADLINE_SEC)
        except Exception as e:
            log_banner("连接异常")
            log(f"[警告] wait_update 异常: {_clean_err_msg(e, 120)}")
            log("[重连] 准备重连")
            try:
                api.close()
            except:
                pass
            reconnect_pending = True
            updated = False
            error_count += 1

        now = time.time()
        trading = is_trading_time()
        session_id = _current_session_id()

        if updated and not reconnect_pending:
            last_packet_ts = now
            packets_since_heartbeat += 1
            changed = _scan_changed_quotes(api, quote_map, quote_signatures)
            if changed:
                quote_dirty_syms |= changed
                greeks_dirty_syms |= changed
                quote_changes_since_heartbeat += len(changed)

        # ---- 2. 重连 ----
        # 重连判定严格走天勤 API 语义: wait_update 多久未返回任何推送 (last_packet_ts).
        # 不使用 last_quote_flush, 避免低流动性合约几分钟无变化时误判为掩线。
        # 90s 阈值为“较严”推送静默, 300s 为“严重”静默。
        idle_secs = now - last_packet_ts
        quote_stale = trading and quote_map and idle_secs > QUOTE_STALE_RECONNECT_SEC
        if reconnect_pending or (trading and idle_secs > RECONNECT_AFTER_IDLE_SEC) or quote_stale:
            if not reconnect_pending:
                log_banner(f"天勤推送静默 {idle_secs:.0f}s 超阈值, 触发重连")
            try:
                api, quote_map = _do_reconnect_and_resubscribe(targets_by_exchange)
                log("[重连] 完成, 下一次 flush 将全量重写")
            except Exception as e:
                log(f"[错误] 重连失败: {_clean_err_msg(e, 120)}, 60s 后重试")
                error_count += 1
                _smart_sleep(60)
                continue
            reconnect_pending = False
            last_packet_ts = time.time()
            quote_signatures = {sym: _quote_change_signature(q) for sym, q in quote_map.items()}
            quote_dirty_syms = set(quote_map.keys())
            greeks_dirty_syms = set(quote_map.keys())
            pending_subscription_syms.clear()
            missing_after_reconnect = _missing_subscription_syms(targets_by_exchange, quote_map)
            if missing_after_reconnect:
                added_pending = _merge_pending_subscriptions(pending_subscription_syms, missing_after_reconnect)
                log(f"[补订] 重连后缺失 {len(missing_after_reconnect)} 个, 入队 {added_pending} 个, 将后台小批补订")
            last_backfill_try = 0.0
            continue

        # ---- 3. 目标合约: 进入新交易段时刷新 ----
        if trading and session_id and session_id != last_session_id:
            log_banner(f"进入新交易段: {session_id}")
            log(f"[事件] 前一段: {last_session_id}  开始刷新目标合约...")
            try:
                old_syms_set = set(_flatten_target_syms(targets_by_exchange))
                new_targets = _collect_all_targets(api)
                # 换月后也需保留历史持仓的远 OTM 合约 (避免被 _diff_refresh 当作锁口移除)
                try:
                    sim_extra = _collect_sim_position_targets()
                    if sim_extra:
                        new_targets = _merge_sim_position_targets(new_targets, sim_extra)
                except Exception as _e:
                    log(f"[事件] 换月后合并模拟持仓订阅失败: {_clean_err_msg(_e, 80)}")
                quote_map = _diff_refresh_targets(api, targets_by_exchange, new_targets, quote_map)
                targets_by_exchange = new_targets
                targets_by_exchange, quote_map = _refresh_invalid_target_groups(api, targets_by_exchange, quote_map)
                _prune_latest_to_targets(targets_by_exchange)
                quote_signatures = {sym: _quote_change_signature(q) for sym, q in quote_map.items()}
                new_syms_set = set(_flatten_target_syms(targets_by_exchange))
                added = len(new_syms_set - old_syms_set)
                removed = len(old_syms_set - new_syms_set)
                log(f"[事件] 目标合约换月: 新增 {added} 个, 失效 {removed} 个")
                quote_dirty_syms |= set(quote_map.keys())
                greeks_dirty_syms |= set(quote_map.keys())
                pending_subscription_syms.clear()
                missing_after_refresh = _missing_subscription_syms(targets_by_exchange, quote_map)
                if missing_after_refresh:
                    added_pending = _merge_pending_subscriptions(pending_subscription_syms, missing_after_refresh)
                    log(f"[补订] 目标刷新后缺失 {len(missing_after_refresh)} 个, 入队 {added_pending} 个")
                _log_subscription_coverage("订阅覆盖", targets_by_exchange, quote_map)
            except Exception as e:
                log(f"[错误] 目标合约刷新失败: {_clean_err_msg(e, 120)}")
                error_count += 1
            last_session_id = session_id

        # ---- 4. 缺失合约补订 ----
        if trading and QUOTE_BACKFILL_INTERVAL_SEC > 0 and (now - last_backfill_try) >= QUOTE_BACKFILL_INTERVAL_SEC:
            try:
                added_syms, tried, failed_syms, pending_count, failed_batches = _backfill_missing_quotes(api, targets_by_exchange, quote_map, quote_signatures, pending_subscription_syms)
                if added_syms:
                    quote_dirty_syms |= added_syms
                    greeks_dirty_syms |= added_syms
                    backfill_success_total += len(added_syms)
                    _log_subscription_coverage("补订覆盖", targets_by_exchange, quote_map)
                if tried:
                    backfill_fail_total += failed_syms
                if tried and pending_count == 0:
                    log(f"[补订] 缺失队列已清空, 当前订阅 {len(quote_map)}/{len(_flatten_target_syms(targets_by_exchange))}")
            except Exception as e:
                error_count += 1
                log(f"[错误] 缺失合约补订异常: {_clean_err_msg(e, 120)}")
            last_backfill_try = time.time()

        # ---- 5. 落库节流 ----
        quote_flush_interval = QUOTE_LATEST_FLUSH_INTERVAL_SEC if trading else DB_FLUSH_INTERVAL_OFF_SEC
        if quote_flush_interval > 0 and quote_dirty_syms and (now - last_quote_flush) >= quote_flush_interval:
            t_flush = time.time()
            current_dirty = set(quote_dirty_syms)
            quote_dirty_syms.clear()
            try:
                saved, products = _update_latest_quotes(current_dirty, targets_by_exchange, quote_map)
                flush_dur = time.time() - t_flush
                quote_flush_count += 1
                should_log_quote_flush = saved > 0 and (quote_flush_count <= 2 or (QUOTE_FLUSH_LOG_INTERVAL_SEC > 0 and (time.time() - last_quote_flush_log) >= QUOTE_FLUSH_LOG_INTERVAL_SEC))
                if should_log_quote_flush:
                    log(f"[行情] 快刷 #{quote_flush_count:<5} 覆盖 {saved:>4}条/{products:>2}品种  变化 {len(current_dirty):>4}  耗时 {flush_dur*1000:.0f}ms")
                    last_quote_flush_log = time.time()
                if flush_dur > quote_flush_interval * 0.8:
                    log(f"[警告] 最新行情 #{quote_flush_count} 耗时 {flush_dur*1000:.0f}ms 接近节流间隔 {quote_flush_interval*1000:.0f}ms")
            except Exception as e:
                error_count += 1
                if _is_db_locked_error(e):
                    log(f"[警告] 最新行情数据库忙: {_clean_err_msg(e, 80)}")
                else:
                    log(f"[错误] 最新行情异常: {_clean_err_msg(e, 160)}")
            last_quote_flush = time.time()

        flush_interval = GREEKS_DB_FLUSH_INTERVAL_SEC if trading else DB_FLUSH_INTERVAL_OFF_SEC
        if flush_interval > 0 and greeks_dirty_syms and (now - last_greeks_flush) >= flush_interval:
            t_flush = time.time()
            current_dirty = set(greeks_dirty_syms)
            greeks_dirty_syms.clear()
            try:
                saved, groups, deferred = _flush_dirty_groups(api, current_dirty, targets_by_exchange, quote_map, GREEKS_MAX_GROUPS_PER_FLUSH)
                if deferred:
                    greeks_dirty_syms |= deferred
                flush_dur = time.time() - t_flush
                flush_count += 1
                saved_total += saved
                should_log_greeks_flush = saved > 0 and (flush_count <= 2 or (GREEKS_FLUSH_LOG_INTERVAL_SEC > 0 and (time.time() - last_greeks_flush_log) >= GREEKS_FLUSH_LOG_INTERVAL_SEC))
                if should_log_greeks_flush:
                    log(f"[Greeks] 慢刷 #{flush_count:<5} 入库 {saved:>4}条/{groups:>2}组  延后 {len(deferred):>4}  耗时 {flush_dur*1000:.0f}ms")
                    last_greeks_flush_log = time.time()
                if flush_dur > flush_interval * 0.8:
                    log(f"[警告] IV/Greeks #{flush_count} 耗时 {flush_dur*1000:.0f}ms 接近节流间隔 {flush_interval*1000:.0f}ms")
            except Exception as e:
                error_count += 1
                if _is_db_locked_error(e):
                    log(f"[警告] IV/Greeks 数据库忙: {_clean_err_msg(e, 80)}")
                else:
                    log(f"[错误] IV/Greeks 异常: {_clean_err_msg(e, 160)}")
            last_greeks_flush = time.time()

        # ---- 6. GUI 节流 ----
        if (now - last_gui_refresh) >= GUI_REFRESH_INTERVAL_SEC:
            try:
                refresh_memory_snapshot()
            except Exception:
                pass
            last_gui_refresh = now

        # ---- 7. PnL 快照 已拆到独立后台线程 _pnl_snapshot_worker, 主循环不再参与 ----

        # ---- 8. 心跳 (交易段 60s / 非交易段 300s) ----
        hb_interval = HEARTBEAT_TRADING_SEC if trading else HEARTBEAT_OFF_SEC
        if (now - last_heartbeat) >= hb_interval:
            runtime_s = now - startup_ts
            if runtime_s >= 3600:
                runtime_str = f"{int(runtime_s // 3600)}h{int((runtime_s % 3600) // 60)}m"
            else:
                runtime_str = f"{int(runtime_s // 60)}m{int(runtime_s % 60)}s"
            if saved_total >= 10000:
                saved_disp = f"{saved_total/10000:.1f}w"
            else:
                saved_disp = str(saved_total)
            mem = get_mem_mb()
            missing_now = _missing_subscription_syms(targets_by_exchange, quote_map)
            if missing_now:
                _merge_pending_subscriptions(pending_subscription_syms, missing_now)
            total_target_count = len(_flatten_target_syms(targets_by_exchange))
            sub_count = total_target_count - len(missing_now)
            if trading:
                log(f"[状态] 运行 {runtime_str}  包 {packets_since_heartbeat}  变动 {quote_changes_since_heartbeat}  错误 {error_count}  内存 {mem:.0f}MB")
                log(f"[订阅] 覆盖 {sub_count}/{total_target_count}  缺 {len(missing_now)}  待补 {len(pending_subscription_syms)}  补成 {backfill_success_total}  补失 {backfill_fail_total}")
                log(f"[写库] 快刷 {quote_flush_count}  慢刷 {flush_count}  dirty报价 {len(quote_dirty_syms)}  dirtyGreeks {len(greeks_dirty_syms)}  慢表入库 {saved_disp}")
                if missing_now and QUOTE_COVERAGE_LOG_INTERVAL_SEC > 0 and (now - last_coverage_log) >= QUOTE_COVERAGE_LOG_INTERVAL_SEC:
                    _log_subscription_coverage("订阅覆盖", targets_by_exchange, quote_map)
                    last_coverage_log = now
            else:
                log(f"[状态] 非交易段  收到包 {packets_since_heartbeat}/{int(hb_interval/60)}min  连接活跃  内存 {mem:.0f}MB")
                log(f"[订阅] 覆盖 {sub_count}/{total_target_count}  缺 {len(missing_now)}  待补 {len(pending_subscription_syms)}")
            packets_since_heartbeat = 0
            quote_changes_since_heartbeat = 0
            last_heartbeat = now

        # ---- 9. Watchdog: 本轮主循环耗时异常则警告 (便于定位 GUI 冻结原因) ----
        loop_dur = time.time() - loop_start_ts
        if loop_dur > 5.0:
            log(f"[警告] 主循环本轮耗时 {loop_dur:.1f}s, 期间 GUI 快照不会刷新 — 检查相邻【警告】/【错误】/【重连】/【事件】日志定位卡顿来源")

    # ---- 退出清理 ----
    log_banner("收到停止信号")
    try:
        if api:
            api.close()
            log("[退出] 天勤连接已关闭")
    except:
        pass
    runtime_h = (time.time() - startup_ts) / 3600
    if saved_total >= 10000:
        saved_disp_final = f"{saved_total/10000:.2f}w"
    else:
        saved_disp_final = str(saved_total)
    log(f"[退出] 总运行 {runtime_h:.2f}h  快刷 {quote_flush_count} 次  慢刷 {flush_count} 次  入库 {saved_disp_final} 条  错误 {error_count} 次")


# ============================================================
#  主程序 — 事件驱动模式 (v7)
# ============================================================
def main():
    global running
    running = True

    def _sig_handler(sig, frame):
        global running
        log("[退出] 收到停止信号, 正在退出...")
        running = False
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)

    log_gap()
    log("[启动配置] 期权T型报价获取器 v7")
    focus_desc = "  ".join(f"{ex}:{','.join(FOCUS_PRODUCTS.get(ex, [])) or '-'}" for ex, _, _ in EXCHANGE_PRODUCT_CONFIG)
    coverage_desc = "DCE,CZCE,SHFE 全交易所全品种" if FETCH_SCOPE == "all" else f"关注交易所:{','.join(FOCUS_EXCHANGES)}  {focus_desc}"
    win_desc = f"ATM±{OPTION_ATM_WINDOW}"
    month_desc = {"near": "仅近月", "main": "仅主力", "near_main": "近月+主力"}.get(OPTION_MONTH_SCOPE, OPTION_MONTH_SCOPE)
    mem0 = get_mem_mb()
    log(f"├─ 模式: {FETCH_SCOPE}；{coverage_desc}")
    log(f"├─ 节流: 最新行情 {QUOTE_LATEST_FLUSH_INTERVAL_SEC}s；IV/Greeks {GREEKS_DB_FLUSH_INTERVAL_SEC}s；非交易段 {DB_FLUSH_INTERVAL_OFF_SEC}s；GUI {GUI_REFRESH_INTERVAL_SEC}s；PnL {PNL_SNAPSHOT_SEC}s")
    log(f"├─ 批次: 行情 {QUOTE_BATCH_SIZE}/批；补订 {QUOTE_BACKFILL_BATCH_SIZE}/批/{QUOTE_BACKFILL_INTERVAL_SEC}s；Greeks {GREEKS_BATCH_SIZE}/批/{GREEKS_REFRESH_SEC}s缓存")
    log(f"├─ 目标: 行权价窗口 {win_desc}；月份 {month_desc}；换月策略 仅在新交易段开盘时刷新")
    log(f"├─ 行情库: {DB_PATH_QUOTES} (启动清空, option_chain / option_chain_latest)")
    log(f"├─ 模拟库: {DB_PATH_SIM} (持久化, sim_strategy / sim_strategy_leg / sim_trade / sim_pnl_snapshot)")
    log(f"└─ 进程: PID {os.getpid()}；内存 {mem0:.0f}MB；按 Ctrl+C 停止")

    init_db()

    api = None
    connect_attempts = max(1, CONNECT_RETRY_MAX + 1)
    for attempt in range(1, connect_attempts + 1):
        try:
            log_gap()
            log("[连接天勤]")
            log(f"├─ 尝试: 第 {attempt}/{connect_attempts} 次")
            log("├─ 操作: 创建 TqApi 连接")
            tc = time.time()
            api = _create_api()
            _clear_api_bound_caches()
            log(f"├─ 结果: 成功")
            log(f"└─ 耗时: {time.time()-tc:.1f}s")
            break
        except Exception as e:
            try:
                if api:
                    api.close()
            except:
                pass
            api = None
            if attempt < connect_attempts:
                log(f"├─ 结果: 失败")
                log(f"├─ 原因: {_clean_err_msg(e, 200)}")
                log(f"└─ 下一步: {CONNECT_RETRY_SLEEP_SEC:g}s 后重试")
                _smart_sleep(CONNECT_RETRY_SLEEP_SEC)
            else:
                log(f"├─ 结果: 失败")
                log(f"├─ 原因: {_clean_err_msg(e, 200)}")
                log("└─ 下一步: 已达到最大重试次数，停止程序")
                running = False
                return

    try:
        _event_loop(api)
    except Exception as e:
        log(f"[错误] 事件循环异常退出: {_clean_err_msg(e, 200)}")
    finally:
        try:
            if api:
                api.close()
        except:
            pass
        running = False

def _smart_sleep(total_sec):
    """分段sleep以便响应Ctrl+C"""
    global running
    remaining = total_sec
    while running and remaining > 0:
        chunk = min(remaining, 5)
        time.sleep(chunk)
        remaining -= chunk


# ============================================================
#  PyQt5 GUI — 专业期权T型报价窗口 (浅色护眼风格 · 全中文)
# ============================================================
_gui_root = None
_gui_data_ready = False

def _start_gui():
    """在独立线程中启动PyQt5 GUI窗口"""
    global _gui_root
    from PyQt5.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
                                 QVBoxLayout, QHBoxLayout, QWidget, QLabel, QPushButton,
                                 QComboBox, QHeaderView, QFrame, QAbstractItemView,
                                 QScrollArea, QSizePolicy, QButtonGroup, QTabWidget,
                                 QLineEdit, QSpinBox, QSplitter, QDialog, QTextBrowser,
                                 QMessageBox)
    from PyQt5.QtCore import Qt, QTimer
    from PyQt5.QtGui import QColor, QFont, QPainter, QPen
    log("[图形] 启动 PyQt5 窗口...")

    # ===== 风格12: 纸质纹理(宋体) — 仿报纸/古籍排版风格 =====
    C = type('C', (), {
        # ---- 底色系 (暖调米黄纸张质感) ----
        'WIN_BG':         '#FDFBF7',      # 窗口背景(宣纸白)
        'TOP_BAR_BG':     '#E8E0D0',      # 顶栏(暖棕)
        'TOOL_BAR_BG':    '#F4EAD6',      # 工具栏(浅驼色)
        'TABLE_BG':       '#FFFAF0',      # 表格主体(纸白)
        'TABLE_ALT_ROW':  '#F7EFDF',      # 交替行(微黄)
        'GRID_LINE':      '#E1D4BF',      # 网格线(浅褐)
        'HDR_BG':         '#EADFC9',      # 表头(淡牛皮纸)

        # ---- 文字颜色 (深棕墨色系) ----
        'TEXT_DARK':      '#3E2723',      # 主要文字(浓墨)
        'TEXT_MID':       '#6D4C41',      # 次要文字(中墨棕)
        'TEXT_LIGHT':     '#8D6E63',      # 辅助文字(淡褐)

        # ---- 强调色 (传统中国红/绿) ----
        'PRICE_UP':       '#AD1457',      # Put/看跌-胭脂红
        'PRICE_DOWN':     '#33691E',      # Call/看涨-松烟绿
        'STRIKE_TEXT':    '#4E342E',      # 行权价(深棕)

        # ---- ATM平值行 ----
        'ATM_HIGHLIGHT':  '#FFF8E1',      # ATM行(象牙白高亮)
        'ATM_STRIKE_BG':  '#FFE79B',      # ATM行权价列(杏色)
        'ATM_PRICE_TXT':  '#BF360C',      # ATM最新价(朱砂红)
        'ATM_BAND_5_BG':  '#FFF4D6',
        'ATM_BAND_7_BG':  '#FFF4D6',
        'ATM_BAND_9_BG':  '#FFF4D6',

        # ---- Call实值/虚值 (绿色系，区分明显) ----
        'CALL_ITM_BG':    '#F4FBF1',      # 实值Call底(护眼浅绿)
        'CALL_ITM_TXT':   '#1B5E20',      # 实值Call价字(深绿)
        'CALL_ITM_AUX':   '#558B2F',      # 实值Call辅助字段(中绿)
        'CALL_OTM_BG':   '#F5F5DC',       # 虚值Call底(米白)
        'CALL_OTM_TXT':   '#7CB342',      # 虚值Call字(橄榄绿)

        # ---- Put实值/虚值 (红色系，区分明显) ----
        'PUT_ITM_BG':     '#FFF4F7',      # 实值Put底(护眼浅粉)
        'PUT_ITM_TXT':    '#880E4F',      # 实值Put价字(深玫红)
        'PUT_ITM_AUX':    '#C2185B',      # 实值Put辅助字段(中玫红)
        'PUT_OTM_BG':     '#F5F5DC',      # 虚值Put底(米白)
        'PUT_OTM_TXT':    '#A1887F',      # 虚值Put字(灰褐)

        # ---- 成交量Top3 (黄色高亮) ----
        'VOL_1_BG':'#FFF59D','VOL_1_TXT':'#F57F17',
        'VOL_2_BG':'#FFF176','VOL_2_TXT':'#F9A825',
        'VOL_3_BG':'#FFEE58','VOL_3_TXT':'#FBC02D',

        # ---- 交互与状态 ----
        'SEL_ROW_BG':     '#F8EDCE',      # 行权价列(浅米黄)
        'BORDER_COLOR':   '#BCAAA4',      # 边框(灰褐)
        'BTN_NORMAL':     '#EFEBE9',      # 按钮(纸色)
        'TRADING_ON':     '#2E7D32',      # 交易中(翠绿)
        'TRADING_OFF':    '#E65100',      # 收盘(琥珀)

        # ---- 品牌色 ----
        'BRAND':          '#5D4037',      # 标题(咖啡棕)
    })()

    class MiniLineChart(QWidget):
        def __init__(self, title="曲线", parent=None):
            super().__init__(parent)
            self.title = title
            self.values = []
            self.latest_time = ""
            self.setMinimumHeight(90)

        def set_values(self, values, latest_time=""):
            self.values = [float(x) for x in values if isinstance(x, (int, float))]
            self.latest_time = str(latest_time or "")
            self.update()

        def paintEvent(self, event):
            painter = QPainter(self)
            painter.fillRect(self.rect(), QColor(C.TABLE_BG))
            painter.setRenderHint(QPainter.Antialiasing)
            w, h = self.width(), self.height()
            m = 24
            painter.setPen(QPen(QColor(C.GRID_LINE), 1))
            painter.drawRect(6, 6, max(1, w - 12), max(1, h - 12))
            painter.setPen(QColor(C.TEXT_MID))
            painter.drawText(12, 20, self.title)
            vals = self.values
            if len(vals) < 2:
                painter.drawText(12, h // 2 + 8, "暂无曲线数据")
                return
            v_min, v_max = min(vals), max(vals)
            if abs(v_max - v_min) < 1e-9:
                v_max += 1
                v_min -= 1
            zero_y = h - m - (0 - v_min) / (v_max - v_min) * max(1, h - 2 * m)
            if m <= zero_y <= h - m:
                painter.setPen(QPen(QColor(C.BORDER_COLOR), 1, Qt.DashLine))
                painter.drawLine(m, int(zero_y), w - m, int(zero_y))
            pts = []
            for i, v in enumerate(vals):
                x = m + i * max(1, w - 2 * m) / (len(vals) - 1)
                y = h - m - (v - v_min) / (v_max - v_min) * max(1, h - 2 * m)
                pts.append((int(x), int(y)))
            painter.setPen(QPen(QColor(C.TRADING_ON if vals[-1] >= vals[0] else C.PRICE_UP), 2))
            for a, b in zip(pts, pts[1:]):
                painter.drawLine(a[0], a[1], b[0], b[1])
            painter.setPen(QColor(C.TEXT_DARK))
            painter.drawText(w - 170, 20, f"最新 {vals[-1]:.2f}元")
            if self.latest_time:
                painter.setPen(QColor(C.TEXT_LIGHT))
                painter.drawText(w - 220, 36, f"数据 {self.latest_time[-8:]}")

    class PayoffChart(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.points = []
            self.current_price = None
            self.open_underlying_price = None
            self.open_proxy_vix = None
            self.current_proxy_vix = None
            self.open_underlying_prices = []
            self.strikes = []
            self.x_ticks = []
            self.title = "组合到期损益结构"
            self.setMinimumHeight(90)

        def set_strategy(self, strategy):
            self.points = []
            self.current_price = _sim_num((strategy or {}).get("current_price"))
            self.open_underlying_price = _sim_num((strategy or {}).get("open_underlying_price"))
            self.open_proxy_vix = _sim_num((strategy or {}).get("open_proxy_vix"), None)
            self.current_proxy_vix = _sim_num((strategy or {}).get("current_proxy_vix"), None)
            if self.current_proxy_vix is None:
                self.current_proxy_vix = _sim_num((strategy or {}).get("proxy_vix"), None)
            self.open_underlying_prices = []
            self.strikes = []
            self.x_ticks = []
            legs = (strategy or {}).get("legs", [])
            for v in (strategy or {}).get("x_ticks", []):
                tv = _sim_num(v)
                if tv is not None:
                    self.x_ticks.append(tv)
            strikes = []
            for x in legs:
                k = x.get("strike_price") if x.get("strike_price") is not None else x.get("strike")
                k = _sim_num(k)
                if k is not None:
                    strikes.append(k)
                if self.current_price is None:
                    self.current_price = _sim_num(x.get("underlying_price"))
                oup = _sim_num(x.get("open_underlying_price"), None)
                if oup is None:
                    oup = _sim_num(x.get("underlying_price"), None)
                if oup is not None and oup > 0:
                    self.open_underlying_prices.append(oup)
                if self.open_proxy_vix is None:
                    self.open_proxy_vix = _sim_num(x.get("open_proxy_vix"), None)
                if self.open_proxy_vix is None:
                    self.open_proxy_vix = _sim_num(x.get("proxy_vix"), None)
                if self.current_proxy_vix is None:
                    self.current_proxy_vix = _sim_num(x.get("current_proxy_vix"), None)
                if self.current_proxy_vix is None:
                    self.current_proxy_vix = _sim_num(x.get("proxy_vix"), None)
            if self.open_underlying_price is None and self.open_underlying_prices:
                vals = self.open_underlying_prices
                self.open_underlying_price = vals[0]
            if strikes:
                self.strikes = sorted(set(float(x) for x in strikes))
                if not self.x_ticks:
                    self.x_ticks = list(self.strikes)
                else:
                    self.x_ticks = sorted(set(float(x) for x in self.x_ticks))
                tick_diffs = [abs(b - a) for a, b in zip(self.x_ticks, self.x_ticks[1:]) if abs(b - a) > 1e-9]
                tick_step = min(tick_diffs) if tick_diffs else None
                anchors = list(self.strikes)
                if self.current_price is not None:
                    anchors.append(self.current_price)
                if self.open_underlying_price is not None:
                    anchors.append(self.open_underlying_price)
                lo, hi = min(anchors), max(anchors)
                base_span = max(hi - lo, (tick_step or 0) * 2, max(abs(hi), 1) * 0.02)
                margin = max(base_span * 0.25, tick_step or base_span * 0.15, 1)
                x_min, x_max = lo - margin, hi + margin
                grid_xs = [x_min + i * (x_max - x_min) / 96 for i in range(97)]
                ref_prices = []
                if self.current_price is not None:
                    ref_prices.append(self.current_price)
                if self.open_underlying_price is not None:
                    ref_prices.append(self.open_underlying_price)
                xs = sorted(set(round(x, 8) for x in (grid_xs + self.strikes + ref_prices)))
                for s in xs:
                    pnl = 0.0
                    for leg in legs:
                        side = str(leg.get("side") or "BUY").upper()
                        vol = int(_sim_num(leg.get("volume"), 0) or 0)
                        mult = _sim_leg_multiplier(leg)
                        k = leg.get("strike_price") if leg.get("strike_price") is not None else leg.get("strike")
                        k = _sim_num(k, 0) or 0
                        premium = _sim_num(leg.get("open_price"), None)
                        if premium is None:
                            premium = _sim_leg_price(leg, "OPEN")
                        premium = _sim_num(premium, 0) or 0
                        opt_class = str(leg.get("option_class") or "").upper()
                        if opt_class not in ("CALL", "C", "PUT", "P"):
                            opt_sym = str(leg.get("option_sym") or "").upper()
                            opt_class = "CALL" if "-C-" in opt_sym else "PUT" if "-P-" in opt_sym else opt_class
                        opt_payoff = max(s - k, 0) if opt_class in ("CALL", "C") else max(k - s, 0)
                        leg_pnl = opt_payoff - premium if side == "BUY" else premium - opt_payoff
                        pnl += leg_pnl * vol * mult
                    self.points.append((s, pnl))
            self.update()

        def paintEvent(self, event):
            painter = QPainter(self)
            painter.fillRect(self.rect(), QColor(C.TABLE_BG))
            painter.setRenderHint(QPainter.Antialiasing)
            w, h = self.width(), self.height()
            m_left, m_top, m_right, m_bottom = 58, 28, 24, 50
            painter.setPen(QPen(QColor(C.GRID_LINE), 1))
            painter.drawRect(6, 6, max(1, w - 12), max(1, h - 12))
            painter.setPen(QColor(C.TEXT_MID))
            painter.drawText(12, 20, self.title)
            if len(self.points) < 2:
                painter.drawText(12, h // 2 + 8, "点击组合持仓查看损益结构")
                return
            xs = [p[0] for p in self.points]
            ys = [p[1] for p in self.points]
            x_min, x_max = min(xs), max(xs)
            raw_y_min, raw_y_max = min(ys), max(ys)
            y_min, y_max = min(raw_y_min, 0), max(raw_y_max, 0)
            if abs(y_max - y_min) < 1e-9:
                y_max += 1
                y_min -= 1
            y_pad = max((y_max - y_min) * 0.10, 1)
            y_min -= y_pad
            y_max += y_pad
            plot_w = max(1, w - m_left - m_right)
            plot_h = max(1, h - m_top - m_bottom)
            def tx(x):
                return m_left + (x - x_min) / max(1e-9, x_max - x_min) * plot_w
            def ty(y):
                return h - m_bottom - (y - y_min) / (y_max - y_min) * plot_h
            profit_color = "#16833A"
            loss_color = "#D64A4A"
            open_price_color = "#F57C00"
            neutral_color = "#8D7B72"
            zero_y = ty(0)
            if m_top < zero_y < h - m_bottom:
                painter.fillRect(m_left + 1, m_top + 1, plot_w - 1, max(1, int(zero_y - m_top)), QColor("#F4FBF5"))
                painter.fillRect(m_left + 1, int(zero_y), plot_w - 1, max(1, int(h - m_bottom - zero_y)), QColor("#FFF5F4"))
            elif y_min >= 0:
                painter.fillRect(m_left + 1, m_top + 1, plot_w - 1, plot_h - 1, QColor("#F4FBF5"))
            else:
                painter.fillRect(m_left + 1, m_top + 1, plot_w - 1, plot_h - 1, QColor("#FFF5F4"))
            painter.setPen(QPen(QColor(C.GRID_LINE), 1))
            painter.drawRect(m_left, m_top, plot_w, plot_h)
            painter.setPen(QColor(C.TEXT_LIGHT))
            painter.drawText(12, m_top - 5, "盈亏(元)")
            y_ticks = [raw_y_min, 0, raw_y_max]
            y_ticks = sorted(set(round(x, 6) for x in y_ticks))
            for yv in y_ticks:
                py = int(ty(yv))
                if py < m_top or py > h - m_bottom:
                    continue
                painter.setPen(QPen(QColor(neutral_color if abs(yv) < 1e-9 else C.GRID_LINE), 1, Qt.DashLine if abs(yv) < 1e-9 else Qt.DotLine))
                painter.drawLine(m_left, py, w - m_right, py)
                painter.setPen(QColor(profit_color if yv > 0 else loss_color if yv < 0 else neutral_color))
                painter.drawLine(m_left - 4, py, m_left, py)
                painter.drawText(4, py + 4, f"{yv:.0f}")
            axis_ticks = [x for x in self.x_ticks if x_min <= x <= x_max]
            if len(axis_ticks) <= 2 and len(self.strikes) >= 2:
                diffs = [abs(b - a) for a, b in zip(self.strikes, self.strikes[1:]) if abs(b - a) > 1e-9]
                step = min(diffs) if diffs else max((x_max - x_min) / 4, 1)
                base = self.strikes[0]
                tick = base
                while tick - step >= x_min:
                    tick -= step
                while tick < x_min:
                    tick += step
                axis_ticks = []
                while tick <= x_max + step * 0.1:
                    axis_ticks.append(tick)
                    tick += step
            axis_ticks = sorted(set(round(x, 8) for x in (axis_ticks + self.strikes)))
            strike_label_set = {round(x, 8) for x in self.strikes}
            max_label_count = max(3, int(plot_w // 78))
            label_gap = max(1, (len(axis_ticks) + max_label_count - 1) // max_label_count)
            for i, xv in enumerate(axis_ticks):
                px = int(tx(xv))
                painter.setPen(QColor(C.TEXT_LIGHT))
                painter.drawLine(px, h - m_bottom, px, h - m_bottom + 4)
                force_label = round(xv, 8) in strike_label_set
                if i % label_gap == 0 or force_label:
                    label = f"K{xv:.0f}" if force_label else f"{xv:.0f}"
                    painter.setPen(QColor(C.BRAND if force_label else C.TEXT_LIGHT))
                    painter.drawText(px - max(18, len(label) * 4), h - m_bottom + (18 if not force_label else 32), label)
            def draw_payoff_segment(x1, y1, x2, y2, color):
                painter.setPen(QPen(QColor(color), 2))
                painter.drawLine(int(tx(x1)), int(ty(y1)), int(tx(x2)), int(ty(y2)))
            for (x1, y1), (x2, y2) in zip(self.points, self.points[1:]):
                if y1 == 0 and y2 == 0:
                    draw_payoff_segment(x1, y1, x2, y2, C.BRAND)
                elif y1 >= 0 and y2 >= 0:
                    draw_payoff_segment(x1, y1, x2, y2, profit_color)
                elif y1 <= 0 and y2 <= 0:
                    draw_payoff_segment(x1, y1, x2, y2, loss_color)
                else:
                    zx = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
                    draw_payoff_segment(x1, y1, zx, 0, profit_color if y1 > 0 else loss_color)
                    draw_payoff_segment(zx, 0, x2, y2, profit_color if y2 > 0 else loss_color)
            painter.setPen(QColor(C.TEXT_MID))
            def fmt_vix(v):
                try:
                    return f"{float(v):.2f}%"
                except (TypeError, ValueError):
                    return "-"
            def axis_marker(x, top_text, bottom_text, color, row=0, style=Qt.DotLine, label=True):
                if x is None or not (x_min <= x <= x_max):
                    return False
                px = int(tx(x))
                painter.setPen(QPen(QColor(color), 1, style))
                painter.drawLine(px, m_top, px, h - m_bottom)
                painter.setPen(QPen(QColor(color), 1))
                painter.drawLine(px, h - m_bottom - 4, px, h - m_bottom + 4)
                if label:
                    for text, ly in ((top_text, m_top + 14 + row * 13), (bottom_text, h - 6 - row * 13)):
                        if not text:
                            continue
                        label_w = max(34, len(text) * 8)
                        lx = min(max(m_left, px - label_w // 2), max(m_left, w - m_right - label_w))
                        painter.drawText(lx, ly, text)
                return True
            marked = False
            same_ref_price = (self.open_underlying_price is not None and self.current_price is not None
                              and abs(self.current_price - self.open_underlying_price) <= 1e-6)
            if same_ref_price:
                vix_parts = []
                if self.open_proxy_vix is not None:
                    vix_parts.append(f"开仓VIX {fmt_vix(self.open_proxy_vix)}")
                if self.current_proxy_vix is not None:
                    vix_parts.append(f"现VIX {fmt_vix(self.current_proxy_vix)}")
                top_text = " / ".join(vix_parts)
                marked = axis_marker(self.current_price, top_text, f"建仓/现价 {self.current_price:.0f}", C.BRAND, 0, Qt.DashLine) or marked
            else:
                if self.open_underlying_price is not None:
                    marked = axis_marker(self.open_underlying_price, f"开仓VIX {fmt_vix(self.open_proxy_vix)}", f"建仓期货价 {self.open_underlying_price:.0f}", open_price_color, 0, Qt.DashLine) or marked
                if self.current_price is not None:
                    marked = axis_marker(self.current_price, f"现VIX {fmt_vix(self.current_proxy_vix)}", f"期货现价 {self.current_price:.0f}", C.BRAND, 1, Qt.DashLine) or marked
            left_x, left_y = self.points[0]
            right_x, right_y = self.points[-1]
            def endpoint_label(px_anchor, py_anchor, text, color, align_right):
                painter.setPen(QColor(color))
                tw = max(40, len(text) * 8)
                if align_right:
                    lx = min(w - m_right - tw - 2, int(px_anchor) + 4)
                else:
                    lx = max(m_left + 2, int(px_anchor) - tw - 4)
                ly = max(m_top + 12, min(h - m_bottom - 4, int(py_anchor) - 6))
                painter.drawText(lx, ly, text)
            endpoint_label(tx(left_x), ty(left_y), f"左端 {left_y:+.0f}", profit_color if left_y >= 0 else loss_color, align_right=True)
            endpoint_label(tx(right_x), ty(right_y), f"右端 {right_y:+.0f}", profit_color if right_y >= 0 else loss_color, align_right=False)
            if not marked:
                painter.drawText(12, h - 10, f"K区间 {x_min:.0f}-{x_max:.0f}")

    # ========================================
    #  比例价差信号 - 弹窗 (非模态, 可堆叠)
    # ========================================
    class SignalAlertDialog(QDialog):
        """非模态弹窗 - 显示一条新触发的比例价差信号 + 公式。"""
        _open_dialogs = []   # 类级注册表, 控制屏幕错位与生命周期

        def __init__(self, sig, on_open_panel=None, parent=None):
            super().__init__(parent)
            self.sig = sig
            self._on_open_panel = on_open_panel
            self.setWindowFlags(Qt.Window | Qt.WindowStaysOnTopHint | Qt.WindowCloseButtonHint)
            self.setModal(False)
            self.setAttribute(Qt.WA_DeleteOnClose, True)
            self.setWindowTitle(f"比例价差信号 [{sig.get('underlying_sym','')} {sig.get('side','')} {sig.get('ratio','')}]")
            self.setMinimumSize(460, 420)
            self._build_ui()
            self._apply_position()
            SignalAlertDialog._open_dialogs.append(self)
            try:
                self.destroyed.connect(lambda *_: SignalAlertDialog._open_dialogs.remove(self) if self in SignalAlertDialog._open_dialogs else None)
            except Exception:
                pass

        def _build_ui(self):
            sig = self.sig
            # 弹窗高度加大以容纳 PayoffChart
            self.setMinimumSize(520, 640)
            layout = QVBoxLayout(self)
            layout.setContentsMargins(14, 12, 14, 12)
            layout.setSpacing(8)
            self.setStyleSheet(f"""
                QDialog {{background:{C.WIN_BG};}}
                QTextBrowser {{background:{C.TABLE_BG};border:1px solid {C.BORDER_COLOR};
                    color:{C.TEXT_DARK};font:13px 'Consolas','SimSun';padding:8px;}}
                QPushButton {{background:{C.BTN_NORMAL};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};
                    border-radius:5px;padding:6px 16px;font:bold 12px 'SimSun';min-width:80px;}}
                QPushButton:hover {{background:{C.HDR_BG};}}
                QPushButton#primaryBtn {{background:{C.BRAND};color:#FDFBF7;border:none;}}
                QPushButton#primaryBtn:hover {{background:#7B5B4C;}}
            """)
            tb = QTextBrowser()
            tb.setOpenExternalLinks(False)
            tb.setHtml(self._build_html(sig))
            tb.setMinimumHeight(320)
            layout.addWidget(tb, 3)

            # 到期损益图区域
            chart_box = QFrame()
            chart_box.setStyleSheet(f"QFrame {{background:{C.TABLE_BG};border:1px solid {C.BORDER_COLOR};border-radius:6px;}}")
            cbl = QVBoxLayout(chart_box)
            cbl.setContentsMargins(8, 6, 8, 8)
            cbl.setSpacing(3)
            ct = QLabel("到期损益结构")
            ct.setStyleSheet(f"color:{C.BRAND};font:bold 12px 'SimSun';")
            cbl.addWidget(ct)
            self.payoff_chart = PayoffChart()
            self.payoff_chart.setMinimumHeight(220)
            try:
                self.payoff_chart.set_strategy(self._build_payoff_strategy(sig))
            except Exception:
                pass
            cbl.addWidget(self.payoff_chart, 1)
            layout.addWidget(chart_box, 2)

            btn_row = QHBoxLayout()
            btn_row.addStretch()
            self.panel_btn = QPushButton("查看面板")
            self.panel_btn.setObjectName("primaryBtn")
            self.panel_btn.clicked.connect(self._open_panel)
            self.close_btn = QPushButton("关闭")
            self.close_btn.clicked.connect(self.close)
            btn_row.addWidget(self.panel_btn)
            btn_row.addWidget(self.close_btn)
            layout.addLayout(btn_row)

        @staticmethod
        def _build_payoff_strategy(sig):
            """把 signal dict 转成 PayoffChart.set_strategy 所需的格式。
            current_price 从快照取实时标的价 (不只是触发时价)。
            """
            ratio_n = int(sig.get("ratio_n") or 2)
            side = (sig.get("side") or "CALL").upper()
            opt_class = "CALL" if side.startswith("C") else "PUT"
            trigger_S = _sim_num(sig.get("underlying_price"), None)
            current_S = trigger_S
            try:
                snap = get_memory_snapshot() or {}
                for pi in snap.get("products", []):
                    if (pi.get("exchange") == sig.get("exchange")
                            and pi.get("product") == sig.get("product")
                            and pi.get("sym") == sig.get("underlying_sym")):
                        v = _sim_num(pi.get("price"), None)
                        if v is not None and v > 0:
                            current_S = v
                        break
            except Exception:
                pass
            buy_leg = {
                "side": "BUY", "volume": 1, "option_class": opt_class,
                "strike_price": sig.get("buy_strike"),
                "open_price": sig.get("buy_price"),
                "option_sym": sig.get("buy_option_sym"),
                "exchange": sig.get("exchange"),
                "product": sig.get("product"),
            }
            sell_leg = {
                "side": "SELL", "volume": ratio_n, "option_class": opt_class,
                "strike_price": sig.get("sell_strike"),
                "open_price": sig.get("sell_price"),
                "option_sym": sig.get("sell_option_sym"),
                "exchange": sig.get("exchange"),
                "product": sig.get("product"),
            }
            return {
                "legs": [buy_leg, sell_leg],
                "current_price": current_S,
                "open_underlying_price": trigger_S,
            }

        def _build_html(self, sig):
            def f(v, nd=2):
                try:
                    return f"{float(v):.{nd}f}"
                except (TypeError, ValueError):
                    return "-"
            def f0(v):
                try:
                    return f"{float(v):.0f}"
                except (TypeError, ValueError):
                    return "-"
            ratio_n = int(sig.get("ratio_n") or 2)
            atm_avg = sig.get("atm_avg") or 0
            threshold_disp = (f"ATM均值 {f(atm_avg)}" if ratio_n == 2
                              else f"ATM均值 {f(atm_avg)}*2")
            buy_p = sig.get("buy_price") or 0
            sell_p = sig.get("sell_price") or 0
            np_calc = sell_p * ratio_n - buy_p * 1
            np_color = "#33691E" if (sig.get("net_premium") or 0) > 0 else "#B71C1C"
            gap_color = "#33691E" if (sig.get("strike_gap") or 0) > (sig.get("threshold") or 0) else "#B71C1C"
            vol_disp = f"{(sig.get('product_volume') or 0)/10000:.1f}万" if (sig.get("product_volume") or 0) >= 10000 else f"{sig.get('product_volume') or 0}"
            vix_disp = f(sig.get("proxy_vix"), 2) + "%" if sig.get("proxy_vix") is not None else "-"
            side = sig.get("side") or ""
            buy_label = f"买入 (1张, 接近ATM贵腿)"
            sell_label = f"卖出 ({ratio_n}张, 更虚便宜腿)"

            html = f"""
            <p style='font:bold 14px SimSun;color:{C.BRAND};margin:0 0 4px 0;'>
            比例价差信号 &nbsp;[{sig.get('underlying_sym','')} {side} {sig.get('ratio','')}]
            </p>
            <table cellspacing='0' cellpadding='2' style='font:12px Consolas,SimSun;color:{C.TEXT_DARK};'>
            <tr><td style='color:{C.TEXT_LIGHT};'>触发时间:</td><td><b>{sig.get('trigger_time','-')}</b></td></tr>
            <tr><td style='color:{C.TEXT_LIGHT};'>品种:</td>
                <td>{sig.get('name','-')} &nbsp; {sig.get('underlying_sym','-')} &nbsp;
                    <span style='color:{C.TEXT_LIGHT};'>到期</span> {sig.get('days','-')} 天</td></tr>
            <tr><td style='color:{C.TEXT_LIGHT};'>代理 VIX:</td>
                <td>{vix_disp} &nbsp;
                    <span style='color:{C.TEXT_LIGHT};'>品种成交量:</span> {vol_disp}</td></tr>
            <tr><td style='color:{C.TEXT_LIGHT};'>标的价:</td><td>{f(sig.get('underlying_price'))}</td></tr>
            </table>
            <hr style='border:none;border-top:1px dashed {C.BORDER_COLOR};margin:6px 0;'>
            <p style='margin:2px 0;font:bold 12px SimSun;color:{C.BRAND};'>【腿】</p>
            <table cellspacing='0' cellpadding='2' style='font:12px Consolas,SimSun;color:{C.TEXT_DARK};'>
            <tr><td style='color:{C.TEXT_LIGHT};width:60px;'>买入:</td>
                <td><b>{sig.get('buy_option_sym','-')}</b> × 1 &nbsp;
                    @ ask = <b>{f(buy_p)}</b>
                    <span style='color:{C.TEXT_LIGHT};'>&nbsp;vol={sig.get('buy_volume','-')}</span></td></tr>
            <tr><td style='color:{C.TEXT_LIGHT};'>卖出:</td>
                <td><b>{sig.get('sell_option_sym','-')}</b> × {ratio_n} &nbsp;
                    @ bid = <b>{f(sell_p)}</b>
                    <span style='color:{C.TEXT_LIGHT};'>&nbsp;vol={sig.get('sell_volume','-')}</span></td></tr>
            </table>
            <hr style='border:none;border-top:1px dashed {C.BORDER_COLOR};margin:6px 0;'>
            <p style='margin:2px 0;font:bold 12px SimSun;color:{C.BRAND};'>【条件 A: 净权利金 > 0】</p>
            <p style='margin:2px 0 2px 12px;font:12px Consolas,SimSun;'>
              净权利金 = 卖价 × {ratio_n} − 买价 × 1<br>
              &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;= {f(sell_p)} × {ratio_n} − {f(buy_p)} × 1<br>
              &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;= <b style='color:{np_color};'>{('+' if np_calc>0 else '')}{f(np_calc)}</b>
              &nbsp; <span style='color:{np_color};'>✓ 满足</span>
            </p>
            <p style='margin:2px 0;font:bold 12px SimSun;color:{C.BRAND};'>【条件 B: 行权价间距 > ATM均值 × 倍数】</p>
            <p style='margin:2px 0 2px 12px;font:12px Consolas,SimSun;'>
              ATM均值 = (ATM_CALL.last + ATM_PUT.last) / 2 = <b>{f(atm_avg)}</b><br>
              行权价间距: <b>{f0(sig.get('strike_gap'))}</b>
              &nbsp; > &nbsp; <b>{threshold_disp}</b>
              &nbsp; <span style='color:{gap_color};'>✓ 满足</span>
            </p>
            """
            return html

        def _open_panel(self):
            if self._on_open_panel:
                try:
                    self._on_open_panel()
                except Exception:
                    pass

        def _apply_position(self):
            try:
                screen = QApplication.primaryScreen().availableGeometry()
                w, h = self.width() or 460, self.height() or 420
                base_x = screen.right() - w - 20
                base_y = screen.bottom() - h - 60
                offset = (len(SignalAlertDialog._open_dialogs) % 5) * 30
                self.move(base_x - offset, base_y - offset)
            except Exception:
                pass


    # ========================================
    #  比例价差信号 - 面板 (Tab)
    # ========================================
    class SignalPanelTab(QWidget):
        """信号监控 Tab: 显示当前激活信号 + 已开仓信号 (合并, 满宽展开)
        含 [开仓] 按钮 + 实时浮盈亏列。
        历史信号已拆分到独立 Tab (SignalHistoryTab)。
        """
        # 列: 时间 / 品种 / 两腿(含CALL/PUT) / 阈值 / 触发净权 / 浮盈亏 / 持续 / 状态 / 操作
        LIVE_COLS    = ["时间", "品种", "两腿 (CALL/PUT · 买×1 / 卖×N)", "阈值", "触发净权", "浮盈亏", "持续", "状态", "操作"]
        LIVE_WIDTHS  = [62,     90,     360,                              90,     74,         80,       62,     86,     78]

        def __init__(self, monitor, parent=None, open_handler=None):
            super().__init__(parent)
            self.monitor = monitor
            self._open_handler = open_handler
            self._strategy_pnl_map = {}
            self._strategy_data_map = {}
            self._build_ui()
            self._refresh_timer = QTimer(self)
            self._refresh_timer.timeout.connect(self._refresh)
            self._refresh_timer.start(1000)
            self._pnl_refresh_timer = QTimer(self)
            self._pnl_refresh_timer.timeout.connect(self._refresh_strategy_pnl_map)
            self._pnl_refresh_timer.start(1000)  # 浮盈亏 1 秒一次, 与行同步消失
            QTimer.singleShot(200, self._refresh_strategy_pnl_map)
            QTimer.singleShot(300, self._refresh)

        def set_open_handler(self, fn):
            self._open_handler = fn

        def _build_ui(self):
            outer = QVBoxLayout(self)
            outer.setContentsMargins(8, 6, 8, 8)
            outer.setSpacing(6)
            self.setStyleSheet(f"""
                QLabel {{background:transparent;}}
                QTableWidget {{background:{C.TABLE_BG};color:{C.TEXT_DARK};gridline-color:{C.GRID_LINE};
                    alternate-background-color:{C.TABLE_ALT_ROW};border:1px solid {C.BORDER_COLOR};
                    font:12px 'Consolas','SimSun';selection-background-color:{C.SEL_ROW_BG};selection-color:{C.TEXT_DARK};}}
                QHeaderView::section {{background:{C.HDR_BG};color:{C.TEXT_DARK};border:1px solid {C.GRID_LINE};
                    padding:4px 4px;font:bold 11px 'SimSun';}}
                QPushButton#openBtn {{background:{C.BRAND};color:#FDFBF7;border:none;
                    border-radius:4px;padding:2px 8px;font:bold 11px 'SimSun';}}
                QPushButton#openBtn:hover {{background:#7B5B4C;}}
                QPushButton#openedBtn {{background:#E0E0E0;color:#888;border:1px solid #C0C0C0;
                    border-radius:4px;padding:2px 8px;font:11px 'SimSun';}}
            """)

            head = QHBoxLayout()
            self.title_lbl = QLabel("比例价差信号监控")
            self.title_lbl.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")
            self.hint_lbl = QLabel("仅交易段扫描 · 边沿触发弹窗 · 已开仓信号不再弹窗 · 双击行查看到期损益图")
            self.hint_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:11px 'SimSun';")
            head.addWidget(self.title_lbl); head.addWidget(self.hint_lbl); head.addStretch()
            self.summary_lbl = QLabel("")
            self.summary_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:bold 12px 'Consolas';")
            head.addWidget(self.summary_lbl)
            outer.addLayout(head)

            live_box = QFrame()
            live_box.setStyleSheet(f"QFrame {{background:{C.TABLE_BG};border:1px solid {C.BORDER_COLOR};border-radius:6px;}}")
            lb_layout = QVBoxLayout(live_box)
            lb_layout.setContentsMargins(6, 4, 6, 6)
            lb_layout.setSpacing(3)
            self.live_caption = QLabel("当前激活/已开仓 0")
            self.live_caption.setStyleSheet(f"color:{C.BRAND};font:bold 12px 'SimSun';padding:2px 4px;")
            lb_layout.addWidget(self.live_caption)
            self.live_table = QTableWidget()
            self.live_table.setColumnCount(len(self.LIVE_COLS))
            self.live_table.setHorizontalHeaderLabels(self.LIVE_COLS)
            self.live_table.verticalHeader().setVisible(False)
            self.live_table.verticalHeader().setDefaultSectionSize(28)
            self.live_table.setSelectionBehavior(QAbstractItemView.SelectRows)
            self.live_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            self.live_table.setAlternatingRowColors(True)
            self.live_table.horizontalHeader().setStretchLastSection(True)
            for ci, w in enumerate(self.LIVE_WIDTHS):
                self.live_table.setColumnWidth(ci, w)
            self.live_table.cellDoubleClicked.connect(self._on_live_double_click)
            lb_layout.addWidget(self.live_table, 1)
            outer.addWidget(live_box, 1)

        # ============ 数据来源 ============
        def _refresh_strategy_pnl_map(self):
            """每 3 秒查 sim_list_strategies 拿最新 unrealized_pnl, 用于浮盈亏列"""
            try:
                rows = sim_list_strategies(save_snapshots=False)
                self._strategy_pnl_map = {r["strategy_id"]: r.get("unrealized_pnl") for r in rows}
                self._strategy_data_map = {r["strategy_id"]: r for r in rows}
            except Exception as e:
                log(f"[信号监控] 浮盈亏刷新失败: {_clean_err_msg(e, 80)}")

        def _build_live_rows(self):
            """合并当前激活 + 已开仓信号. 已开仓信号从 signal_log 读取 (含已 EXPIRED 但 OPENED 的)"""
            rows = []
            try:
                rows.extend(self.monitor.get_active_signals())
            except Exception:
                pass
            try:
                # 已开仓信号: 关联 sim_strategy 仍 OPEN
                conn = _db_connect(row_factory=True, kind="sim")
                cur = conn.cursor()
                cur.execute("""SELECT s.* FROM signal_log s
                               LEFT JOIN sim_strategy st ON st.strategy_id = s.opened_strategy_id
                               WHERE s.status='OPENED' AND st.status='OPEN'
                               ORDER BY s.opened_ts DESC""")
                opened = [dict(r) for r in cur.fetchall()]
                conn.close()
                # 标记 opened 标志, 字段补齐(供下方公用渲染)
                for o in opened:
                    o["_is_opened"] = True
                    o["status"] = "OPENED"
                rows.extend(opened)
            except Exception as e:
                log(f"[信号监控] 读取已开仓信号失败: {_clean_err_msg(e, 80)}")
            # 排序: (交易所, 品种, CALL→PUT, 行权价高→低, 触发时间新→旧)
            #   - side='C' 字典序在 'P' 之前, 实现 CALL→PUT
            #   - sell_strike 为主腿行权价，取负号实现高→低
            def _sig_strike(s):
                return -float(s.get("sell_strike") or s.get("buy_strike") or 0)
            rows.sort(key=lambda s: (
                str(s.get("exchange") or ""),
                str(s.get("product") or ""),
                str(s.get("side") or ""),
                _sig_strike(s),
                -(s.get("trigger_ts") or 0),
            ))
            return rows

        # ============ 刷新 ============
        def _refresh(self):
            try:
                self.monitor.sync_opened_from_strategies()
                self._refresh_live()
            except Exception:
                pass

        def _refresh_live(self):
            rows = self._build_live_rows()
            self.live_caption.setText(f"当前激活/已开仓 {len(rows)}")
            self.live_table.setRowCount(len(rows))
            now_ts = time.time()
            n_active = sum(1 for r in rows if not r.get("_is_opened"))
            n_opened = sum(1 for r in rows if r.get("_is_opened"))
            self.summary_lbl.setText(f"激活 {n_active} · 已开仓 {n_opened}")
            for i, r in enumerate(rows):
                self._fill_live_row(i, r, now_ts)

        # ============ 公用格式化 ============
        @staticmethod
        def _legs_text(r, full=False):
            """两腿展示, 内嵌 CALL/PUT 标识
            示例: 'CALL 1:2  买K3050@18.50 / 卖K3100@9.50×2'
            """
            ratio_n = int(r.get("ratio_n") or 2)
            side = (r.get("side") or "").upper()
            cls_tag = "CALL" if side.startswith("C") else ("PUT" if side.startswith("P") else side)
            ratio_tag = r.get("ratio") or f"1:{ratio_n}"
            bk = r.get("buy_strike"); sk = r.get("sell_strike")
            bp = r.get("buy_price"); sp = r.get("sell_price")
            def fk(v): return f"{v:g}" if isinstance(v, (int, float)) else "-"
            def fp(v): return f"{v:.2f}" if isinstance(v, (int, float)) else "-"
            if full:
                return f"{cls_tag} {ratio_tag}    买 K{fk(bk)} @ {fp(bp)} × 1   |   卖 K{fk(sk)} @ {fp(sp)} × {ratio_n}"
            return f"{cls_tag} {ratio_tag}  买K{fk(bk)}@{fp(bp)} / 卖K{fk(sk)}@{fp(sp)}×{ratio_n}"

        @staticmethod
        def _threshold_text(r):
            gap = r.get("strike_gap"); thr = r.get("threshold")
            ratio_n = int(r.get("ratio_n") or 2)
            atm = r.get("atm_avg")
            atm_disp = (f"{atm:.1f}" if ratio_n == 2 else f"{atm:.1f}*2") if isinstance(atm, (int, float)) else "-"
            if isinstance(gap, (int, float)):
                return f"{gap:.0f}>{atm_disp}"
            return "-"

        @staticmethod
        def _status_text(status, is_opened=False):
            if is_opened or status == "OPENED":
                return "🟡 已开仓"
            if status == "ACTIVE":
                return "🟢 激活"
            if status == "EXPIRED":
                return "🔘 已失效"
            return str(status or "-")

        # ============ 表行填充 ============
        def _fill_live_row(self, row, s, now_ts):
            is_opened = bool(s.get("_is_opened"))
            trigger_np = s.get("net_premium")
            trigger_np_text = f"{trigger_np:+.2f}" if isinstance(trigger_np, (int, float)) else "-"
            dur_ref = s.get("opened_ts") if is_opened else s.get("trigger_ts")
            dur_sec = int(now_ts - (dur_ref or now_ts))
            if dur_sec < 3600:
                dur_text = f"{dur_sec//60}'{dur_sec%60:02d}\""
            else:
                dur_text = f"{dur_sec//3600}h{(dur_sec%3600)//60}m"
            trig_short = (s.get("trigger_time") or "")[-8:]
            prod_label = f"{s.get('name','-')} {s.get('underlying_sym','-')}"
            if is_opened:
                sid = s.get("opened_strategy_id")
                pnl = self._strategy_pnl_map.get(sid)
                pnl_text = f"{pnl:+.0f}" if isinstance(pnl, (int, float)) else "..."
            else:
                pnl = None; pnl_text = "—"
            values = [
                trig_short, prod_label, self._legs_text(s),
                self._threshold_text(s),
                trigger_np_text, pnl_text, dur_text,
                self._status_text(s.get("status"), is_opened), "",
            ]
            for ci, v in enumerate(values):
                it = QTableWidgetItem(str(v))
                # 两腿列(ci=2) 左对齐, 其余居中
                it.setTextAlignment(Qt.AlignVCenter | (Qt.AlignLeft if ci == 2 else Qt.AlignCenter))
                # 触发净权
                if ci == 4 and isinstance(trigger_np, (int, float)):
                    it.setForeground(QColor("#33691E" if trigger_np > 0 else "#B71C1C"))
                    f = QFont(); f.setBold(True); it.setFont(f)
                # 浮盈亏
                if ci == 5 and isinstance(pnl, (int, float)):
                    it.setForeground(QColor("#33691E" if pnl >= 0 else "#B71C1C"))
                    f = QFont(); f.setBold(True); it.setFont(f)
                self.live_table.setItem(row, ci, it)
            # 操作列: [开仓] / [已开仓]
            btn = QPushButton("已开仓" if is_opened else "开仓")
            btn.setObjectName("openedBtn" if is_opened else "openBtn")
            btn.setCursor(Qt.PointingHandCursor)
            btn.setFixedHeight(22)
            if is_opened:
                btn.setEnabled(False)
            else:
                btn.clicked.connect(lambda _, sig=s: self._on_open_clicked(sig))
            self.live_table.setCellWidget(row, len(self.LIVE_COLS) - 1, btn)

        # ============ 交互 ============
        def _on_open_clicked(self, sig):
            if self._open_handler is None:
                QMessageBox.information(self.window(), "未连接", "开仓回调未注册")
                return
            try:
                self._open_handler(sig)
                # 立刻刷新, 反映新开仓
                self._refresh_strategy_pnl_map()
                self._refresh_live()
            except Exception as e:
                QMessageBox.warning(self.window(), "开仓失败", f"{_clean_err_msg(e, 200)}")

        def _on_live_double_click(self, row, col):
            try:
                rows = self._build_live_rows()
                if 0 <= row < len(rows):
                    s = rows[row]
                    dlg = SignalAlertDialog(s, on_open_panel=None, parent=self.window())
                    dlg.show()
            except Exception:
                pass


    # ========================================
    #  比例价差信号 - 历史 Tab
    # ========================================
    class SignalHistoryTab(QWidget):
        """历史信号 Tab: 显示今日所有触发过的信号 (ACTIVE/EXPIRED/OPENED)
        与「信号监控」Tab 并列, 满宽展开, 列更详细。
        """
        HIST_COLS   = ["触发时间", "品种", "两腿 (CALL/PUT · 买×1 / 卖×N)", "阈值", "触发净权", "状态", "持续/失效", "关联组合"]
        HIST_WIDTHS = [142,        90,     360,                              90,     74,         86,     130,         140]

        def __init__(self, monitor, parent=None):
            super().__init__(parent)
            self.monitor = monitor
            self._build_ui()
            self._refresh_timer = QTimer(self)
            self._refresh_timer.timeout.connect(self._refresh)
            self._refresh_timer.start(3000)  # 3 秒刷新, 历史变化不快
            QTimer.singleShot(300, self._refresh)

        def _build_ui(self):
            outer = QVBoxLayout(self)
            outer.setContentsMargins(8, 6, 8, 8)
            outer.setSpacing(6)
            self.setStyleSheet(f"""
                QLabel {{background:transparent;}}
                QTableWidget {{background:{C.TABLE_BG};color:{C.TEXT_DARK};gridline-color:{C.GRID_LINE};
                    alternate-background-color:{C.TABLE_ALT_ROW};border:1px solid {C.BORDER_COLOR};
                    font:12px 'Consolas','SimSun';selection-background-color:{C.SEL_ROW_BG};selection-color:{C.TEXT_DARK};}}
                QHeaderView::section {{background:{C.HDR_BG};color:{C.TEXT_DARK};border:1px solid {C.GRID_LINE};
                    padding:4px 4px;font:bold 11px 'SimSun';}}
            """)
            head = QHBoxLayout()
            self.title_lbl = QLabel("历史信号")
            self.title_lbl.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")
            self.hint_lbl = QLabel("今日所有触发过的信号 · 双击查看到期损益图")
            self.hint_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:11px 'SimSun';")
            head.addWidget(self.title_lbl); head.addWidget(self.hint_lbl); head.addStretch()
            self.summary_lbl = QLabel("")
            self.summary_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:bold 12px 'Consolas';")
            head.addWidget(self.summary_lbl)
            self.refresh_btn = QPushButton("刷新")
            self.refresh_btn.setCursor(Qt.PointingHandCursor)
            self.refresh_btn.setFixedHeight(22)
            self.refresh_btn.setStyleSheet(f"""
                QPushButton {{background:{C.BTN_NORMAL};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};
                    border-radius:4px;padding:0 12px;font:11px 'SimSun';}}
                QPushButton:hover {{background:{C.HDR_BG};}}
            """)
            self.refresh_btn.clicked.connect(self._refresh)
            head.addWidget(self.refresh_btn)
            outer.addLayout(head)

            box = QFrame()
            box.setStyleSheet(f"QFrame {{background:{C.TABLE_BG};border:1px solid {C.BORDER_COLOR};border-radius:6px;}}")
            bl = QVBoxLayout(box)
            bl.setContentsMargins(6, 4, 6, 6); bl.setSpacing(3)
            self.caption = QLabel("今日历史 0")
            self.caption.setStyleSheet(f"color:{C.BRAND};font:bold 12px 'SimSun';padding:2px 4px;")
            bl.addWidget(self.caption)
            self.table = QTableWidget()
            self.table.setColumnCount(len(self.HIST_COLS))
            self.table.setHorizontalHeaderLabels(self.HIST_COLS)
            self.table.verticalHeader().setVisible(False)
            self.table.verticalHeader().setDefaultSectionSize(26)
            self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
            self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            self.table.setAlternatingRowColors(True)
            self.table.horizontalHeader().setStretchLastSection(True)
            for ci, w in enumerate(self.HIST_WIDTHS):
                self.table.setColumnWidth(ci, w)
            self.table.cellDoubleClicked.connect(self._on_double_click)
            bl.addWidget(self.table, 1)
            outer.addWidget(box, 1)

        def _refresh(self):
            try:
                rows = self.monitor.get_today_history(_current_trade_date())
            except Exception:
                rows = []
            self.caption.setText(f"今日历史 {len(rows)}")
            n_a = sum(1 for r in rows if r.get("status") == "ACTIVE")
            n_e = sum(1 for r in rows if r.get("status") == "EXPIRED")
            n_o = sum(1 for r in rows if r.get("status") == "OPENED")
            self.summary_lbl.setText(f"激活 {n_a} · 已开仓 {n_o} · 已失效 {n_e}")
            self.table.setRowCount(len(rows))
            for i, r in enumerate(rows):
                self._fill_row(i, r)

        def _fill_row(self, row, r):
            status = r.get("status") or "-"
            is_opened = (status == "OPENED")
            is_expired = (status == "EXPIRED")
            trigger_np = r.get("net_premium")
            trig_np_text = f"{trigger_np:+.2f}" if isinstance(trigger_np, (int, float)) else "-"
            prod_label = f"{r.get('name','-')} {r.get('underlying_sym','-')}"
            # 持续/失效信息
            if is_expired and r.get("expired_ts"):
                try:
                    expired_time = datetime.fromtimestamp(float(r.get("expired_ts"))).strftime("%H:%M:%S")
                    dur_sec = int(float(r.get("expired_ts")) - float(r.get("trigger_ts") or 0))
                    dur_text = f"失效 {expired_time} ({dur_sec//60}'{dur_sec%60:02d}\")"
                except Exception:
                    dur_text = "-"
            elif is_opened and r.get("opened_ts"):
                try:
                    opened_time = datetime.fromtimestamp(float(r.get("opened_ts"))).strftime("%H:%M:%S")
                    dur_text = f"开仓 {opened_time}"
                except Exception:
                    dur_text = "-"
            elif r.get("last_seen_ts") and r.get("trigger_ts"):
                try:
                    dur_sec = int(float(r.get("last_seen_ts")) - float(r.get("trigger_ts")))
                    dur_text = f"已持续 {dur_sec//60}'{dur_sec%60:02d}\""
                except Exception:
                    dur_text = "-"
            else:
                dur_text = "-"
            related = r.get("opened_strategy_id") or "-"
            values = [
                r.get("trigger_time") or "-",
                prod_label,
                SignalPanelTab._legs_text(r),
                SignalPanelTab._threshold_text(r),
                trig_np_text,
                SignalPanelTab._status_text(status, is_opened),
                dur_text, related,
            ]
            for ci, v in enumerate(values):
                it = QTableWidgetItem(str(v))
                # 两腿列(ci=2) 左对齐, 其余居中
                it.setTextAlignment(Qt.AlignVCenter | (Qt.AlignLeft if ci == 2 else Qt.AlignCenter))
                if is_expired:
                    it.setForeground(QColor(C.TEXT_LIGHT))
                # 触发净权
                if ci == 4 and isinstance(trigger_np, (int, float)) and not is_expired:
                    it.setForeground(QColor("#33691E" if trigger_np > 0 else "#B71C1C"))
                    f = QFont(); f.setBold(True); it.setFont(f)
                self.table.setItem(row, ci, it)

        def _on_double_click(self, row, col):
            try:
                rows = self.monitor.get_today_history(_current_trade_date())
                if 0 <= row < len(rows):
                    dlg = SignalAlertDialog(rows[row], on_open_panel=None, parent=self.window())
                    dlg.show()
            except Exception:
                pass


    class OptionWindow(QMainWindow):
        def __init__(self):
            super().__init__()
            self.setWindowTitle("期权T型报价")
            self.setGeometry(10, 20, 1600, 920)
            self.data = None
            self.current_key = None
            self.vix_rank_rows = []
            self.trade_basket = {}
            self.pnl_history = {}
            self.selected_strategy_id = None
            self._latest_strategy_rows = []
            self._sidebar_signature = None
            self._freshness_labels = {}
            self._loading_snapshot = False
            self._pending_snapshot = None
            self._pending_error = None
            self._pending_snapshot_lock = threading.Lock()
            self._last_applied_market_time = None
            self._last_applied_snapshot_signature = None
            self._stable_products_by_key = {}
            # 比例价差信号监控器 (扫描+状态机+持久化); 回调在 _setup_ui 后绑定
            try:
                self.signal_monitor = SignalMonitor()
            except Exception as _e:
                self.signal_monitor = None
                log(f"[信号监控] 实例化失败: {_clean_err_msg(_e, 80)}")
            self.signal_panel = None
            self._setup_ui()
            # 窗口显示后立即加载数据 (用单次定时器确保UI先渲染完成)
            QTimer.singleShot(100, self._load_data)
            # 定时刷新: 1.5 秒一次 (主循环每 1s 更新 _memory_snapshot, GUI 这边及时读)
            # _load_data 内部用 _snapshot_signature 比对, 无变化跳过 _apply_data, 不会过度 CPU
            self._timer = QTimer(self)
            self._timer.timeout.connect(self._load_data)
            self._timer.start(1500)
            self._snapshot_timer = QTimer(self)
            self._snapshot_timer.timeout.connect(self._apply_pending_snapshot)
            self._snapshot_timer.start(200)
            self._collector_timer = QTimer(self)
            self._collector_timer.timeout.connect(self._close_if_collector_stopped)
            self._collector_timer.start(1000)
            # 每秒刷新左侧栏「Xs前」状态文本 (不重建 sidebar, 仅 setText)
            self._freshness_timer = QTimer(self)
            self._freshness_timer.timeout.connect(self._refresh_freshness_labels)
            self._freshness_timer.start(1000)
            # 比例价差信号监控: 每 1 秒扫描一次, 边沿触发弹窗 + 提示音 + 写库
            if self.signal_monitor is not None:
                try:
                    self.signal_monitor.set_alert_callback(self._on_new_signal_triggered)
                    self._signal_scan_timer = QTimer(self)
                    self._signal_scan_timer.timeout.connect(self._scan_signal_monitor)
                    self._signal_scan_timer.start(1000)
                except Exception as _e:
                    log(f"[信号监控] 启动定时器失败: {_clean_err_msg(_e, 80)}")
            log("[图形] 窗口就绪")

        def _scan_signal_monitor(self):
            try:
                self.signal_monitor.scan_once()
            except Exception as e:
                log(f"[信号监控] 扫描异常: {_clean_err_msg(e, 80)}")

        def _on_new_signal_triggered(self, sig):
            """SignalMonitor 边沿触发回调: 注意此回调在 GUI 主线程 (定时器线程)。"""
            try:
                _play_alert_sound()
            except Exception:
                pass
            try:
                dlg = SignalAlertDialog(sig, on_open_panel=self._focus_signal_panel, parent=self)
                dlg.show()
            except Exception as e:
                log(f"[信号监控] 弹窗异常: {_clean_err_msg(e, 80)}")
            try:
                if hasattr(self, "signal_panel") and self.signal_panel is not None:
                    self.signal_panel._refresh()
            except Exception:
                pass

        def _focus_signal_panel(self):
            """弹窗里点击"查看面板"按钮: 切到信号 Tab"""
            try:
                if hasattr(self, "trade_tab") and hasattr(self, "signal_panel"):
                    idx = self.trade_tab.indexOf(self.signal_panel)
                    if idx >= 0:
                        self.trade_tab.setCurrentIndex(idx)
                self.activateWindow(); self.raise_()
            except Exception:
                pass

        def _open_signal_position(self, sig):
            """信号面板 [开仓] 按钮回调:
            把信号 (买1+卖N) 包装为 sim 组合 legs, 调 sim_open_strategy 写库,
            然后 monitor.mark_opened 把该 signal_key 加入 OPENED (不再弹窗)。
            """
            if not sig:
                return
            if self.signal_monitor and self.signal_monitor.is_opened(sig):
                return
            ratio_n = int(sig.get("ratio_n") or 2)
            opt_class = "CALL" if str(sig.get("side") or "").upper().startswith("C") else "PUT"
            # 从当前快照里拿: vol_mult + 每腿的实时 last/bid/ask (用于减少开仓滑点)
            vol_mult = None
            S_now = _sim_num(sig.get("underlying_price"), None)
            proxy_vix = _sim_num(sig.get("proxy_vix"), None)
            buy_last = _sim_num(sig.get("buy_price"), None)   # fallback = sig 触发时 ask
            sell_last = _sim_num(sig.get("sell_price"), None) # fallback = sig 触发时 bid
            try:
                snap = get_memory_snapshot() or {}
                for pi in snap.get("products", []):
                    if not (pi.get("exchange") == sig.get("exchange")
                            and pi.get("product") == sig.get("product")
                            and pi.get("sym") == sig.get("underlying_sym")):
                        continue
                    vol_mult = pi.get("volume_multiple") or pi.get("multiplier")
                    sv = _sim_num(pi.get("price"), None)
                    if sv is not None and sv > 0:
                        S_now = sv
                    pv = self._contract_atm_vix(pi)
                    if pv is not None:
                        proxy_vix = pv
                    # 拿每腿的实时 last (减少滑点)
                    for rw in pi.get("rows", []) or []:
                        for sd_key in ("c", "p"):
                            opt = rw.get(sd_key)
                            if not opt:
                                continue
                            osym = opt.get("option_sym")
                            lp = _sim_num(opt.get("last"), None)
                            if osym == sig.get("buy_option_sym") and lp is not None and lp > 0:
                                buy_last = lp
                            if osym == sig.get("sell_option_sym") and lp is not None and lp > 0:
                                sell_last = lp
                    break
            except Exception:
                pass
            common = {
                "exchange": sig.get("exchange"),
                "product": sig.get("product"),
                "product_name": sig.get("name"),
                "underlying_sym": sig.get("underlying_sym"),
                "option_class": opt_class,
                "days": sig.get("days"),
                "volume_multiple": vol_mult,
                "underlying_price": S_now,
                "open_underlying_price": S_now,
                "open_proxy_vix": proxy_vix,
                "current_proxy_vix": proxy_vix,
                "proxy_vix": proxy_vix,
            }
            # 说明: 开仓用 last 价 (接近 mid), 避免一开仓即后浮亏含完整 bid-ask spread
            # ask/bid 设为 None 让 _sim_leg_price 回退到 last; sim_list_strategies 后续会从快照重填实时 bid/ask
            buy_leg = dict(common, **{
                "option_sym": sig.get("buy_option_sym"),
                "strike": sig.get("buy_strike"),
                "side": "BUY",
                "volume": 1,
                "last": buy_last,
                "ask": None, "bid": None,
            })
            sell_leg = dict(common, **{
                "option_sym": sig.get("sell_option_sym"),
                "strike": sig.get("sell_strike"),
                "side": "SELL",
                "volume": ratio_n,
                "last": sell_last,
                "ask": None, "bid": None,
            })
            strategy_name = f"信号-{sig.get('underlying_sym','?')} {sig.get('side','?')} {sig.get('ratio','?')}"
            sid = sim_open_strategy(strategy_name, [buy_leg, sell_leg])
            try:
                self.signal_monitor.mark_opened(sig, sid)
            except Exception as e:
                log(f"[信号监控] mark_opened 失败: {_clean_err_msg(e, 80)}")
            # 主面板组合持仓也刷新
            try:
                self._render_positions()
            except Exception:
                pass
            log(f"[信号监控] 已开仓: {strategy_name}  strategy_id={sid}")

        def _close_if_collector_stopped(self):
            if not running:
                try:
                    self._collector_timer.stop()
                except:
                    pass
                log("[图形] 数据采集线程已停止, 正在关闭窗口")
                self.close()

        def _setup_ui(self):
            cw = QWidget()
            self.setCentralWidget(cw)
            main_layout = QVBoxLayout(cw)
            main_layout.setContentsMargins(0, 0, 0, 0)
            main_layout.setSpacing(0)
            cw.setStyleSheet(f"background:{C.WIN_BG}")

            # ════════════════ 顶部信息条 ════════════════
            top_bar = QWidget()
            top_bar.setStyleSheet(
                f"background:{C.TOP_BAR_BG};border-bottom:1px solid {C.BORDER_COLOR};")
            top_bar.setFixedHeight(46)
            h = QHBoxLayout(top_bar)
            h.setContentsMargins(20, 0, 20, 0)
            h.setSpacing(16)

            left_area = QHBoxLayout(); left_area.setSpacing(10)
            self.title_lbl = QLabel("期权T型报价终端")
            self.title_lbl.setStyleSheet(
                f"color:{C.BRAND};font:bold 16px 'SimSun','KaiTi','STSong';background:transparent;")
            self.status_dot = QLabel("\u25CF")
            self.status_dot.setFont(QFont("SimSun", 11)); self.status_dot.setStyleSheet("background:transparent;")
            self.trading_lbl = QLabel("")
            self.trading_lbl.setFont(QFont("SimSun", 12)); self.trading_lbl.setStyleSheet("background:transparent;color:{C.TEXT_MID};")
            left_area.addWidget(self.title_lbl); left_area.addWidget(self.status_dot); left_area.addWidget(self.trading_lbl)
            h.addLayout(left_area); h.addStretch(50)

            right_area = QHBoxLayout(); right_area.setSpacing(18)
            self.count_lbl = QLabel("")
            self.count_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:12px 'SimSun';background:transparent;")
            self.ft_lbl = QLabel("")
            self.ft_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:12px 'SimSun',Consolas;background:transparent;")
            sep = QFrame(); sep.setFrameShape(QFrame.VLine); sep.setStyleSheet(f"color:{C.GRID_LINE};max-height:16px;margin:0 4px;")
            self.clock_lbl = QLabel("--:--:--")
            self.clock_lbl.setStyleSheet(f"color:{C.TEXT_DARK};font:14px 'SimSun';font-weight:bold;background:transparent;")
            right_area.addWidget(self.count_lbl); right_area.addWidget(self.ft_lbl); right_area.addWidget(sep)
            right_area.addWidget(self.clock_lbl); h.addLayout(right_area)
            main_layout.addWidget(top_bar)

            # ════════════════ 主体: 左侧品种栏 | 右侧表格 ════════════════
            body = QWidget()
            body_layout = QHBoxLayout(body)
            body_layout.setContentsMargins(0, 0, 0, 0)
            body_layout.setSpacing(0)

            # ===== 左侧品种栏 =====
            self.sidebar = QWidget()
            self.sidebar.setFixedWidth(360)
            self.sidebar.setStyleSheet(f"""
                QWidget {{background:{C.HDR_BG};}}
                QScrollArea {{background:{C.HDR_BG};border:none;}}
                QScrollArea > QWidget {{background:{C.HDR_BG};}}
            """)
            sidebar_outer = QVBoxLayout(self.sidebar)
            sidebar_outer.setContentsMargins(0, 0, 0, 0)
            sidebar_outer.setSpacing(0)

            # 右侧: 工具栏 + 可滚动品种卡片列表
            side_content = QWidget()
            side_content_layout = QVBoxLayout(side_content)
            side_content_layout.setContentsMargins(0, 0, 0, 0)
            side_content_layout.setSpacing(0)

            side_toolbar = QWidget()
            side_toolbar.setStyleSheet(f"background:{C.HDR_BG};border-bottom:1px solid {C.GRID_LINE};")
            side_toolbar_layout = QHBoxLayout(side_toolbar)
            side_toolbar_layout.setContentsMargins(8, 6, 8, 6)
            side_toolbar_layout.setSpacing(6)
            side_title = QLabel("品种切换")
            side_title.setStyleSheet(f"color:{C.BRAND};font:bold 13px 'SimSun','KaiTi';background:transparent;")
            self.product_search_edit = QLineEdit()
            self.product_search_edit.setPlaceholderText("搜索品种 / 代码")
            self.product_search_edit.setFixedHeight(24)
            self.product_search_edit.setStyleSheet(
                f"background:{C.TABLE_BG};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};"
                f"border-radius:7px;padding:2px 8px;font:12px 'SimSun';")
            self.product_search_edit.textChanged.connect(self._apply_sidebar_filter)
            self.refresh_btn = QPushButton("刷新")
            self.refresh_btn.clicked.connect(self._load_data)
            self.refresh_btn.setCursor(Qt.PointingHandCursor)
            self.refresh_btn.setFixedHeight(24)
            self.refresh_btn.setStyleSheet(f"""
                QPushButton {{background:{C.BTN_NORMAL};color:{C.TEXT_DARK};
                    border:1px solid {C.BORDER_COLOR};border-radius:7px;
                    font:12px 'SimSun';padding:2px 8px;}}
                QPushButton:hover {{background:{C.ATM_HIGHLIGHT};}}
            """)
            side_toolbar_layout.addWidget(side_title)
            side_toolbar_layout.addWidget(self.product_search_edit, 1)
            side_toolbar_layout.addWidget(self.refresh_btn)
            side_content_layout.addWidget(side_toolbar)

            self.filter_lbl = QLabel("按代理VIX降序  ·  ★=近月即主力")
            self.filter_lbl.setWordWrap(True)
            self.filter_lbl.setStyleSheet(
                f"color:{C.TEXT_MID};font:11px 'SimSun';background:{C.TABLE_ALT_ROW};"
                f"border-bottom:1px solid {C.GRID_LINE};padding:4px 8px;")
            side_content_layout.addWidget(self.filter_lbl)

            self.product_scroll = QScrollArea()
            self.product_scroll.setWidgetResizable(True)
            self.product_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            self.sidebar_widget = QWidget()
            self.sidebar_layout = QVBoxLayout(self.sidebar_widget)
            self.sidebar_layout.setContentsMargins(6, 6, 6, 6)
            self.sidebar_layout.setSpacing(5)
            self.product_scroll.setWidget(self.sidebar_widget)
            side_content_layout.addWidget(self.product_scroll, 1)
            sidebar_outer.addWidget(side_content, 1)

            # 合约按钮容器 (动态填充)
            self.sidebar_btn_group = QButtonGroup(self)
            self.sidebar_btn_group.setExclusive(True)
            self.prod_buttons = []   # 存储(key, btn)映射
            body_layout.addWidget(self.sidebar)

            # ===== 右侧表格区 =====
            right_area = QWidget()
            right_layout = QVBoxLayout(right_area)
            right_layout.setContentsMargins(0, 0, 0, 0)
            right_layout.setSpacing(0)

            self.table_context = QFrame()
            self.table_context.setStyleSheet(f"""
                QFrame {{background:{C.TOOL_BAR_BG};border-bottom:1px solid {C.BORDER_COLOR};}}
                QLabel {{background:transparent;}}
            """)
            context_layout = QHBoxLayout(self.table_context)
            context_layout.setContentsMargins(12, 6, 12, 6)
            context_layout.setSpacing(10)
            self.table_title = QLabel("请选择品种")
            self.table_title.setMinimumWidth(250)
            self.table_title.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun','KaiTi';")
            context_layout.addWidget(self.table_title)
            self.table_metric_labels = {}
            def _add_metric(key, title):
                lbl = QLabel(f"<b>-</b><br><span>{title}</span>")
                lbl.setTextFormat(Qt.RichText)
                lbl.setStyleSheet(
                    f"color:{C.TEXT_LIGHT};font:11px 'SimSun';border-left:1px solid {C.BORDER_COLOR};padding-left:8px;")
                lbl.setMinimumWidth(64)
                self.table_metric_labels[key] = lbl
                context_layout.addWidget(lbl)
            for key, title in (
                ("price", "标的价格"),
                ("vix", "代理VIX"),
                ("fresh", "行情时间"),
                ("volume", "成交量"),
                ("days", "到期剩余"),
                ("cp", "C/P档数"),
            ):
                _add_metric(key, title)
            context_layout.addStretch()
            right_layout.addWidget(self.table_context)

            # ════════════════ T型报价表格 ════════════════
            self.table = QTableWidget()
            self.table.setColumnCount(23); self.table.setRowCount(0)
            self.table.verticalHeader().setVisible(False)
            self.table.verticalHeader().setDefaultSectionSize(30)
            self.table.horizontalHeader().setVisible(True)
            self.table.horizontalHeader().setFixedHeight(30)
            self.table.horizontalHeader().setHighlightSections(False)
            self.table.setSelectionMode(QAbstractItemView.NoSelection)
            self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            self.table.setFocusPolicy(Qt.NoFocus)
            self.table.setWordWrap(True)
            self.table.setTextElideMode(Qt.ElideNone)
            self.table.setShowGrid(True)
            self.table.horizontalHeader().setStretchLastSection(False)
            self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)

            # 列定义: 买C/卖C | Call(9) | Strike(1) | Put(9) | 买P/卖P —— 全中文
            self.call_cols = ["iv_chg_c","iv_c","delta_c","oi_c","volume_c","ask_c","bid_c","chg_c","last_c"]
            self.put_cols  = ["last_p","chg_p","bid_p","ask_p","volume_p","oi_p","delta_p","iv_p","iv_chg_p"]
            self.all_cols = ["buy_c","sell_c"] + self.call_cols + ["strike"] + self.put_cols + ["buy_p","sell_p"]
            self.col_headers = {
                "buy_c":"买C", "sell_c":"卖C", "buy_p":"买P", "sell_p":"卖P",
                "iv_chg_c":"隐波涨跌","iv_c":"隐含波动率","delta_c":"Delta","oi_c":"持仓量","volume_c":"成交量",
                "ask_c":"卖价","bid_c":"买价","chg_c":"涨跌","last_c":"最新价",
                "strike":"行权价",
                "last_p":"最新价","chg_p":"涨跌","bid_p":"买价","ask_p":"卖价","volume_p":"成交量",
                "oi_p":"持仓量","delta_p":"Delta","iv_p":"隐含波动率","iv_chg_p":"隐波涨跌",
            }
            col_w = {}
            for c in self.call_cols: col_w[c] = 72
            for c in self.put_cols:  col_w[c] = 72
            for c in ("buy_c", "sell_c", "buy_p", "sell_p"): col_w[c] = 42
            col_w["strike"] = 82; col_w["last_c"] = col_w["last_p"] = 68
            col_w["volume_c"] = col_w["volume_p"] = 68; col_w["oi_c"] = col_w["oi_p"] = 60
            col_w["delta_c"] = col_w["delta_p"] = 58; col_w["iv_c"] = col_w["iv_p"] = 82
            col_w["iv_chg_c"] = col_w["iv_chg_p"] = 74; col_w["chg_c"] = col_w["chg_p"] = 60
            col_w["bid_c"] = col_w["bid_p"] = col_w["ask_c"] = col_w["ask_p"] = 64

            for ci, ck in enumerate(self.all_cols):
                hdr_item = QTableWidgetItem(self.col_headers.get(ck, ck))
                if ck == "strike":
                    hdr_item.setForeground(QColor(C.STRIKE_TEXT))
                elif ck in self.call_cols or ck == "last_c":
                    hdr_item.setForeground(QColor(C.PRICE_DOWN))
                elif ck in self.put_cols or ck == "last_p":
                    hdr_item.setForeground(QColor(C.PRICE_UP))
                else:
                    hdr_item.setForeground(QColor(C.TEXT_LIGHT))
                hdr_item.setFont(QFont("SimSun", 10, QFont.Bold))
                hdr_item.setTextAlignment(Qt.AlignCenter)
                self.table.setHorizontalHeaderItem(ci, hdr_item)
                self.table.setColumnWidth(ci, col_w.get(ck, 62))

            self._apply_table_style()
            self.table.cellClicked.connect(self._on_table_cell_clicked)
            right_splitter = QSplitter(Qt.Vertical)
            right_splitter.setChildrenCollapsible(False)
            right_splitter.setHandleWidth(6)
            right_splitter.addWidget(self.table)

            bottom_splitter = QSplitter(Qt.Horizontal)
            bottom_splitter.setChildrenCollapsible(False)
            bottom_splitter.setHandleWidth(6)

            self.trade_tab = QTabWidget()
            self.trade_tab.setObjectName("tradeTabs")
            self.trade_tab.setStyleSheet(f"""
                QTabWidget#tradeTabs {{background:{C.TABLE_BG};border:none;}}
                QTabWidget#tradeTabs::pane {{border:1px solid {C.BORDER_COLOR};background:{C.TABLE_BG};top:-1px;}}
                QTabBar::tab {{background:{C.BTN_NORMAL};color:{C.TEXT_MID};border:1px solid {C.BORDER_COLOR};
                    border-bottom:none;border-top-left-radius:7px;border-top-right-radius:7px;
                    min-width:60px;padding:5px 10px;margin-right:3px;font:bold 12px 'SimSun';}}
                QTabBar::tab:selected {{background:{C.TABLE_BG};color:{C.BRAND};border-bottom:2px solid {C.TABLE_BG};}}
                QTabBar::tab:hover {{background:{C.ATM_HIGHLIGHT};}}
                QLabel {{background:transparent;}}
            """)

            basket_panel = QFrame()
            basket_panel.setObjectName("basketPanel")
            basket_panel.setStyleSheet(f"""
                QFrame#basketPanel {{background:{C.TABLE_BG};border:none;}}
                QFrame#tradeToolBar {{background:{C.TABLE_ALT_ROW};border:1px solid {C.GRID_LINE};border-radius:8px;}}
                QLabel {{background:transparent;}}
                QLabel#sectionTitle {{color:{C.BRAND};font:bold 13px 'SimSun';}}
                QLabel#fieldLabel {{color:{C.TEXT_MID};font:12px 'SimSun';}}
                QLabel#summaryChip {{color:{C.TEXT_MID};background:{C.HDR_BG};border:1px solid {C.GRID_LINE};border-radius:9px;padding:3px 8px;font:12px 'SimSun';}}
                QLineEdit {{background:{C.TABLE_BG};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};border-radius:5px;padding:3px 7px;font:12px 'SimSun';}}
                QComboBox,QSpinBox {{background:{C.TABLE_BG};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};border-radius:4px;min-height:22px;font:12px 'SimSun';}}
                QPushButton {{background:{C.BTN_NORMAL};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};border-radius:5px;padding:3px 10px;font:12px 'SimSun';}}
                QPushButton:hover {{background:{C.ATM_HIGHLIGHT};}}
                QPushButton#primaryTradeBtn {{background:{C.BRAND};color:{C.TABLE_BG};font:bold 12px 'SimSun';}}
                QPushButton#dangerTradeBtn {{color:{C.PRICE_UP};}}
                QPushButton#rowActionBtn {{padding:2px 6px;}}
            """)
            basket_layout = QVBoxLayout(basket_panel)
            basket_layout.setContentsMargins(6, 5, 6, 6)
            basket_layout.setSpacing(5)
            basket_bar = QFrame()
            basket_bar.setObjectName("tradeToolBar")
            basket_top = QHBoxLayout(basket_bar)
            basket_top.setContentsMargins(7, 3, 7, 3)
            basket_top.setSpacing(6)
            basket_title = QLabel("组合篮子")
            basket_title.setObjectName("sectionTitle")
            basket_top.addWidget(basket_title)
            name_lbl = QLabel("组合名称")
            name_lbl.setObjectName("fieldLabel")
            basket_top.addWidget(name_lbl)
            self.strategy_name_edit = QLineEdit()
            self.strategy_name_edit.setPlaceholderText("留空则自动生成")
            self.strategy_name_edit.setFixedHeight(26)
            self.strategy_name_edit.setMinimumWidth(130)
            basket_top.addWidget(self.strategy_name_edit, 1)
            self.basket_summary_lbl = QLabel("篮子为空")
            self.basket_summary_lbl.setObjectName("summaryChip")
            basket_top.addWidget(self.basket_summary_lbl)
            self.clear_basket_btn = QPushButton("清空")
            self.open_strategy_btn = QPushButton("组合开仓")
            self.clear_basket_btn.setObjectName("dangerTradeBtn")
            self.open_strategy_btn.setObjectName("primaryTradeBtn")
            self.clear_basket_btn.setFixedHeight(26)
            self.open_strategy_btn.setFixedHeight(26)
            self.clear_basket_btn.clicked.connect(self._clear_basket)
            self.open_strategy_btn.clicked.connect(self._open_strategy_from_basket)
            basket_top.addWidget(self.clear_basket_btn)
            basket_top.addWidget(self.open_strategy_btn)
            basket_layout.addWidget(basket_bar)
            self.basket_table = QTableWidget()
            self.basket_table.setColumnCount(8)
            self.basket_table.setHorizontalHeaderLabels(["方向","手数","合约","C/P","K","参考价","IV","删"])
            self.basket_table.verticalHeader().setVisible(False)
            self.basket_table.verticalHeader().setDefaultSectionSize(30)
            self.basket_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            self.basket_table.setWordWrap(False)
            self.basket_table.setTextElideMode(Qt.ElideRight)
            self.basket_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            self.basket_table.setAlternatingRowColors(True)
            self.basket_table.setFont(QFont("SimSun", 10))
            self.basket_table.horizontalHeader().setFont(QFont("SimSun", 10, QFont.Bold))
            self.basket_table.horizontalHeader().setFixedHeight(32)
            self.basket_table.horizontalHeader().setDefaultAlignment(Qt.AlignCenter)
            self.basket_table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
            for ci, cw in enumerate([56, 48, 170, 42, 62, 70, 58, 42]):
                self.basket_table.setColumnWidth(ci, cw)
            self.basket_table.setStyleSheet(f"""
                QTableWidget {{background:{C.TABLE_BG};alternate-background-color:{C.TABLE_ALT_ROW};gridline-color:{C.GRID_LINE};border:1px solid {C.GRID_LINE};font:10px 'SimSun';selection-background-color:{C.SEL_ROW_BG};}}
                QTableWidget::item {{padding:2px 4px;}}
                QHeaderView::section {{background:{C.HDR_BG};color:{C.TEXT_DARK};border:1px solid {C.GRID_LINE};padding:5px 4px;font:bold 10px 'SimSun';}}
            """)
            basket_layout.addWidget(self.basket_table, 1)
            self.trade_tab.addTab(basket_panel, "组合篮子")

            pos_panel = QFrame()
            pos_panel.setObjectName("positionPanel")
            pos_panel.setStyleSheet(f"""
                QFrame#positionPanel {{background:{C.TABLE_BG};border:none;}}
                QFrame#positionToolBar {{background:{C.TABLE_ALT_ROW};border:1px solid {C.GRID_LINE};border-radius:8px;}}
                QLabel {{background:transparent;}}
                QLabel#sectionTitle {{color:{C.BRAND};font:bold 13px 'SimSun';}}
                QLabel#summaryChip {{color:{C.TEXT_MID};background:{C.HDR_BG};border:1px solid {C.GRID_LINE};border-radius:9px;padding:3px 8px;font:12px 'SimSun';}}
                QPushButton {{background:{C.BTN_NORMAL};color:{C.TEXT_DARK};border:1px solid {C.BORDER_COLOR};border-radius:5px;padding:3px 9px;font:12px 'SimSun';}}
                QPushButton:hover {{background:{C.ATM_HIGHLIGHT};}}
                QPushButton:disabled {{color:{C.TEXT_LIGHT};background:{C.TABLE_ALT_ROW};}}
                QPushButton#rowActionBtn {{padding:2px 6px;}}
            """)
            pos_layout = QVBoxLayout(pos_panel)
            pos_layout.setContentsMargins(6, 5, 6, 6)
            pos_layout.setSpacing(5)
            pos_bar = QFrame()
            pos_bar.setObjectName("positionToolBar")
            pos_top = QHBoxLayout(pos_bar)
            pos_top.setContentsMargins(7, 3, 7, 3)
            pos_top.setSpacing(6)
            pos_title = QLabel("组合持仓")
            pos_title.setObjectName("sectionTitle")
            self.positions_summary_lbl = QLabel("暂无模拟持仓")
            self.positions_summary_lbl.setObjectName("summaryChip")
            self.refresh_positions_btn = QPushButton("刷新持仓")
            self.refresh_positions_btn.setFixedHeight(26)
            self.refresh_positions_btn.clicked.connect(self._render_positions)
            pos_top.addWidget(pos_title)
            pos_top.addWidget(self.positions_summary_lbl)
            pos_top.addStretch()
            pos_top.addWidget(self.refresh_positions_btn)
            pos_layout.addWidget(pos_bar)
            self.positions_table = QTableWidget()
            self.positions_table.setColumnCount(9)
            self.positions_table.setHorizontalHeaderLabels(["状态","组合名","腿数","开仓(元)","浮盈亏(元)","已实现(元)","期权腿","平仓","清除"])
            self.positions_table.verticalHeader().setVisible(False)
            self.positions_table.verticalHeader().setDefaultSectionSize(58)
            self.positions_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            self.positions_table.setWordWrap(True)
            self.positions_table.setTextElideMode(Qt.ElideRight)
            self.positions_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            self.positions_table.setAlternatingRowColors(True)
            self.positions_table.setFont(QFont("SimSun", 10))
            self.positions_table.horizontalHeader().setFont(QFont("SimSun", 10, QFont.Bold))
            self.positions_table.horizontalHeader().setFixedHeight(34)
            self.positions_table.horizontalHeader().setDefaultAlignment(Qt.AlignCenter)
            self.positions_table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
            self.positions_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.Stretch)
            for ci, cw in enumerate([44, 86, 34, 56, 68, 56, 260, 46, 46]):
                self.positions_table.setColumnWidth(ci, cw)
            self.positions_table.setStyleSheet(f"""
                QTableWidget {{background:{C.TABLE_BG};alternate-background-color:{C.TABLE_ALT_ROW};gridline-color:{C.GRID_LINE};border:1px solid {C.GRID_LINE};font:10px 'SimSun';selection-background-color:{C.SEL_ROW_BG};}}
                QTableWidget::item {{padding:2px 4px;}}
                QHeaderView::section {{background:{C.HDR_BG};color:{C.TEXT_DARK};border:1px solid {C.GRID_LINE};padding:5px 4px;font:bold 10px 'SimSun';}}
            """)
            self.positions_table.cellClicked.connect(self._on_position_row_clicked)
            pos_layout.addWidget(self.positions_table, 1)
            self.trade_tab.addTab(pos_panel, "组合持仓")
            # === 比例价差信号监控 Tab ===
            self.signal_history_tab = None
            if self.signal_monitor is not None:
                try:
                    self.signal_panel = SignalPanelTab(
                        self.signal_monitor, parent=self,
                        open_handler=self._open_signal_position,
                    )
                    self.trade_tab.addTab(self.signal_panel, "信号监控")
                except Exception as _e:
                    log(f"[信号监控] 面板注册失败: {_clean_err_msg(_e, 80)}")
                try:
                    self.signal_history_tab = SignalHistoryTab(self.signal_monitor, parent=self)
                    self.trade_tab.addTab(self.signal_history_tab, "历史信号")
                except Exception as _e:
                    log(f"[信号监控] 历史 Tab 注册失败: {_clean_err_msg(_e, 80)}")
            trade_panel = QFrame()
            trade_panel.setObjectName("tradePanel")
            trade_panel.setStyleSheet(f"""
                QFrame#tradePanel {{background:{C.TABLE_BG};border:1px solid {C.BORDER_COLOR};border-radius:10px;}}
                QLabel {{background:transparent;}}
            """)
            trade_panel_layout = QVBoxLayout(trade_panel)
            trade_panel_layout.setContentsMargins(8, 6, 8, 8)
            trade_panel_layout.setSpacing(5)
            trade_head = QHBoxLayout()
            trade_title = QLabel("组合管理区")
            trade_title.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")
            trade_hint = QLabel("组合篮子 / 组合持仓")
            trade_hint.setStyleSheet(f"color:{C.TEXT_MID};font:12px 'SimSun';")
            trade_head.addWidget(trade_title)
            trade_head.addWidget(trade_hint)
            trade_head.addStretch()
            trade_panel_layout.addLayout(trade_head)
            trade_panel_layout.addWidget(self.trade_tab, 1)
            bottom_splitter.addWidget(trade_panel)

            chart_panel = QFrame()
            chart_panel.setObjectName("chartPanel")
            chart_panel.setStyleSheet(f"""
                QFrame#chartPanel {{background:{C.TABLE_BG};border:2px solid {C.BRAND};border-radius:10px;}}
                QFrame#subChartPanel {{background:{C.TABLE_ALT_ROW};border:1px solid {C.BORDER_COLOR};border-radius:8px;}}
                QLabel {{background:transparent;}}
            """)
            chart_layout = QVBoxLayout(chart_panel)
            chart_layout.setContentsMargins(10, 8, 10, 10)
            chart_layout.setSpacing(8)
            chart_top = QHBoxLayout()
            chart_title = QLabel("组合图表区")
            chart_title.setStyleSheet(f"color:{C.BRAND};font:bold 14px 'SimSun';")
            self.chart_hint_lbl = QLabel("选择持仓或构建篮子时显示到期损益结构")
            self.chart_hint_lbl.setStyleSheet(f"color:{C.TEXT_MID};font:12px 'SimSun';")
            chart_top.addWidget(chart_title)
            chart_top.addWidget(self.chart_hint_lbl)
            chart_top.addStretch()
            chart_layout.addLayout(chart_top)
            chart_row = QVBoxLayout()
            chart_row.setSpacing(8)
            self.payoff_chart = PayoffChart()
            self.pnl_chart = MiniLineChart("实时浮盈亏曲线")
            self.payoff_chart.setMinimumHeight(230)
            self.pnl_chart.setMinimumHeight(120)
            payoff_box = QFrame()
            payoff_box.setObjectName("subChartPanel")
            payoff_box_layout = QVBoxLayout(payoff_box)
            payoff_box_layout.setContentsMargins(8, 6, 8, 8)
            payoff_box_layout.setSpacing(4)
            payoff_box_title = QLabel("到期损益图")
            payoff_box_title.setStyleSheet(f"color:{C.BRAND};font:bold 12px 'SimSun';")
            payoff_box_layout.addWidget(payoff_box_title)
            payoff_box_layout.addWidget(self.payoff_chart, 1)
            pnl_box = QFrame()
            pnl_box.setObjectName("subChartPanel")
            pnl_box_layout = QVBoxLayout(pnl_box)
            pnl_box_layout.setContentsMargins(8, 6, 8, 8)
            pnl_box_layout.setSpacing(4)
            self.pnl_box_title = QLabel("实时盈亏曲线 · 选中组合")
            self.pnl_box_title.setStyleSheet(f"color:{C.BRAND};font:bold 12px 'SimSun';")
            pnl_box_layout.addWidget(self.pnl_box_title)
            pnl_box_layout.addWidget(self.pnl_chart, 1)
            chart_row.addWidget(payoff_box, 2)
            chart_row.addWidget(pnl_box, 1)
            chart_layout.addLayout(chart_row, 1)
            bottom_splitter.addWidget(chart_panel)
            bottom_splitter.setSizes([560, 900])

            right_splitter.addWidget(bottom_splitter)
            right_splitter.setSizes([500, 420])
            right_layout.addWidget(right_splitter, 1)

            body_layout.addWidget(right_area, 1)
            main_layout.addWidget(body, 1)

            self._clock_timer = QTimer(self)
            self._clock_timer.timeout.connect(self._update_clock)
            self._clock_timer.start(1000)
            self._update_clock()

        def _apply_table_style(self):
            ss = f"""QTableWidget {{
                background:{C.TABLE_BG};color:{C.TEXT_DARK};gridline-color:{C.GRID_LINE};
                border:none;font:13px 'SimSun','Consolas',serif;
                selection-background-color:{C.SEL_ROW_BG};selection-color:{C.TEXT_DARK};
                alternate-background-color:{C.TABLE_ALT_ROW};}}
                QTableWidget::section {{background:{C.HDR_BG};color:{C.TEXT_MID};
                    border:none;border-right:1px solid {C.GRID_LINE};border-bottom:2px solid {C.BORDER_COLOR};
                    padding:6px 3px;font:bold 11px 'SimSun','KaiTi';}}
                QTableWidget::item {{padding:3px 8px;margin:-1px;}}"""
            self.table.setStyleSheet(ss)

        def _update_clock(self):
            try:
                self.clock_lbl.setText(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            except:
                pass

        def _query_db(self):
            """查询数据库返回结构化数据"""
            return build_snapshot_from_db()

        def _fmt_num(self, v):
            if v is None or v=="-" or v=="": return ""
            if isinstance(v, float):
                if abs(v)>=10000: return f"{v:.0f}"
                if abs(v)>=100: return f"{v:.1f}"
                return f"{v:.2f}"
            return str(v)

        def _get_cell_val(self, data_dict, col_key):
            if not data_dict: return ""
            v = data_dict.get(col_key, "-")
            return self._fmt_num(v) if v != "-" else ""

        def _product_cache_key(self, pi):
            return (pi.get("exchange"), pi.get("product"), pi.get("sym"), pi.get("days_key", int(pi.get("days") or 0)))

        def _merge_stable_products(self, d):
            if not d:
                return d
            if GUI_STALE_PRODUCT_KEEP_SEC <= 0:
                self._stable_products_by_key = {}
                return d
            now_ts = time.time()
            current_keys = set()
            for pi in d.get("products", []) or []:
                if pi.get("rows"):
                    key = self._product_cache_key(pi)
                    current_keys.add(key)
                    self._stable_products_by_key[key] = (pi, now_ts)
            if not self._stable_products_by_key:
                return d
            merged = dict(d)
            products = list(d.get("products", []) or [])
            for key, item in list(self._stable_products_by_key.items()):
                if isinstance(item, tuple) and len(item) == 2:
                    pi, ts = item
                else:
                    pi, ts = item, now_ts
                    self._stable_products_by_key[key] = (pi, ts)
                if now_ts - float(ts or 0) > GUI_STALE_PRODUCT_KEEP_SEC:
                    self._stable_products_by_key.pop(key, None)
                    continue
                if key not in current_keys:
                    products.append(pi)
            merged["products"] = products
            return merged

        def _load_data(self):
            try:
                d = get_memory_snapshot()
                if d is not None:
                    if self._snapshot_signature(d) != self._last_applied_snapshot_signature:
                        self._apply_data(d)
                elif not self._loading_snapshot:
                    self.count_lbl.setText("正在后台加载数据...")
                if d is None:
                    self._start_snapshot_loader()
            except Exception as e:
                self.count_lbl.setText(f"错误:{e}")

        def _apply_data(self, d):
            if d is None:
                self.count_lbl.setText("暂无数据"); return
            raw_sig = self._snapshot_signature(d)
            d = self._merge_stable_products(d)
            self.data = d
            market_times = [p.get("market_time") for p in d.get("products", []) if p.get("market_time")]
            self._last_applied_market_time = max(market_times) if market_times else None
            self._last_applied_snapshot_signature = raw_sig
            self.ft_lbl.setText("行情时间按品种/合约")
            total = sum(len(p['rows'])*2 for p in d['products'])
            stat_text = self._exchange_stats_text(d["products"])
            self.count_lbl.setText(f"合约{total}  月份{len(d['products'])}  订阅 {stat_text}")
            self.filter_lbl.setText("按代理VIX降序  ·  ★=近月即主力")
            
            if d.get('is_trading'):
                self.trading_lbl.setText(" 交易中 "); self.trading_lbl.setStyleSheet(
                    f"color:{C.TRADING_ON};font:bold 12px 'SimSun';background:transparent;")
                self.status_dot.setStyleSheet(f"color:{C.TRADING_ON};font-size:11px;background:transparent;")
            else:
                self.trading_lbl.setText(" 已收盘 "); self.trading_lbl.setStyleSheet(
                    f"color:{C.TRADING_OFF};font:bold 12px 'SimSun';background:transparent;")
                self.status_dot.setStyleSheet(f"color:{C.TEXT_LIGHT};font-size:11px;background:transparent;")

            sig = self._sidebar_signature_full(d["products"])
            if sig != self._sidebar_signature:
                self._build_sidebar(d["products"])
                self._sidebar_signature = sig
            else:
                ts_by_key = {}
                for pi in d["products"]:
                    key = (pi.get("exchange"), pi.get("product"))
                    ts = pi.get("market_ts")
                    old_ts = ts_by_key.get(key)
                    if ts is not None and (old_ts is None or float(ts) > float(old_ts)):
                        ts_by_key[key] = ts
                for key, (lbl, _) in list(self._freshness_labels.items()):
                    if key in ts_by_key:
                        self._freshness_labels[key] = (lbl, ts_by_key[key])
            self._render_table()
            self._render_positions()

        def _start_snapshot_loader(self):
            if self._loading_snapshot:
                return
            self._loading_snapshot = True
            threading.Thread(target=self._snapshot_worker, name="GuiSnapshotLoader", daemon=True).start()

        def _snapshot_worker(self):
            snapshot = None
            err = None
            try:
                snapshot = build_snapshot_from_db()
                if snapshot:
                    set_memory_snapshot(snapshot)
            except Exception as e:
                err = e
            with self._pending_snapshot_lock:
                self._pending_snapshot = snapshot
                self._pending_error = err
            self._loading_snapshot = False

        def _apply_pending_snapshot(self):
            snapshot = None
            err = None
            with self._pending_snapshot_lock:
                if self._pending_snapshot is not None:
                    snapshot = self._pending_snapshot
                    self._pending_snapshot = None
                if self._pending_error is not None:
                    err = self._pending_error
                    self._pending_error = None
            if snapshot is not None:
                if self._snapshot_signature(snapshot) != self._last_applied_snapshot_signature:
                    try:
                        self._apply_data(snapshot)
                    except Exception as e:
                        self.count_lbl.setText(f"刷新失败:{e}")
            elif err is not None and self.data is None:
                self.count_lbl.setText(f"后台加载失败:{err}")

        def _snapshot_signature(self, d):
            products = d.get("products", []) if d else []
            row_count = 0
            iv_count = 0
            exchanges = set()
            for p in products:
                exchanges.add(p.get("exchange"))
                for rw in p.get("rows", []):
                    row_count += 1
                    for side in ("c", "p"):
                        cell = rw.get(side)
                        if cell and cell.get("iv") != "-":
                            iv_count += 1
            market_vals = [safe_float(p.get("market_ts"), None) for p in products]
            market_vals = [x for x in market_vals if x is not None and x > 0]
            return (max(market_vals) if market_vals else None, len(products), row_count, iv_count, tuple(sorted(exchanges)))

        def _products_signature(self, products):
            return tuple(sorted(
                (p.get("exchange"), p.get("product"), p.get("sym"), p.get("days_key", int(p.get("days") or 0)), p.get("ctype"))
                for p in products
            ))

        def _refresh_freshness_labels(self):
            """每秒被调用: 把每个品种 head 上的「Xs前」标签按当前时间更新文本与颜色。"""
            if not self._freshness_labels:
                return
            now_ts = time.time()
            stale_keys = []
            for key, (lbl, ts) in list(self._freshness_labels.items()):
                try:
                    if ts is None:
                        lbl.setText("待更新")
                        lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:9px 'SimSun';background:{C.TABLE_BG};border:1px solid {C.GRID_LINE};border-radius:7px;padding:1px 5px;")
                        continue
                    elapsed = max(0, int(now_ts - float(ts)))
                    if elapsed < 60:
                        text = f"{elapsed}s前"
                    elif elapsed < 3600:
                        text = f"{elapsed // 60}m前"
                    else:
                        text = f"{elapsed // 3600}h前"
                    if elapsed <= 5:
                        color = C.TRADING_ON      # 绿: 新鲜
                    elif elapsed <= 30:
                        color = "#F57C00"         # 黄: 略旧
                    else:
                        color = C.TRADING_OFF     # 红: 明显滞后
                    lbl.setText(text)
                    lbl.setStyleSheet(f"color:{color};font:9px 'SimSun';background:{C.TABLE_BG};border:1px solid {C.GRID_LINE};border-radius:7px;padding:1px 5px;")
                except RuntimeError:
                    # QLabel 已被 Qt 删除 (sidebar 重建竞争), 标记移除
                    stale_keys.append(key)
            for key in stale_keys:
                self._freshness_labels.pop(key, None)

        def _sidebar_signature_full(self, products):
            """左侧栏重建签名: 合约结构 + 品种代理 VIX(保留2位小数)。
            任一变化都触发 _build_sidebar, 解决 IV/VIX 在初始为空、后续到位时左侧不刷新的问题。"""
            structure = self._products_signature(products)
            try:
                vix_rank = self._product_vix_rank(products)
            except Exception:
                vix_rank = []
            vix_sig = tuple(
                (r.get("exchange"), r.get("product"), r.get("name"),
                 None if r.get("vix") is None else round(float(r["vix"]), 2))
                for r in vix_rank
            )
            return (structure, vix_sig)

        def _apply_sidebar_filter(self):
            text = ""
            if hasattr(self, "product_search_edit"):
                text = (self.product_search_edit.text() or "").strip().lower()
            for i in range(self.sidebar_layout.count()):
                w = self.sidebar_layout.itemAt(i).widget()
                if not w:
                    continue
                ft = str(w.property("filter_text") or "").lower()
                if ft:
                    w.setVisible((not text) or text in ft)

        def _exchange_subscription_stats(self, products):
            out = []
            products = products or []
            for ex, _, config in EXCHANGE_PRODUCT_CONFIG:
                success_products = set()
                success_groups = 0
                for pi in products:
                    if pi.get("exchange") != ex:
                        continue
                    prod = pi.get("product")
                    if prod:
                        success_products.add(prod)
                    success_groups += 1
                out.append({
                    "exchange": ex,
                    "success_products": len(success_products),
                    "configured_products": len(config),
                    "success_groups": success_groups,
                })
            return out

        def _exchange_stats_text(self, products):
            parts = []
            for s in self._exchange_subscription_stats(products):
                ex = s["exchange"]
                ok = s["success_products"]
                total = s["configured_products"]
                groups = s["success_groups"]
                text = f"{ex} {ok}/{total}"
                if groups != ok:
                    text += f"({groups}组)"
                parts.append(text)
            return "  ".join(parts)

        def _age_text(self, ts):
            if ts is None:
                return "-"
            try:
                elapsed = max(0, int(time.time() - float(ts)))
            except:
                return "-"
            if elapsed < 60:
                return f"{elapsed}s前"
            if elapsed < 3600:
                return f"{elapsed // 60}m前"
            return f"{elapsed // 3600}h前"

        def _time_text(self, dt_text, ts=None):
            if ts:
                try:
                    return datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S")
                except:
                    pass
            if dt_text:
                s = str(dt_text)
                return s[-8:] if len(s) >= 8 else s
            return "-"

        def _clear_sidebar_layout(self):
            while self.sidebar_layout.count():
                item = self.sidebar_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()

        def _contract_volume(self, pi):
            total = 0
            for rw in pi.get("rows", []):
                if rw.get("c"):
                    total += rw["c"].get("volume", 0) or 0
                if rw.get("p"):
                    total += rw["p"].get("volume", 0) or 0
            return total

        def _contract_counts(self, pi):
            calls, puts = 0, 0
            for rw in pi.get("rows", []):
                if rw.get("c"): calls += 1
                if rw.get("p"): puts += 1
            return calls, puts

        def _fmt_vol_cn(self, v):
            try:
                v = float(v)
                if v >= 10000:
                    return f"{v/10000:.1f}万"
                return f"{v:.0f}"
            except:
                return "-"

        def _safe_vix_num(self, v):
            try:
                if v is None or v == "-" or v == "":
                    return None
                x = float(v)
                if x != x or x <= 0 or x >= 300:
                    return None
                return x
            except:
                return None

        def _contract_atm_vix(self, pi):
            vals = []
            rows = pi.get("rows", [])
            atm_rows = [rw for rw in rows if rw.get("atm")]
            if not atm_rows and rows:
                S = pi.get("price", 0) or 0
                atm_rows = sorted(rows, key=lambda rw: abs((rw.get("K") or 0) - S))[:1]
            for rw in atm_rows:
                for side in ("c", "p"):
                    d = rw.get(side)
                    if not d:
                        continue
                    iv = self._safe_vix_num(d.get("iv"))
                    if iv is not None:
                        vals.append(iv)
            return sum(vals) / len(vals) if vals else None

        def _find_current_proxy_vix(self, exchange, product, underlying_sym=None, days=None):
            if not self.data:
                return None
            candidates = []
            for pi in self.data.get("products", []) or []:
                if exchange and pi.get("exchange") != exchange:
                    continue
                if product and pi.get("product") != product:
                    continue
                if underlying_sym and pi.get("sym") == underlying_sym:
                    candidates = [pi]
                    break
                candidates.append(pi)
            if not candidates:
                return None
            if days is not None:
                try:
                    days_val = int(float(days))
                    candidates = sorted(candidates, key=lambda p: abs(int(p.get("days") or 0) - days_val))
                except Exception:
                    pass
            else:
                candidates = sorted(candidates, key=lambda p: 0 if p.get("ctype") in ("near", "near_main") else 1)
            return self._contract_atm_vix(candidates[0])

        def _atm_band_rows(self, rows, S):
            if not rows or not S or S <= 0:
                return {}
            strikes = [(i, safe_float(rw.get("K"), None)) for i, rw in enumerate(rows)]
            strikes = [(i, k) for i, k in strikes if k is not None and k > 0]
            if not strikes:
                return {}
            out = {}
            for pct in (5, 7, 9):
                for side, sign in (("call", 1), ("put", -1)):
                    target = S * (1 + sign * pct / 100.0)
                    ri, _ = min(strikes, key=lambda x: abs(x[1] - target))
                    row = out.setdefault(ri, {})
                    item = row.setdefault(side, {"labels": [], "level": pct, "direction": "+" if sign > 0 else "-"})
                    label = f"{pct}"
                    if label not in item["labels"]:
                        item["labels"].append(label)
                    item["level"] = max(item["level"], pct)
            return {ri: {side: {"text": "/".join(v["labels"]), "level": v["level"], "direction": v.get("direction", "")} for side, v in sides.items()} for ri, sides in out.items()}

        def _atm_band_mark_text(self, atm_band):
            labels = []
            for side in ("call", "put"):
                info = ((atm_band or {}).get(side) or {})
                text = info.get("text", "")
                direction = info.get("direction", "")
                for label in text.split("/"):
                    if label:
                        mark = f"{direction}{label}%" if direction else f"{label}%"
                        if mark not in labels:
                            labels.append(mark)
            return "/".join(labels)

        def _product_vix_rank(self, products):
            """代理VIX = 近月平值期权 IV 均值 (call+put ATM IV)。
            仅取 ctype 为 near 或 near_main 的合约; 没有近月时退回到到期最短的合约。"""
            from collections import OrderedDict
            groups = OrderedDict()
            for pi in products:
                ex = pi.get("exchange") or ""
                code = pi.get("product") or ""
                name = pi.get("name", code)
                key = (ex, code, name)
                if key not in groups:
                    groups[key] = []
                groups[key].append(pi)
            out = []
            for (ex, code, name), items in groups.items():
                near_pi = next((p for p in items if p.get("ctype") in ("near", "near_main")), None)
                if not near_pi:
                    near_pi = min(items, key=lambda p: p.get("days", 999999) or 999999)
                v = self._contract_atm_vix(near_pi)
                if v is not None:
                    out.append({"exchange": ex, "product": code, "name": name,
                                "vix": v, "n": 1, "sym": near_pi.get("sym")})
            out.sort(key=lambda x: x["vix"], reverse=True)
            return out

        def _exchange_name(self, ex):
            return {"DCE":"大商所", "CZCE":"郑商所", "SHFE":"上期所"}.get(ex, ex or "")

        def _build_sidebar(self, products):
            """构建左侧品种导航: 全市场按代理VIX降序, 品种下挂近月/主力合约按钮"""
            from PyQt5.QtWidgets import QPushButton as QPB
            from collections import OrderedDict

            for _, b in self.prod_buttons:
                self.sidebar_btn_group.removeButton(b)
            self.prod_buttons.clear()
            self._clear_sidebar_layout()
            # 旧 freshness QLabel 已随 sidebar widget 删除, 直接清空注册表
            self._freshness_labels = {}

            groups = OrderedDict()
            for pi in products:
                ex = pi.get("exchange") or (pi.get("sym", "").split(".")[0] if "." in pi.get("sym", "") else "")
                code = pi.get("product") or ""
                name = pi.get("name", code)
                gkey = (ex, code, name)
                if gkey not in groups:
                    groups[gkey] = {"exchange": ex, "product": code, "name": name, "items": []}
                groups[gkey]["items"].append(pi)

            rank_map = {
                (r["exchange"], r["product"], r["name"]): r
                for r in self._product_vix_rank(products)
            }
            order_map = {"DCE": 0, "CZCE": 1, "SHFE": 2}
            for g in groups.values():
                r = rank_map.get((g["exchange"], g["product"], g["name"]))
                g["vix"] = r.get("vix") if r else None
                g["vix_n"] = r.get("n") if r else 0
                market_ts_vals = [safe_float(it.get("market_ts"), None) for it in g["items"]]
                market_ts_vals = [x for x in market_ts_vals if x is not None and x > 0]
                g["market_ts"] = max(market_ts_vals) if market_ts_vals else None
            group_list = sorted(
                groups.values(),
                key=lambda g: (-(g["vix"] if g["vix"] is not None else -1), order_map.get(g["exchange"], 9), g["name"], g["product"])
            )

            section_text = f"全市场品种 {len(group_list)} · 按代理VIX降序 · ★=近月即主力"
            section_lbl = QLabel(section_text)
            section_lbl.setStyleSheet(
                f"color:{C.BRAND};font:bold 12px 'SimSun';background:transparent;padding:2px 4px 4px;")
            self.sidebar_layout.addWidget(section_lbl)

            if not group_list:
                empty_lbl = QLabel("暂无合约数据")
                empty_lbl.setAlignment(Qt.AlignCenter)
                empty_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:12px 'SimSun';background:transparent;padding:28px 4px;")
                self.sidebar_layout.addWidget(empty_lbl)
                self.sidebar_layout.addStretch()
                return

            for g in group_list:
                items = g["items"]
                # 按合约去重 (sym, days)
                seen_keys = set()
                items_dedup = []
                for pi in items:
                    k = (pi.get("sym"), pi.get("days_key", int(pi.get("days") or 0)))
                    if k in seen_keys:
                        continue
                    seen_keys.add(k)
                    items_dedup.append(pi)
                # 只渲染实际抓取到的合约: 优先近月(含 near_main), 其次显式主力
                near_pi = next((p for p in items_dedup if p.get("ctype") in ("near", "near_main")), None)
                if not near_pi and items_dedup:
                    near_pi = min(items_dedup, key=lambda p: p.get("days", 999999) or 999999)
                main_pi = next((p for p in items_dedup if p.get("ctype") == "main" and p is not near_pi), None)

                card = QFrame()
                card.setObjectName("productCard")
                card.setProperty("filter_text", " ".join([
                    str(g.get("name", "")),
                    str(g.get("product", "")),
                    str(g.get("exchange", "")),
                    " ".join(str(it.get("sym", "")) for it in g.get("items", [])),
                ]))
                card.setStyleSheet(f"""
                    QFrame#productCard {{background:{C.TABLE_BG};border:1px solid {C.GRID_LINE};border-radius:8px;}}
                    QLabel {{background:transparent;}}
                """)
                card_lyt = QVBoxLayout(card)
                card_lyt.setContentsMargins(0, 0, 0, 4)
                card_lyt.setSpacing(4)

                head = QWidget()
                head.setStyleSheet(f"background:{C.TABLE_ALT_ROW};border-bottom:1px solid {C.GRID_LINE};border-radius:8px 8px 0 0;")
                head_lyt = QHBoxLayout(head)
                head_lyt.setContentsMargins(7, 5, 7, 5)
                head_lyt.setSpacing(5)
                name_lbl = QLabel(f"{g['name']}期权")
                name_lbl.setStyleSheet(f"color:{C.TEXT_DARK};font:bold 13px 'SimSun';background:transparent;")
                code_lbl = QLabel(g["product"])
                code_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'Consolas','SimSun';background:transparent;")
                vix_lbl = QLabel(f"VIX {g['vix']:.2f}%" if g.get("vix") is not None else "VIX -")
                vix_lbl.setStyleSheet(
                    f"color:{C.PRICE_UP};font:bold 12px 'Consolas','SimSun';background:#FFF2F0;"
                    f"border:1px solid #EFBBB5;border-radius:7px;padding:1px 5px;")
                ex_lbl = QLabel(self._exchange_name(g["exchange"]))
                ex_lbl.setStyleSheet(
                    f"color:{C.TRADING_ON};font:9px 'SimSun';background:#E8F5E9;"
                    f"border:1px solid #C8E6C9;border-radius:7px;padding:1px 5px;")
                fresh_lbl = QLabel("")
                fresh_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:9px 'SimSun';background:{C.TABLE_BG};border:1px solid {C.GRID_LINE};border-radius:7px;padding:1px 5px;")
                self._freshness_labels[(g["exchange"], g["product"])] = (fresh_lbl, g.get("market_ts"))
                head_lyt.addWidget(name_lbl)
                head_lyt.addWidget(code_lbl)
                head_lyt.addWidget(ex_lbl)
                head_lyt.addWidget(fresh_lbl)
                head_lyt.addStretch()
                head_lyt.addWidget(vix_lbl)
                card_lyt.addWidget(head)

                # 严格按 ctype 渲染: 不再因为缺主力数据而虚构按钮
                button_items = []
                if near_pi:
                    if near_pi.get("ctype") == "near_main":
                        button_items.append(("both", "近月合约 (即主力)", near_pi))
                    else:
                        button_items.append(("near", "近月合约", near_pi))
                if main_pi:
                    button_items.append(("main", "主力合约", main_pi))
                if not button_items:
                    continue

                for kind, title, pi in button_items:
                    key = (pi["sym"], pi.get("days_key", int(pi.get("days") or 0)))
                    short_sym = pi["sym"].split(".")[-1] if "." in pi["sym"] else pi["sym"]
                    price = pi.get("price", 0)
                    days = pi.get("days", 0)
                    calls, puts = self._contract_counts(pi)
                    vol_raw = self._contract_volume(pi)
                    vol = self._fmt_vol_cn(vol_raw)
                    low_vol = vol_raw < 10000
                    left_color = "#BF360C" if kind == "both" else (C.TEXT_LIGHT if kind == "near" else "#F57F17")

                    btn = QPB()
                    btn.setCheckable(True)
                    btn.setCursor(Qt.PointingHandCursor)
                    btn.setFixedHeight(62)
                    btn.setStyleSheet(f"""
                        QPushButton {{background:{C.TABLE_BG};border:1px solid {C.GRID_LINE};
                            border-left:4px solid {left_color};border-radius:7px;text-align:left;padding:0;margin:0;}}
                        QPushButton:hover {{background:{C.TABLE_ALT_ROW};}}
                        QPushButton:checked {{background:{C.ATM_HIGHLIGHT};border:1px solid {C.BRAND};
                            border-left:4px solid {C.BRAND};}}
                    """)
                    btn_lyt = QVBoxLayout(btn)
                    btn_lyt.setContentsMargins(7, 4, 6, 4)
                    btn_lyt.setSpacing(3)

                    top_row = QHBoxLayout(); top_row.setSpacing(5)
                    if kind == "both":
                        tag_text = "★ 近月=主力"
                        tag_bg = "#FFE0B2"; tag_color = "#BF360C"
                    elif kind == "near":
                        tag_text = "近月"; tag_bg = C.TABLE_ALT_ROW; tag_color = C.TEXT_MID
                    else:
                        tag_text = "主力"; tag_bg = "#FFF3CD"; tag_color = "#9A5B00"
                    tag_lbl = QLabel(tag_text)
                    tag_lbl.setStyleSheet(
                        f"color:{tag_color};font:9px 'SimSun';background:{tag_bg};"
                        f"border:1px solid {C.GRID_LINE};border-radius:6px;padding:1px 5px;")
                    sym_lbl = QLabel(short_sym)
                    sym_lbl.setStyleSheet(f"color:{C.TEXT_LIGHT};font:10px 'Consolas','SimSun';background:transparent;")
                    pr_lbl = QLabel(f"标的 {price:.0f}" if price else "标的 -")
                    pr_lbl.setStyleSheet(f"color:{C.PRICE_DOWN};font:bold 13px 'Consolas','SimSun';background:transparent;")
                    top_row.addWidget(tag_lbl)
                    top_row.addWidget(sym_lbl)
                    top_row.addStretch()
                    top_row.addWidget(pr_lbl)
                    btn_lyt.addLayout(top_row)

                    bot_row = QHBoxLayout(); bot_row.setSpacing(5)
                    expiry_text, expiry_status = _expiry_status_text(days)
                    info1 = QLabel(expiry_text)
                    info2 = QLabel(f"C/P {calls}/{puts}")
                    info3 = QLabel(f"量 {vol}" + (" ⚠" if low_vol else ""))
                    if expiry_status in ("expired", "urgent"):
                        info1.setStyleSheet(
                            f"color:{C.TABLE_BG};font:bold 9px 'SimSun';background:{C.PRICE_UP};"
                            f"border-radius:6px;padding:1px 5px;")
                    else:
                        info1.setStyleSheet(f"color:{C.TEXT_LIGHT};font:9px 'SimSun';background:{C.TABLE_BG};border:1px solid {C.GRID_LINE};border-radius:6px;padding:1px 5px;")
                    bot_row.addWidget(info1)
                    info2.setStyleSheet(f"color:{C.TEXT_LIGHT};font:9px 'SimSun';background:{C.TABLE_BG};border:1px solid {C.GRID_LINE};border-radius:6px;padding:1px 5px;")
                    if low_vol:
                        info3.setStyleSheet(f"color:{C.PRICE_UP};font:bold 9px 'Consolas','SimSun';background:#FFE5E3;border:1px solid #E3AAA5;border-radius:6px;padding:1px 5px;")
                    else:
                        info3.setStyleSheet(f"color:{C.TRADING_ON};font:bold 9px 'Consolas','SimSun';background:#EAF6EA;border:1px solid #B9D6B8;border-radius:6px;padding:1px 5px;")
                    bot_row.addWidget(info2)
                    bot_row.addWidget(info3)
                    bot_row.addStretch()
                    btn_lyt.addLayout(bot_row)

                    btn.clicked.connect(lambda checked, k=key, b=btn: self._on_sidebar_click(k, b))
                    self.sidebar_btn_group.addButton(btn)
                    self.prod_buttons.append((key, btn))
                    card_lyt.addWidget(btn)

                self.sidebar_layout.addWidget(card)

            selected_btn = None
            for k, b in self.prod_buttons:
                if k == self.current_key and selected_btn is None:
                    selected_btn = b
            if selected_btn:
                selected_btn.setChecked(True)
            elif self.prod_buttons:
                self.current_key = self.prod_buttons[0][0]
                self.prod_buttons[0][1].setChecked(True)
            self.sidebar_layout.addStretch()
            self._apply_sidebar_filter()

        def _on_sidebar_click(self, key, btn):
            """侧栏品种点击切换"""
            self.current_key = key
            # 确保按钮选中状态正确
            for k, b in self.prod_buttons:
                b.setChecked(b is btn)
            self._render_table()

        def _current_product(self):
            d = self.data
            if not d:
                return None
            key = self.current_key or ("", 0)
            for p in d.get("products", []):
                if p.get("sym") == key[0] and p.get("days_key", int(p.get("days") or 0)) == key[1]:
                    return p
            prods = d.get("products", [])
            return prods[0] if prods else None

        def _make_basket_leg(self, pi, rw, side_key):
            d = rw.get(side_key)
            if not d or not d.get("option_sym"):
                return None
            proxy_vix = self._contract_atm_vix(pi)
            return {
                "exchange": pi.get("exchange"), "product": pi.get("product"), "product_name": pi.get("name"),
                "underlying_sym": pi.get("sym"), "option_sym": d.get("option_sym"),
                "option_class": "CALL" if side_key == "c" else "PUT",
                "strike": rw.get("K"), "days": pi.get("days"), "side": "BUY", "volume": 1,
                "underlying_price": pi.get("price"),
                "open_underlying_price": pi.get("price"),
                "open_proxy_vix": proxy_vix,
                "current_proxy_vix": proxy_vix,
                "proxy_vix": proxy_vix,
                "multiplier": d.get("multiplier") or pi.get("multiplier") or _product_multiplier(pi.get("exchange"), pi.get("product"), SIM_OPTION_MULTIPLIER), "last": d.get("last"), "bid": d.get("bid"),
                "ask": d.get("ask"), "delta": d.get("delta"), "iv": d.get("iv"),
            }

        def _on_table_cell_clicked(self, row, col):
            if col >= len(self.all_cols):
                return
            ck = self.all_cols[col]
            if ck not in ("buy_c", "sell_c", "buy_p", "sell_p"):
                return
            d = self.data
            if not d:
                return
            key = self.current_key or ("", 0)
            show_prods = [p for p in d["products"] if p["sym"] == key[0] and p.get("days_key", int(p.get("days") or 0)) == key[1]]
            if not show_prods:
                return
            pi = show_prods[0]
            rows = pi.get("rows", [])
            if row < 0 or row >= len(rows):
                return
            side_key = "c" if ck in ("buy_c", "sell_c") else "p"
            target_side = "BUY" if ck in ("buy_c", "buy_p") else "SELL"
            leg = self._make_basket_leg(pi, rows[row], side_key)
            if not leg:
                return
            opt = leg["option_sym"]
            old = self.trade_basket.get(opt)
            if old and old.get("side") == target_side:
                self.trade_basket.pop(opt, None)
            else:
                leg["side"] = target_side
                if old:
                    leg["volume"] = old.get("volume", leg.get("volume", 1))
                self.trade_basket[opt] = leg
            self._render_basket()
            self._render_table()

        def _set_basket_side(self, opt, side):
            if opt in self.trade_basket:
                self.trade_basket[opt]["side"] = side
                self._render_basket()
                self._render_table()

        def _set_basket_volume(self, opt, volume):
            if opt in self.trade_basket:
                self.trade_basket[opt]["volume"] = int(volume)
                self._render_basket()
                self._show_basket_payoff_preview()

        def _remove_basket_leg(self, opt):
            self.trade_basket.pop(opt, None)
            self._render_basket()
            self._render_table()

        def _clear_basket(self):
            self.trade_basket.clear()
            self._render_basket()
            self._render_table()

        def _render_basket(self):
            legs = list(self.trade_basket.values())
            self.basket_table.setRowCount(len(legs))
            net = 0.0
            scopes = sorted({f"{x.get('exchange')}.{x.get('product')}" for x in legs})
            for ri, leg in enumerate(legs):
                opt = leg["option_sym"]
                short_opt = opt.split(".")[-1] if "." in opt else opt
                combo = QComboBox()
                combo.addItem("买入", "BUY")
                combo.addItem("卖出", "SELL")
                combo.setFixedHeight(24)
                combo.setCurrentIndex(0 if leg.get("side") == "BUY" else 1)
                combo.currentIndexChanged.connect(lambda idx, k=opt, c=combo: self._set_basket_side(k, c.currentData()))
                self.basket_table.setCellWidget(ri, 0, combo)
                spin = QSpinBox()
                spin.setRange(1, 999)
                spin.setFixedHeight(24)
                spin.setValue(int(leg.get("volume") or 1))
                spin.valueChanged.connect(lambda v, k=opt: self._set_basket_volume(k, v))
                self.basket_table.setCellWidget(ri, 1, spin)
                opt_class = "C" if leg.get("option_class") == "CALL" else "P"
                ref_price = _sim_leg_price(leg, "OPEN")
                iv_val = leg.get("iv", "-")
                if isinstance(iv_val, (int, float)):
                    iv_val = f"{iv_val:.2f}"
                vals = [
                    short_opt, opt_class, f"{leg.get('strike'):g}" if leg.get("strike") is not None else "-",
                    f"{ref_price:.2f}", iv_val
                ]
                for ci, val in enumerate(vals, 2):
                    item = QTableWidgetItem(str(val))
                    item.setTextAlignment(Qt.AlignCenter)
                    if ci == 2:
                        item.setToolTip(opt)
                    self.basket_table.setItem(ri, ci, item)
                del_btn = QPushButton("删除")
                del_btn.setObjectName("rowActionBtn")
                del_btn.setFixedHeight(24)
                del_btn.clicked.connect(lambda checked, k=opt: self._remove_basket_leg(k))
                self.basket_table.setCellWidget(ri, 7, del_btn)
                net += _sim_premium_cashflow(leg.get("side"), _sim_leg_price(leg, "OPEN"), leg.get("volume") or 1, _sim_leg_multiplier(leg))
            if legs:
                self.basket_summary_lbl.setText(f"{len(legs)}腿 | {' + '.join(scopes)} | 净权利金 {net:.2f}元")
            else:
                self.basket_summary_lbl.setText("篮子为空")
            self._show_basket_payoff_preview()

        def _show_basket_payoff_preview(self):
            legs = list(self.trade_basket.values())
            if legs:
                cur_pi = self._current_product()
                ticks = [r.get("K") for r in (cur_pi or {}).get("rows", []) if r.get("K") is not None]
                open_underlying = _sim_num(legs[0].get("open_underlying_price"), _sim_num((cur_pi or {}).get("price"), None))
                current_vix = self._contract_atm_vix(cur_pi) if cur_pi else None
                open_vix_vals = [_sim_num(x.get("open_proxy_vix"), None) for x in legs]
                open_vix_vals = [x for x in open_vix_vals if x is not None and x > 0]
                open_vix = sum(open_vix_vals) / len(open_vix_vals) if open_vix_vals else current_vix
                self.payoff_chart.set_strategy({
                    "legs": legs,
                    "current_price": (cur_pi or {}).get("price"),
                    "open_underlying_price": open_underlying,
                    "open_proxy_vix": open_vix,
                    "current_proxy_vix": current_vix,
                    "x_ticks": ticks,
                })
                self.pnl_chart.set_values([], "")
                if hasattr(self, "pnl_box_title"):
                    self.pnl_box_title.setText("实时盈亏曲线 · 未开仓篮子无曲线")
                if hasattr(self, "chart_hint_lbl"):
                    self.chart_hint_lbl.setText("当前显示：组合篮子到期损益预览；虚线上方=代理VIX，虚线下方=期货价；橙色=建仓，品牌色=现价")
            elif self.selected_strategy_id:
                self._show_strategy_detail(self.selected_strategy_id)
            else:
                self.payoff_chart.set_strategy({"legs": []})
                self.pnl_chart.set_values([], "")
                if hasattr(self, "chart_hint_lbl"):
                    self.chart_hint_lbl.setText("选择持仓或构建篮子时显示到期损益结构")

        def _open_strategy_from_basket(self):
            try:
                sid = sim_open_strategy(self.strategy_name_edit.text().strip(), list(self.trade_basket.values()))
                self.count_lbl.setText(f"模拟组合已开仓: {sid}")
                self.selected_strategy_id = sid
                self.trade_basket.clear()
                self.strategy_name_edit.clear()
                self._render_basket()
                self._render_positions()
                self._render_table()
            except Exception as e:
                self.count_lbl.setText(f"开仓失败:{e}")

        def _render_positions(self):
            try:
                rows = sim_list_strategies(save_snapshots=False)
            except Exception as e:
                self.positions_summary_lbl.setText(f"持仓读取失败:{e}")
                return
            # 排序: OPEN 在前, 同状态下按 (交易所, 品种, CALL→PUT, 行权价高→低, 开仓时间新→旧)
            #   - 以首腿为策略代表 (sim_list_strategies 内 leg 按 leg_id 升序, 则首腿一般为买腿)
            #   - open_time 为 ISO 时间字符串, 字典序与时间序一致, 故先正序排再 stable 主排
            try:
                rows.sort(key=lambda st: str(st.get("open_time") or ""), reverse=True)
            except Exception:
                pass
            def _strategy_sort_key(st):
                legs = st.get("legs") or []
                first = legs[0] if legs else {}
                # status: OPEN < CLOSED 字典序已反了, 手动映射
                status_order = 0 if (st.get("status") == "OPEN") else 1
                strike = float(first.get("strike_price") or 0)
                return (
                    status_order,
                    str(first.get("exchange") or ""),
                    str(first.get("product") or ""),
                    str(first.get("option_class") or ""),  # CALL < PUT
                    -strike,                                 # 高行权价在前
                )
            rows.sort(key=_strategy_sort_key)
            self._latest_strategy_rows = rows
            valid_ids = {x.get("strategy_id") for x in rows if x.get("strategy_id")}
            if self.selected_strategy_id not in valid_ids:
                first_open = next((x.get("strategy_id") for x in rows if x.get("status") == "OPEN"), None)
                self.selected_strategy_id = first_open or (rows[0].get("strategy_id") if rows else None)
            for st in rows:
                sid = st.get("strategy_id")
                if sid and st.get("status") == "OPEN":
                    hist_rows = sim_get_pnl_history(sid)
                    self.pnl_history[sid] = [x.get("total_pnl") or 0 for x in hist_rows]
            self.positions_table.setRowCount(len(rows))
            open_cnt = sum(1 for x in rows if x.get("status") == "OPEN")
            pnl = sum((x.get("unrealized_pnl") or 0) for x in rows if x.get("status") == "OPEN")
            latest_ts = max([x.get("snapshot_time") for x in rows if x.get("snapshot_time")] or [self._last_applied_market_time or ""])
            self.positions_summary_lbl.setText(f"组合{len(rows)}个 | 持仓{open_cnt}个 | 浮盈亏 {pnl:.2f}元 | 数据{latest_ts[-8:] if latest_ts else '-'}")
            for ri, st in enumerate(rows):
                legs_text = self._strategy_legs_text(st)
                vals = [st.get("status"), st.get("strategy_name"), st.get("leg_count"),
                        f"{st.get('open_premium') or 0:.2f}", f"{st.get('unrealized_pnl') or 0:.2f}",
                        f"{st.get('realized_pnl') or 0:.2f}", legs_text]
                leg_lines = max(1, len([x for x in str(legs_text).splitlines() if x.strip()]))
                self.positions_table.setRowHeight(ri, max(54, min(86, 28 + leg_lines * 20)))
                for ci, val in enumerate(vals):
                    if ci == 6:
                        leg_lbl = QLabel(str(val))
                        leg_lbl.setWordWrap(True)
                        leg_lbl.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                        leg_lbl.setToolTip(str(val))
                        leg_lbl.setStyleSheet(
                            f"color:{C.TEXT_DARK};font:11px 'Consolas','SimSun';"
                            f"background:transparent;padding:3px 6px;line-height:18px;")
                        self.positions_table.setItem(ri, ci, QTableWidgetItem(""))
                        self.positions_table.setCellWidget(ri, ci, leg_lbl)
                        continue
                    item = QTableWidgetItem(str(val))
                    item.setTextAlignment(Qt.AlignCenter)
                    if ci == 1:
                        item.setToolTip(str(val))
                    if ci == 4:
                        item.setForeground(QColor(C.TRADING_ON if (st.get("unrealized_pnl") or 0) >= 0 else C.PRICE_UP))
                    self.positions_table.setItem(ri, ci, item)
                btn = QPushButton("组合平仓" if st.get("status") == "OPEN" else "已平")
                btn.setObjectName("rowActionBtn")
                btn.setFixedHeight(24)
                btn.setEnabled(st.get("status") == "OPEN")
                btn.clicked.connect(lambda checked, sid=st.get("strategy_id"): self._close_strategy(sid))
                self.positions_table.setCellWidget(ri, 7, btn)
                del_btn = QPushButton("清除")
                del_btn.setObjectName("rowActionBtn")
                del_btn.setFixedHeight(24)
                del_btn.setEnabled(st.get("status") != "OPEN")
                if st.get("status") == "OPEN":
                    del_btn.setToolTip("请先平仓，再清除组合记录")
                del_btn.clicked.connect(lambda checked, sid=st.get("strategy_id"): self._delete_strategy(sid))
                self.positions_table.setCellWidget(ri, 8, del_btn)
                if st.get("strategy_id") == self.selected_strategy_id:
                    self.positions_table.selectRow(ri)
            if self.selected_strategy_id:
                self._show_strategy_detail(self.selected_strategy_id)
            elif hasattr(self, "pnl_box_title"):
                self.pnl_box_title.setText("实时盈亏曲线 · 请选择组合")

        def _strategy_legs_text(self, strategy):
            # 腿展示顺序: CALL→PUT, 同类型内 行权价高→低
            legs = sorted(
                strategy.get("legs", []) or [],
                key=lambda l: (
                    str(l.get("option_class") or ""),
                    -float(l.get("strike_price") or 0),
                ),
            )
            parts = []
            for leg in legs:
                short_opt = leg.get("option_sym") or ""
                short_opt = short_opt.split(".")[-1] if "." in short_opt else short_opt
                side = "买" if leg.get("side") == "BUY" else "卖"
                vol = int(leg.get("volume") or 0)
                quote_tag = "  缺行情/无法估值" if leg.get("quote_status") == "MISSING" else ""
                parts.append(f"{side} {short_opt}  {vol}手{quote_tag}")
            if not parts:
                return "-"
            return "\n".join(parts)

        def _delete_strategy(self, strategy_id):
            try:
                st = next((x for x in self._latest_strategy_rows if x.get("strategy_id") == strategy_id), None)
                if st and st.get("status") == "OPEN":
                    self.count_lbl.setText("请先平仓，再清除组合记录")
                    return
                sim_delete_strategy(strategy_id)
                if self.selected_strategy_id == strategy_id:
                    self.selected_strategy_id = None
                    self.payoff_chart.set_strategy({"legs": []})
                    self.pnl_chart.set_values([], "")
                self.count_lbl.setText("组合记录已清除")
                self._render_positions()
            except Exception as e:
                self.count_lbl.setText(f"清除失败:{e}")

        def _close_strategy(self, strategy_id):
            try:
                pnl = sim_close_strategy(strategy_id)
                self.count_lbl.setText(f"组合已平仓: {pnl:.2f}")
                self._render_positions()
            except Exception as e:
                self.count_lbl.setText(f"平仓失败:{e}")

        def _on_position_row_clicked(self, row, col):
            if row < 0 or row >= len(self._latest_strategy_rows):
                return
            sid = self._latest_strategy_rows[row].get("strategy_id")
            self.selected_strategy_id = sid
            self._show_strategy_detail(sid)

        def _show_strategy_detail(self, strategy_id):
            st = next((x for x in self._latest_strategy_rows if x.get("strategy_id") == strategy_id), None)
            if not st:
                self.payoff_chart.set_strategy({"legs": []})
                self.pnl_chart.set_values([], "")
                if hasattr(self, "pnl_box_title"):
                    self.pnl_box_title.setText("实时盈亏曲线 · 未找到组合")
                if hasattr(self, "chart_hint_lbl"):
                    self.chart_hint_lbl.setText("组合不存在或已被清除，请刷新持仓")
                return
            st = dict(st)
            # 计算当前代理VIX (基于当前快照的近月ATM IV均值)
            legs = st.get("legs", []) or []
            first_leg = legs[0] if legs else {}
            cur_vix = self._find_current_proxy_vix(first_leg.get("exchange"), first_leg.get("product"), first_leg.get("underlying_sym"), first_leg.get("expire_rest_days"))
            if cur_vix is not None:
                st["current_proxy_vix"] = cur_vix
            # 确保腿上也带上开仓/当前VIX, 以便 PayoffChart 回退
            for lg in legs:
                if lg.get("open_proxy_vix") is None:
                    lg["open_proxy_vix"] = st.get("open_proxy_vix")
                if lg.get("current_proxy_vix") is None:
                    lg["current_proxy_vix"] = cur_vix
            ticks = self._strategy_axis_ticks(st)
            if ticks:
                st["x_ticks"] = ticks
            open_underlying_vals = [_sim_num(x.get("open_underlying_price"), None) for x in st.get("legs", [])]
            open_underlying_vals = [x for x in open_underlying_vals if x is not None and x > 0]
            if open_underlying_vals:
                st["open_underlying_price"] = open_underlying_vals[0]
            self.payoff_chart.set_strategy(st)
            hist_rows = sim_get_pnl_history(strategy_id)
            values = [x.get("total_pnl") or 0 for x in hist_rows] or self.pnl_history.get(strategy_id, [])
            latest_time = (hist_rows[-1].get("snapshot_time") if hist_rows else None) or st.get("snapshot_time") or self._last_applied_market_time or ""
            if len(values) < 2:
                base = st.get("realized_pnl") if st.get("status") == "CLOSED" else st.get("unrealized_pnl")
                values = [0, base or 0]
            if hasattr(self, "pnl_box_title"):
                self.pnl_box_title.setText(f"实时盈亏曲线 · {st.get('strategy_name') or strategy_id}")
            if hasattr(self, "chart_hint_lbl"):
                oup = st.get("open_underlying_price")
                self.chart_hint_lbl.setText(
                    f"当前显示：{st.get('strategy_name') or strategy_id}；虚线上方=代理VIX，虚线下方=期货价；橙色=建仓，品牌色=现价" if oup else
                    "当前显示：选中组合；虚线上方=代理VIX，虚线下方=期货价；橙色=建仓，品牌色=现价"
                )
            self.pnl_chart.set_values(values, latest_time)

        def _strategy_axis_ticks(self, strategy):
            legs = (strategy or {}).get("legs", [])
            if not legs or not self.data:
                return []
            leg = legs[0]
            sym = leg.get("underlying_sym")
            days = int(_sim_num(leg.get("expire_rest_days"), leg.get("days") or 0) or 0)
            for p in self.data.get("products", []):
                if p.get("sym") == sym and int(p.get("days") or 0) == days:
                    return [r.get("K") for r in p.get("rows", []) if r.get("K") is not None]
            return []

        def _render_table(self):
            d = self.data
            if not d: return
            key = self.current_key or ("", 0)
            # 用复合key精确匹配品种+月份
            show_prods = [p for p in d["products"]
                         if p["sym"] == key[0] and p.get("days_key", int(p.get("days") or 0)) == key[1]]
            if not show_prods:
                show_prods = [d["products"][0]] if d["products"] else []
            if not show_prods: return
            pi = show_prods[0]; S = pi.get("price", 0); rows = pi.get("rows", [])
            if not rows: return
            vix = self._contract_atm_vix(pi)
            ex_name = self._exchange_name(pi.get("exchange"))
            short_sym = pi.get("sym", "").split(".")[-1] if "." in pi.get("sym", "") else pi.get("sym", "")
            ctype_text = {"near":"近月", "main":"主力", "near_main":"★ 近月=主力"}.get(pi.get("ctype"), pi.get("ctype") or "")
            expiry_text, _ = _expiry_status_text(pi.get("days", 0))
            calls, puts = self._contract_counts(pi)
            vol_raw = self._contract_volume(pi)
            market_ts = pi.get("market_ts")
            fresh_text = self._time_text(pi.get("market_time"), market_ts)
            fresh_age = self._age_text(market_ts)
            self.table_title.setText(f"{ex_name} · {pi.get('name', pi.get('product', ''))}期权 {short_sym}  {ctype_text}")
            metric_values = {
                "price": (f"{S:g}" if S else "-", C.TEXT_DARK),
                "vix": (f"{vix:.2f}%" if vix is not None else "-", C.PRICE_UP),
                "fresh": (fresh_text, C.TRADING_ON),
                "volume": (f"{self._fmt_vol_cn(vol_raw)}" + (" ⚠" if vol_raw < 10000 else ""), C.PRICE_UP if vol_raw < 10000 else C.TRADING_ON),
                "days": (expiry_text, C.TEXT_DARK),
                "cp": (f"{calls} / {puts}", C.TEXT_DARK),
            }
            metric_titles = {"price":"标的价格", "vix":"代理VIX", "fresh":f"行情时间 {fresh_age}", "volume":"成交量", "days":"到期剩余", "cp":"C/P档数"}
            for mk, lbl in getattr(self, "table_metric_labels", {}).items():
                val, color = metric_values.get(mk, ("-", C.TEXT_DARK))
                lbl.setText(f"<b style='color:{color};font-family:Consolas'>{val}</b><br><span style='color:{C.TEXT_LIGHT}'>{metric_titles.get(mk, '')}</span>")
            self.table.setUpdatesEnabled(False)
            self.table.blockSignals(True)
            try:
                otm_threshold = max(S * 0.005, 1)
                call_vol_info = [(ri, rw["c"]["volume"]) for ri, rw in enumerate(rows)
                                 if rw.get("c") and (rw.get("K") - S) > otm_threshold and (rw["c"].get("volume") or 0) > 0]
                put_vol_info = [(ri, rw["p"]["volume"]) for ri, rw in enumerate(rows)
                                if rw.get("p") and (S - rw.get("K")) > otm_threshold and (rw["p"].get("volume") or 0) > 0]
                call_vol_info.sort(key=lambda x: x[1], reverse=True)
                put_vol_info.sort(key=lambda x: x[1], reverse=True)
                call_vol_ranks = {ri: rank for rank, (ri, _) in enumerate(call_vol_info[:3], 1)}
                put_vol_ranks = {ri: rank for rank, (ri, _) in enumerate(put_vol_info[:3], 1)}
                atm_band_rows = self._atm_band_rows(rows, S)
                
                n_rows = len(rows)
                if self.table.rowCount() != n_rows:
                    self.table.setRowCount(n_rows)

                for ri, rw in enumerate(rows):
                    K = rw["K"]; cd = rw["c"]; pd_ = rw["p"]; atm = rw["atm"]
                    atm_band = atm_band_rows.get(ri)
                    band_mark = self._atm_band_mark_text(atm_band)
                    if atm or band_mark:
                        self.table.setRowHeight(ri, 36)
                    else:
                        self.table.setRowHeight(ri, 30)
                    otm_c = (K - S) > otm_threshold
                    otm_p = (S - K) > otm_threshold
                    call_vr = call_vol_ranks.get(ri, 0)
                    put_vr = put_vol_ranks.get(ri, 0)

                    for ci, ck in enumerate(self.all_cols):
                        item = self.table.item(ri, ci)
                        if item is None:
                            item = QTableWidgetItem()
                            item.setTextAlignment(Qt.AlignCenter)
                            self.table.setItem(ri, ci, item)

                        if ck in ("buy_c", "sell_c", "buy_p", "sell_p"):
                            d0 = cd if ck in ("buy_c", "sell_c") else pd_
                            opt = d0.get("option_sym") if d0 else None
                            leg = self.trade_basket.get(opt) if opt else None
                            is_buy_col = ck in ("buy_c", "buy_p")
                            target_side = "BUY" if is_buy_col else "SELL"
                            active = bool(leg and leg.get("side") == target_side)
                            if active and target_side == "BUY":
                                item.setText("✓")
                                item.setBackground(QColor("#E8F5E9"))
                                item.setForeground(QColor(C.TRADING_ON))
                            elif active:
                                item.setText("✓")
                                item.setBackground(QColor("#FCE4EC"))
                                item.setForeground(QColor(C.PRICE_UP))
                            else:
                                item.setText("□")
                                item.setBackground(QColor(C.TABLE_BG))
                                item.setForeground(QColor(C.TEXT_LIGHT))
                            item.setFont(QFont("SimSun", 12))

                        elif ck == "strike":
                            strike_widget = self.table.cellWidget(ri, ci)
                            if strike_widget is None:
                                strike_widget = QLabel()
                                strike_widget.setAlignment(Qt.AlignCenter)
                                strike_widget.setTextFormat(Qt.RichText)
                                self.table.setCellWidget(ri, ci, strike_widget)
                            if atm:
                                item.setText("")
                                strike_widget.setText(
                                    f"<div style='line-height:14px;'><span style='font:13px Consolas;color:{C.STRIKE_TEXT};'>{K:g}</span><br>"
                                    f"<span style='font:10px SimSun;color:white;background:{C.BRAND};border-radius:6px;padding:1px 8px;'>ATM</span></div>" if K else "")
                                strike_widget.setStyleSheet(f"background:{C.ATM_STRIKE_BG};")
                                item.setBackground(QColor(C.ATM_STRIKE_BG)); item.setForeground(QColor(C.ATM_PRICE_TXT))
                                item.setFont(QFont("SimSun", 12))
                                item.setToolTip("平值行权价")
                            elif band_mark:
                                mark_color = C.BRAND
                                if "9" in band_mark:
                                    mark_color = C.PRICE_UP
                                elif "7" in band_mark:
                                    mark_color = "#E87500"
                                item.setText("")
                                strike_widget.setText(
                                    f"<div style='line-height:14px;'><span style='font:13px Consolas;color:{C.STRIKE_TEXT};'>{K:g}</span><br>"
                                    f"<span style='font:10px SimSun;color:white;background:{mark_color};border-radius:6px;padding:1px 8px;'>{band_mark}</span></div>" if K else "")
                                strike_widget.setStyleSheet(f"background:{C.ATM_BAND_5_BG};")
                                item.setBackground(QColor(C.ATM_BAND_5_BG)); item.setForeground(QColor(C.STRIKE_TEXT))
                                item.setFont(QFont("SimSun", 11))
                                item.setToolTip(f"虚值距离标识: {band_mark}")
                            else:
                                item.setText("")
                                strike_widget.setText(f"<span style='font:13px Consolas;color:{C.STRIKE_TEXT};'>{K:g}</span>" if K else "")
                                strike_widget.setStyleSheet(f"background:{C.SEL_ROW_BG};")
                                item.setBackground(QColor(C.SEL_ROW_BG)); item.setForeground(QColor(C.STRIKE_TEXT))
                                item.setFont(QFont("SimSun", 12))

                        elif ck in self.call_cols:
                            fk = ck.replace("_c", ""); val = self._get_cell_val(cd, fk)
                            item.setText(val); self._set_cell(item, fk, call_vr, "call", atm, otm_c, (atm_band or {}).get("call"))

                        elif ck in self.put_cols:
                            fk = ck.replace("_p", ""); val = self._get_cell_val(pd_, fk)
                            item.setText(val); self._set_cell(item, fk, put_vr, "put", atm, otm_p, (atm_band or {}).get("put"))
            finally:
                self.table.blockSignals(False)
                self.table.setUpdatesEnabled(True)

        def _set_cell(self, item, fk, vol_rank, side, atm, is_otm, atm_band=None):
            """风格12: 纸质纹理宋体 — 只突出完全虚值, 买卖价不加粗"""
            is_vol = (fk == "volume")
            emph = fk in ("last", "iv")
            item.setToolTip("")

            # 成交量Top3: 黄色高亮
            if is_vol and is_otm and vol_rank > 0:
                if vol_rank == 1:
                    item.setBackground(QColor(C.VOL_1_BG)); item.setForeground(QColor(C.VOL_1_TXT))
                elif vol_rank == 2:
                    item.setBackground(QColor(C.VOL_2_BG)); item.setForeground(QColor(C.VOL_2_TXT))
                elif vol_rank == 3:
                    item.setBackground(QColor(C.VOL_3_BG)); item.setForeground(QColor(C.VOL_3_TXT))
                item.setFont(QFont("SimSun", 11)); return

            # ATM平值行: 象牙白高亮带
            if atm:
                item.setBackground(QColor(C.ATM_HIGHLIGHT))
                if fk == "last":
                    item.setForeground(QColor(C.ATM_PRICE_TXT))
                    item.setFont(QFont("SimSun", 13, QFont.Bold))
                elif fk == "iv":
                    item.setForeground(QColor(C.ATM_PRICE_TXT))
                    item.setFont(QFont("SimSun", 12, QFont.Bold))
                elif fk in ("bid", "ask"):
                    # ★ 买卖价不加粗，只设颜色
                    item.setForeground(QColor(C.TEXT_DARK))
                    item.setFont(QFont("SimSun", 12))
                else:
                    if side == "call":
                        item.setForeground(QColor(C.PRICE_DOWN))   # 松烟绿
                    else:
                        item.setForeground(QColor(C.PRICE_UP))     # 胭脂红
                    item.setFont(QFont("SimSun", 11))
                return

            # ===== CALL看涨期权 =====
            if side == "call":
                if is_otm:
                    # ★ 虚值Call: 绿底色 + 深绿文字
                    item.setBackground(QColor(C.CALL_ITM_BG))
                    if fk == "last":
                        item.setForeground(QColor(C.CALL_ITM_TXT))
                        item.setFont(QFont("SimSun", 12, QFont.Bold))
                    elif fk in ("bid", "ask"):
                        # ★ 买卖价只着色，不加粗
                        item.setForeground(QColor(C.CALL_ITM_TXT))
                        item.setFont(QFont("SimSun", 12))
                    elif fk == "chg":
                        item.setForeground(QColor("#2E7D32"))
                        item.setFont(QFont("SimSun", 11))
                    else:
                        item.setForeground(QColor(C.CALL_ITM_AUX))
                        item.setFont(QFont("SimSun", 12 if emph else 11, QFont.Bold if emph else QFont.Normal))
                else:
                    # ○ 实值/近似平值Call: 普通护眼底色
                    item.setBackground(QColor(C.TABLE_BG))
                    item.setForeground(QColor(C.TEXT_MID if fk not in ("last", "bid", "ask") else C.TEXT_DARK))
                    item.setFont(QFont("SimSun", 12 if emph else 11, QFont.Bold if emph else QFont.Normal))

            # ===== PUT看跌期权 =====
            elif side == "put":
                if is_otm:
                    # ★ 虚值Put: 红底色 + 深玫红文字
                    item.setBackground(QColor(C.PUT_ITM_BG))
                    if fk == "last":
                        item.setForeground(QColor(C.PUT_ITM_TXT))
                        item.setFont(QFont("SimSun", 12, QFont.Bold))
                    elif fk in ("bid", "ask"):
                        # ★ 买卖价只着色，不加粗
                        item.setForeground(QColor(C.PUT_ITM_TXT))
                        item.setFont(QFont("SimSun", 12))
                    elif fk == "chg":
                        item.setForeground(QColor("#C62828"))
                        item.setFont(QFont("SimSun", 11))
                    else:
                        item.setForeground(QColor(C.PUT_ITM_AUX))
                        item.setFont(QFont("SimSun", 12 if emph else 11, QFont.Bold if emph else QFont.Normal))
                else:
                    # ○ 实值/近似平值Put: 普通护眼底色
                    item.setBackground(QColor(C.TABLE_BG))
                    item.setForeground(QColor(C.TEXT_MID if fk not in ("last", "bid", "ask") else C.TEXT_DARK))
                    item.setFont(QFont("SimSun", 12 if emph else 11, QFont.Bold if emph else QFont.Normal))

    app = QApplication.instance()
    if app is None: app = QApplication([])
    win = OptionWindow(); win.show()
    _gui_root = win
    rc = app.exec_()
    log(f"[图形] GUI 已关闭, 返回码 {rc}")


if __name__ == "__main__":
    # 启动 天勤-gui.py（包含 TQSDK 数据线程 + PyQt5 界面）
    import subprocess
    gui_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "天勤-gui.py")
    log(f"[启动] 正在启动 GUI: {gui_path}")
    try:
        subprocess.run([sys.executable, gui_path])
    except FileNotFoundError:
        log(f"[错误] 找不到 {gui_path}，请确认 天勤-gui.py 与本文件在同一目录")
    except KeyboardInterrupt:
        pass
   