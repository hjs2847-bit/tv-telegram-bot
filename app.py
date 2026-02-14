# -*- coding: utf-8 -*-
import os, re, json, time, hmac, hashlib, logging, threading
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, parse_qs
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
LOCK = threading.Lock()

# Redis 장애/웹훅 plain-text 상황에서도 동작하도록 메모리 보조 캐시
MEM_THROTTLE: Dict[str, int] = {}

# ===== ENV =====
def E(k: str, d: str = "") -> str:
    return os.getenv(k, d).strip()

def Ei(k: str, d: int) -> int:
    try:
        return int(E(k, str(d)))
    except:
        return d

BOT_TOKEN = E("BOT_TOKEN")
BOT_TOKEN_POSITION = E("BOT_TOKEN_POSITION", BOT_TOKEN)

CHAT_IDS = [x.strip() for x in E("CHAT_IDS").split(",") if x.strip()]
CHAT_IDS_POSITION = [x.strip() for x in E("CHAT_IDS_POSITION").split(",") if x.strip()]
ADMIN_USER_IDS = set(x.strip() for x in E("ADMIN_USER_IDS").split(",") if x.strip())

TV_WEBHOOK_SECRET = E("TV_WEBHOOK_SECRET")
TG_CONTROL_SECRET = E("TG_CONTROL_SECRET")
POSITIONS_CHECK_TOKEN = E("POSITIONS_CHECK_TOKEN")
DAILY_REPORT_TOKEN = E("DAILY_REPORT_TOKEN")

UPSTASH_URL = E("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = E("UPSTASH_REDIS_REST_TOKEN")

BINGX_API_KEY = E("BINGX_API_KEY")
BINGX_API_SECRET = E("BINGX_API_SECRET")
BINGX_BASE_URL = E("BINGX_BASE_URL", "https://open-api.bingx.com")

REPORT_AUTO_DEFAULT = E("REPORT_AUTO_DEFAULT", "off").lower()
REPORT_AUTO_HOUR_DEFAULT = Ei("REPORT_AUTO_HOUR", 23)      # ✅ 매일 23시
REPORT_AUTO_MINUTE_DEFAULT = Ei("REPORT_AUTO_MINUTE", 50)  # ✅ 50분
REPORT_AUTO_CHAT_DEFAULT = E("REPORT_AUTO_CHAT")

TAKER_FEE_RATE = float(E("TAKER_FEE_RATE", "0.0005"))
TIMEOUT = Ei("REQUEST_TIMEOUT", 15)

# ===== Utils =====
def now_kst() -> datetime:
    return datetime.now(tz=KST)

def to_kst(dt: Optional[datetime] = None, sec: bool = False) -> str:
    dt = dt or now_kst()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    dt = dt.astimezone(KST)
    return dt.strftime("%Y-%m-%d %H:%M:%S (KST)" if sec else "%Y-%m-%d %H:%M (KST)")

def hm(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%m-%d %H:%M")

def asf(v: Any, d: float = 0.0) -> float:
    try:
        if v is None or isinstance(v, bool):
            return d
        return float(str(v).replace(",", "").strip())
    except:
        return d

def asi(v: Any, d: int = 0) -> int:
    try:
        return int(str(v).strip())
    except:
        return d

def iso_parse(s: str) -> Optional[datetime]:
    try:
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except:
        return None

def sjson(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":"))

def pjson(s: Any, d: Any):
    try:
        if s is None:
            return d
        if isinstance(s, (dict, list)):
            return s
        return json.loads(s)
    except:
        return d

def sign(v: float) -> str:
    return f"+{v:,.2f}" if asf(v) > 0 else f"{asf(v):,.2f}"

def pct(v: float) -> str:
    return f"+{asf(v):.2f}%" if asf(v) > 0 else f"{asf(v):.2f}%"

def fmt_num(v: float, d: int = 2) -> str:
    return f"{asf(v):,.{d}f}"

def fmt_price(v: float) -> str:
    x = abs(asf(v))
    d = 2 if x >= 100 else (4 if x >= 1 else 6)
    return f"{asf(v):,.{d}f}"

def fmt_qty(v: float) -> str:
    x = abs(asf(v))
    d = 2 if x >= 100 else 4
    return f"{x:,.{d}f}"

def base_asset(symbol: str) -> str:
    s = (symbol or "").upper()
    for sep in ["-", "/", "_"]:
        if sep in s:
            return s.split(sep)[0]
    for q in ["USDT", "USD", "PERP", ".P"]:
        if s.endswith(q):
            return s[:-len(q)] or s
    return s


# ===== Upstash =====
class Redis:
    def __init__(self, url: str, token: str):
        self.url = (url or "").rstrip("/")
        self.token = token or ""

    @property
    def ok(self):
        return bool(self.url and self.token)

    def cmd(self, arr: List[Any]):
        if not self.ok:
            return None
        try:
            r = requests.post(
                self.url,
                headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
                data=sjson(arr).encode("utf-8"),
                timeout=TIMEOUT,
            )
            if r.status_code >= 400:
                logging.warning("Upstash %s %s", r.status_code, r.text[:200])
                return None
            return r.json().get("result")
        except Exception as e:
            logging.warning("Upstash error: %s", e)
            return None

    def get(self, k: str):
        v = self.cmd(["GET", k])
        return None if v is None else str(v)

    def set(self, k: str, v: str):
        return self.cmd(["SET", k, v]) == "OK"

    def delete(self, k: str):
        return asi(self.cmd(["DEL", k])) >= 0

    def lpush(self, k: str, v: str):
        return asi(self.cmd(["LPUSH", k, v]))

    def ltrim(self, k: str, a: int, b: int):
        return self.cmd(["LTRIM", k, a, b]) == "OK"

    def lrange(self, k: str, a: int, b: int):
        v = self.cmd(["LRANGE", k, a, b])
        return [str(x) for x in v] if isinstance(v, list) else []


R = Redis(UPSTASH_URL, UPSTASH_TOKEN)

def rget_json(k: str, d):
    return pjson(R.get(k), d)

def rset_json(k: str, v):
    R.set(k, sjson(v))

def rpush_json(k: str, v: Dict[str, Any], keep=2000):
    R.lpush(k, sjson(v))
    R.ltrim(k, 0, keep - 1)


# ===== Telegram =====
def tg_send(token: str, chat_id: str, text: str, preview=True) -> bool:
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # 1) Markdown 시도
    try:
        r = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": preview,
            },
            timeout=TIMEOUT,
        )
        if r.status_code < 400:
            j = r.json()
            if j.get("ok"):
                return True
            logging.warning("TG send fail markdown json=%s", j)
        else:
            logging.warning("TG send fail markdown %s %s", r.status_code, r.text[:300])
    except Exception as e:
        logging.warning("TG send err markdown %s", e)

    # 2) Markdown 파싱 에러 대비 plain-text 재시도
    try:
        r2 = requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": preview},
            timeout=TIMEOUT,
        )
        if r2.status_code >= 400:
            logging.warning("TG send fail plain %s %s", r2.status_code, r2.text[:300])
            return False
        j2 = r2.json()
        if not j2.get("ok"):
            logging.warning("TG send fail plain json=%s", j2)
            return False
        return True
    except Exception as e:
        logging.warning("TG send err plain %s", e)
        return False

def tg_send_plain(token: str, chat_id: str, text: str, preview=True) -> bool:
    """
    ✅ CHANGE: kind 미정(unknown) 시 '포맷 없이 원문 그대로' 보내기용 (parse_mode 미사용)
    다른 메시지에는 영향 없음.
    """
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": preview},
            timeout=TIMEOUT,
        )
        if r.status_code >= 400:
            logging.warning("TG send plain-only fail %s %s", r.status_code, r.text[:300])
            return False
        j = r.json()
        if not j.get("ok"):
            logging.warning("TG send plain-only fail json=%s", j)
            return False
        return True
    except Exception as e:
        logging.warning("TG send plain-only err %s", e)
        return False

def tg_send_chunk(token: str, chat_id: str, text: str, n=3500):
    if len(text) <= n:
        tg_send(token, chat_id, text)
        return
    cur = ""
    for ln in text.splitlines():
        add = ln + "\n"
        if len(cur) + len(add) > n:
            if cur.strip():
                tg_send(token, chat_id, cur.rstrip())
            cur = add
        else:
            cur += add
    if cur.strip():
        tg_send(token, chat_id, cur.rstrip())


# ===== Switch/Config =====
def is_group(chat_id: str) -> bool:
    try:
        return int(chat_id) < 0
    except:
        return chat_id.startswith("-")

def is_admin(uid: str) -> bool:
    return (not ADMIN_USER_IDS) or (str(uid) in ADMIN_USER_IDS)

def sw_key(kind: str, chat_id: str) -> str:
    return f"switch:{kind}:{chat_id}"

def sw_get(kind: str, chat_id: str) -> str:
    d = "0" if is_group(chat_id) else "1"
    v = R.get(sw_key(kind, chat_id))
    return "1" if (v if v is not None else d) == "1" else "0"

def sw_set(kind: str, chat_id: str, on: bool):
    R.set(sw_key(kind, chat_id), "1" if on else "0")

def cfg_get(k: str, d=""):
    v = R.get(f"cfg:{k}")
    return str(v) if v is not None else d

def cfg_set(k: str, v: str):
    R.set(f"cfg:{k}", str(v))

def cfg_init():
    if cfg_get("report_auto", "") == "":
        cfg_set("report_auto", "on" if REPORT_AUTO_DEFAULT == "on" else "off")

    if cfg_get("report_auto_hour", "") == "":
        cfg_set("report_auto_hour", str(REPORT_AUTO_HOUR_DEFAULT))

    if cfg_get("report_auto_minute", "") == "":
        cfg_set("report_auto_minute", str(REPORT_AUTO_MINUTE_DEFAULT))

    if cfg_get("report_auto_chat", "") == "":
        target = REPORT_AUTO_CHAT_DEFAULT
        if not target:
            cands = [c for c in CHAT_IDS_POSITION if not is_group(c)]
            target = cands[0] if cands else (CHAT_IDS_POSITION[0] if CHAT_IDS_POSITION else (CHAT_IDS[0] if CHAT_IDS else ""))
        cfg_set("report_auto_chat", target)

def switch_log(cmd: str, uid: str, note: str = ""):
    rpush_json("logs:switch", {"ts": now_kst().isoformat(), "cmd": cmd, "uid": uid, "note": note}, keep=300)


# ===== BingX =====
def bingx_req(path: str, params: Optional[Dict[str, Any]] = None, method="GET") -> Optional[Dict[str, Any]]:
    if not (BINGX_API_KEY and BINGX_API_SECRET):
        return None

    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p["recvWindow"] = 5000
    qs = urlencode(sorted(p.items(), key=lambda x: x[0]), doseq=True)
    sig = hmac.new(BINGX_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"{BINGX_BASE_URL}{path}?{qs}&signature={sig}"

    try:
        r = (
            requests.post(url, headers={"X-BX-APIKEY": BINGX_API_KEY}, timeout=TIMEOUT)
            if method == "POST"
            else requests.get(url, headers={"X-BX-APIKEY": BINGX_API_KEY}, timeout=TIMEOUT)
        )
        if r.status_code >= 400:
            logging.warning("BingX %s %s %s", r.status_code, path, r.text[:240])
            return None
        return r.json()
    except Exception as e:
        logging.warning("BingX err %s %s", path, e)
        return None

def data_list(x: Any) -> List[Dict[str, Any]]:
    if isinstance(x, list):
        return [i for i in x if isinstance(i, dict)]
    if isinstance(x, dict):
        for k in ["positions", "positionData", "list", "data", "items"]:
            v = x.get(k)
            if isinstance(v, list):
                return [i for i in v if isinstance(i, dict)]
        if x.get("symbol") or x.get("ticker"):
            return [x]
    return []

def fetch_positions_raw() -> List[Dict[str, Any]]:
    eps = [
        "/openApi/swap/v2/user/positions",
        "/openApi/swap/v2/user/position",
        "/openApi/swap/v1/user/positions",
        "/openApi/swap/v1/user/position",
    ]
    for ep in eps:
        j = bingx_req(ep, {})
        if not j:
            continue
        arr = data_list(j.get("data", j))
        if arr:
            return arr
    return []

def norm_pos(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    symbol = str(it.get("symbol") or it.get("ticker") or it.get("pair") or "").strip()
    if not symbol:
        return None

    qty_signed = asf(
        it.get("positionAmt")
        or it.get("positionSize")
        or it.get("position")
        or it.get("positionAmount")
        or it.get("holdVolume")
        or it.get("size")
        or 0
    )
    sraw = str(it.get("positionSide") or it.get("side") or it.get("holdSide") or it.get("posSide") or "").lower()

    if "short" in sraw or sraw in ("sell", "2"):
        side = "Short"
    elif "long" in sraw or sraw in ("buy", "1"):
        side = "Long"
    else:
        side = "Short" if qty_signed < 0 else "Long"

    qty = abs(qty_signed)
    if qty <= 0:
        qty = abs(asf(it.get("availableAmt") or 0))
    if qty <= 0:
        return None

    entry = asf(it.get("avgPrice") or it.get("entryPrice") or it.get("avgOpenPrice") or it.get("openPrice") or 0)
    mark = asf(it.get("markPrice") or it.get("lastPrice") or it.get("indexPrice") or it.get("closePrice") or entry, entry)
    upl = asf(it.get("unrealizedProfit") or it.get("unRealizedProfit") or it.get("unrealizedPnl") or it.get("upl") or it.get("positionProfit") or 0)
    rpl = asf(it.get("realizedProfit") or it.get("realisedPnl") or it.get("realizedPnl") or it.get("rpl") or 0)
    lev = asf(it.get("leverage") or it.get("positionLeverage") or 0, 0.0)

    mm = str(it.get("marginType") or it.get("marginMode") or it.get("isolated") or "")
    margin_mode = "Isolated" if ("isol" in mm.lower() or mm in ("true", "1")) else "Cross"

    value = asf(it.get("positionValue") or it.get("notional") or it.get("positionNotional") or it.get("value") or qty * mark, qty * mark)
    margin = asf(it.get("positionMargin") or it.get("isolatedMargin") or it.get("margin") or (value / lev if lev > 0 else 0), (value / lev if lev > 0 else 0))
    if lev <= 0:
        lev = (value / margin) if margin > 0 else 1.0

    u_pct = (upl / margin * 100) if margin else 0

    return {
        "symbol": symbol,
        "base": base_asset(symbol),
        "side": side,
        "qty": qty,
        "entry_price": entry,
        "mark_price": mark,
        "u_pnl": upl,
        "r_pnl": rpl,
        "leverage": lev,
        "margin_mode": margin_mode,
        "value": value,
        "margin": margin,
        "u_pnl_pct": u_pct,
        "raw": it,
    }

def fetch_positions() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in fetch_positions_raw():
        n = norm_pos(it)
        if n:
            out.append(n)
    return out

def fetch_income(symbol: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    eps = ["/openApi/swap/v2/user/income", "/openApi/swap/v1/user/income", "/openApi/swap/v2/user/income/list"]
    p = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 200}
    for ep in eps:
        j = bingx_req(ep, p)
        if not j:
            continue
        arr = data_list(j.get("data", j))
        if arr:
            return arr
    return []


# ===== Template Lock =====
def tpl_open(p: Dict[str, Any]) -> str:
    head = "📈 *포지션 오픈*" if p["side"] == "Long" else "📉 *포지션 오픈*"
    return (
        f"{head}\n"
        f"━━━━━━━━━━━━━━\n"
        f"BingX · {p['symbol']}\n"
        f"{p['side']} · {p['margin_mode']} · {int(round(p['leverage']))}x\n\n"
        f"*Entry*    : *{fmt_price(p['entry_price'])} USDT*\n"
        f"*Position* : *{fmt_qty(p['qty'])} {p['base']}*\n"
        f"Value      : {fmt_num(p['value'],2)} USDT\n"
        f"Margin     : {fmt_num(p['margin'],2)} USDT\n\n"
        f"uPnL : {sign(p['u_pnl'])} USDT ({pct(p['u_pnl_pct'])})\n"
        f"rPnL : {sign(p['r_pnl'])} USDT\n\n"
        f"🕒 {to_kst()}"
    )

# ✅ 요청 포맷 반영 (Entry 윗줄 유지)
def tpl_add(prev: Dict[str, Any], cur: Dict[str, Any]) -> str:
    return (
        f"➕ *포지션 추가 진입*\n"
        f"━━━━━━━━━━━━━━\n"
        f"BingX · {cur['symbol']}\n"
        f"{cur['side']} · {cur['margin_mode']} · {int(round(cur['leverage']))}x\n\n"
        f"*Entry*  : *{fmt_price(prev['entry_price'])}  →  {fmt_price(cur['entry_price'])} USDT*\n"
        f"Position : {fmt_qty(prev['qty'])} {cur['base']}  →  {fmt_qty(cur['qty'])} {cur['base']}\n"
        f"Value    : {fmt_num(prev['value'],2)}      →  {fmt_num(cur['value'],2)} USDT\n"
        f"Margin   : {fmt_num(prev['margin'],2)}       →  {fmt_num(cur['margin'],2)} USDT\n\n"
        f"uPnL : {sign(cur['u_pnl'])} USDT ({pct(cur['u_pnl_pct'])})\n"
        f"rPnL : {sign(cur['r_pnl'])} USDT\n\n"
        f"🕒 {to_kst()}"
    )

# ✅ 포지션 감소 알림 추가 (같은 레이아웃)
def tpl_reduce(prev: Dict[str, Any], cur: Dict[str, Any]) -> str:
    return (
        f"➖ *포지션 감소*\n"
        f"━━━━━━━━━━━━━━\n"
        f"BingX · {cur['symbol']}\n"
        f"{cur['side']} · {cur['margin_mode']} · {int(round(cur['leverage']))}x\n\n"
        f"*Entry*  : *{fmt_price(prev['entry_price'])}  →  {fmt_price(cur['entry_price'])} USDT*\n"
        f"Position : {fmt_qty(prev['qty'])} {cur['base']}  →  {fmt_qty(cur['qty'])} {cur['base']}\n"
        f"Value    : {fmt_num(prev['value'],2)}      →  {fmt_num(cur['value'],2)} USDT\n"
        f"Margin   : {fmt_num(prev['margin'],2)}       →  {fmt_num(cur['margin'],2)} USDT\n\n"
        f"uPnL : {sign(cur['u_pnl'])} USDT ({pct(cur['u_pnl_pct'])})\n"
        f"rPnL : {sign(cur['r_pnl'])} USDT\n\n"
        f"🕒 {to_kst()}"
    )

def tpl_close(sess: Dict[str, Any], close_price: float, closed: float, fee: float, realized: float) -> str:
    st = iso_parse(sess.get("start_ts", "")) or now_kst()
    en = now_kst()
    period = f"{st.astimezone(KST).strftime('%m-%d %H:%M')} ~ {en.astimezone(KST).strftime('%H:%M')} (KST)"
    return (
        f"✅ *포지션 종료*\n"
        f"━━━━━━━━━━━━━━\n"
        f"BingX · {sess['symbol']}\n"
        f"{sess['side']} · {sess['margin_mode']} · {int(round(sess['leverage']))}x\n\n"
        f"기간       : {period}\n"
        f"진입가     : {fmt_price(sess['entry_price_init'])} USDT\n"
        f"종료가     : {fmt_price(close_price)} USDT\n\n"
        f"총 진입금액 : {fmt_num(sess['total_entry_value'],2)} USDT\n"
        f"총 종료금액 : {fmt_num(sess['total_exit_value'],2)} USDT\n\n"
        f"Closed PnL : {sign(closed)} USDT\n"
        f"Fee+Funding: {sign(fee)} USDT\n"
        f"*Realized   : {sign(realized)} USDT*\n\n"
        f"🕒 {to_kst(en)}"
    )

def tpl_barcode(side, symbol, price, tf, ts):
    title = "🟢🐳 *바코드 · 매수(Long)*" if side == "buy" else "🔴🐳 *바코드 · 매도(Short)*"
    return f"{title}\n{symbol} | {price} | {tf}\n_\"바코드 신호는 보조근거로 활용하시길 권장드립니다.\"_\n🕒 {to_kst(ts)}"

# ✅ CHANGE: zone(구간 번호) 반영. zone 없으면 "" → "구간  지지/저항" 형태로 공백 유지
def tpl_prism(side, symbol, lo, hi, zone, ts):
    zone = "" if zone is None else str(zone)
    title = f"🟢 *구간 {zone} 지지 준비 (Prism)* 🟢 " if side == "buy" else f"🔴 *구간 {zone} 저항 준비 (Prism)* 🔴 "
    return f"{title}\n{symbol} | {lo} ~ {hi}\n_\"분할 진입을 권장드립니다.\"_ \n🕒 {to_kst(ts)}"

def tpl_rsi(side, symbol, price, tf, fire, ts):
    title = f"🟢🤖 *RSI · 매수(Long) · {fire}*" if side == "buy" else f"🔴🤖 *RSI · 매도(Short) · {fire}*"
    return f"{title}\n{symbol} | {price} | {tf}\n_\"RSI 신호는 보조근거로 활용하시길 권장드립니다.\"_ \n🕒 {to_kst(ts)}"

# ⚠️ 요청사항 유지: 판테라 문구/노랑별/노랑 표기 절대 유지
def tpl_panterra(side, symbol, price, tf, ts):
    strategy = "PanTerra"
    if side == "buy":
        return (
            f"*🟢🐳[ 매수(Long) 알림 ] ({strategy})🐳*\n"
            f"{symbol} | {price} | {tf}\n"
            f"*지표* : {strategy}\n"
            f"*시그널* : 파랑별(매수)\n"
            f"_\"첫 시그널 이후 30분간 동일 방향 알림이 오지 않습니다.\n"
            f"동일 비중 분할 진입 / 역방향 시그널 시 포지션 종료 후 재진입을 권장드립니다.\"_\n"
            f"🕒 {to_kst(ts)}"
        )
    return (
        f"*🔴🐳[ 매도(Short) 알림 ] ({strategy})🐳*\n"
        f"{symbol} | {price} | {tf}\n"
        f"*지표* : {strategy}\n"
        f"*시그널* : 노랑별(매도)\n"
        f"_\"첫 시그널 이후 30분간 동일 방향 알림이 오지 않습니다.\n"
        f"동일 비중 분할 진입 / 역방향 시그널 시 포지션 종료 후 재진입을 권장드립니다.\"_\n"
        f"🕒 {to_kst(ts)}"
    )

HELP_TEXT = (
    "🧭 *도움말 (/help)*\n"
    "━━━━━━━━━━━━━━\n"
    "/status - 스위치/리포트 상태 확인\n"
    "/help - 도움말\n\n"
    "/sig_on - 시그널 그룹 알림 ON\n"
    "/sig_off - 시그널 그룹 알림 OFF\n"
    "/pos_on - 포지션 그룹 알림 ON\n"
    "/pos_off - 포지션 그룹 알림 OFF\n\n"
    "/report_summary - 당일 요약 리포트\n"
    "/report_detail - 당일 상세 리포트\n"
    "/report - 당일 요약 리포트\n"
    "/report YYYY-MM-DD - 해당일 요약 리포트\n\n"
    "/report_auto_on - 자동 리포트 ON(매일 23:50)\n"
    "/report_auto_off - 자동 리포트 OFF\n"
    "/report_auto_status - 자동 리포트 상태\n\n"
    "/say 내용 - 포지션 수신방 공지\n"
    "/say_sig 내용 - 시그널 수신방 공지\n"
    "/say_pos 내용 - 포지션 수신방 공지\n"
    "/switch_logs [N] - 최근 스위치 로그\n"
    "/pos_snapshot - 현재 포지션 스냅샷\n"
    "/state_reset - 내부 상태 초기화(주의)\n\n"
    "🕒 {now}"
)


# ===== Signal Parse =====
def _parse_plain_text_payload(raw: str) -> Dict[str, Any]:
    """
    TradingView가 text/plain으로 보내는 경우 대응
    - JSON 문자열
    - querystring(key=value&...)
    - 라인/쉼표 기반 key:value 또는 key=value
    """
    out: Dict[str, Any] = {}
    raw = (raw or "").strip()
    if not raw:
        return out

    # 1) JSON 문자열
    try:
        j = json.loads(raw)
        if isinstance(j, dict):
            return j
    except:
        pass

    # 2) querystring
    if "=" in raw and "&" in raw and "\n" not in raw:
        try:
            qs = parse_qs(raw, keep_blank_values=True)
            for k, v in qs.items():
                if v:
                    out[k] = v[-1]
            if out:
                out["text"] = raw
                return out
        except:
            pass

    # 3) line/csv key:value or key=value
    lines: List[str] = []
    for block in raw.splitlines():
        block = block.strip()
        if not block:
            continue
        parts = [p.strip() for p in block.split(",")] if (":" in block or "=" in block) else [block]
        lines.extend([p for p in parts if p])

    kv_pat = re.compile(r'^\s*([A-Za-z0-9_.\-]+)\s*[:=]\s*(.+?)\s*$')
    for ln in lines:
        m = kv_pat.match(ln)
        if m:
            k, v = m.group(1), m.group(2)
            out[k] = v.strip().strip('"').strip("'")

    out["text"] = raw
    return out

def parse_tv_payload(req) -> Dict[str, Any]:
    # JSON 우선
    j = req.get_json(silent=True)
    if isinstance(j, dict):
        return j

    # form-data 대응
    try:
        if req.form:
            d = dict(req.form)
            if d:
                return d
    except:
        pass

    # text/plain 대응
    raw = (req.data or b"").decode("utf-8", errors="ignore")
    p = _parse_plain_text_payload(raw)
    if p:
        return p
    return {}

# --- 가격 1 오인 방지 보조 ---
_TF_NUM_CANDIDATES = {"1", "3", "5", "10", "15", "30", "45", "60", "120", "180", "240", "360", "720", "1440"}

def _is_num_token(v: Any) -> bool:
    if v is None:
        return False
    s = str(v).strip().replace(",", "")
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", s))

def _tf_num(tf: str) -> str:
    return re.sub(r"\D", "", str(tf or ""))

def _looks_like_tf_multiplier(v: Any, tf: str) -> bool:
    s = str(v).strip()
    n = _tf_num(tf)
    return s.isdigit() and n and s == n and s in _TF_NUM_CANDIDATES

def _extract_symbol_from_text(raw_text: str) -> Optional[str]:
    t = (raw_text or "").upper()
    m = re.search(r'([A-Z0-9]+[:·])?([A-Z]{2,20}(?:USDT|USD)(?:\.P|PERP)?)', t)
    if not m:
        return None
    return m.group(0)

def _extract_tf_from_text(raw_text: str) -> Optional[str]:
    t = (raw_text or "").lower()
    m = re.search(r'\b(\d+\s*[mhdw])\b', t)
    if m:
        return m.group(1).replace(" ", "")
    return None

def _extract_price_from_text(raw_text: str, tf: str) -> str:
    t = (raw_text or "").strip()
    if not t:
        return "-"

    # 1) "symbol | price | tf" 패턴 우선
    if "|" in t:
        parts = [p.strip() for p in t.split("|")]
        if len(parts) >= 3:
            mid = parts[1]
            right = parts[2].lower().replace(" ", "")
            if _is_num_token(mid):
                if not (_looks_like_tf_multiplier(mid, tf) and re.fullmatch(r"\d+[mhdw]", right)):
                    return mid

    # 2) 소수점 가격 우선
    m_dec = re.search(r'(?<!\d)(\d+\.\d+)(?!\d)', t.replace(",", ""))
    if m_dec:
        return m_dec.group(1)

    # 3) 정수는 3자리 이상만 가격으로 인정
    m_int = re.search(r'(?<!\d)(\d{3,})(?!\d)', t.replace(",", ""))
    if m_int:
        return m_int.group(1)

    return "-"

def _extract_prism_zone(payload: Dict[str, Any], raw_text: str) -> str:
    """
    ✅ CHANGE: Prism '구간 N' 숫자 추출
    - 없으면 "" (요청: 기본값 4 금지, 공백 유지)
    """
    # 1) payload 기반 후보
    for k in ["zone", "zone_no", "zone_num", "level", "lvl", "step", "segment", "stage", "section", "area"]:
        if k in payload:
            v = str(payload.get(k) or "").strip()
            if v.isdigit():
                return v

    # 2) text 기반: "구간6", "구간 6"
    t = (raw_text or "")
    m = re.search(r"구간\s*(\d+)", t)
    if m:
        return m.group(1)

    # 3) 영문도 혹시
    m2 = re.search(r"\bzone\s*(\d+)\b", t, flags=re.IGNORECASE)
    if m2:
        return m2.group(1)

    return ""

# ✅ (추가) Prism 레인지 텍스트 파싱: "65777.8 ~ 65811.9"
def _extract_range_from_text(raw_text: str) -> Tuple[str, str]:
    t = (raw_text or "").replace(",", "")
    m = re.search(r'(?<!\d)(\d+(?:\.\d+)?)[ ]*~[ ]*(\d+(?:\.\d+)?)(?!\d)', t)
    if not m:
        return "-", "-"
    return m.group(1), m.group(2)

def infer_signal(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw_text = str(payload.get("text", "") or payload.get("message", "") or payload.get("comment", "") or "")

    symbol = payload.get("symbol") or payload.get("ticker") or payload.get("pair") or payload.get("market") or payload.get("instrument") or ""
    tf = str(payload.get("interval") or payload.get("timeframe") or payload.get("tf") or payload.get("period") or "")
    action = str(payload.get("action") or payload.get("side") or payload.get("signal") or payload.get("order_action") or "").lower()

    blob = " | ".join(
        [
            str(payload.get("strategy", "")),
            str(payload.get("strategy_name", "")),
            str(payload.get("indicator", "")),
            str(payload.get("title", "")),
            str(payload.get("name", "")),
            str(payload.get("message", "")),
            str(payload.get("comment", "")),
            raw_text,
            str(payload),
        ]
    ).lower()

    kind = "unknown"
    if ("barcode" in blob) or ("바코드" in blob):
        kind = "barcode"
    # ✅ CHANGE: "구간\s*[1-9]" 문자열 포함이 아니라 정규식으로 안정 판별
    elif ("prism" in blob) or ("프리즘" in blob) or ("지지 준비" in blob) or ("저항 준비" in blob) or re.search(r"구간\s*\d+", blob):
        kind = "prism"
    elif ("rsi" in blob) or re.search(r"\brsi\b", blob):
        kind = "rsi"
    elif ("panterra" in blob) or ("판테라" in blob) or re.search(r"pan\s*terra", blob):
        kind = "panterra"

    side = "buy"
    if any(x in blob for x in ["short", "sell", "매도", "노랑별", "저항", "숏"]):
        side = "sell"
    if any(x in action for x in ["sell", "short"]):
        side = "sell"
    if any(x in action for x in ["buy", "long"]):
        side = "buy"

    # symbol 보강
    ss = str(symbol).strip() if symbol else ""
    if not ss:
        ex_sym = _extract_symbol_from_text(raw_text)
        if ex_sym:
            ss = ex_sym
    if not ss:
        ss = "BYBIT·BTCUSDT.P"

    # tf 보강
    if tf in ("", "None", "none"):
        ex_tf = _extract_tf_from_text(raw_text)
        if ex_tf:
            tf = ex_tf
    if not tf:
        tf = "1m"

    # price: 신뢰 높은 필드 우선
    price = "-"
    for k in ["close", "last", "mark", "entry", "open", "high", "low", "price"]:
        if k not in payload:
            continue
        v = payload.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s == "" or s.lower() in ("none", "null", "nan", "-"):
            continue
        if not _is_num_token(s):
            continue
        if k == "price" and _looks_like_tf_multiplier(s, tf):
            continue
        price = s
        break

    # 필드에서 못 찾으면 text에서 보강
    if price == "-":
        price = _extract_price_from_text(raw_text, tf)

    # ===== 여기만 Prism 레인지 보강 수정(그 외 로직 변경 없음) =====
    lo = payload.get("low") or payload.get("support_low") or payload.get("zone_low") or payload.get("from") or payload.get("min")
    hi = payload.get("high") or payload.get("support_high") or payload.get("zone_high") or payload.get("to") or payload.get("max")

    if kind == "prism":
        # Prism은 메시지 텍스트에 "65777.8 ~ 65811.9"가 들어오는 케이스가 많아서 우선 보강
        tlo, thi = _extract_range_from_text(raw_text)
        if (lo is None or str(lo).strip() in ("", "-", "None", "none")) and tlo != "-":
            lo = tlo
        if (hi is None or str(hi).strip() in ("", "-", "None", "none")) and thi != "-":
            hi = thi

    if lo is None:
        lo = payload.get("zone1") or payload.get("price1") or "-"
    if hi is None:
        hi = payload.get("zone2") or payload.get("price2") or "-"

    fire = "🔥🔥" if str(payload.get("fire") or payload.get("strength") or payload.get("level") or "").strip() in ("2", "high", "strong", "🔥🔥") else "🔥"

    if "·" not in ss and (ss.endswith(".P") or "USDT" in ss.upper()):
        ss = f"BYBIT·{ss}"

    zone = _extract_prism_zone(payload, raw_text) if kind == "prism" else ""

    return {"kind": kind, "side": side, "symbol": str(ss), "tf": tf, "price": price, "low": lo, "high": hi, "fire": fire, "zone": zone}

def panterra_throttle(symbol: str, side: str, sec=1800) -> bool:
    k = f"throttle:panterra:{symbol}:{side}"
    now = int(time.time())

    # Redis 우선
    last = R.get(k)
    if last is not None:
        try:
            if now - int(last) < sec:
                return True
        except:
            pass
        R.set(k, str(now))
        return False

    # Redis 없을 때 메모리 fallback
    last_mem = MEM_THROTTLE.get(k)
    if last_mem is not None and (now - int(last_mem) < sec):
        return True
    MEM_THROTTLE[k] = now
    return False

def build_signal_msg(payload: Dict[str, Any]) -> Optional[str]:
    f = infer_signal(payload)
    ts = now_kst()
    if f["kind"] == "barcode":
        return tpl_barcode(f["side"], f["symbol"], f["price"], f["tf"], ts)
    if f["kind"] == "prism":
        return tpl_prism(f["side"], f["symbol"], f["low"], f["high"], f.get("zone", ""), ts)
    if f["kind"] == "rsi":
        return tpl_rsi(f["side"], f["symbol"], f["price"], f["tf"], f["fire"], ts)
    if f["kind"] == "panterra":
        if panterra_throttle(f["symbol"], f["side"]):
            return None
        return tpl_panterra(f["side"], f["symbol"], f["price"], f["tf"], ts)
    return None


# ===== Position state =====
def pkey(symbol: str, side: str) -> str:
    return f"{symbol}|{side}"

def open_state() -> Dict[str, Dict[str, Any]]:
    return rget_json("state:open_positions", {})

def save_open_state(m: Dict[str, Dict[str, Any]]):
    rset_json("state:open_positions", m)

def init_done() -> bool:
    return (R.get("state:init_done") or "") == "1"

def mark_init_done():
    R.set("state:init_done", "1")

def sess_get(k: str) -> Dict[str, Any]:
    return rget_json(f"sess:position:{k}", {})

def sess_set(k: str, v: Dict[str, Any]):
    rset_json(f"sess:position:{k}", v)

def sess_del(k: str):
    R.delete(f"sess:position:{k}")

def hist_key(d: str) -> str:
    return f"history:trades:{d}"

def hist_push(tr: Dict[str, Any]):
    d = (iso_parse(tr.get("close_ts", "")) or now_kst()).astimezone(KST).strftime("%Y-%m-%d")
    rpush_json(hist_key(d), tr, keep=5000)

def hist_list(d: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for s in R.lrange(hist_key(d), 0, 5000):
        j = pjson(s, None)
        if isinstance(j, dict):
            out.append(j)
    return out


# ====== ✅ 수정(1): income 기반 분해/정산 함수 추가 + fee_calc 교체 (그 외 로직/포맷 영향 없음) ======
def _income_split(symbol: str, st: datetime, en: datetime) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    ✅ 거래소 정산(income) 기반으로
    - closed_pnl(정산 손익)
    - fee_funding(수수료+펀딩)
    - realized(최종 실현손익)
    를 최대한 안정적으로 분해해서 리턴

    실패(None)면 호출부에서 기존 추정치 로직으로 fallback
    """
    recs = fetch_income(symbol, int(st.astimezone(UTC).timestamp() * 1000), int(en.astimezone(UTC).timestamp() * 1000))
    if not recs:
        return None, None, None

    closed_sum = 0.0
    fee_funding_sum = 0.0
    realized_sum = 0.0

    # ✅ 혹시 API가 심볼 필터를 무시/느슨하게 처리하는 경우가 있어서
    #    응답에 섞여 들어온 다른 심볼 income을 2차로 걸러줌 (Realized 튐 방지)
    req_norm = re.sub(r"[^A-Za-z0-9]", "", symbol).upper()

    for r in recs:
        rec_sym = str(r.get("symbol") or r.get("contract") or r.get("ticker") or "").strip()
        if rec_sym:
            rec_norm = re.sub(r"[^A-Za-z0-9]", "", rec_sym).upper()
            if req_norm and rec_norm and (req_norm not in rec_norm and rec_norm not in req_norm):
                continue

        typ = str(r.get("incomeType") or r.get("type") or r.get("bizType") or r.get("income_type") or "").lower()
        inc = asf(r.get("income") or r.get("profit") or r.get("amount") or r.get("realizedPnl") or 0)

        # income 원본 합 = 최종 실현(거래소 정산에 가장 가까운 기준)
        realized_sum += inc

        # 수수료/펀딩은 타입 키워드로 최대한 분리
        if any(x in typ for x in ["commission", "fee", "funding", "fund"]):
            fee_funding_sum += inc
            continue

        # 나머지는 손익으로 귀속(거래소 타입명이 달라도 보통 손익성 항목)
        closed_sum += inc

    return closed_sum, fee_funding_sum, realized_sum


def fee_calc(symbol: str, st: datetime, en: datetime, closed: float, entry_v: float, exit_v: float) -> Tuple[float, float, float]:
    """
    ✅ CLOSE 정산값을 서로 일치시키기 위한 계산
    - return: (closed_pnl, fee_funding, realized)
    """
    c, ff, real = _income_split(symbol, st, en)
    if c is not None:
        # income 기반 확정
        return c, ff, (c + ff)

    # income 못 가져오면 기존 추정 fallback
    ff = -(abs(entry_v) + abs(exit_v)) * TAKER_FEE_RATE
    return closed, ff, closed + ff
# ====== ✅ 수정(1) 끝 ======



def send_signal_alert(text: str):
    sent = 0
    for cid in CHAT_IDS:
        if (not is_group(cid)) or sw_get("signal", cid) == "1":
            if tg_send(BOT_TOKEN, cid, text):
                sent += 1
    logging.info("signal alert sent=%s/%s", sent, len(CHAT_IDS))

def send_signal_alert_plain(text: str):
    """
    ✅ CHANGE: kind 미정(unknown) 원문 그대로 발송(포맷/마크다운 없음)
    """
    sent = 0
    for cid in CHAT_IDS:
        if (not is_group(cid)) or sw_get("signal", cid) == "1":
            if tg_send_plain(BOT_TOKEN, cid, text):
                sent += 1
    logging.info("signal alert plain sent=%s/%s", sent, len(CHAT_IDS))

def send_pos_alert(text: str):
    sent = 0
    for cid in CHAT_IDS_POSITION:
        if (not is_group(cid)) or sw_get("position", cid) == "1":
            if tg_send(BOT_TOKEN_POSITION, cid, text):
                sent += 1
    logging.info("position alert sent=%s/%s", sent, len(CHAT_IDS_POSITION))

# -----------------------
# 이하 (포지션/리포트/명령/라우트/부트스트랩) 원본 그대로
# 단, ✅ 수정(2): OPEN/ADD/REDUCE rPnL 표시만 income으로 보강
# -----------------------

def process_positions(send_alert=True) -> Dict[str, Any]:
    cur: Dict[str, Dict[str, Any]] = {}
    for p in fetch_positions():
        cur[pkey(p["symbol"], p["side"])] = p
    prev = open_state()

    # ====== ✅ 수정(2): rPnL 표시를 세션 시작 이후 income 합계로 보강 (표시만, 로직/포맷 불변) ======
    def _rpnL_since(symbol: str, start_iso: str) -> Optional[float]:
        st = iso_parse(start_iso) or now_kst()
        en = now_kst()
        c, ff, real = _income_split(symbol, st, en)
        if real is None:
            return None
        return real
    # ====== ✅ 수정(2) 끝 ======

    if not init_done():
        for k, p in cur.items():
            sess_set(
                k,
                {
                    "symbol": p["symbol"],
                    "side": p["side"],
                    "base": p["base"],
                    "margin_mode": p["margin_mode"],
                    "leverage": p["leverage"],
                    "start_ts": now_kst().isoformat(),
                    "entry_price_init": p["entry_price"],
                    "last_entry_price": p["entry_price"],
                    "total_entry_value": p["value"],
                    "total_exit_value": 0.0,
                    "last_qty": p["qty"],
                    "last_mark_price": p["mark_price"],
                    "last_r_pnl": p["r_pnl"],
                },
            )
        save_open_state(cur)
        mark_init_done()
        return {"ok": True, "initial_sync": True, "positions_now": len(cur), "events": {"open": 0, "add": 0, "reduce": 0, "close": 0}, "closed_trades": []}

    events = {"open": 0, "add": 0, "reduce": 0, "close": 0}
    closed_rows: List[Dict[str, Any]] = []

    # open/add/reduce
    for k, p in cur.items():
        o = prev.get(k)
        if not o:
            events["open"] += 1
            start_iso = now_kst().isoformat()
            sess_set(
                k,
                {
                    "symbol": p["symbol"],
                    "side": p["side"],
                    "base": p["base"],
                    "margin_mode": p["margin_mode"],
                    "leverage": p["leverage"],
                    "start_ts": start_iso,
                    "entry_price_init": p["entry_price"],
                    "last_entry_price": p["entry_price"],
                    "total_entry_value": p["value"],
                    "total_exit_value": 0.0,
                    "last_qty": p["qty"],
                    "last_mark_price": p["mark_price"],
                    "last_r_pnl": p["r_pnl"],
                },
            )
            if send_alert:
                # ✅ rPnL 표시 보강(알림 표시만)
                p_show = dict(p)
                rp = _rpnL_since(p.get("symbol", ""), start_iso)
                if rp is not None:
                    p_show["r_pnl"] = rp
                send_pos_alert(tpl_open(p_show))
        else:
            q0, q1 = asf(o.get("qty")), asf(p.get("qty"))
            s = sess_get(k) or {
                "symbol": p["symbol"],
                "side": p["side"],
                "base": p["base"],
                "margin_mode": p["margin_mode"],
                "leverage": p["leverage"],
                "start_ts": now_kst().isoformat(),
                "entry_price_init": p["entry_price"],
                "total_entry_value": p["value"],
                "total_exit_value": 0.0,
            }

            if q1 > q0 + 1e-12:
                events["add"] += 1
                dv = max(asf(p.get("value")) - asf(o.get("value")), (q1 - q0) * asf(p.get("entry_price")))
                s["total_entry_value"] = asf(s.get("total_entry_value")) + max(dv, 0.0)
                if send_alert:
                    # ✅ rPnL 표시 보강(알림 표시만)
                    p_show = dict(p)
                    rp = _rpnL_since(p.get("symbol", ""), str(s.get("start_ts", "")))
                    if rp is not None:
                        p_show["r_pnl"] = rp
                    send_pos_alert(tpl_add(o, p_show))

            elif q1 + 1e-12 < q0:
                events["reduce"] += 1
                rq = q0 - q1
                rv = max(asf(o.get("value")) - asf(p.get("value")), rq * asf(p.get("mark_price")))
                s["total_exit_value"] = asf(s.get("total_exit_value")) + max(rv, 0.0)
                if send_alert:
                    # ✅ rPnL 표시 보강(알림 표시만)
                    p_show = dict(p)
                    rp = _rpnL_since(p.get("symbol", ""), str(s.get("start_ts", "")))
                    if rp is not None:
                        p_show["r_pnl"] = rp
                    send_pos_alert(tpl_reduce(o, p_show))

            s.update(
                {
                    "last_qty": q1,
                    "last_mark_price": p["mark_price"],
                    "last_entry_price": p["entry_price"],
                    "last_r_pnl": p["r_pnl"],
                    "margin_mode": p["margin_mode"],
                    "leverage": p["leverage"],
                }
            )
            sess_set(k, s)

    # close
    for k, o in prev.items():
        if k in cur:
            continue
        events["close"] += 1
        s = sess_get(k) or {
            "symbol": o.get("symbol"),
            "side": o.get("side"),
            "base": o.get("base", base_asset(o.get("symbol", ""))),
            "margin_mode": o.get("margin_mode", "Isolated"),
            "leverage": o.get("leverage", 1),
            "start_ts": now_kst().isoformat(),
            "entry_price_init": o.get("entry_price", 0),
            "total_entry_value": o.get("value", 0),
            "total_exit_value": 0.0,
        }

        remain_qty = asf(o.get("qty"))
        close_p = asf(o.get("mark_price") or o.get("entry_price"))
        s["total_exit_value"] = asf(s.get("total_exit_value")) + max(remain_qty * close_p, 0.0)

        tv_in, tv_out = asf(s.get("total_entry_value")), asf(s.get("total_exit_value"))
        closed = (tv_out - tv_in) if s.get("side") == "Long" else (tv_in - tv_out)
        st = iso_parse(s.get("start_ts", "")) or now_kst()
        en = now_kst()
        closed_pnl, fee, real = fee_calc(s.get("symbol", ""), st, en, closed, tv_in, tv_out)

        row = {
            "symbol": s.get("symbol"),
            "side": s.get("side"),
            "start_ts": s.get("start_ts"),
            "close_ts": en.isoformat(),
            "entry_price": asf(s.get("entry_price_init")),
            "close_price": close_p,
            "total_entry_value": tv_in,
            "total_exit_value": tv_out,
            "closed_pnl": float(f"{closed_pnl:.8f}"),
            "fee_funding": float(f"{fee:.8f}"),
            "realized": float(f"{real:.8f}"),
            "margin_mode": s.get("margin_mode", "Isolated"),
            "leverage": s.get("leverage", 1),
        }
        hist_push(row)
        closed_rows.append(row)

        if send_alert:
            send_pos_alert(
                tpl_close(
                    {
                        "symbol": row["symbol"],
                        "side": row["side"],
                        "margin_mode": row["margin_mode"],
                        "leverage": row["leverage"],
                        "start_ts": row["start_ts"],
                        "entry_price_init": row["entry_price"],
                        "total_entry_value": row["total_entry_value"],
                        "total_exit_value": row["total_exit_value"],
                    },
                    row["close_price"],
                    row["closed_pnl"],
                    row["fee_funding"],
                    row["realized"],
                )
            )
        sess_del(k)

    save_open_state(cur)
    return {"ok": True, "positions_now": len(cur), "events": events, "closed_trades": closed_rows}

# ===== Report =====
def rows_until(date_str: str, end_dt: datetime) -> List[Dict[str, Any]]:
    rows = hist_list(date_str)
    out: List[Dict[str, Any]] = []
    start = datetime.strptime(date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    today = now_kst().strftime("%Y-%m-%d")
    end_limit = (
        end_dt.astimezone(KST)
        if date_str == today
        else datetime.strptime(date_str + " 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    )
    for r in rows:
        c = iso_parse(r.get("close_ts", ""))
        if c and start <= c.astimezone(KST) <= end_limit:
            out.append(r)
    return out

def report_summary_text(date_str: str, now_dt: datetime, rows: List[Dict[str, Any]]) -> str:
    total = len(rows)
    win = sum(1 for r in rows if (asf(r.get("closed_pnl")) + asf(r.get("fee_funding"))) > 0)
    lose = total - win
    wr = (win / total * 100) if total else 0

    s_closed = sum(asf(r.get("closed_pnl")) for r in rows)
    s_fee = sum(asf(r.get("fee_funding")) for r in rows)
    s_real = s_closed + s_fee

    cnt: Dict[str, int] = {}
    for r in rows:
        sym = str(r.get("symbol", ""))
        if sym:
            cnt[sym] = cnt.get(sym, 0) + 1
    sym_text = ", ".join([f"{k}({v})" for k, v in sorted(cnt.items())]) if cnt else "-"

    st = f"{date_str[5:]} 00:00"
    en = now_dt.astimezone(KST).strftime("%H:%M")
    return (
        f"📊 *일일 요약 리포트*\n"
        f"━━━━━━━━━━━━━━\n"
        f"기간 : {st} ~ {en} (KST)\n\n"
        f"거래종목 : {sym_text}\n"
        f"총 거래  : {total}회 (승 {win} / 패 {lose}, 승률 *{wr:.2f}%*)\n\n"
        f"합계 Closed PnL : {sign(s_closed)} USDT\n"
        f"합계 Fee+Funding: {sign(s_fee)} USDT\n"
        f"*총 Realized     : {sign(s_real)} USDT*\n\n"
        f"🕒 {to_kst(now_dt)}"
    )

def report_detail_text(date_str: str, now_dt: datetime, rows: List[Dict[str, Any]]) -> str:
    st = f"{date_str[5:]} 00:00"
    en = now_dt.astimezone(KST).strftime("%H:%M")
    s_closed = sum(asf(r.get("closed_pnl")) for r in rows)
    s_fee = sum(asf(r.get("fee_funding")) for r in rows)
    s_real = s_closed + s_fee

    p = ["📑 *일일 상세 리포트*", "━━━━━━━━━━━━━━", f"기간 : {st} ~ {en} (KST)", ""]
    if not rows:
        p += [
            "해당 기간 거래 내역이 없습니다.",
            "",
            "━━━━━━━━━━━━━━",
            f"합계 Closed PnL : {sign(0)} USDT",
            f"합계 Fee+Funding: {sign(0)} USDT",
            f"*총 Realized     : {sign(0)} USDT*",
            f"🕒 {to_kst(now_dt)}",
        ]
        return "\n".join(p)

    for r in sorted(rows, key=lambda x: x.get("close_ts", "")):
        sd = iso_parse(r.get("start_ts", "")) or now_dt
        cd = iso_parse(r.get("close_ts", "")) or now_dt
        per = f"{sd.astimezone(KST).strftime('%m-%d %H:%M')} ~ {cd.astimezone(KST).strftime('%H:%M')} (KST)"
        p += [
            f"✅ {r.get('symbol','')} ({r.get('side','')})",
            f"기간       : {per}",
            f"진입가     : {fmt_price(asf(r.get('entry_price')))} USDT",
            f"종료가     : {fmt_price(asf(r.get('close_price')))} USDT",
            f"총 진입금액 : {fmt_num(asf(r.get('total_entry_value')),2)} USDT",
            f"총 종료금액 : {fmt_num(asf(r.get('total_exit_value')),2)} USDT",
            f"Closed PnL : {sign(asf(r.get('closed_pnl')))} USDT",
            f"Fee+Funding: {sign(asf(r.get('fee_funding')))} USDT",
            f"*Realized   : {sign((asf(r.get('closed_pnl')) + asf(r.get('fee_funding'))))} USDT*",
            "",
        ]

    p += [
        "━━━━━━━━━━━━━━",
        f"합계 Closed PnL : {sign(s_closed)} USDT",
        f"합계 Fee+Funding: {sign(s_fee)} USDT",
        f"*총 Realized     : {sign(s_real)} USDT*",
        f"🕒 {to_kst(now_dt)}",
    ]
    return "\n".join(p)

def send_report_summary(chat_id: str, date_str: Optional[str] = None):
    now = now_kst()
    date_str = date_str or now.strftime("%Y-%m-%d")
    tg_send(BOT_TOKEN_POSITION, chat_id, report_summary_text(date_str, now, rows_until(date_str, now)))

def send_report_detail(chat_id: str, date_str: Optional[str] = None):
    now = now_kst()
    date_str = date_str or now.strftime("%Y-%m-%d")
    tg_send_chunk(BOT_TOKEN_POSITION, chat_id, report_detail_text(date_str, now, rows_until(date_str, now)))

def maybe_auto_report() -> Dict[str, Any]:
    """
    ✅ 자동 리포트: 매일 23:50에만 1회 발송
    """
    cfg_init()
    if cfg_get("report_auto", "off").lower() != "on":
        return {"sent": False, "reason": "auto_off"}

    hour = asi(cfg_get("report_auto_hour", str(REPORT_AUTO_HOUR_DEFAULT)), REPORT_AUTO_HOUR_DEFAULT)
    minute = asi(cfg_get("report_auto_minute", str(REPORT_AUTO_MINUTE_DEFAULT)), REPORT_AUTO_MINUTE_DEFAULT)

    now = now_kst()
    if now.hour != hour or now.minute != minute:
        return {"sent": False, "reason": "not_target_time"}

    slot = now.strftime("%Y-%m-%d %H:%M")
    if cfg_get("report_auto_last_slot", "") == slot:
        return {"sent": False, "reason": "already_sent"}

    chat = cfg_get("report_auto_chat", "")
    if not chat:
        return {"sent": False, "reason": "no_target_chat"}

    send_report_summary(chat, now.strftime("%Y-%m-%d"))
    cfg_set("report_auto_last_slot", slot)
    return {"sent": True, "slot": slot, "chat_id": chat}


# ===== Command =====
def parse_cmd(text: str) -> Tuple[str, str]:
    t = (text or "").strip()
    if not t.startswith("/"):
        return "", ""
    first, *rest = t.split(maxsplit=1)
    return first.split("@", 1)[0].lower(), (rest[0].strip() if rest else "")

def status_text() -> str:
    cfg_init()
    h = cfg_get("report_auto_hour", str(REPORT_AUTO_HOUR_DEFAULT))
    m = cfg_get("report_auto_minute", str(REPORT_AUTO_MINUTE_DEFAULT))
    lines = ["🧾 *현재 상태 (/status)*", "━━━━━━━━━━━━━━"]
    for cid in CHAT_IDS:
        if is_group(cid):
            lines.append(f"Signal Group : {cid} : {'ON' if sw_get('signal',cid)=='1' else 'OFF'}")
    for cid in CHAT_IDS_POSITION:
        if is_group(cid):
            lines.append(f"Position Group : {cid} : {'ON' if sw_get('position',cid)=='1' else 'OFF'}")
    lines += [
        "",
        f"Report Auto : {cfg_get('report_auto','off').upper()} (매일 {int(h):02d}:{int(m):02d})",
        f"Report Chat : {cfg_get('report_auto_chat','-')}",
        "",
        f"🕒 {to_kst()}",
    ]
    return "\n".join(lines)

def toggle_groups(kind: str, on: bool) -> str:
    ids = CHAT_IDS if kind == "signal" else CHAT_IDS_POSITION
    gs = [c for c in ids if is_group(c)]
    if not gs:
        return "그룹 chat_id가 없습니다."
    for g in gs:
        sw_set(kind, g, on)
    return f"✅ {'시그널' if kind=='signal' else '포지션'} 그룹 알림을 *{'ON' if on else 'OFF'}* 으로 설정했어."

def switch_logs(n=10) -> str:
    n = max(1, min(50, n))
    raw = R.lrange("logs:switch", 0, n - 1)
    if not raw:
        return "최근 스위치 로그가 없어."
    lines = [f"🧾 *최근 스위치 로그 {len(raw)}건*", "━━━━━━━━━━━━━━"]
    for s in raw:
        j = pjson(s, {})
        dt = iso_parse(j.get("ts", "")) or now_kst()
        lines.append(f"- {hm(dt)} | {j.get('cmd','')} | uid:{j.get('uid','-')} | {j.get('note','')}")
    lines += ["", f"🕒 {to_kst()}"]
    return "\n".join(lines)

def snapshot_text() -> str:
    ps = fetch_positions()
    if not ps:
        return f"📭 현재 오픈 포지션이 없어.\n\n🕒 {to_kst()}"
    lines = ["📌 *현재 포지션 스냅샷*", "━━━━━━━━━━━━━━"]
    for p in ps:
        lines += [
            f"{p['symbol']} | {p['side']}",
            f"Entry {fmt_price(p['entry_price'])} | Pos {fmt_qty(p['qty'])} {p['base']}",
            f"uPnL {sign(p['u_pnl'])} ({pct(p['u_pnl_pct'])}) | rPnL {sign(p['r_pnl'])}",
            "",
        ]
    lines.append(f"🕒 {to_kst()}")
    return "\n".join(lines).strip()

def state_reset() -> str:
    R.delete("state:open_positions")
    R.delete("state:init_done")
    return f"⚠️ state:open_positions / state:init_done 초기화 완료\n🕒 {to_kst()}"

def handle_command(chat_id: str, uid: str, text: str) -> Optional[str]:
    cmd, arg = parse_cmd(text)
    if not cmd:
        return None
    if cmd == "/help":
        return HELP_TEXT.format(now=to_kst())
    if cmd == "/status":
        return status_text()

    # report 조회는 누구나 가능
    if cmd in ("/report_summary", "/report"):
        d = arg if re.match(r"^\d{4}-\d{2}-\d{2}$", arg or "") else None
        send_report_summary(chat_id, d)
        return None

    if cmd == "/report_detail":
        d = arg if re.match(r"^\d{4}-\d{2}-\d{2}$", arg or "") else None
        send_report_detail(chat_id, d)
        return None

    if cmd == "/report_auto_status":
        h = cfg_get("report_auto_hour", str(REPORT_AUTO_HOUR_DEFAULT))
        m = cfg_get("report_auto_minute", str(REPORT_AUTO_MINUTE_DEFAULT))
        return (
            "🧾 자동 리포트 상태\n━━━━━━━━━━━━━━\n"
            f"상태 : {cfg_get('report_auto','off').upper()}\n"
            f"시간 : {int(h):02d}:{int(m):02d} (KST)\n"
            f"대상 : {cfg_get('report_auto_chat','-')}\n"
            f"최근발송 : {cfg_get('report_auto_last_slot','-')}\n\n"
            f"🕒 {to_kst()}"
        )

    # 이하 관리자
    if not is_admin(uid):
        return "권한이 없어. (ADMIN_USER_IDS 확인)"

    if cmd == "/sig_on":
        switch_log(cmd, uid, "signal on")
        return toggle_groups("signal", True)
    if cmd == "/sig_off":
        switch_log(cmd, uid, "signal off")
        return toggle_groups("signal", False)
    if cmd == "/pos_on":
        switch_log(cmd, uid, "position on")
        return toggle_groups("position", True)
    if cmd == "/pos_off":
        switch_log(cmd, uid, "position off")
        return toggle_groups("position", False)

    if cmd == "/report_auto_on":
        cfg_set("report_auto", "on")
        cfg_set("report_auto_hour", "23")
        cfg_set("report_auto_minute", "50")
        return "✅ 자동 리포트 ON (매일 23:50, 요약본)"
    if cmd == "/report_auto_off":
        cfg_set("report_auto", "off")
        return "✅ 자동 리포트 OFF"

    if cmd in ("/say", "/say_pos"):
        m = (arg or "").strip()
        if not m:
            return "사용법: /say 내용"
        for cid in CHAT_IDS_POSITION:
            tg_send(BOT_TOKEN_POSITION, cid, m)
        return "✅ 포지션 수신방 공지 전송 완료"

    if cmd == "/say_sig":
        m = (arg or "").strip()
        if not m:
            return "사용법: /say_sig 내용"
        for cid in CHAT_IDS:
            tg_send(BOT_TOKEN, cid, m)
        return "✅ 시그널 수신방 공지 전송 완료"

    if cmd == "/switch_logs":
        n = int(arg) if (arg or "").isdigit() else 10
        return switch_logs(n)

    if cmd == "/pos_snapshot":
        return snapshot_text()
    if cmd == "/state_reset":
        return state_reset()
    if cmd == "/health_check":
        return f"ok\n🕒 {to_kst()}"

    return "알 수 없는 명령어야. /help 확인해줘."

def parse_update(update: Dict[str, Any]) -> Tuple[str, str, str]:
    m = update.get("message") or update.get("edited_message") or {}
    c = m.get("chat", {}) if isinstance(m, dict) else {}
    u = m.get("from", {}) if isinstance(m, dict) else {}
    return str(c.get("id", "")), str(u.get("id", "")), (m.get("text") or "").strip()


# ===== Routes =====
@app.route("/", methods=["GET"])
def root():
    cfg_init()
    return jsonify({"ok": True, "service": "tv-telegram-bot", "time": to_kst()})

@app.route("/tv-webhook", methods=["POST"])
def tv_webhook():
    payload = parse_tv_payload(request)

    # secret를 query/body/header 모두 허용
    req_secret = (
        request.args.get("secret", "")
        or str(payload.get("secret", ""))
        or str(payload.get("passphrase", ""))
        or request.headers.get("X-Webhook-Secret", "")
    )
    if TV_WEBHOOK_SECRET and req_secret != TV_WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    if not isinstance(payload, dict):
        payload = {}

    inf = infer_signal(payload)
    msg = build_signal_msg(payload)

    # ✅ CHANGE: kind 미정이면 포맷 없이 '원문 그대로' 발송
    if not msg and inf.get("kind") == "unknown":
        raw_text = str(payload.get("text") or payload.get("message") or payload.get("comment") or "").strip()
        if not raw_text:
            raw_text = json.dumps(payload, ensure_ascii=False)
        logging.info("tv-webhook unknown passthrough sent=%s", bool(raw_text))
        if raw_text:
            send_signal_alert_plain(raw_text)
            return jsonify({"ok": True, "sent": True, "infer": inf, "mode": "unknown_passthrough"})

    logging.info("tv-webhook payload=%s infer=%s sent=%s", payload, inf, bool(msg))

    if msg:
        send_signal_alert(msg)
        return jsonify({"ok": True, "sent": True, "infer": inf})

    return jsonify({"ok": True, "sent": False, "reason": "ignored_or_throttled_or_unknown", "infer": inf})

@app.route("/tg/position", methods=["POST"])
def tg_position():
    if TG_CONTROL_SECRET and request.args.get("secret", "") != TG_CONTROL_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    upd = request.get_json(silent=True) or {}
    if not isinstance(upd, dict):
        return jsonify({"ok": True, "ignored": True})
    chat_id, uid, text = parse_update(upd)
    if chat_id and text.startswith("/"):
        resp = handle_command(chat_id, uid, text)
        if resp:
            tg_send(BOT_TOKEN_POSITION, chat_id, resp)
    return jsonify({"ok": True})

@app.route("/tg/signal", methods=["POST"])
def tg_signal():
    if TG_CONTROL_SECRET and request.args.get("secret", "") != TG_CONTROL_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return jsonify({"ok": True})

@app.route("/positions_check", methods=["GET", "POST"])
def positions_check():
    if POSITIONS_CHECK_TOKEN and request.args.get("token", "") != POSITIONS_CHECK_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    with LOCK:
        res = process_positions(send_alert=True)
        auto = maybe_auto_report()
    return jsonify({"ok": True, "result": res, "auto_report": auto, "time": to_kst()})

@app.route("/daily_report", methods=["GET", "POST"])
def daily_report():
    if DAILY_REPORT_TOKEN and request.args.get("token", "") != DAILY_REPORT_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    d = request.args.get("date", "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", d or ""):
        d = None
    sent = 0
    for cid in CHAT_IDS_POSITION:
        send_report_summary(cid, d)
        sent += 1
    return jsonify({"ok": True, "sent": sent, "date": d or now_kst().strftime("%Y-%m-%d")})

@app.route("/health_check", methods=["GET"])
def health():
    with LOCK:
        auto = maybe_auto_report()
    return jsonify({"ok": True, "auto_report": auto, "time": to_kst()})


# ===== Bootstrap =====
def bootstrap():
    cfg_init()
    om = open_state()
    if om and not init_done():
        mark_init_done()

    for cid in CHAT_IDS:
        if is_group(cid) and R.get(sw_key("signal", cid)) is None:
            sw_set("signal", cid, False)
    for cid in CHAT_IDS_POSITION:
        if is_group(cid) and R.get(sw_key("position", cid)) is None:
            sw_set("position", cid, False)

    logging.info("boot ok | CHAT_IDS=%s | CHAT_IDS_POSITION=%s", CHAT_IDS, CHAT_IDS_POSITION)

bootstrap()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
