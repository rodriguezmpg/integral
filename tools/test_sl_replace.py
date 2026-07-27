"""Prueba en TESTNET si el Algo Order API acepta un segundo STOP_MARKET (closePosition=true)
mientras ya hay uno vivo, y si después se puede cancelar el viejo.

Es la pregunta que decide si handle_take_profit puede pasar a "colocar primero, cancelar después"
como ya hace advance_trailing_stop.

    python tools/test_sl_replace.py           # solo muestra qué haría
    python tools/test_sl_replace.py --run     # ejecuta (abre y cierra una posición real de testnet)

Abre una posición mínima en SOLUSDT, coloca dos SL encadenados y limpia todo al final (el
cleanup corre en un finally: si algo falla en el medio, igual cancela las órdenes y cierra).
"""
import os, sys, time, hmac, hashlib, json
from urllib.parse import urlencode
import requests
from dotenv import load_dotenv

SYMBOL = "SOLUSDT"
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Traba de seguridad: este script SOLO puede correr contra testnet.
# El VPS opera con dinero real sobre las mismas rutas de código; un descuido acá se paga caro.
# ─────────────────────────────────────────────────────────────────────────────
BASE_URL = "https://testnet.binancefuture.com"
API_KEY    = os.getenv("BINANCE_TESTNET_API_KEY")
API_SECRET = os.getenv("BINANCE_TESTNET_API_SECRET")

if not API_KEY or not API_SECRET:
    sys.exit("ERROR: faltan BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET en el .env")
if "testnet" not in BASE_URL:
    sys.exit("ERROR: BASE_URL no es testnet. Abortado.")


def _sign(params: dict) -> str:
    return hmac.new(API_SECRET.encode(), urlencode(params, doseq=True).encode(), hashlib.sha256).hexdigest()


def _signed(method: str, path: str, params: dict = None):
    p = dict(params or {})
    p["timestamp"]  = int(time.time() * 1000)
    p["recvWindow"] = 5000
    p["signature"]  = _sign(p)
    r = requests.request(method, f"{BASE_URL}{path}", params=p,
                         headers={"X-MBX-APIKEY": API_KEY}, timeout=10)
    return r.status_code, (r.json() if r.text else {})


def _public(path: str, params: dict = None):
    return requests.get(f"{BASE_URL}{path}", params=params or {}, timeout=10).json()


def filtros(symbol):
    """tick size, step size y notional mínimo del símbolo."""
    info = _public("/fapi/v1/exchangeInfo")
    s = next(x for x in info["symbols"] if x["symbol"] == symbol)
    f = {x["filterType"]: x for x in s["filters"]}
    tick = float(f["PRICE_FILTER"]["tickSize"])
    step = float(f["LOT_SIZE"]["stepSize"])
    notional = float(f.get("MIN_NOTIONAL", {}).get("notional", 5))
    dec_p = s["pricePrecision"]
    dec_q = s["quantityPrecision"]
    return tick, step, notional, dec_p, dec_q


def poner_sl(symbol, side, trigger, dec_p):
    """Mismo payload que order_sl_stop_market de binance_ext."""
    params = {
        "algoType":         "CONDITIONAL",
        "symbol":           symbol,
        "side":             side,
        "type":             "STOP_MARKET",
        "triggerPrice":     round(trigger, dec_p),
        "closePosition":    "true",
        "timeInForce":      "GTC",
        "newOrderRespType": "RESULT",
    }
    return _signed("POST", "/fapi/v1/algoOrder", params)


def cancelar(symbol, algo_id):
    return _signed("DELETE", "/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id})


def abiertas(symbol):
    """Lista de algo orders abiertas. El nombre del endpoint cambió entre versiones,
    así que se prueban las dos variantes conocidas y se devuelve la que responda."""
    for path in ("/fapi/v1/openAlgoOrders", "/fapi/v1/algoOrders"):
        code, data = _signed("GET", path, {"symbol": symbol})
        if code == 200:
            return path, data
    return None, None


def main():
    if "--run" not in sys.argv:
        print(__doc__)
        print(f"Modo simulación. Agregá --run para ejecutar de verdad contra {BASE_URL}")
        return

    tick, step, min_notional, dec_p, dec_q = filtros(SYMBOL)
    precio = float(_public("/fapi/v1/ticker/price", {"symbol": SYMBOL})["price"])
    qty = round(max(min_notional / precio * 1.2, step), dec_q)

    print(f"Endpoint : {BASE_URL}")
    print(f"API key  : {API_KEY[:6]}...{API_KEY[-4:]}")
    print(f"{SYMBOL}  precio={precio}  tick={tick}  qty a abrir={qty}\n")

    id1 = id2 = None
    try:
        # 1) Posición mínima LONG
        code, r = _signed("POST", "/fapi/v1/order",
                          {"symbol": SYMBOL, "side": "BUY", "type": "MARKET", "quantity": qty})
        print(f"[1] Abrir posición LONG      -> HTTP {code}  {r.get('status', r)}")
        if code != 200:
            return

        # 2) Primer SL
        code, r = poner_sl(SYMBOL, "SELL", precio * 0.95, dec_p)
        id1 = r.get("algoId")
        print(f"[2] SL #1 en {round(precio*0.95, dec_p)}  -> HTTP {code}  algoId={id1}")
        if code != 200:
            print(f"    respuesta: {json.dumps(r)}")
            return

        # 3) LA PRUEBA: segundo SL sin cancelar el primero
        code, r = poner_sl(SYMBOL, "SELL", precio * 0.96, dec_p)
        id2 = r.get("algoId")
        print(f"\n[3] >>> SL #2 en {round(precio*0.96, dec_p)} SIN cancelar el #1")
        print(f"    HTTP {code}  algoId={id2}")
        if code == 200:
            print("    ==> ACEPTA dos SL simultáneos: 'colocar primero, cancelar después' es viable\n")
        else:
            print(f"    ==> RECHAZA el segundo: {json.dumps(r)}")
            print("    ==> habría que cancelar antes de colocar, y proteger el fallo de otra forma\n")

        # 4) Cancelar el viejo
        if id2:
            code, r = cancelar(SYMBOL, id1)
            print(f"[4] Cancelar el SL #1 (algoId={id1}) -> HTTP {code}  {r}")
            id1 = None if code == 200 else id1

        path, data = abiertas(SYMBOL)
        if path:
            n = len(data) if isinstance(data, list) else len(data.get("orders", []))
            print(f"[5] Órdenes algo abiertas ({path}): {n}")

    finally:
        print("\n--- limpieza ---")
        for aid in (i for i in (id1, id2) if i):
            code, r = cancelar(SYMBOL, aid)
            print(f"    cancelar algoId={aid} -> HTTP {code}")
        code, r = _signed("POST", "/fapi/v1/order",
                          {"symbol": SYMBOL, "side": "SELL", "type": "MARKET",
                           "quantity": qty, "reduceOnly": "true"})
        print(f"    cerrar posición -> HTTP {code}  {r.get('status', r)}")


if __name__ == "__main__":
    main()
