"""Conector para Binance Futures (REST + WebSocket), usado para el feed de precios y la ejecución de
órdenes de cripto. Todo corre sobre un único event loop de asyncio, el user-data stream (fills de
órdenes), un socket @ticker por símbolo y el scheduler de funding fees. Las llamadas REST síncronas del
cliente se envuelven en asyncio.to_thread para que nunca se bloquee el loop.
Las TP/SL se colocan con la Algo Order API (sus IDs son algoId, no orderId)."""

import os
import re
import math
import asyncio
import hmac
import hashlib
import requests
import logging
import time as _time
import threading
import websockets
import json
from urllib.parse import urlencode
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
from core.classes import OrderError
from core.utils import pending_warnings

logger = logging.getLogger('reg')


class BinanceExchange:
    def __init__(self):
        load_dotenv()
        self.testnet = os.getenv("DEMO", "true").split("#")[0].strip().lower() == "true"
        #Define si conecta real o demo segun como este definido en el .env
        if self.testnet:
            self.base_url    = "https://testnet.binancefuture.com"
            self.ws_base_url = "wss://stream.binancefuture.com/market/ws"
            self.api_key     = os.getenv("BINANCE_TESTNET_API_KEY")
            self.api_secret  = os.getenv("BINANCE_TESTNET_API_SECRET")
        else:
            self.base_url    = "https://fapi.binance.com"
            self.ws_base_url = "wss://fstream.binance.com/market/ws"
            self.api_key     = os.getenv("BINANCE_API_KEY")
            self.api_secret  = os.getenv("BINANCE_API_SECRET")

        self.client = Client(self.api_key, self.api_secret, tld='com', testnet=self.testnet)
        self._exchange_info_cache = None
        self._last_ping = 0  
        self._used_weight_1m = 0    # último X-MBX-USED-WEIGHT-1M visto; lo lee margin_monitor_scheduler para autolimitarse.
        self._used_weight_ts = 0.0  
        
        # Atributos para manejo de sockets
        self.active_tasks = {}
        self.event_loop = None

    # ── Helpers internos ──────────────────────────────────────────────────────

    def _sign(self, params: dict) -> str:
        """Firma HMAC-SHA256 que requiere Binance para endpoints firmados."""
        query = urlencode(params, doseq=True)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _algo_order_post(self, params: dict) -> dict:
        """
        POST a /fapi/v1/algoOrder (Algo Order API).
        Las órdenes condicionales (STOP_MARKET, TAKE_PROFIT_MARKET) se envían acá.
        """
        params = dict(params)
        params["timestamp"] = int(_time.time() * 1000)
        params["recvWindow"] = 5000
        params["signature"] = self._sign(params)

        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"{self.base_url}/fapi/v1/algoOrder"

        resp = requests.post(url, params=params, headers=headers, timeout=10)
        if resp.status_code != 200:
            raise OrderError(f"Algo API {resp.status_code}: {resp.text}")
        return resp.json()

    # ── Exchange info ─────────────────────────────────────────────────────────

    def preload_exchange_info(self):
        """Carga la configuración de símbolos e información de mercado."""
        if self._exchange_info_cache is None:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            try:
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                self._exchange_info_cache = resp.json()
                logger.info("[BINANCE] Exchange info loaded into memory")
                return True
            except Exception as e:
                logger.error(f"[BINANCE] Critical error loading exchange info: {e}")
                return False
        return True

    def get_exchange_info(self):
        """Devuelve la caché, cargándola si está vacía."""
        if self._exchange_info_cache is None:
            self.preload_exchange_info()
        return self._exchange_info_cache

    def Qty_min(self, symbol, currentprice):
        """Calcula la cantidad mínima permitida segun LOT_SIZE y MIN_NOTIONAL."""
        data = self.get_exchange_info()
        symbol = symbol.upper()
        min_notional, min_qty, step = None, None, None

        for s in data.get("symbols", []):
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "MIN_NOTIONAL":
                        min_notional = float(f["notional"])
                    elif f["filterType"] == "LOT_SIZE":
                        min_qty = float(f["minQty"])
                        step    = float(f["stepSize"])
                break

        if min_notional is None or min_qty is None or step is None:
            return None

        needed = min_notional / float(currentprice)
        qty = math.ceil(needed / step) * step
        return max(min_qty, qty)

    def get_symbol_precision(self, symbol: str):
        """Devuelve la cantidad de decimales permitidos para precio y cantidad correspondiente a cada simbolo"""
        if not symbol:
            raise ValueError("Símbolo vacío.")

        sym  = symbol.upper().strip()
        data = self.get_exchange_info()
        info = next((s for s in data.get("symbols", []) if s.get("symbol") == sym), None)

        if info is None:
            raise ValueError(f"Símbolo no encontrado en Binance Futures: {sym}")

        price_filter = next((f for f in info["filters"] if f["filterType"] == "PRICE_FILTER"), {})
        lot_size     = next((f for f in info["filters"] if f["filterType"] == "LOT_SIZE"), {})

        tick_size = price_filter.get("tickSize", "")
        step_size = lot_size.get("stepSize", "")

        price_decimals = len(tick_size.split(".")[1].rstrip("0")) if "." in tick_size else 0
        dec_qty    = len(step_size.split(".")[1].rstrip("0")) if "." in step_size else 0

        return price_decimals, dec_qty

    # ── Órdenes ───────────────────────────────────────────────────────────────

    async def order_market(self, symbol: str, side: str, quantity: float, reduce: bool = False):
        """Orden de MARKET por endpoint clásico /fapi/v1/order."""
        try:
            if not reduce:
                # to_thread: el client es sync (requests); sin esto el HTTP congela el event loop entero
                positions = await asyncio.to_thread(self.client.futures_position_information, symbol=symbol)
                if any(float(p.get('positionAmt', 0)) != 0 for p in positions):
                    pending_warnings[symbol] = f'Position already open for {symbol}, order skipped.'
                    logger.warning(f"[BINANCE][{symbol.upper()}] Position already open, order skipped")
                    return 0
            order = await asyncio.to_thread(
                self.client.futures_create_order,
                symbol=symbol,
                side=side,
                type=FUTURE_ORDER_TYPE_MARKET,
                quantity=quantity,
                reduceOnly=reduce,
                newOrderRespType='FULL'
            )
            id_order = order['orderId']
            logger.info(f"[BINANCE][{symbol.upper()}] Market order sent - ID: {id_order}")
            return id_order
        except Exception as e:
            logger.error(f"[BINANCE][{symbol.upper()}] Error sending main order: {e}")
            raise OrderError(f"[{symbol}] Error en la orden principal: {e}")

    async def order_tp_market(self, symbol: str, side: str, quantity: float, trigger_price: float):
        """Take Profit Market vía Algo Order API."""
        try:
            params = {
                "algoType":        "CONDITIONAL",
                "symbol":          symbol,
                "side":            side,
                "type":            "TAKE_PROFIT_MARKET",
                "triggerPrice":    trigger_price,
                "quantity":        quantity,
                "reduceOnly":      "true",
                "timeInForce":     "GTC",
                "newOrderRespType":"RESULT",
            }
            result   = await asyncio.to_thread(self._algo_order_post, params)
            id_order = result.get("algoId")
            logger.info(f"[BINANCE][{symbol.upper()}] TP market order sent - AlgoID: {id_order}")
            return id_order
        except Exception as e:
            logger.error(f"[BINANCE][{symbol.upper()}] TP_MARKET order error: {e}")
            raise OrderError(f"[{symbol}] Error TP_MARKET order: {e}")

    async def order_sl_stop_market(self, symbol: str, side: str, stop_price: float):
        """Stop Market vía Algo Order API. Cierra toda la posición (closePosition=true)."""
        try:
            params = {
                "algoType":        "CONDITIONAL",
                "symbol":          symbol,
                "side":            side,
                "type":            "STOP_MARKET",
                "triggerPrice":    stop_price,
                "closePosition":   "true",
                "timeInForce":     "GTC",
                "newOrderRespType":"RESULT",
            }
            result   = await asyncio.to_thread(self._algo_order_post, params)
            id_order = result.get("algoId")
            logger.info(f"[BINANCE][{symbol.upper()}] SL market order sent - ID: {id_order}")
            return id_order
        except Exception as e:
            logger.error(f"[BINANCE][{symbol.upper()}] SL STOP_MARKET order error: {e}")
            raise OrderError(f"[{symbol}] Error SL STOP_MARKET order: {e}")

    async def get_order_info(self, symbol, id_order, max_attempts=10, wait_seconds=2):
        """Consulta trades de una orden y devuelve (PE, pnl, fee, qty)."""
        if id_order is None:
            return 0, 0, 0, 0

        for _ in range(max_attempts):
            trades = await asyncio.to_thread(self.client.futures_account_trades, symbol=symbol, orderId=id_order)
            if trades:
                fee       = sum(float(t['commission'])   for t in trades)
                pnl       = sum(float(t['realizedPnl'])  for t in trades)
                total_qty = sum(float(t['qty'])           for t in trades)
                PE_order  = (
                    sum(float(t['price']) * float(t['qty']) for t in trades) / total_qty
                    if total_qty > 0 else 0
                )
                return PE_order, pnl, fee, total_qty
            await asyncio.sleep(wait_seconds)

        raise Exception(
            f"[BINANCE][{symbol.upper()}][GET-ORDER-INFO] Order {id_order} not found "
            f"after {max_attempts} attempts."
        )

    async def close_total(self, symbol):
        """Cierra la posición completa de un símbolo (con reintento si queda remanente)."""
        def get_amt():
            positions = self.client.futures_position_information(symbol=symbol)
            for p in positions:
                amt = float(p.get('positionAmt', 0))
                if amt != 0:
                    return amt
            return 0.0

        amt = 0.0
        for _ in range(3):
            amt = await asyncio.to_thread(get_amt)  
            if amt != 0:
                break
            await asyncio.sleep(0.4)

        if amt == 0:
            logger.info(f"[BINANCE][{symbol.upper()}][CLOSE] No open position")
            return None

        qty  = abs(amt)
        side = SIDE_BUY if amt < 0 else SIDE_SELL
        id_order = await self.order_market(symbol, side=side, quantity=qty, reduce=True)

        await asyncio.sleep(0.4)
        rem = await asyncio.to_thread(get_amt)  

        if rem != 0:
            qty2  = abs(rem)
            side2 = SIDE_BUY if rem < 0 else SIDE_SELL
            logger.warning(f"[BINANCE][{symbol.upper()}][CLOSE] Remainder {rem} left, retrying close...")
            id_order = await self.order_market(symbol, side=side2, quantity=qty2, reduce=True)

        logger.info(f"[BINANCE][{symbol.upper()}][CLOSE] Success - Order ID: {id_order}")
        return id_order

    def cancel_algo_order(self, symbol: str, algo_id: int):
        """Cancela una orden algo abierta por su algoId."""
        if not algo_id:
            return None
        try:
            params = {
                "symbol":      symbol,
                "algoId":      algo_id,
                "timestamp":   int(_time.time() * 1000),
                "recvWindow":  5000,
            }
            params["signature"] = self._sign(params)
            headers = {"X-MBX-APIKEY": self.api_key}
            resp = requests.delete(
                f"{self.base_url}/fapi/v1/algoOrder",
                params=params,
                headers=headers,
                timeout=10,
            )
            if resp.status_code in (400, 404):
                logger.debug(f"[BINANCE][{symbol.upper()}][CANCEL] AlgoId={algo_id} no longer exists, nothing to cancel")
                return None
            if resp.status_code != 200:
                raise Exception(f"[BINANCE][{symbol.upper()}][CANCEL] Error cancelling algoId={algo_id}: {resp.text}")
            logger.info(f"[BINANCE][{symbol.upper()}][CANCEL] Cancelled algoId={algo_id}")
            return resp.json()
        except Exception as e:
            logger.error(f"[BINANCE][{symbol.upper()}][CANCEL] ERROR: {e}")
            return None

    # ── User data stream ──────────────────────────────────────────────────────

    def get_listen_key(self):
        """Obtiene el listenKey para escuchar eventos de cuenta por WS."""
        url     = f"{self.base_url}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}
        resp    = requests.post(url, headers=headers, timeout=10)
        return resp.json()["listenKey"]

    def keepalive_listen_key(self, listen_key: str):
        """Renueva el listenKey para que no expire."""
        url     = f"{self.base_url}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}
        params  = {"listenKey": listen_key}
        resp    = requests.put(url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            raise Exception(f"[BINANCE][USER-STREAM] listenKey keepalive failed {resp.status_code}: {resp.text}")

    def check_connection(self):
        """Verifica la conectividad basica con la API de Binance"""
        now = _time.time()
        if now - self._last_ping < 60:
            return True
        self._last_ping = now
        try:
            self.client.futures_ping()
            return True
        except Exception as e:
            logger.warning(f"[BINANCE] Connection check failed: {e}")
            return False


    async def _process_tick(self, symbol, datasocket):
        """Lógica de cálculos cuando llega data del socket."""
        from core.utils import get_vars
        from core.logic import open_position, manage_position
        
        ps, fd, rt = get_vars(symbol)
        rt.current_price = float(datasocket['c'])

        if fd.restart:
            fd.restart = False
            rt.current_price = float(datasocket['c'])
            fd.r0 = rt.current_price
            fd.price_decimals, fd.dec_qty = self.get_symbol_precision(symbol)
            fd.Qty_min = round(self.Qty_min(symbol, rt.current_price), fd.dec_qty)
            rt.current_datetime = datetime.now(timezone.utc).replace(tzinfo=None)
            fd.open_time = rt.current_datetime
            await open_position(symbol, ps, fd, rt)

        if rt.current_price != rt.previous_price:
            rt.previous_price = rt.current_price
            rt.current_datetime = datetime.now(timezone.utc).replace(tzinfo=None)
            await manage_position(ps, fd, rt, symbol)

    def start_socket_task(self, symbol):
        """Inicia socket async para un símbolo. Devuelve True si la tarea arrancó,
        False si no pudo."""
        if symbol in self.active_tasks:
            logger.warning(f"[BINANCE][{symbol.upper()}][SOCKET] Already active")
            return False

        if self.event_loop is None:
            logger.error(f"[BINANCE][{symbol.upper()}][SOCKET] Event loop unavailable, cannot start feed")
            return False

        task = asyncio.run_coroutine_threadsafe(self.start_socket(symbol), self.event_loop)
        self.active_tasks[symbol] = task
        return True

    async def start_socket(self, symbol):
        """Conecta al WebSocket del ticker."""
        url = f"{self.ws_base_url}/{symbol}@ticker"
        
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as websocket:
                    logger.info(f"[BINANCE][{symbol.upper()}][SOCKET] Connected")
                    while True:
                        try:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=300)
                            data = json.loads(msg)
                            await self._process_tick(symbol, data)
                        except asyncio.TimeoutError:
                            logger.warning(f"[BINANCE][{symbol.upper()}][SOCKET] No data for 5 min (possible stalled stream), reconnecting...")
                            break
                        except asyncio.CancelledError:
                            logger.info(f"[BINANCE][{symbol.upper()}][SOCKET] Cancelled")
                            return
                        except OrderError as oe:
                            logger.warning(f"[BINANCE][{symbol.upper()}][SOCKET] Order error (socket still alive): {oe}")
                            pending_warnings[symbol] = f'Order error for {symbol}: {oe}'
                            continue
                        except Exception as e:
                            logger.warning(f"[BINANCE][{symbol.upper()}][SOCKET] recv error: {e}")
                            break
            except asyncio.CancelledError:
                logger.info(f"[BINANCE][{symbol.upper()}][SOCKET] Cancelled")
                return
            except Exception as e:
                logger.warning(f"[BINANCE][{symbol.upper()}][SOCKET] Connection lost ({e}). Retrying in 5s...")

            await asyncio.sleep(5)

    async def stop_socket(self, symbol, ps, fd, rt):
        """Detiene el socket de un símbolo."""
        from core.logic import handle_close
        
        task = self.active_tasks.get(symbol)
        try:
            if fd.control and rt.stop_manual:
                logger.info(f"[BINANCE][{symbol.upper()}][STOP] Manual stop")
                await handle_close(symbol, ps, fd, rt)
            else:
                logger.info(f"[BINANCE][{symbol.upper()}][STOP] Automatic stop")
        except Exception as e:
            logger.error(f"[BINANCE][{symbol.upper()}][STOP] Error closing position: {e} - Socket stopped")
        finally:
            if task:
                task.cancel()
                del self.active_tasks[symbol]

    def start_event_loop(self):
        """Inicia el event loop async: user data stream, funding fees y monitor de margin."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.event_loop = loop
        loop.create_task(self.start_user_data_socket_order_update())
        loop.create_task(self.funding_fee_scheduler())
        loop.create_task(self.margin_monitor_scheduler())   
        loop.run_forever()

    async def start_user_data_socket_order_update(self):
        """Escucha eventos de órdenes y actualizaciones de cuenta."""
        from core.utils import crypto_list, get_vars
        
        while True:
            try:
                listen_key = await asyncio.to_thread(self.get_listen_key)
                if self.testnet:
                    url = f"wss://stream.binancefuture.com/ws/{listen_key}"
                else:
                    url = f"wss://fstream.binance.com/private/ws/{listen_key}"  

                last_keepalive = asyncio.get_event_loop().time()
                acum = {}

                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("[BINANCE][USER-STREAM] Connected")
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=60)
                            data = json.loads(msg)
                            evento = data.get("e")

                            if evento == "ORDER_TRADE_UPDATE":
                                o = data.get("o", {})
                                algo_id_vinculado = o.get("si") or o.get("ca")
                                order_id_real = o.get("i")
                                symbol_raw = o.get("s")
                                status = o.get("X")

                                if status in ("PARTIALLY_FILLED", "FILLED"):
                                    if order_id_real not in acum:
                                        acum[order_id_real] = {"pnl": 0.0, "commission": 0.0, "qty": 0.0}
                                    acum[order_id_real]["pnl"]      += float(o.get("rp", 0))
                                    acum[order_id_real]["commission"] += abs(float(o.get("n", 0)))
                                    acum[order_id_real]["qty"]      += float(o.get("l", 0))

                                if status == "FILLED":
                                    if symbol_raw and algo_id_vinculado:
                                        try:
                                            ps, fd, rt = get_vars(symbol_raw.lower())
                                            if ps.socket_active and algo_id_vinculado in (rt.id_order_r1, rt.id_order_r2, rt.id_order_r_1):
                                                if algo_id_vinculado == rt.id_order_r1:
                                                    rt.r1_active = True
                                                elif algo_id_vinculado == rt.id_order_r2:
                                                    rt.r2_active = True
                                                else:
                                                    rt.r_1_active = True

                                                rt.ALGO_orderid  = order_id_real
                                                rt.ALGO_pnl      = acum[order_id_real]["pnl"]
                                                rt.ALGO_commission = acum[order_id_real]["commission"]
                                                rt.ALGO_QtymVar  = acum[order_id_real]["qty"]
                                                rt.ALGO_PE       = float(o.get("ap"))

                                        except Exception as var_error:
                                            logger.error(f"[BINANCE][{symbol_raw.upper()}] Could not assign algo position: {var_error}")

                                    del acum[order_id_real]

                        except asyncio.TimeoutError:
                            ahora = asyncio.get_event_loop().time()
                            if ahora - last_keepalive > 1800:
                                try:
                                    await asyncio.to_thread(self.keepalive_listen_key, listen_key)
                                    last_keepalive = ahora
                                except Exception as ka_error:
                                    logger.warning(f"[BINANCE][USER-STREAM] Keepalive failed, reconnecting: {ka_error}")
                                    break

            except Exception as e:
                logger.warning(f"[BINANCE][USER-STREAM] Connection lost ({e}). Retrying in 10s...")
                await asyncio.sleep(10)

    async def funding_fee_scheduler(self):
        """Scheduler para procesar los funding fees que llegan."""
        from core.utils import crypto_list, get_vars
        from core.logic import apply_funding_fee
        
        while True:
            now = datetime.now(timezone.utc)

            if now.hour in (0, 8, 16) and now.minute == 5 and now.second < 10:
                try:
                    ref = now.replace(minute=0, second=0, microsecond=0)
                    params = {
                        "incomeType": "FUNDING_FEE",
                        "startTime":  int(ref.timestamp() * 1000) - 60000,
                        "endTime":    int(now.timestamp() * 1000),
                        "limit":      100,
                        "timestamp":  int(_time.time() * 1000),
                    }
                    query = "&".join(f"{k}={v}" for k, v in params.items())
                    params["signature"] = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()

                    def _get():
                        return requests.get(
                            f"{self.base_url}/fapi/v1/income",
                            params=params,
                            headers={"X-MBX-APIKEY": self.api_key},
                            timeout=10,
                        ).json()

                    fees = await asyncio.to_thread(_get)

                    if isinstance(fees, list):
                        for item in fees:
                            symbol_raw = item.get("symbol")
                            fee = float(item.get("income", 0))
                            if symbol_raw and symbol_raw.lower() in crypto_list:
                                try:
                                    ps, fd, rt = get_vars(symbol_raw.lower())
                                    if ps.socket_active and fd.control:
                                        await apply_funding_fee(symbol_raw.lower(), ps, fd, rt, fee)
                                except Exception as e:
                                    logger.error(f"[BINANCE][{symbol_raw.upper()}][FUNDING] Error: {e}")
                    else:
                        logger.warning(f"[BINANCE][FUNDING] Bad response from Binance: {fees}")

                except Exception as e:
                    logger.error(f"[BINANCE][FUNDING] General error: {e}")

                await asyncio.sleep(60)

            await asyncio.sleep(5)


    async def margin_monitor_scheduler(self):
        """Poll de SOLO LECTURA del margin de Binance (cada 20 s en real / cada 10 min en demo, en el
        event loop asyncio). Es la unica
        fuente de datos de margin: refresca en gl los totales de cuenta y en rt (por símbolo). """
        from core.utils import symbols
        from core.classes import gl

        WEIGHT_LIMIT     = 6000                    # REQUEST_WEIGHT por IP y por minuto (confirmado en /fapi/v1/exchangeInfo)
        WEIGHT_SAFE_STOP = WEIGHT_LIMIT * 0.75      # a partir de acá el poll se saltea un ciclo

        while True:
            weight_fresh = (_time.time() - self._used_weight_ts) < 60
            if not self.testnet and weight_fresh and self._used_weight_1m >= WEIGHT_SAFE_STOP:
                logger.debug(f"[BINANCE][MARGIN] Skipping cycle, used_weight_1m={self._used_weight_1m}")
            else:
                try:
                    acc = await asyncio.to_thread(self.client.futures_account)
                    self._update_used_weight()
                    mb          = float(acc.get("totalMarginBalance", 0) or 0)
                    maint_total = float(acc.get("totalMaintMargin", 0) or 0)
                    gl.margin_balance_binance    = mb
                    gl.wallet_balance_binance    = float(acc.get("totalWalletBalance", 0) or 0)   # capital sin uPnL, base del control
                    gl.maint_margin_binance      = maint_total
                    gl.available_balance_binance = float(acc.get("availableBalance", 0) or 0)
                    gl.margin_ratio_binance      = (maint_total / mb * 100) if mb else 0.0
                    positions = await asyncio.to_thread(self.client.futures_position_information)
                    self._update_used_weight()
                    liq_by_symbol = {
                        p.get("symbol", ""): float(p.get("liquidationPrice", 0) or 0)
                        for p in positions if float(p.get("positionAmt", 0) or 0) != 0
                    }
                    for ap in acc.get("positions", []):
                        if float(ap.get("positionAmt", 0) or 0) == 0:
                            continue
                        sym = ap.get("symbol", "")
                        container = symbols.get(sym.lower())
                        if not container or not container.ps.socket_active:
                            continue
                        pd = container.fd.price_decimals   # liq price redondeado con los decimales del símbolo
                        container.rt.leverage     = float(ap.get("leverage", 0) or 0)      # de futures_account (positionRisk v3 ya NO lo trae)
                        container.rt.maint_margin = float(ap.get("maintMargin", 0) or 0)   # se formatea a 2 dec en el front
                        container.rt.liquid_price = round(liq_by_symbol.get(sym, 0.0), pd)  # de positionRisk si vino; si no, 0 -> "—"

                except Exception as e:
                    logger.error(f"[BINANCE][MARGIN] Error: {e}")
                    m = re.search(r"banned until (\d+)", str(e))
                    if m:
                        wait = min(max(0.0, int(m.group(1)) / 1000 - _time.time()) + 10, 3600)
                        logger.warning(f"[BINANCE][MARGIN] Ban active: pausing {int(wait)}s until it expires")
                        await asyncio.sleep(wait)

            await asyncio.sleep(600 if self.testnet else 20)

    def _update_used_weight(self):
        """Lee X-MBX-USED-WEIGHT-1M de la última respuesta del cliente y lo cachea en self._used_weight_1m.
        Es el weight REAL de la IP (compartido entre las instancias que corren en este VPS), no una
        estimación local -> margin_monitor_scheduler lo usa para autolimitarse sin adivinar cuándo
        resetea la ventana (Binance no documenta si es fija o deslizante)."""
        try:
            headers = self.client.response.headers
            for key, value in headers.items():
                if key.upper() == "X-MBX-USED-WEIGHT-1M":
                    self._used_weight_1m = int(value)
                    self._used_weight_ts = _time.time()
                    return
        except Exception:
            pass

    # El control de apertura (margin_precheck) es común a ambos brokers esta en core/utils.py.

    def get_futures_symbols(self) -> list: #Devuelve todos los sinmbolos para la lista de index.html
        data = self.get_exchange_info()
        return sorted([
            s["symbol"]
            for s in data.get("symbols", [])
            if s.get("status") == "TRADING"
        ])

    def get_chart_data(self, symbol: str, limit: int = 180):
        klines = self.client.futures_klines(
            symbol=symbol.upper(),
            interval="1d",
            limit=limit,
        )
        candles = [
            {
                "time":   int(k[0]) // 1000,
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
            }
            for k in klines
        ]
        return {
            "symbol":  symbol.upper(),
            "candles": candles,
        }

binance = BinanceExchange()