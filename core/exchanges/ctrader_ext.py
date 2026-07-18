"""Conector para cTrader Open API (Pepperstone), usado para el feed de precios y la ejecución de
órdenes de forex/CFD. Corre sobre el reactor de Twisted: los mensajes son fire-and-forget y las
respuestas vuelven como eventos en _on_message.
Al conectar autentica la cuenta y descarga el catálogo completo, que queda separado en dos caches
(_forex_cache / _cfd_cache) que las rutas de admin sirven al frontend para agregar nuevos simbolos"""

import os
import json
import time
import threading
import requests
from dotenv import load_dotenv
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
    ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
    ProtoOASymbolsListReq, ProtoOASymbolsListRes,
    ProtoOASymbolByIdReq, ProtoOASymbolByIdRes,
    ProtoOASubscribeSpotsReq,
    ProtoOAUnsubscribeSpotsReq,
    ProtoOASpotEvent,
    ProtoOAErrorRes,
    ProtoOANewOrderReq,
    ProtoOAExecutionEvent,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAClosePositionReq,
    ProtoOACancelOrderReq,
    ProtoOAAssetListReq, ProtoOAAssetListRes,
    ProtoOAAssetClassListReq, ProtoOAAssetClassListRes,
    ProtoOASymbolCategoryListReq, ProtoOASymbolCategoryListRes,
    ProtoOAGetTrendbarsReq, ProtoOAGetTrendbarsRes,
    ProtoOATraderReq, ProtoOATraderRes, ProtoOATraderUpdatedEvent,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAOrderType, ProtoOATradeSide, ProtoOAExecutionType, ProtoOATrendbarPeriod
from twisted.internet import reactor, defer

load_dotenv()
logger = logging.getLogger('reg')

class CTraderExchange:
    def __init__(self):
        self.demo = os.getenv("DEMO", "true").split("#")[0].strip().lower() == "true"
        self.client_id     = os.getenv("CTRADER_CLIENT_ID")
        self.client_secret = os.getenv("CTRADER_CLIENT_SECRET")
        if self.demo:
            self.account_id = int(os.getenv("CTRADER_ACCOUNT_ID_DEMO"))
            seed_access  = os.getenv("CTRADER_ACCESS_TOKEN_DEMO")
            seed_refresh = os.getenv("CTRADER_REFRESH_TOKEN_DEMO")
        else:
            self.account_id = int(os.getenv("CTRADER_ACCOUNT_ID"))
            seed_access  = os.getenv("CTRADER_ACCESS_TOKEN")
            seed_refresh = os.getenv("CTRADER_REFRESH_TOKEN")

        # Tokens: el access token vence a los 30 días; el refresh token lo renueva (y en cada renovación rota).
        # Se guardan en un JSON aparte por modo (demo/live) porque el refresh token cambia cada vez y el .env es
        # estático. Al arrancar se lee ese archivo si existe (tokens ya rotados); si no, se obtiene del .env.
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self._token_file = os.path.join(project_root, f"ctrader_token_{'demo' if self.demo else 'live'}.json")
        self._last_auth_warning = 0    # timestamp del último aviso de "no autorizado" a la UI (throttle anti-spam)
        self._last_refresh_attempt = 0 # timestamp del último intento de refresh reactivo (throttle: no martillar si falla)
        self.tokens = self._load_tokens(seed_access, seed_refresh)
        self.access_token = self.tokens["access_token"]

        self._forex_cache = []  # catálogo de símbolos forex disponibles para agregar desde la UI
        self._cfd_cache   = []  # catálogo de símbolos CFD (todo lo que no es forex va aca)
        self._asset_class_names = {}  # assetClassId a nombre de clase (ej. 1: "Forex (Spot)")
        self._category_class    = {}  # symbolCategoryId a assetClassId
        self._symbols_raw       = []  # [(symbolName, symbolCategoryId)] pendientes de clasificar
        self._id_map      = {}  # nombre a symbolId (ej. "EURUSD": 1)
        self._rev_id_map  = {}  # symbolId a nombre (mapa inverso de _id_map)
        self._digits_map  = {}  # nombre a decimales del precio
        self._min_volume_map  = {}  # nombre a volumen mínimo que acepta el broker (en cents)
        self._lot_size_map    = {}  # nombre a valor de un lote (en cents)
        self._step_volume_map = {}  # nombre a salto mínimo de volumen (en cents)
        self._quote_asset_id_map = {}  # nombre a id de la moneda quote, para convertir a USD cuando la quote no lo es
        self._asset_map = {}  # assetId a nombre de moneda (ej. 5: "USD")
        self._price_map = {}  # nombre a último precio de cada símbolo suscrito; base para convertir monedas quote a USD
        self._bid_map = {}  # nombre a último bid descalado (para el spread del modal de Start)
        self._ask_map = {}  # nombre a último ask descalado (bid y ask llegan en ticks separados)
        self._position_volume = {}  # positionId a volumen abierto actual (en cents); cTrader lo actualiza al reducirse, permite cerrar todo sin calcular el remanente
        self._pending_orders = {}  # positionId a los orderId de las TP LIMIT pendientes, para cancelarlas al cerrar la posición
        self._pending_subscribe = set()  # symbolIds esperando la respuesta de detalles antes de suscribirse
        self._subscribed = set()  # symbolIds con suscripción spot activa; se re-suscriben tras una reconexión (la suscripción es por conexión)
        self._schedule_map = {}  # nombre a intervalos del horario de mercado (segundos desde el domingo 00:00 en la timezone del símbolo)
        self._schedule_tz_map = {}  # nombre a timezone del horario, para evaluar si el mercado está abierto
        self._pending_details = set()  # symbolIds pedidos solo para cachear el horario, sin suscribir el precio
        self._trendbar_events = {}  # symbolId a threading.Event; la ruta del gráfico espera acá hasta la respuesta
        self._trendbar_data   = {}  # symbolId a las velas decodificadas para devolver a la ruta

        # Las cuentas demo se conectan al host demo y las reales al host live.
        host = EndPoints.PROTOBUF_DEMO_HOST if self.demo else EndPoints.PROTOBUF_LIVE_HOST
        self.client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.client.setConnectedCallback(self._on_connected)
        self.client.setMessageReceivedCallback(self._on_message)

    # Manejo de tokens OAuth (refresh automático)

    def _load_tokens(self, seed_access, seed_refresh):
        # Lee el JSON de tokens si existe (son los últimos, ya rotados). Si no, toma desde el .env.
        if os.path.exists(self._token_file):
            try:
                with open(self._token_file) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"[CTRADER][TOKEN] Token file unreadable ({e}), using .env seed")
        return {
            "access_token":  seed_access,
            "refresh_token": seed_refresh,
            "expires_at":    time.time() + 2628000,   # 30 días desde ahora.
        }

    def _save_tokens(self):
        # Escribe en un temporal y renombra: un corte a mitad de escritura no deja el JSON roto.
        tmp = self._token_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.tokens, f)
        os.replace(tmp, self._token_file)

    def refresh_access_token(self):
        # Canjea el refresh token por un access token nuevo (y un refresh token nuevo: el viejo muere).
        # Guarda los dos en el JSON y actualiza el access token en memoria para la próxima autenticación.
        from core.utils import pending_warnings
        resp = requests.get("https://openapi.ctrader.com/apps/token", params={
            "grant_type":    "refresh_token",
            "refresh_token": self.tokens["refresh_token"],
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
        }, timeout=15).json()
        if resp.get("errorCode"):
            logger.error(f"[CTRADER][TOKEN] Token refresh failed: {resp.get('errorCode')} - {resp.get('description')}")
            pending_warnings['ctrader'] = f"cTrader token refresh failed ({resp.get('errorCode')}) - check /log"
            return False
        self.tokens = {
            "access_token":  resp["accessToken"],
            "refresh_token": resp["refreshToken"],
            "expires_at":    time.time() + resp["expiresIn"],
        }
        self._save_tokens()
        self.access_token = self.tokens["access_token"]
        logger.info(f"[CTRADER][TOKEN] Access token refreshed (expires in ~{int(resp['expiresIn'] // 86400)} days)")
        # Refrescar invalida el token viejo y corta la sesión - hay que abrir una sesión nueva (reconectar).
        reactor.callFromThread(self._reconnect)
        return True

    def _maybe_refresh(self):
        # Renueva solo si hay refresh token y falta poco (< 2 días) para el vencimiento.
        if self.tokens.get("refresh_token") and time.time() > self.tokens["expires_at"] - 2 * 86400:
            self.refresh_access_token()

    def _token_refresh_loop(self):
        # Chequea una vez por día si toca renovar. Corre en su propio hilo: no toca el reactor ni las posiciones.
        while True:
            time.sleep(86400)
            try:
                self._maybe_refresh()
            except Exception as e:
                logger.error(f"[CTRADER][TOKEN] Error in token refresh loop: {e}")

    def _send(self, message):
        # Envía todos los mensajes por acá para descartar el timeout del Deferred de la librería
        # (nuestras respuestas llegan como eventos separados, sin clientMsgId).
        d = self.client.send(message)
        d.addErrback(lambda f: None if f.check(defer.TimeoutError, defer.CancelledError) else f)
        return d

    def _on_connected(self, client):
        # Al conectar, autentica la aplicación; la cadena de auth continúa en _on_message.
        req = ProtoOAApplicationAuthReq()
        req.clientId = self.client_id
        req.clientSecret = self.client_secret
        self._send(req)

    def _auth_account(self):
        # Autentica la CUENTA con el access token actual. Se usa en la cadena normal, tras el app-auth
        # (ProtoOAApplicationAuthRes). La respuesta (ProtoOAAccountAuthRes) dispara la lista de símbolos
        # y la re-suscripción de precios.
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = self.account_id
        req.accessToken = self.access_token
        self._send(req)

    def _reconnect(self):
        # Reconexión limpia tras un refresh.
        logger.info("[CTRADER] Reconnecting with new token")
        d = self.client.stopService()
        if d is not None:
            d.addCallback(lambda _: self.client.startService())
        else:
            self.client.startService()

    def _classify_symbols(self):
        # Separa el catálogo en forex / CFD. 
        if not (self._symbols_raw and self._category_class and self._asset_class_names):
            return
        forex_class_ids = {cid for cid, name in self._asset_class_names.items() if 'forex' in name.lower()}
        forex_cat_ids   = {cat_id for cat_id, class_id in self._category_class.items() if class_id in forex_class_ids}
        forex, cfd = [], []
        for name, cat_id in self._symbols_raw:
            (forex if cat_id in forex_cat_ids else cfd).append(name)
        self._forex_cache = sorted(forex)
        self._cfd_cache   = sorted(cfd)
        clases = [self._asset_class_names[cid] for cid in forex_class_ids]
        logger.info(f"[CTRADER] Symbols loaded: {len(self._forex_cache)} forex, {len(self._cfd_cache)} CFD")
        self._symbols_raw = []
        self._category_class = {}
        self._asset_class_names = {}

    def _on_message(self, client, message): #Aca procesa los mensajes que llegan y filtra segun el tipo
        if message.payloadType == ProtoOAErrorRes().payloadType:
            err = Protobuf.extract(message)
            logger.warning(f"[CTRADER] Server error: {err.errorCode} - {err.description}")
            desc = (err.description or "").lower()
            code = err.errorCode or ""
            token_error = ("not authorized" in desc or "access token" in desc
                           or code in ("CH_ACCESS_TOKEN_INVALID", "CH_ACCESS_TOKEN_EXPIRED"))
            if token_error:
                now = time.time()
                if now - self._last_auth_warning > 60:
                    self._last_auth_warning = now
                    from core.utils import pending_warnings
                    pending_warnings['ctrader'] = 'cTrader token inválido/no autorizado - renovando (ver /log)'
                if self.tokens.get("refresh_token") and now - self._last_refresh_attempt > 300:
                    self._last_refresh_attempt = now
                    logger.info(f"[CTRADER][TOKEN] {code or 'token error'} -> attempting token refresh (reactive)")
                    reactor.callInThread(self.refresh_access_token)
            return

        # Aplicación autenticada: continúa autenticando la cuenta.
        if message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            self._auth_account()

        # Cuenta autenticada: pide el catálogo de símbolos, la lista de assets (moneda quote),
        # el balance real de la cuenta y las clases/categorías necesarias para clasificar forex/CFD.
        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            req = ProtoOASymbolsListReq()
            req.ctidTraderAccountId = self.account_id
            self._send(req)
            req_assets = ProtoOAAssetListReq()
            req_assets.ctidTraderAccountId = self.account_id
            self._send(req_assets)
            req_trader = ProtoOATraderReq()   # balance real de la cuenta (base del control de margin); luego se actualiza vía ProtoOATraderUpdatedEvent
            req_trader.ctidTraderAccountId = self.account_id
            self._send(req_trader)
            req_ac = ProtoOAAssetClassListReq()
            req_ac.ctidTraderAccountId = self.account_id
            self._send(req_ac)
            req_cat = ProtoOASymbolCategoryListReq()
            req_cat.ctidTraderAccountId = self.account_id
            self._send(req_cat)


        # Catálogo de símbolos recibido: guarda los mapas de lookup y delega la clasificación forex/CFD.
        elif message.payloadType == ProtoOASymbolsListRes().payloadType:
            response = Protobuf.extract(message)
            self._symbols_raw = []
            for s in response.symbol:
                self._id_map[s.symbolName]     = s.symbolId
                self._rev_id_map[s.symbolId]   = s.symbolName
                self._quote_asset_id_map[s.symbolName] = s.quoteAssetId
                self._symbols_raw.append((s.symbolName, s.symbolCategoryId))
            self._classify_symbols()

            # Este paso cierra la cadena de auth de cada conexión. Como la suscripción spot es por conexión,
            # tras una reconexión se re-suscribe todo lo que estaba activo (símbolos operando + pares de
            # conversión). En el arranque el set está vacío y el bucle no hace nada.
            for sid in list(self._subscribed):
                req_sub = ProtoOASubscribeSpotsReq()
                req_sub.ctidTraderAccountId = self.account_id
                req_sub.symbolId.append(sid)
                self._send(req_sub)
                logger.info(f"[CTRADER][{self._rev_id_map.get(sid, sid)}] Re-subscribed after reconnection")

        # Lista de assets: arma el mapa de assetId a nombre de moneda (ej. 5: "USD").
        elif message.payloadType == ProtoOAAssetListRes().payloadType:
            response = Protobuf.extract(message)
            for asset in response.asset:
                self._asset_map[asset.assetId] = asset.name

        # Clases de activo: identifican cuál es Forex por nombre de clase.
        elif message.payloadType == ProtoOAAssetClassListRes().payloadType:
            response = Protobuf.extract(message)
            self._asset_class_names = {a.id: a.name for a in response.assetClass}
            self._classify_symbols()

        # Asocia cada símbolo con su clase de activo.
        elif message.payloadType == ProtoOASymbolCategoryListRes().payloadType:
            response = Protobuf.extract(message)
            self._category_class = {c.id: c.assetClassId for c in response.symbolCategory}
            self._classify_symbols()

        # Balance real de la cuenta
        elif message.payloadType in (ProtoOATraderRes().payloadType, ProtoOATraderUpdatedEvent().payloadType):
            from core.classes import gl
            trader = Protobuf.extract(message).trader
            gl.wallet_balance_ctrader = trader.balance / (10 ** trader.moneyDigits)
            logger.info(f"[CTRADER] Account real balance: {gl.wallet_balance_ctrader} USD")

        # Detalles de un símbolo (respuesta a start_price_feed): cachea digits, volúmenes y horario, y
        # suscribe el precio si el símbolo se pidió para operar.
        elif message.payloadType == ProtoOASymbolByIdRes().payloadType:
            from core.utils import symbols
            response = Protobuf.extract(message)
            for sym in response.symbol:
                is_subscribe = sym.symbolId in self._pending_subscribe   # pedido para operar: hay que suscribir el precio
                is_details   = sym.symbolId in self._pending_details     # pedido solo para cachear el horario, sin suscribir
                if not (is_subscribe or is_details):
                    continue
                self._pending_subscribe.discard(sym.symbolId)
                self._pending_details.discard(sym.symbolId)
                name = self._rev_id_map.get(sym.symbolId)
                if not name:
                    continue
                self._digits_map[name] = sym.digits  # decimales para escalar los precios del SpotEvent
                self._min_volume_map[name]  = sym.minVolume  # volumen mínimo, tamaño de lote y step (en cents) para el lote mínimo y el redondeo
                self._lot_size_map[name]    = sym.lotSize
                self._step_volume_map[name] = sym.stepVolume
                self._schedule_map[name] = list(sym.schedule)  # sesiones de apertura/cierre de la semana
                self._schedule_tz_map[name] = sym.scheduleTimeZone  # timezone del horario, para el pre-chequeo de mercado abierto

                if not is_subscribe:   # era solo precache del horario: no se suscribe el precio
                    continue

                req = ProtoOASubscribeSpotsReq()
                req.ctidTraderAccountId = self.account_id
                req.symbolId.append(sym.symbolId)
                self._send(req)
                self._subscribed.add(sym.symbolId)  # se anota para re-suscribirlo si la conexión se cae

                # Solo se loguea una posición real operando. El preview de spread del modal de Start y los
                # pares de conversión (sin container propio) pasan por acá pero no dejan rastro en el log.
                container = symbols.get(name.lower())
                if container and container.ps.socket_active:
                    logger.info(f"[CTRADER][{name.upper()}] Subscribed")

        # Actualiza el precio actual del símbolo. El try/except evita que una excepción
        # escape a Twisted y tire la conexión entera.
        elif message.payloadType == ProtoOASpotEvent().payloadType:
            try:
                self._process_tick(Protobuf.extract(message))
            except Exception as e:
                logger.error(f"[CTRADER] Error processing tick: {e}")

        # Confirmación de ejecución de orden. Mismo motivo que el tick para el try/except.
        elif message.payloadType == ProtoOAExecutionEvent().payloadType:
            try:
                self._process_execution(Protobuf.extract(message))
            except Exception as e:
                logger.error(f"[CTRADER] Error processing execution: {e}")

        # Velas históricas para el gráfico.
        elif message.payloadType == ProtoOAGetTrendbarsRes().payloadType:
            response = Protobuf.extract(message)
            symbol_id = response.symbolId
            candles = []
            for tb in response.trendbar:
                # cTrader manda los precios delta-encoded sobre 'low' y escalados x100000 (igual que el spot).
                # open/high/close = low + su delta; el timestamp viene en MINUTOS.
                low = tb.low / 100000
                candles.append({
                    "time":   tb.utcTimestampInMinutes * 60,      # segundos UTC (lo que espera lightweight-charts)
                    "open":   (tb.low + tb.deltaOpen)  / 100000,
                    "high":   (tb.low + tb.deltaHigh)  / 100000,
                    "low":    low,
                    "close":  (tb.low + tb.deltaClose) / 100000,
                    "volume": tb.volume,
                })
            self._trendbar_data[symbol_id] = candles
            ev = self._trendbar_events.get(symbol_id)
            if ev:
                ev.set()   # despierta a get_chart_data() que está bloqueado esperando

    def _process_tick(self, spot_event):
        # Procesa un tick de precio: actualiza el precio del símbolo y, si está operando, abre o gestiona la posición.
        from core.utils import get_vars, symbols
        from core.logic_c import open_position_phase1, manage_position

        symbol_id = spot_event.symbolId # Buscamos el nombre del símbolo usando el ID que viene en el evento
        name = self._rev_id_map.get(symbol_id)
        digits = self._digits_map.get(name) # digits define a cuántos decimales redondear el precio ya escalado (varía por símbolo: EURUSD=5, AUDJPY=3)
        
        bid = spot_event.bid # bid y ask llegan como enteros; pueden no actualizarse en cada tick (valen 0 si no cambiaron)
        ask = spot_event.ask

        # Cache de bid/ask por separado (0 = sin cambio en este tick) para el spread del modal de Start.
        # Se descalan con el mismo factor FIJO ÷100000 que el precio (no por digits).
        if bid > 0: self._bid_map[name] = bid / 100000
        if ask > 0: self._ask_map[name] = ask / 100000

        if bid > 0 and ask > 0: raw_price = (bid + ask) / 2
        elif bid > 0: raw_price = bid
        elif ask > 0: raw_price = ask
        else: return# Tick sin precio utilizable (puede ser un trendbar u otro tipo de evento)

        price = round(raw_price / 100000, digits) # cTrader manda los precios spot en formato relativo escalado por 100000 (FIJO, no por digits). Se convierte a precio real dividiendo por 100000 y recién ahí se redondea a los digits del símbolo.
        self._price_map[name] = price # Guardamos el ultimo precio de todo simbolo suscrito (aunque no se opere) para las conversiones a USD

        container = symbols.get(name.lower()) #esto evita que lleguen tiks tardios cuando ya se cancelo
        if not container or not container.ps.socket_active:
            return

        # INICIO DEL PROCESAMIENTO DE TICK
        ps, fd, rt = get_vars(name.lower())
        rt.current_price = price

        if fd.restart:
            if self.conv_quote_usd(name) == 0: #Repite el restart para si la quote no es USD no cargue hasta tener el par quote para dimensionar.
                return
            fd.restart = False
            rt.current_price = price
            fd.r0 = rt.current_price
            fd.price_decimals = digits 
            fd.Qty_min = self.get_min_lot(name)
            fd.steps_Qty_var = self._step_volume_map.get(name) / self._lot_size_map.get(name) 
            rt.current_datetime = datetime.now(timezone.utc).replace(tzinfo=None)
            fd.open_time = rt.current_datetime
            open_position_phase1(name, ps, fd, rt)

        if rt.current_price != rt.previous_price:
            rt.previous_price = rt.current_price
            rt.current_datetime = datetime.now(timezone.utc).replace(tzinfo=None)
            manage_position(ps, fd, rt, name)

    def _process_execution(self, execution_event):
        # Procesa la confirmación de una orden: fill de apertura/cierre, rechazo, o swap overnight.
        from core.utils import get_vars, symbols
        from core.logic_c import open_position_phase2,handle_take_profit, handle_close

        exec_type = execution_event.executionType # Semáforo del evento: FILLED (ok), REJECTED/CANCELLED/EXPIRED (error), ACCEPTED (aún no llenada)

        # Cuando cTrader ACEPTA una TP LIMIT (atada a una posición) guardamos su orderId para poder
        # cancelarla al cerrar la posición (cTrader NO las cancela solo). El id llega recién acá.
        if exec_type == ProtoOAExecutionType.ORDER_ACCEPTED:
            order = execution_event.order
            if order.orderType == ProtoOAOrderType.LIMIT and order.positionId:
                self._pending_orders.setdefault(order.positionId, set()).add(order.orderId)
            return

        # SWAP: cTrader lo manda como un ExecutionEvent aparte, identificamos el símbolo por la posición y aplicamos el monto en USD.
        if exec_type == ProtoOAExecutionType.SWAP:
            from core.logic_c import apply_swap  # función sync definida en logic_c.py
            pos  = execution_event.position
            name = self._rev_id_map.get(pos.tradeData.symbolId)   # símbolo desde la posición a la que se le cobró el swap
            if not name:
                logger.warning(f"[CTRADER][SWAP] Could not identify symbol (positionId={pos.positionId})")
                return
            container = symbols.get(name.lower())
            if not container or not container.ps.socket_active:   # evita swaps tardíos de una posición ya cerrada
                return
            ps, fd, rt = get_vars(name.lower())

            # El swap viene acumulado asi que le restamos el acumulado anterior.
            new_swap = pos.swap / (10 ** pos.moneyDigits)
            swap     = new_swap - rt.swap

            rt.current_datetime = datetime.now(timezone.utc).replace(tzinfo=None)
            logger.info(f"[CTRADER][{name.upper()}] Swap {swap} USD (pos.swap={pos.swap}, moneyDigits={pos.moneyDigits}, accumulated={new_swap})")
            apply_swap(name, ps, fd, rt, swap)
            return

        if exec_type not in (ProtoOAExecutionType.ORDER_FILLED, 
                             ProtoOAExecutionType.ORDER_REJECTED,
                             ProtoOAExecutionType.ORDER_CANCELLED,
                             ProtoOAExecutionType.ORDER_EXPIRED):
            return

        if exec_type != ProtoOAExecutionType.ORDER_FILLED:
            from core.utils import var_restart, pending_warnings
            order = execution_event.order
            name  = self._rev_id_map.get(order.tradeData.symbolId)
            if not name:
                logger.warning(f"[CTRADER] Errored order {exec_type} (errorCode={execution_event.errorCode}) with no identifiable symbol")
                return
            container = symbols.get(name.lower())
            if not container or not container.ps.socket_active:
                return
            ps, fd, rt = get_vars(name.lower())
            if fd.id_order:   # la posición YA estaba abierta -> el rechazo es de un condicional (TP/SL), no la tocamos
                # Aviso en la UI: la posición puede haber quedado SIN SL o sin un TP. No se re-coloca
                # automáticamente; el usuario decide (cerrar manual / re-colocar desde cTrader).
                logger.warning(f"[CTRADER][{name.upper()}] Conditional order rejected: {exec_type} - errorCode={execution_event.errorCode}")
                pending_warnings[name.lower()] = f'TP/SL rejected for {name} ({execution_event.errorCode}): check position protection!'
                return
            # Rechazo de la orden PRINCIPAL -> la posición no se abrió: limpiamos el socket fantasma.
            logger.warning(f"[CTRADER][{name.upper()}] Main order rejected: {exec_type} - errorCode={execution_event.errorCode}")
            pending_warnings[name.lower()] = f'Order rejected for {name} ({execution_event.errorCode})'
            self.stop_price_feed(name.upper())
            var_restart([name.lower()])
            return

        deal = execution_event.deal # El deal es la ejecución concreta contra el mercado: trae el symbolId, el orderId
        name = self._rev_id_map.get(deal.symbolId) # Traducimos el symbolId del deal al nombre del símbolo
        if not name:
            return

        container = symbols.get(name.lower()) #esto evita que lleguen ejecuciones tardías cuando ya se cerró el socket
        if not container or not container.ps.socket_active:
            return
        ps, fd, rt = get_vars(name.lower())

        pos = execution_event.position #Guardamos el volumen abierto para que sepa que hay en caso de un cierre manual:
        if pos.positionId:
            self._position_volume[pos.positionId] = pos.tradeData.volume

        if exec_type == ProtoOAExecutionType.ORDER_FILLED:
            if deal.HasField('closePositionDetail'): # Fill de CIERRE (TP1, TP2, SL o cierre manual): el deal reduce la posición
                cpd         = deal.closePositionDetail
                price       = deal.executionPrice
                order_id    = deal.orderId
                position_id = deal.positionId                                    # id de la posición cerrada
                qty_closed  = deal.filledVolume / self._lot_size_map.get(name)   # volumen cerrado en lotes
                # grossProfit es BRUTO: le restamos la comisión del deal (abs: algunos brokers la mandan
                # negativa) para que balance/DB queden NETOS, como en Binance. Si el broker no cobra, llega 0.
                commission  = abs(deal.commission) / (10 ** deal.moneyDigits)
                pnl         = cpd.grossProfit / (10 ** cpd.moneyDigits) - commission
                if commission:
                    logger.debug(f"[CTRADER][{name.upper()}] Close commission: {commission} (raw={deal.commission}, moneyDigits={deal.moneyDigits})")

                # Como la orden se ejecuto la sacamos de las pendientes para no intentar cancelarla después
                self._pending_orders.get(position_id, set()).discard(order_id)

                
                if not rt.stop_manual:
                    # Solo incluimos los TP que siguen PENDIENTES (Qty>0): así, cuando el SL trailea
                    # sobre un nivel de TP ya ejecutado, no hay empate y el cierre se clasifica bien.
                    levels = {'SL': rt.r_1}
                    if rt.Qty_r1 > 0: levels['TP1'] = fd.r1
                    if rt.Qty_r2 > 0: levels['TP2'] = fd.r2
                    which  = min(levels, key=lambda k: abs(price - levels[k]))
                    if which == 'TP1':
                        rt.r1_active = True
                        handle_take_profit(name, ps, fd, rt,price, cpd, order_id, pnl, qty_closed)   # en logic_c
                    elif which == 'TP2':
                        rt.r2_active = True
                        handle_take_profit(name, ps, fd, rt,price, cpd, order_id, pnl, qty_closed)   # en logic_c
                    elif which == 'SL':
                        handle_close(name, ps, fd, rt,price, cpd, order_id, pnl, qty_closed)
                    logger.info(f"[CTRADER][{name.upper()}] {which} executed - positionId={position_id}, price={price}, volume={qty_closed}, pnl={pnl}")
                else: #Cierre manual
                    handle_close(name, ps, fd, rt,price, cpd, order_id, pnl, qty_closed)
                    logger.info(f"[CTRADER][{name.upper()}] Manual stop executed - positionId={position_id}, price={price}, volume={qty_closed}, pnl={pnl}")

            else: # Fill de APERTURA: el deal abre la posición principal -> sigue con la Fase 2
                fd.id_order = deal.positionId          # id de la posicion que abrió la posición, se usa para abrir condicionales pero no se registra
                id_order = deal.orderId #Es la que se registra en la DB y sirve para buscar en ctrader
                PE_order    = deal.executionPrice   # precio real al que se ejecutó la orden
                rt.commission = abs(deal.commission) / (10 ** deal.moneyDigits)
                if rt.commission:
                    logger.debug(f"[CTRADER][{name.upper()}] Open commission: {rt.commission} (raw={deal.commission}, moneyDigits={deal.moneyDigits})")
                rt.used_margin = pos.usedMargin / (10 ** pos.moneyDigits)
                open_position_phase2(name, ps, fd, rt, PE_order, id_order)
                logger.info(f"[CTRADER][{name.upper()}] Main order executed - id={fd.id_order}, entry_price={PE_order}")

    def get_min_lot(self, symbol_name: str) -> float:
        """Devuelve el tamaño mínimo de lote del símbolo (minVolume / lotSize)."""
        min_volume = self._min_volume_map.get(symbol_name)
        lot_size   = self._lot_size_map.get(symbol_name)
        if not min_volume or not lot_size:
            return 0.0
        return min_volume / lot_size

    def _get_quote_currency(self, symbol_name: str) -> str:
        """Devuelve el nombre de la moneda quote del símbolo (ej. 'USD', 'JPY')."""
        asset_id = self._quote_asset_id_map.get(symbol_name)         # combina los mapas nombre->quoteAssetId y assetId->nombre
        return self._asset_map.get(asset_id, "")

    def conv_quote_usd(self, symbol_name: str) -> float:
        """Factor para convertir el P&L (en moneda quote) a USD. Devuelve 1.0 si la quote ya es USD."""
        quote = self._get_quote_currency(symbol_name)
        if quote == "USD": return 1.0
        directo = self._price_map.get(quote + "USD") # Quote no es USD: buscamos el par de conversion contra USD en los precios cacheados.
        if directo:return directo # Directo: quote es la base (ej. EUR en EURUSD, el precio ya es USD por 1 unidad de quote)
        inverso = self._price_map.get("USD" + quote) # Inverso: quote es la quote del par (ej. JPY en USDJPY da JPY por USD, hay que invertir)
        if inverso: return 1.0 / inverso
        return 0.0 # Sin precio del par de conversion no podemos convertir (el par debe estar suscrito)

    def _ensure_conversion_feed(self, symbol_name: str): #Se ejecuta en start_price_feed
        """Si la quote del símbolo no es USD, suscribe también el par de conversión a USD."""
        from core.utils import symbols
        quote = self._get_quote_currency(symbol_name)
        if quote == "USD" or not quote: return  # ya cotiza en USD (o no sabemos la quote todavía): no hace falta convertir
        directo = quote + "USD" # Elegimos el par de conversión que exista en el catálogo: directo (quote+USD) o inverso (USD+quote)
        inverso = "USD" + quote
        par = directo if directo in self._id_map else (inverso if inverso in self._id_map else None)
        if not par:
            logger.warning(f"[CTRADER] No conversion pair found for {quote}")
            return
        if par in self._digits_map or self._id_map.get(par) in self._pending_subscribe: #Evitamos resuscribir, por que al susqcribir el simbolo quote volveriamos aca
            return
        self.start_price_feed(par)
        # El par de conversión (ej. AUDUSD) no tiene container propio -> el criterio de "real" se
        # chequea sobre symbol_name (el instrumento original que lo necesita, ej. EURAUD).
        container = symbols.get(symbol_name.lower())
        if container and container.ps.socket_active:
            logger.info(f"[CTRADER][{symbol_name.upper()}] Conversion pair subscribed: {par}")

    def calculate_lots(self, symbol_name: str, risk_usd: float, sl_distance: float): #Se ejecuta en logic para obtener el Qty_min
        """Calcula el tamaño de posición en LOTES para arriesgar risk_usd si el precio toca el SL.
        sl_distance es la distancia en precio entre la entrada y el stop (|r0 - r_1|).
        Redondea hacia abajo al step del broker; NO chequea el mínimo (esa decisión la toma la logic)."""
        conv = self.conv_quote_usd(symbol_name) #conv: cuántos USD vale 1 unidad de la moneda quote (1.0 si la quote ya es USD)        
        lot_size = self._lot_size_map.get(symbol_name) #lot_size: tamaño de 1 lote expresado en cents (unidades × 100)        
        step = self._step_volume_map.get(symbol_name) #step: incremento mínimo de volumen permitido por el broker, en cents

        if conv == 0 or sl_distance == 0 or not lot_size or not step: return 0.0#Chequeamos que si falta algun dato de 0 para que logic no abra la posicion
        
        units = risk_usd / (sl_distance * conv) #Despejamos units para que la pérdida al tocar el SL sea exactamente risk_usd.
        volume = int(units * 100) #Pasamos las unidades a cents (la unidad entera con la que opera el broker: units × 100)
        volume = (volume // step) * step #Redondeamos HACIA ABAJO al múltiplo de step más cercano.

        return volume / lot_size #Devolvemos el resultado en lotes (0.1, 1.15), que es la unidad que usa el resto del sistema

    def min_risk_usd(self, symbol_name: str, sl_distance: float): #Me da la cantidad minima de riesgo USD si no pasa el filtro.
        conv       = self.conv_quote_usd(symbol_name)
        min_volume = self._min_volume_map.get(symbol_name)
        if conv == 0 or sl_distance == 0 or not min_volume:
            return 0.0
        units_min = (min_volume / 100) * 4
        return round(units_min * sl_distance * conv, 2)

    def pnl_usd(self, symbol_name: str, price_diff: float, lots: float):
        """P&L en USD de 'lots' cuando el precio se movió price_diff a favor (ya con signo).
        Convierte lotes a unidades (× lot_size/100) y quote a USD (× conv), la misma relación
        que usan calculate_lots y min_risk_usd."""
        conv     = self.conv_quote_usd(symbol_name)
        lot_size = self._lot_size_map.get(symbol_name)   # cents por lote
        if not lot_size or conv == 0:   # falta algún dato (arranque / feed de conversión sin precio aún): evita crash
            return 0.0
        units = lots * lot_size / 100                     # lotes a unidades (cents/100)
        return price_diff * units * conv                  # a USD

    def start_price_feed(self, symbol_name: str):
        """Devuelve True si se pidió la suscripción, False si el símbolo no está en el
        catálogo (cTrader sin conectar todavía o ticker inexistente) para que la ruta haga rollback."""
        symbol_id = self._id_map.get(symbol_name) # Verificamos que el símbolo exista en el catálogo descargado al conectar
        if not symbol_id:
            logger.warning(f"[CTRADER][{symbol_name.upper()}] Symbol not found in catalog, cannot start feed")
            return False

        self._pending_subscribe.add(symbol_id) # Marcamos el symbolId como pendiente: cuando llegue ProtoOASymbolByIdRes el handler existente guardará los digits y enviará ProtoOASubscribeSpotsReq

        def _send_request(): # Pedimos los detalles del símbolo para obtener los digits de escala del precio
            req = ProtoOASymbolByIdReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId.append(symbol_id)
            self._send(req)

        reactor.callFromThread(_send_request) #reactor.callFromThread es necesario porque esta función se llama desde el hilo de Flask, no desde el hilo de Twisted donde corre el reactor
        self._ensure_conversion_feed(symbol_name) # Si la quote no es USD, aseguramos el feed del par de conversión (ej. EURGBP suscribe GBPUSD)
        return True

    def stop_price_feed(self, symbol_name: str):
        from core.utils import symbols
        symbol_id = self._id_map.get(symbol_name)
        if not symbol_id:
            logger.warning(f"[CTRADER][{symbol_name.upper()}] Symbol not found to cancel subscription")
            return

        self._subscribed.discard(symbol_id)  # ya no hay que re-suscribirlo si la conexión se cae

        # Sin feed, el bid/ask cacheado envejece sin que nadie lo pise: si quedara, el próximo
        # get_spread_snapshot lo devolvería como "actual" (horas viejo) y el gate de phase1 podría
        # mezclar un lado fresco con uno viejo (spread falso o negativo). Se borra acá.
        self._bid_map.pop(symbol_name, None)
        self._ask_map.pop(symbol_name, None)

        def _send_request():
            req = ProtoOAUnsubscribeSpotsReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId.append(symbol_id)
            self._send(req)

        reactor.callFromThread(_send_request) # Igual que en start_price_feed: hay que enviar desde el hilo del reactor

        # Mismo criterio que en la confirmación de "Suscripto": solo se loguea si era una posición
        # REAL (socket_active). El cierre se llama antes de var_restart, que es quien apaga el flag,
        # así que en un cierre real todavía está en True acá.
        container = symbols.get(symbol_name.lower())
        if container and container.ps.socket_active:
            logger.info(f"[CTRADER][{symbol_name.upper()}] Cancelled")

    def spread(self, symbol_name: str):
        """Spread ACTUAL (ask - bid) del cache, en precio real. None si falta algún lado (fail-open
        en el gate). En phase1 el símbolo ya está suscrito, así que normalmente ambos están frescos."""
        bid = self._bid_map.get(symbol_name)
        ask = self._ask_map.get(symbol_name)
        if not bid or not ask:
            return None
        if ask < bid:
            return None   # cruce imposible en datos sanos = un lado quedó viejo -> no usar (fail-open)
        return ask - bid

    def get_spread_snapshot(self, symbol_name: str, timeout: float = 3.0):
        """One-shot para el modal de Start: devuelve {bid, ask, digits} del símbolo.
        Si ya está suscrito (operando o par de conversión), lee del cache al instante.
        Si no, suscribe un feed EFÍMERO, espera el primer bid+ask y lo desuscribe.
        Blocking (polling con timeout): se llama desde el hilo de Flask, como get_chart_data."""
        import time as _t
        from core.utils import symbols

        symbol_id = self._id_map.get(symbol_name)
        if not symbol_id:
            return {"error": f"Símbolo {symbol_name} no encontrado en catálogo"}

        already_subscribed = symbol_id in self._subscribed
        if not already_subscribed:
            self.start_price_feed(symbol_name)   # pide detalles (digits) + suscribe spots

        # Espera hasta tener bid Y ask cacheados (los pobla _process_tick al llegar cada tick).
        deadline = _t.time() + timeout
        bid = ask = None
        while _t.time() < deadline:
            bid = self._bid_map.get(symbol_name)
            ask = self._ask_map.get(symbol_name)
            if bid and ask:
                break
            _t.sleep(0.1)

        # Desuscribir SOLO si el feed lo abrimos nosotros y NO se abrió una posición real mientras
        # tanto (si el usuario apretó Start durante la ventana, stop_price_feed mataría su feed).
        if not already_subscribed:
            container = symbols.get(symbol_name.lower())
            if not (container and container.ps.socket_active):
                self.stop_price_feed(symbol_name)

        if not (bid and ask):
            return {"error": "timeout esperando cotización"}
        return {"bid": bid, "ask": ask, "digits": self._digits_map.get(symbol_name)}

    def _format_schedule(self, schedule, tz_name):
        # cTrader entrega cada intervalo de sesión en segundos desde el DOMINGO 00:00 en 'tz_name'.
        # Devuelve un dict (todo en UTC):
        #   'text'          -> horario en una línea (para el log)
        #   'rows'          -> lista de {'start','end'} por sesión (para el recuadro de main_loop)
        #   'is_open'       -> si el mercado está abierto AHORA (respeta los cortes de mantenimiento)
        #   'opens_in'      -> string tipo "1 day(s) 07:01" hasta la próxima apertura, o None si ya está abierto
        #   'next_open_idx' -> índice de la fila que abre a continuación (para el cartel "Opens in"), o None
        # Sin horario => siempre abierto, sin filas (no bloqueamos).
        if not schedule:
            return {'text': 'no schedule', 'rows': [], 'is_open': True, 'opens_in': None, 'next_open_idx': None}
        try:
            tz = ZoneInfo(tz_name) if tz_name else timezone.utc
        except Exception:
            tz = timezone.utc

        # Intervalos tal cual los da cTrader, SIN fusionar: mostramos las sesiones reales (con sus cortes de
        # mantenimiento de minutos/horas) para que el chequeo de mercado abierto respete esos huecos.
        intervals = sorted((iv.startSecond, iv.endSecond) for iv in schedule)

        # Cada segundo (desde el domingo 00:00 en la TZ del schedule) -> "Day HH:MM:SS" en UTC.
        today  = datetime.now(tz).date()
        sunday = today - timedelta(days=(today.weekday() + 1) % 7)   # domingo de esta semana como referencia (resuelve offset/DST)
        def _fmt(sec):
            u = (datetime(sunday.year, sunday.month, sunday.day, tzinfo=tz) + timedelta(seconds=sec)).astimezone(timezone.utc)
            return u.strftime('%a %H:%M:%S')   # ej. "Sun 21:01:00" en UTC

        rows = [{'start': _fmt(s), 'end': _fmt(e)} for s, e in intervals]
        text = " | ".join(f"{r['start']} → {r['end']}" for r in rows)

        now = datetime.now(tz)
        now_sec = ((now.weekday() + 1) % 7) * 86400 + now.hour * 3600 + now.minute * 60 + now.second
        is_open = any(s <= now_sec < e for s, e in intervals)

        opens_in = next_open_idx = None
        if not is_open:
            # Próxima apertura: primer intervalo con start > ahora; si no queda ninguno esta semana, el primero de la que viene (+604800).
            future = [(i, s) for i, (s, e) in enumerate(intervals) if s > now_sec]
            if future:
                next_open_idx, nxt = future[0]
            else:
                next_open_idx, nxt = 0, intervals[0][0] + 604800
            delta  = nxt - now_sec
            d, rem = delta // 86400, delta % 86400
            opens_in = (f"{d} day(s) " if d else "") + f"{rem // 3600:02d}:{(rem % 3600) // 60:02d}"

        return {'text': text, 'rows': rows, 'is_open': is_open, 'opens_in': opens_in, 'next_open_idx': next_open_idx}

    def request_symbol_details(self, symbol_name: str):
        # Pide los detalles del símbolo (incluye el horario de mercado) SIN suscribir el precio.
        # Lo usa el recuadro de horarios para cachear el schedule on-demand cuando abrís un símbolo no suscrito.
        symbol_id = self._id_map.get(symbol_name)
        if not symbol_id:
            logger.warning(f"[CTRADER][{symbol_name.upper()}] Symbol not found in catalog (details)")
            return

        self._pending_details.add(symbol_id)  # marcado como "solo detalles": el handler guardará el schedule pero NO suscribirá

        def _send_request():
            req = ProtoOASymbolByIdReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId.append(symbol_id)
            self._send(req)

        reactor.callFromThread(_send_request)  # se llama desde el hilo de Flask, hay que enviar desde el hilo del reactor

    def order_market(self, symbol_name: str, side: str, lots: float):
        symbol_id = self._id_map.get(symbol_name)
        if not symbol_id:
            logger.warning(f"[CTRADER][{symbol_name.upper()}] Symbol not found in catalog")
            return

        lot_size = self._lot_size_map.get(symbol_name)
        if not lot_size:
            logger.warning(f"[CTRADER][{symbol_name.upper()}] Missing lotSize, cannot send order")
            return

        trade_side = ProtoOATradeSide.BUY if side == "BUY" else ProtoOATradeSide.SELL        
        volume = int(lots * lot_size) #Convertimos lotes a volume (cents): lotes × lotSize da el volumen que espera cTrader.

        def _send_request():
            req = ProtoOANewOrderReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId = symbol_id
            req.orderType = ProtoOAOrderType.MARKET
            req.tradeSide = trade_side
            req.volume = volume
            self._send(req)
            # Fallback: si en _OPEN_TIMEOUT s no llegó el fill de apertura (ni el rechazo), asumimos que
            # la orden falló sin evento y limpiamos el socket fantasma. callLater corre en el hilo del reactor.
            reactor.callLater(self._OPEN_TIMEOUT, self._check_open_timeout, symbol_name)

        reactor.callFromThread(_send_request) #Igual que en start_price_feed: hay que enviar desde el hilo del reactor
        logger.info(f"[CTRADER][{symbol_name.upper()}] MARKET order sent")

    _OPEN_TIMEOUT = 10  # segundos a esperar el fill de apertura antes de asumir que la orden no se abrió

    def _check_open_timeout(self, symbol_name):
        """Si tras enviar la MARKET no llegó el fill de apertura (fd.id_order sigue en 0), limpia el fantasma.
        Idempotente: si la posición abrió o el socket ya se cerró, no hace nada."""
        from core.utils import get_vars, symbols, var_restart, pending_warnings
        container = symbols.get(symbol_name.lower())
        if not container or not container.ps.socket_active:
            return                       # ya se cerró/limpió -> nada que hacer
        ps, fd, rt = get_vars(symbol_name.lower())
        if fd.id_order:                  # la posición abrió (llegó el fill) -> ok, no tocar
            return
        logger.warning(f"[CTRADER][{symbol_name.upper()}] No open confirmation in {self._OPEN_TIMEOUT}s -> assuming order not opened, cleaning up")
        pending_warnings[symbol_name.lower()] = f'Order not confirmed for {symbol_name} (timeout)'
        self.stop_price_feed(symbol_name.upper())
        var_restart([symbol_name.lower()])

    def order_tp(self, symbol_name: str, side: str, lots: float, price: float, position_id: int):
        """Take profit como orden LIMIT atada a la posición (positionId la cierra parcialmente)."""
        symbol_id = self._id_map.get(symbol_name)
        lot_size  = self._lot_size_map.get(symbol_name)
        if not symbol_id or not lot_size:
            return

        volume = int(lots * lot_size) #Convertimos lotes a volume (cents): lotes × lotSize
        trade_side = ProtoOATradeSide.BUY if side == "BUY" else ProtoOATradeSide.SELL

        def _send_request():
            req = ProtoOANewOrderReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId   = symbol_id
            req.orderType  = ProtoOAOrderType.LIMIT
            req.tradeSide  = trade_side          # lado opuesto a la entrada (fd.side_close)
            req.volume     = volume
            req.limitPrice = price               # precio del TP (R1 o R2)
            req.positionId = position_id         # ata la orden a la posición para reducirla
            self._send(req)

        reactor.callFromThread(_send_request) #Igual que en order_market: hay que enviar desde el hilo del reactor
        logger.info(f"[CTRADER][{symbol_name.upper()}] TP LIMIT sent")

    def set_position_sl(self, symbol_name: str, position_id: int, sl_price: float):
        """Stop loss atado a la posición: cierra TODA la posición al tocarlo (equivale al closePosition=true de Binance).
        Cubre solo el volumen restante automáticamente. Llamar de nuevo con otro sl_price para MOVER el SL (breakeven/trailing)."""
        def _send_request():
            req = ProtoOAAmendPositionSLTPReq()
            req.ctidTraderAccountId = self.account_id
            req.positionId = position_id
            req.stopLoss   = sl_price          # precio absoluto del SL (double real, sin escalar)
            self._send(req)

        reactor.callFromThread(_send_request) #Igual que en order_market: hay que enviar desde el hilo del reactor
        logger.info(f"[CTRADER][{symbol_name.upper()}] SL set - positionId={position_id}, sl={sl_price}")

    def close_position(self, symbol_name: str, position_id: int):
        """Cierra TODA la posición a mercado (cierre manual). Usa el volumen restante que reporta
        el broker (_position_volume).
        No devuelve nada: el id/pnl/volumen llegan después a _process_execution (rama de cierre)."""
        volume = self._position_volume.get(position_id, 0)  # volumen abierto autoritativo del broker, en cents
        if not volume:
            logger.warning(f"[CTRADER][{symbol_name.upper()}] No known volume for positionId={position_id}, not closing")
            return

        def _send_request():
            req = ProtoOAClosePositionReq()
            req.ctidTraderAccountId = self.account_id
            req.positionId = position_id
            req.volume     = volume          # volumen restante en cents (todo lo que quede abierto)
            self._send(req)

        reactor.callFromThread(_send_request) #Igual que en order_market: hay que enviar desde el hilo del reactor
        logger.info(f"[CTRADER][{symbol_name.upper()}] Full close sent - positionId={position_id}")

    def cancel_position_orders(self, position_id: int):
        """Cancela las TP LIMIT pendientes atadas a una posición.
        Se llama desde handle_close, después de que la posición ya cerró."""
        order_ids = self._pending_orders.pop(position_id, set())  # las sacamos del map y las cancelamos todas
        for order_id in order_ids:
            def _send_request(oid=order_id):  # oid=order_id captura el valor del loop en cada closure
                req = ProtoOACancelOrderReq()
                req.ctidTraderAccountId = self.account_id
                req.orderId = oid
                self._send(req)
            reactor.callFromThread(_send_request)
        if order_ids:
            logger.info(f"[CTRADER] Cancelled {len(order_ids)} pending TP of positionId={position_id}")

    def start(self):
        """Inicia la conexión con cTrader. Llamar al arrancar la app."""
        # Renueva el token si está por vencer ANTES de conectar, así la primera autenticación usa uno fresco.
        try:
            self._maybe_refresh()
        except Exception as e:
            logger.error(f"[CTRADER][TOKEN] Error refreshing token at startup: {e}")
        threading.Thread(target=self._token_refresh_loop, daemon=True).start()  # chequeo diario en segundo plano
        self.client.startService()
        reactor.run(installSignalHandlers=False) #installSignalHandlers=False requerido al correr el reactor fuera del main thread

    def get_chart_data(self, symbol_name: str, count: int = 180):
        # Equivalente cTrader del get_chart_data de Binance: devuelve las últimas 'count' velas diarias (D1).
        # Es BLOQUEANTE: manda el request async y espera (en el hilo de Flask) hasta que la respuesta
        # llega por _on_message (hilo del reactor) y libera el Event. Formato de vela idéntico al de Binance.
        symbol_id = self._id_map.get(symbol_name)
        if not symbol_id:
            return {"error": f"Símbolo {symbol_name} no encontrado en catálogo cTrader"}

        now_ms  = int(datetime.now(timezone.utc).timestamp() * 1000)
        from_ms = now_ms - count * 86400 * 1000   # 'count' días hacia atrás (velas D1)

        event = threading.Event()
        self._trendbar_events[symbol_id] = event   # el handler de ProtoOAGetTrendbarsRes lo va a setear

        def _send_request():
            req = ProtoOAGetTrendbarsReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId      = symbol_id
            req.period        = ProtoOATrendbarPeriod.D1
            req.fromTimestamp = from_ms
            req.toTimestamp   = now_ms
            self._send(req)

        reactor.callFromThread(_send_request)

        if not event.wait(timeout=8):   # la respuesta no llegó a tiempo
            self._trendbar_events.pop(symbol_id, None)
            return {"error": "Timeout esperando velas de cTrader"}

        candles = self._trendbar_data.pop(symbol_id, [])
        self._trendbar_events.pop(symbol_id, None)
        return {"symbol": symbol_name, "candles": candles}



ctrader = CTraderExchange()
