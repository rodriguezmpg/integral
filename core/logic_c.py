"""Algoritmo de R-multiplos para cTrader (forex/CFD, sync sobre el reactor de Twisted).

Es el espejo de logic.py (Binance); misma estrategia, dos diferencias de fondo por el broker:
- La apertura se parte en dos: open_position_phase1 (pre-chequeos + orden a mercado) y
  open_position_phase2, que se ejecuta cuando llega el evento de fill (recién ahí se conocen el precio
  de entrada real y el id de posición para colocar TP/SL). En Binance es una sola función async.
- handle_take_profit y handle_close los dispara un evento de ejecución de cTrader, no el tick de
  precio; manage_position acá solo se ocupa del trailing y las métricas vivas.

Todo es sync porque corre en el hilo del reactor de Twisted (ver ctrader_ext).
"""
from core.dbfunc import write_db, order_event_row, write_tradehistory_db, read_last_dd_index, read_max_dd_index, read_max_drawdown_pct
from core.exchanges.ctrader_ext import ctrader
from core.classes import gl
import os, psycopg2
from core.utils import var_restart, pending_warnings, margin_precheck

import logging
logger = logging.getLogger('reg')

def open_position_phase1(symbol, ps, fd, rt):
    """Primera fase de apertura: arma los niveles R y corre los pre-chequeos (horario de mercado,
    cantidad mínima, margin y spread de apertura) antes de mandar la orden a mercado. No coloca los
    TP/SL todavía: el precio de entrada real llega después como evento de fill (ver phase2)."""
    # Pre-chequeo de horario: si el mercado está cerrado, no mandamos la orden (evita el rechazo MARKET_CLOSED de cTrader).
    # Mismo tratamiento que el filtro de cantidad mínima: avisamos al usuario, cortamos el feed y reseteamos.
    sched = ctrader._format_schedule(ctrader._schedule_map.get(symbol, []), ctrader._schedule_tz_map.get(symbol))
    if not sched['is_open']:
        fd.control = False
        pending_warnings[symbol] = f'Market closed for {symbol}. Opens in {sched["opens_in"]}'
        ctrader.stop_price_feed(symbol.upper())   # corta la suscripción spot en cTrader antes de resetear (si no, sigue llegando el tick)
        var_restart([symbol])
        return

    conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
    cur = conn.cursor()
    cur.execute("SELECT id_pos FROM order_event WHERE symbol = %s ORDER BY pk DESC LIMIT 1", (symbol,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row and row[0]: fd.id_pos = int(row[0]) + 1
    else: fd.id_pos = 1
    
    fd.validation_message = 'Accepted'
    rt.breakeven_stage = -1
    rt.ALGO_pos = 'R0'

    if ps.check_2_1:
        fd.perc2r = round(abs(ps.target_price - fd.r0) / fd.r0 * 100,4)
        fd.perc1r = round((fd.perc2r / 2),4)
        fd.perc1_r = round(-fd.perc1r,4)
        if fd.r0 < ps.target_price:
            fd.type_pos = 'LONG'
            fd.r_1 = round(fd.r0 * (1 - (fd.perc1r / 100)), fd.price_decimals)
            fd.r1 = round(fd.r0 * (1 + (fd.perc1r / 100)), fd.price_decimals)
            fd.r2 = round(ps.target_price, fd.price_decimals)
            side_open = 'BUY'
            fd.side_close = 'SELL'
        else:
            fd.type_pos = 'SHORT'
            fd.r_1 = round(fd.r0 * (1 + (fd.perc1r / 100)), fd.price_decimals)
            fd.r1  = round(fd.r0 * (1 - (fd.perc1r / 100)), fd.price_decimals)
            fd.r2  = round(ps.target_price, fd.price_decimals)
            side_open = 'SELL'
            fd.side_close = 'BUY'
    elif ps.check_BE_R0 or ps.check_BE_percR_1:
        fd.r_1 = round(ps.r_1, fd.price_decimals)
        fd.r1 = round(ps.tp1, fd.price_decimals)
        fd.r2 = round(ps.target_price, fd.price_decimals)
        fd.perc2r = round(abs(ps.target_price - fd.r0) / fd.r0 * 100,4)
        fd.perc1r = round((fd.perc2r / 2),4)
        fd.perc1_r = round(-fd.perc1r,4)
        if fd.r0 < ps.target_price:
            fd.type_pos = 'LONG'
            side_open = 'BUY'
            fd.side_close = 'SELL'
        else:
            fd.type_pos = 'SHORT'
            side_open = 'SELL'
            fd.side_close = 'BUY'

    fd.Qty_mVar = ctrader.calculate_lots(symbol, ps.risk_usdt, abs(fd.r0 - fd.r_1))

    if ((fd.Qty_mVar / 4) < fd.Qty_min):
        fd.control = False
        USDTmin = ctrader.min_risk_usd(symbol, abs(fd.r0 - fd.r_1))
        pending_warnings[symbol] = f'Minimum quantity not met for {symbol}. Minimum required: {USDTmin} USD'
        logger.warning(f"[CTRADER][{symbol.upper()}][QTY-MIN] Minimum quantity not met. Minimum required: {USDTmin} USD")
        ctrader.stop_price_feed(symbol.upper())   # corta la suscripción spot en cTrader antes de resetear (si no, sigue llegando el tick)
        var_restart([symbol])
        return

    # Control de margin (misma regla que Binance): si el riesgo neto supera max_risk_pct del
    # balance real de cTrader, no abrir e inyectar fondos. Fail-open ante error (el broker igual rechaza).
    try:
        ok, reason = margin_precheck(symbol, ps.risk_usdt, 'ctrader')
    except Exception as e:
        ok, reason = True, ""
        logger.warning(f"[CTRADER][{symbol.upper()}][MARGIN] Pre-check failed, continuing: {e}")
    if not ok:
        fd.control = False
        pending_warnings[symbol] = f'Open blocked for {symbol}: {reason}'
        logger.warning(f"[CTRADER][{symbol.upper()}][MARGIN] Open blocked: {reason}")
        ctrader.stop_price_feed(symbol.upper())
        var_restart([symbol])
        return

    # Spread de apertura - Fail-open si falta un lado del spread (queda perc_spread=0).
    spread = ctrader.spread(symbol)
    if spread is not None and abs(fd.r0 - fd.r_1) > 0:
        fd.perc_spread = round(spread / abs(fd.r0 - fd.r_1) * 100, 2)
        if fd.perc_spread > 25:
            fd.control = False
            pending_warnings[symbol] = f'Open blocked for {symbol}: spread {fd.perc_spread:.0f}% of 1R (too wide — rollover/news?)'
            logger.warning(f"[CTRADER][{symbol.upper()}][SPREAD] Open blocked: spread {spread} = {fd.perc_spread}% of 1R (>25%)")
            ctrader.stop_price_feed(symbol.upper())
            var_restart([symbol])
            return

    ctrader.order_market(symbol, side_open, fd.Qty_mVar)

def open_position_phase2(symbol, ps, fd, rt, PE_order, id_order):
    """Segunda fase, disparada al llegar el fill: fija el precio de entrada real, calcula las
    distancias, las cantidades y el P&L por nivel, coloca los TP (R1/R2) y el SL, y registra R0.
    Los niveles NO se mueven con el fill: son los que eligió el usuario (o los que armó phase1).
    Proteger primero, registrar después: los TP/SL van antes de escribir en la DB."""

    rt.r_1 = fd.r_1

    fd.r0 = round(PE_order, fd.price_decimals)

    #aca ya calcula las distancias con el spplitagge aplicado
    fd.dist_1r = round(abs(fd.r0 - fd.r_1), fd.price_decimals)
    fd.mid_dist = fd.dist_1r if ps.check_2_1 else round(abs(fd.r0 - fd.r2) / 2, fd.price_decimals)
    if fd.type_pos == 'LONG':
        rt.r_ts = round(fd.r2 + fd.mid_dist, fd.price_decimals)
    else:
        rt.r_ts = round(fd.r2 - fd.mid_dist, fd.price_decimals)

    n1 = int((fd.Qty_mVar / 2) / fd.steps_Qty_var + 1e-9)   # el 1e-9 corrige los 0.0199999999 pasándolo a 0.02
    n2 = int((fd.Qty_mVar / 4) / fd.steps_Qty_var + 1e-9)   # steps enteros en el cuarto
    fd.Qty_r1 = round(n1 * fd.steps_Qty_var, 8)   # limpia el residuo de coma flotante (0.02000005 -> 0.02)
    fd.Qty_r2 = round(n2 * fd.steps_Qty_var, 8)
    fd.Qty_ts = round(fd.Qty_mVar - fd.Qty_r1 - fd.Qty_r2, 8)

    rt.Qty_r1 = fd.Qty_r1
    rt.Qty_r2 = fd.Qty_r2
    rt.Qty_ts = fd.Qty_ts

    # P&L en USD de cada nivel (sign da la dirección: LONG r0 arriba, SHORT r0 abajo)
    sign = 1 if fd.type_pos == 'LONG' else -1
    fd.pnl1_r = round(ctrader.pnl_usd(symbol, sign * (fd.r_1 - fd.r0), fd.Qty_mVar), 4)
    fd.pnl1r  = round(ctrader.pnl_usd(symbol, sign * (fd.r1  - fd.r0), rt.Qty_r1),  4)
    fd.pnl2r  = round(ctrader.pnl_usd(symbol, sign * (fd.r2  - fd.r0), rt.Qty_r2),  4)

    # Comisión de la pata de entrada (la seteó _process_execution en rt.commission; 0 si el broker no cobra)
    rt.balance -= rt.commission
    gl.capital -= rt.commission

    rt.trade_sequence += 'R0' + " | "
    Data_db = [order_event_row(
        id_order   = id_order,
        id_pos     = fd.id_pos,
        type_pos   = fd.type_pos,
        pos        = 'R0',
        pe         = round(fd.r0, fd.price_decimals),
        sl         = round(fd.r_1, fd.price_decimals),
        r1         = round(fd.r1, fd.price_decimals),
        r2         = round(fd.r2, fd.price_decimals),
        qty        = fd.Qty_mVar,
        v1r        = ps.risk_usdt,
        balance    = round(rt.balance, 4),
        commission = round(rt.commission, 8),
        time       = rt.current_datetime,
    )]
    # PRIMERO proteger: los TP/SL se envían antes que nada (son fire-and-forget vía reactor, no fallan acá).
    ctrader.order_tp(symbol, fd.side_close, fd.Qty_r1, fd.r1, fd.id_order)
    ctrader.order_tp(symbol, fd.side_close, fd.Qty_r2, fd.r2, fd.id_order)
    ctrader.set_position_sl(symbol, fd.id_order, fd.r_1)

    # Registro en DB al FINAL: si la DB está caída la posición ya quedó protegida, solo se avisa.
    try:
        write_db(Data_db, symbol)
    except Exception as e:
        logger.error(f"[CTRADER][{symbol.upper()}][R0] Error registering in DB: {e}")
        pending_warnings[symbol] = f'DB error registering {symbol} open (position is protected)'

def handle_take_profit(symbol, ps, fd, rt, price, cpd, order_id, pnl, qty_closed):
    """Mueve el SL a favor cuando se llena un TP (lo dispara el evento de ejecución de cTrader):
    al tocar R1 el SL va a breakeven (o al nivel de resultado cero si el setup es BE = %R_1) y al
    tocar R2 va a mid_dist de la entrada. El pnl y el precio de fill vienen servidos por el broker.
    A diferencia de Binance no hay que cancelar nada: set_position_sl modifica el SL de la posición."""
    if rt.r1_active:
        logger.info(f"[CTRADER][{symbol.upper()}][R1] Active")
        rt.ALGO_pos = 'R1'
        if ps.check_BE_percR_1 and (abs(fd.r0 - fd.r_1) > abs(fd.r0 - fd.r1)):
            #Va a un punto donde el resultado da cero absorviendo la ganancia,
            #o si TP1 esta mas lejor del precio de entrada que SL va a BE0
            rt.r_1 = round(2 * fd.r0 - price, fd.price_decimals)
        else:
            rt.r_1 = fd.r0
        ctrader.set_position_sl(symbol, fd.id_order, rt.r_1)
        rt.breakeven_stage += 1   # -1 -> 0: SL en breakeven
        rt.r1_active = False
        rt.Qty_r1 = 0
    elif rt.r2_active:
        logger.info(f"[CTRADER][{symbol.upper()}][R2] Active")
        rt.ALGO_pos = 'R2'
        if fd.type_pos == 'SHORT': rt.r_1 = round(fd.r0 - fd.mid_dist, fd.price_decimals)
        elif fd.type_pos == 'LONG': rt.r_1 = round( fd.r0 + fd.mid_dist, fd.price_decimals)
        ctrader.set_position_sl(symbol, fd.id_order, rt.r_1)
        rt.breakeven_stage += 1   # 0 -> 1: SL en R1
        rt.r2_active = False
        rt.Qty_r2 = 0
         

    rt.trade_sequence += rt.ALGO_pos + " | "

    resultado  = pnl
    rt.balance += resultado
    gl.capital += resultado

    Data_db = [order_event_row(
        id_order   = order_id,
        id_pos     = fd.id_pos,
        type_pos   = fd.type_pos,
        pos        = rt.ALGO_pos,
        pe         = price,
        qty        = qty_closed,
        pnl        = pnl,
        balance    = round(rt.balance, 4),
        commission = 0,
        time       = rt.current_datetime,
    )]
    # El SL ya se movió en el broker: si la DB está caída solo se pierde el registro (se avisa).
    try:
        write_db(Data_db, symbol)
    except Exception as e:
        logger.error(f"[CTRADER][{symbol.upper()}][{rt.ALGO_pos}] Error registering in DB: {e}")
        pending_warnings[symbol] = f'DB error registering {symbol} {rt.ALGO_pos}'

def advance_trailing_stop(symbol, ps, fd, rt):
    """Trailing stop: cuando el precio alcanza r_ts, adelanta el SL una distancia mid_dist (1R en el
    setup 2:1, la mitad del camino a R2 en los manuales). No hace falta el patrón de Binance de armar
    los niveles en variables locales: acá set_position_sl modifica el SL de la posición y no falla."""

    if fd.type_pos == 'LONG':
        rt.r_1 = round(rt.r_ts - fd.mid_dist, fd.price_decimals)
        rt.r_ts += round(fd.mid_dist, fd.price_decimals)

    elif fd.type_pos == 'SHORT':
        rt.r_1 = round(rt.r_ts + fd.mid_dist, fd.price_decimals)
        rt.r_ts -= round(fd.mid_dist, fd.price_decimals)

    rt.breakeven_stage += 1
    rt.ALGO_pos = 'TS'

    logger.info(f"[CTRADER][{symbol.upper()}][RTS] Active - breakeven_stage: {rt.breakeven_stage}")

    ctrader.set_position_sl(symbol, fd.id_order, rt.r_1)

def handle_close(symbol, ps, fd, rt, price, cpd, order_id, pnl, qty_closed):
    """Cierra la posición, por SL disparada o por stop manual del usuario. Registra el trade en
    trade_history, actualiza el drawdown y los escalones de riesgo (gl.perc_f), cancela las TP
    huérfanas y baja el feed. La limpieza del broker corre en el finally aunque falle la DB."""

    if rt.stop_manual:   # cierre manual pedido por el usuario
        ALGO_pos = 'CM' 
    else:
        logger.info(f"[CTRADER][{symbol.upper()}][R_1] Triggered")
        if rt.breakeven_stage == -1: 
            ALGO_pos = 'SL'
        else:
            ALGO_pos = f'BE{rt.breakeven_stage}'

    resultado = pnl
    rt.balance += resultado
    gl.capital += resultado
    rt.trade_sequence += ALGO_pos + " | "

    # try/except/finally para que la caída de la DB NO impida limpiar las órdenes del broker:
    # todo lo que toca la DB va en el try (el cálculo del drawdown LEE la DB: read_last_dd_index,
    # read_max_dd_index y read_max_drawdown_pct son SELECTs). Si la DB está caída se pierde el
    # registro del trade (se avisa fuerte), pero el finally corre SIEMPRE -> se cancelan las TP
    # huérfanas y se corta el feed igual, sin dejar un socket zombie.
    try:
        # Índice de drawdown: compone el último dd guardado (o 100) por el retorno TOTAL del trade.
        rt.DD_index = read_last_dd_index() * (1 + rt.balance / gl.capital)
        peak  = max(read_max_dd_index(), rt.DD_index)   # incluye este trade como candidato a nuevo pico
        rt.DD = min(0, (rt.DD_index - peak) / peak * 100)
        rt.max_DD = read_max_drawdown_pct()

        # Reducción de riesgo por drawdown, en escalones FIJOS según la profundidad del DD.
        # rt.DD viene en % y negativo (ej. -7.3 = 7,3% de drawdown) -> se compara la magnitud,
        # de mayor a menor para que cada escalón atrape su rango. Al recuperar (<5%) vuelve a 0.005.
        dd = abs(rt.DD)
        if   dd >= 15: gl.perc_f = 0            # DD profundo: no operar
        elif dd >= 10: gl.perc_f = 0.005 / 4    # cuarto del riesgo
        elif dd >= 5:  gl.perc_f = 0.005 / 2    # mitad del riesgo
        else:          gl.perc_f = 0.005        # riesgo normal

        Data_db = [order_event_row(
            id_order = order_id,
            id_pos   = fd.id_pos,
            type_pos = fd.type_pos,
            pos      = ALGO_pos,
            pe       = price,
            qty      = qty_closed,
            pnl      = pnl,
            balance  = round(rt.balance, 4),
            time     = rt.current_datetime,
        )]
        write_db(Data_db, symbol)

        write_tradehistory_db(
            symbol      = symbol,
            id_pos      = fd.id_pos,
            type_pos    = fd.type_pos,
            pos         = ALGO_pos,
            time_open   = fd.open_time,
            time_close  = rt.current_datetime,
            pe          = fd.r0,
            ps          = price,
            v1r         = ps.risk_usdt,
            resultado   = round(rt.balance, 4),
            trade_sequence   = rt.trade_sequence,
            dd_index    = round(rt.DD_index, 4),
            inst        = fd.type_ins,
            comments    = ps.comment,
            perc_spread = fd.perc_spread   # peaje del spread de apertura (% de 1R), solo cTrader
        )

        # Re-siembra gl.balance/gl.capital desde la DB (ya incluye este cierre) para no dejarlos desfasados.
        gl.load_state_from_db()

    except Exception as e:
        logger.error(f"[CTRADER][{symbol.upper()}][CLOSE] Error registering close in DB: {e}")
        pending_warnings[symbol] = f'DB error registering {symbol} close: check trade_history!'

    finally:
        ctrader.cancel_position_orders(fd.id_order)   # cancela las TP LIMIT huérfanas (cTrader no lo hace solo)
        ctrader.stop_price_feed(symbol.upper())
        var_restart([symbol])

    rt.r_1_active = False



def apply_swap(symbol, ps, fd, rt, swap):
    """Aplica el swap (interés overnight) que llega como evento de cTrader: lo suma al balance/capital
    y lo registra como evento SWAP. El monto es el delta del swap acumulado de la posición."""

    rt.balance += swap
    gl.capital += swap
    rt.swap += swap

    logger.info(f"[CTRADER][{symbol.upper()}][SWAP] Swap applied, accumulated: {rt.swap}")

    Data_db = [order_event_row(
        id_pos   = fd.id_pos,
        type_pos = fd.type_pos,
        pos      = 'SWAP',
        pnl      = swap,
        balance  = round(rt.balance, 4),
        time     = rt.current_datetime,
    )]
    write_db(Data_db, symbol)

def metrics(symbol, ps, fd, rt):
    """Recalcula las métricas vivas en cada tick: distancia % hasta R1 (Panel de Control), P&L no
    realizado en USD (convierte lotes a unidades×conv vía pnl_usd) y equity."""
    # distancia % hasta R1, que alimenta la barra de progreso del Panel de Control
    if rt.breakeven_stage == -1 and ((fd.r1 - fd.r0) != 0):
        rt.position_pct = round((((rt.current_price - fd.r0) / (fd.r1 - fd.r0)) * 100),2)
    elif ((fd.r1 - rt.r_1) != 0): 
        rt.position_pct = round((((rt.current_price - rt.r_1) / (fd.r1 - rt.r_1)) * 100),2)

    # P&L no realizado en USD (lotes a unidades×conv vía ctrader.pnl_usd); diff con signo según dirección
    qty_open = rt.Qty_r1 + rt.Qty_r2 + rt.Qty_ts
    diff = (rt.current_price - fd.r0) if fd.type_pos == 'LONG' else (fd.r0 - rt.current_price)
    rt.unrealized_pnl = round(ctrader.pnl_usd(symbol, diff, qty_open), 2)

    rt.equity = rt.balance + rt.unrealized_pnl


def manage_position(ps, fd, rt, symbol):
    """Se llama en cada tick de precio. A diferencia de Binance, acá solo maneja el avance del
    trailing y refresca las métricas: los TP y el cierre los disparan los eventos de ejecución."""
    # fd.id_order recién existe cuando llega el fill y corre phase2. Entre la orden a mercado y ese
    # evento siguen entrando ticks, y ahí r_ts todavía vale 0: sin este filtro, un LONG cumpliría
    # current_price >= 0 y dispararía el trailing sobre una posición que ni siquiera existe.
    if fd.control and fd.id_order:
        if fd.type_pos == 'LONG':
            if rt.current_price >= rt.r_ts:
                advance_trailing_stop(symbol, ps, fd, rt)
        elif fd.type_pos == 'SHORT':
            if rt.current_price <= rt.r_ts:
                advance_trailing_stop(symbol, ps, fd, rt)

        metrics(symbol, ps, fd, rt)
    
    return


    


