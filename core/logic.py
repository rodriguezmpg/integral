"""Algoritmo de R-múltiplos para Binance (crypto, async).

Es una de las dos implementaciones espejadas de la misma estrategia; la otra es logic_c.py
para cTrader (sync, Twisted).

El flujo de un trade: open_position (niveles R + orden a mercado + TP/SL) -> handle_take_profit
(mueve el SL a favor al tocar R1/R2) -> advance_trailing_stop (sigue subiendo el SL) -> handle_close
(SL o cierre manual: registra el trade y limpia). manage_position despacha todo esto en cada tick.

Se corre dentro del event loop de asyncio de binance_ext, por eso todo es async.
"""
from core.dbfunc import write_db, order_event_row, write_tradehistory_db, read_last_dd_index, read_max_dd_index, read_max_drawdown_pct
from core.exchanges.binance_ext import binance
from core.classes import gl, OrderError
import os, psycopg2
from core.utils import var_restart, pending_warnings, margin_precheck

import logging
logger = logging.getLogger('reg')

async def open_position(symbol, ps, fd, rt):
    """Abre la posición y la deja protegida. Arma los niveles R desde r0 (entrada) y target_price
    (que define LONG o SHORT), valida cantidad mínima y margin, manda la orden a mercado, reajusta
    los niveles al fill real (slippage) y recién ahí coloca los TP (R1/R2) y el SL (R-1). Sigue el
    principio 'proteger primero, registrar después': la escritura en DB va al final, con TP/SL puestos."""

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
        

    fd.Qty_mVar = round(ps.risk_usdt / abs(fd.r0 - fd.r_1), fd.dec_qty) #igual en ambos casos
    

    if ((fd.Qty_mVar / 4) < fd.Qty_min):
        fd.control = False
        USDTmin = round((fd.Qty_min * abs(fd.r0 - fd.r_1))*4,2)  
        pending_warnings[symbol] = f'Minimum quantity not met for {symbol}. Minimum required: {USDTmin} USDT'
        logger.warning(f"[BINANCE][{symbol.upper()}][QTY-MIN] Minimum quantity not met")
        await binance.stop_socket(symbol, ps, fd, rt)
        var_restart([symbol])

    if fd.control:
        # Control de margin: la suma de lo que se puede perder (risk_usdt de las abiertas
        # + esta) no debe superar max_risk_pct del balance real de futures; si no, todo a SL nos dejaría
        # cerca de la liquidación. Fail-open: si el chequeo falla por red, se abre igual (order_market
        # igual rechaza por margen insuficiente).
        try:
            ok, reason = margin_precheck(symbol, ps.risk_usdt, 'binance')
        except Exception as e:
            ok, reason = True, ""
            logger.warning(f"[BINANCE][{symbol.upper()}][MARGIN] Pre-check failed, continuing: {e}")
        if not ok:
            fd.control = False
            pending_warnings[symbol] = f'Open blocked for {symbol}: {reason}'
            logger.warning(f"[BINANCE][{symbol.upper()}][MARGIN] Open blocked: {reason}")
            await binance.stop_socket(symbol, ps, fd, rt)
            var_restart([symbol])
            return

        # order_market devuelve 0 si ya hay posición abierta, o lanza OrderError si la rechazan (ej. margen insuficiente).
        try:
            id_order_r0 = await binance.order_market(symbol, side_open, fd.Qty_mVar)
        except OrderError as oe:
            id_order_r0 = 0
            pending_warnings[symbol] = f'Order rejected for {symbol}: {oe}'
            logger.warning(f"[BINANCE][{symbol.upper()}][OPEN] Main order rejected: {oe}")

        if id_order_r0 == 0:
            # La posición NO se abrió -> limpiar el socket fantasma: resetear estado y frenar la tarea.
            fd.control = False
            await binance.stop_socket(symbol, ps, fd, rt)
            var_restart([symbol])
            return

        # A partir de acá la posición YA está abierta en el broker: si algo falla antes de dejar
        # colocados los TP/SL (consulta del fill, red, rechazo de una condicional), NO nos quedamos
        # expuestos -> cierre de emergencia a mercado y limpieza del socket.
        try:
            PE_order, pnl, Fee, qty = await binance.get_order_info(symbol, id_order_r0, max_attempts=10, wait_seconds=1)

            rt.commission = Fee

            rt.r_1 = fd.r_1

            fd.r0 = round(PE_order, fd.price_decimals)

            #aca ya calcula las distancias con el spplitagge aplicado
            fd.dist_1r = round(abs(fd.r0 - fd.r_1), fd.price_decimals)
            fd.mid_dist = fd.dist_1r if ps.check_2_1 else round(abs(fd.r0 - fd.r2) / 2, fd.price_decimals)
            if fd.type_pos == 'LONG':
                rt.r_ts = round(fd.r2 + fd.mid_dist, fd.price_decimals)
            else:
                rt.r_ts = round(fd.r2 - fd.mid_dist, fd.price_decimals)

            fd.Qty_r1 = round((fd.Qty_mVar / 2), fd.dec_qty)
            fd.Qty_r2 = round((fd.Qty_mVar / 4), fd.dec_qty)
            fd.Qty_ts = round((fd.Qty_mVar - fd.Qty_r1 - fd.Qty_r2), fd.dec_qty)

            rt.Qty_r1 = fd.Qty_r1
            rt.Qty_r2 = fd.Qty_r2
            rt.Qty_ts = fd.Qty_ts

            # sign da la dirección (LONG r0 arriba, SHORT r0 abajo) para que el SL dé pérdida y los TP ganancia.
            # En crypto la qty ya está en USDT (no hace falta convertir como en cTrader), solo el signo.
            sign = 1 if fd.type_pos == 'LONG' else -1
            fd.pnl1_r = round(sign * (fd.r_1 - fd.r0) * fd.Qty_mVar, 4)
            fd.pnl1r  = round(sign * (fd.r1  - fd.r0) * rt.Qty_r1,  4)
            fd.pnl2r  = round(sign * (fd.r2  - fd.r0) * rt.Qty_r2,  4)

            rt.balance -= rt.commission
            gl.capital -= rt.commission

            rt.trade_sequence += 'R0' + " | "

            rt.id_order_r1 = await binance.order_tp_market(symbol, fd.side_close, rt.Qty_r1, fd.r1)
            rt.id_order_r2 = await binance.order_tp_market(symbol, fd.side_close, rt.Qty_r2, fd.r2)
            rt.id_order_r_1 = await binance.order_sl_stop_market(symbol, fd.side_close, fd.r_1)

        except Exception as e:
            # CIERRE DE EMERGENCIA: la posición quedó abierta sin protección completa. Se cancela lo
            # que haya llegado a colocarse (None = no-op) y se cierra todo a mercado. Si hasta el
            # cierre falla, se avisa fuerte para cerrar a mano en Binance.
            logger.error(f"[BINANCE][{symbol.upper()}][OPEN] Could not protect position ({e}) -> emergency close")
            pending_warnings[symbol] = f'EMERGENCY CLOSE {symbol}: opened but TP/SL failed'
            try:
                binance.cancel_algo_order(symbol, rt.id_order_r1)
                binance.cancel_algo_order(symbol, rt.id_order_r2)
                await binance.close_total(symbol)
            except Exception as e2:
                logger.error(f"[BINANCE][{symbol.upper()}][OPEN] Emergency close FAILED: {e2} - CLOSE MANUALLY IN BINANCE")
                pending_warnings[symbol] = f'CLOSE {symbol} MANUALLY IN BINANCE! Emergency close failed'
            fd.control = False
            await binance.stop_socket(symbol, ps, fd, rt)
            var_restart([symbol])
            return

        # Registro en DB al FINAL: la posición ya quedó protegida (TP/SL colocados); si la DB está
        # caída solo se avisa, no se cierra una posición sana por un problema de registro.
        Data_db = [order_event_row(
            id_order   = id_order_r0,
            id_pos     = fd.id_pos,
            type_pos   = fd.type_pos,
            pos        = 'R0',
            pe         = round(fd.r0, fd.price_decimals),
            sl         = round(fd.r_1, fd.price_decimals),
            r1         = round(fd.r1, fd.price_decimals),
            r2         = round(fd.r2, fd.price_decimals),
            qty        = round(fd.Qty_mVar, fd.dec_qty),
            v1r        = ps.risk_usdt,
            balance    = round(rt.balance, 4),
            commission = round(rt.commission, 8),
            time       = rt.current_datetime,
        )]
        try:
            write_db(Data_db, symbol)
        except Exception as e:
            logger.error(f"[BINANCE][{symbol.upper()}][OPEN] Error registering R0 in DB: {e}")
            pending_warnings[symbol] = f'DB error registering {symbol} open (position is protected)'


async def handle_take_profit(symbol, ps, fd, rt):
    """Mueve el SL a favor cuando se llena un TP: al tocar R1 el SL sube a breakeven (r0), al tocar
    R2 sube a R1. Cancela el SL viejo, coloca el nuevo, sube breakeven_stage y registra el evento."""
    
    if rt.r1_active:
        logger.info(f"[BINANCE][{symbol.upper()}][R1] Active")
        rt.ALGO_pos = 'R1'
        binance.cancel_algo_order(symbol, rt.id_order_r_1)
        if ps.check_BE_percR_1 and (abs(fd.r0 - fd.r_1) > abs(fd.r0 - fd.r1)):
            #Va a un punto donde el resultado da cero absorviendo la ganancia, 
            #o si TP1 esta mas lejor del precio de entrada que SL va a BE0
            rt.r_1 = round(2 * fd.r0 - rt.ALGO_PE, fd.price_decimals) 
        else:
            rt.r_1 = fd.r0
        rt.id_order_r_1 = await binance.order_sl_stop_market(symbol, fd.side_close, rt.r_1)
        rt.breakeven_stage += 1   # -1 -> 0: SL en breakeven
        rt.r1_active = False
        rt.Qty_r1 = 0
    elif rt.r2_active:  
        logger.info(f"[BINANCE][{symbol.upper()}][R2] Active")
        rt.ALGO_pos = 'R2'
        binance.cancel_algo_order(symbol, rt.id_order_r_1)
        if fd.type_pos == 'SHORT': rt.r_1 = round(fd.r0 - fd.mid_dist, fd.price_decimals) 
        elif fd.type_pos == 'LONG': rt.r_1 = round( fd.r0 + fd.mid_dist, fd.price_decimals) 
        rt.id_order_r_1 = await binance.order_sl_stop_market(symbol, fd.side_close, rt.r_1)
        rt.breakeven_stage += 1   # 0 -> 1: SL en R1
        rt.r2_active = False
        rt.Qty_r2 = 0
         

    rt.trade_sequence += rt.ALGO_pos + " | "

    resultado  = (rt.ALGO_pnl - rt.ALGO_commission)
    rt.balance += resultado
    gl.capital += resultado

    Data_db = [order_event_row(
        id_order   = rt.ALGO_orderid,
        id_pos     = fd.id_pos,
        type_pos   = fd.type_pos,
        pos        = rt.ALGO_pos,
        pe         = round(rt.ALGO_PE, fd.price_decimals),
        qty        = round(rt.ALGO_QtymVar, fd.dec_qty),
        pnl        = round(rt.ALGO_pnl, 4),
        balance    = round(rt.balance, 4),
        commission = round(rt.ALGO_commission, 8),
        time       = rt.current_datetime,
    )]
    # El SL ya se movió en el broker: si la DB está caída solo se pierde el registro (se avisa).
    try:
        write_db(Data_db, symbol)
    except Exception as e:
        logger.error(f"[BINANCE][{symbol.upper()}][{rt.ALGO_pos}] Error registering in DB: {e}")
        pending_warnings[symbol] = f'DB error registering {symbol} {rt.ALGO_pos}'

async def advance_trailing_stop(symbol, ps, fd, rt):
    """Trailing stop: cuando el precio alcanza r_ts, adelanta el SL una distancia de 1R (dist_1r)."""
    # Niveles nuevos en variables locales, SIN mutar el estado todavía: si la colocación de la SL
    # nueva falla, rt.r_ts queda como estaba y el próximo tick (precio sigue pasado de r_ts) reintenta solo.
    if fd.type_pos == 'LONG':
        new_r_1  = round(rt.r_ts - fd.mid_dist, fd.price_decimals)
        new_r_ts = rt.r_ts + round(fd.mid_dist, fd.price_decimals)

    elif fd.type_pos == 'SHORT':
        new_r_1  = round(rt.r_ts + fd.mid_dist, fd.price_decimals)
        new_r_ts = rt.r_ts - round(fd.mid_dist, fd.price_decimals)

    # PRIMERO colocar la SL nueva y recién DESPUÉS cancelar la vieja: si la colocación falla
    # (OrderError -> la atrapa el socket y el tick sigue), la SL vieja sigue viva en el broker y
    # la posición nunca queda sin stop. 
    old_sl_id = rt.id_order_r_1
    binance.cancel_algo_order(symbol, old_sl_id)

    rt.id_order_r_1 = await binance.order_sl_stop_market(symbol, fd.side_close, new_r_1)
    

    rt.r_1 = new_r_1
    rt.r_ts = new_r_ts
    rt.breakeven_stage += 1
    rt.ALGO_pos = 'TS'

    logger.info(f"[BINANCE][{symbol.upper()}][RTS] Active - breakeven_stage: {rt.breakeven_stage}")

async def handle_close(symbol, ps, fd, rt):
    """Cierra la posición, por SL disparada o por stop manual del usuario. Registra el trade en
    trade_history, actualiza el índice de drawdown y los escalones de riesgo (gl.perc_f), cancela
    los TP remanentes y baja el socket. La limpieza del broker corre en el finally aunque falle la DB."""

    logger.info(f"[BINANCE][{symbol.upper()}][R_1] Triggered")

    if rt.stop_manual:   # cierre manual pedido por el usuario
        id_order = binance.cancel_algo_order(symbol, rt.id_order_r_1)     
        id_order_cm = await binance.close_total(symbol)
        PE_order, pnl, Fee, qty = await binance.get_order_info(symbol, id_order_cm, max_attempts=10, wait_seconds=1)
        rt.ALGO_orderid = id_order_cm
        rt.ALGO_PE = PE_order
        rt.ALGO_pnl = pnl
        rt.ALGO_commission = Fee
        rt.ALGO_QtymVar = qty
        ALGO_pos = 'CM' 
    else:
        if rt.breakeven_stage == -1: 
            ALGO_pos = 'SL'
        else:
            ALGO_pos = f'BE{rt.breakeven_stage}'


    resultado = (rt.ALGO_pnl - rt.ALGO_commission)
    rt.balance += resultado
    gl.capital += resultado
    rt.trade_sequence += ALGO_pos + " | "

    # try/except/finally para que la caída de la DB NO impida limpiar las órdenes del broker:
    # todo lo que toca la DB va en el try (el cálculo del drawdown LEE la DB: read_last_dd_index,
    # read_max_dd_index y read_max_drawdown_pct son SELECTs). Si la DB está caída se pierde el
    # registro del trade (se avisa), pero el finally corre SIEMPRE -> se cancelan los TP
    # remanentes y se corta el socket igual.
    try:
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
            id_order   = rt.ALGO_orderid,
            id_pos     = fd.id_pos,
            type_pos   = fd.type_pos,
            pos        = ALGO_pos,
            pe         = round(rt.ALGO_PE, fd.price_decimals),
            qty        = round(rt.ALGO_QtymVar, fd.dec_qty),
            pnl        = round(rt.ALGO_pnl, 4),
            balance    = round(rt.balance, 4),
            commission = round(rt.ALGO_commission, 8),
            time       = rt.current_datetime,
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
            ps          = rt.ALGO_PE,
            v1r         = ps.risk_usdt,
            resultado   = round(rt.balance, 4),
            trade_sequence   = rt.trade_sequence,
            dd_index    = round(rt.DD_index, 4),
            inst        = fd.type_ins,
            comments    = ps.comment,
            perc_spread = None   
        )

        # Re-siembra gl.balance/gl.capital desde la DB (ya incluye este cierre) para no dejarlos desfasados.
        gl.load_state_from_db()

    except Exception as e:
        logger.error(f"[BINANCE][{symbol.upper()}][CLOSE] Error registering close in DB: {e}")
        pending_warnings[symbol] = f'DB error registering {symbol} close: check trade_history!'

    finally:
        binance.cancel_algo_order(symbol, rt.id_order_r1)
        binance.cancel_algo_order(symbol, rt.id_order_r2)
        rt.r_1_active = False   # antes del stop para que un tick en el medio no re-dispare el cierre

    if not rt.stop_manual:
        await binance.stop_socket(symbol, ps, fd, rt)
        var_restart([symbol])

async def apply_funding_fee(symbol, ps, fd, rt, fee):
    """Aplica el funding fee de Binance Futures: lo suma al balance/capital y lo deja registrado como evento FF."""

    rt.balance += fee
    gl.capital += fee
    rt.funding_fee += fee

    logger.info(f"[BINANCE][{symbol.upper()}][FUNDING] Fee applied, accumulated: {rt.funding_fee}")

    Data_db = [order_event_row(
        id_pos   = fd.id_pos,
        type_pos = fd.type_pos,
        pos      = 'FF',
        pnl      = round(fee, 8),
        balance  = round(rt.balance, 4),
        time     = rt.current_datetime,
    )]
    write_db(Data_db, symbol)

async def metrics(symbol, ps, fd, rt):
    """Recalcula las métricas vivas de la posición en cada tick: distancia % hasta R1 (para el
    Panel de Control), P&L no realizado y equity."""

    # distancia % hasta R1, que alimenta la barra de progreso del Panel de Control
    if rt.breakeven_stage == -1 and ((fd.r1 - fd.r0) != 0):
        rt.position_pct = round((((rt.current_price - fd.r0) / (fd.r1 - fd.r0)) * 100),2)
    elif ((fd.r1 - rt.r_1) != 0): 
        rt.position_pct = round((((rt.current_price - rt.r_1) / (fd.r1 - rt.r_1)) * 100),2)

    # P&L no realizado sobre la cantidad todavía abierta
    if fd.type_pos == 'LONG':
        rt.unrealized_pnl = round((rt.current_price - fd.r0) * (rt.Qty_r1 + rt.Qty_r2 + rt.Qty_ts), 2)
    elif fd.type_pos == 'SHORT':
        rt.unrealized_pnl = round((fd.r0 - rt.current_price) * (rt.Qty_r1 + rt.Qty_r2 + rt.Qty_ts), 2)

    rt.equity = rt.balance + rt.unrealized_pnl


async def manage_position(ps, fd, rt, symbol):
    """Se llama en cada tick de precio. Despacha según los flags activos: TP tocado (R1/R2),
    cierre (SL o manual), o avance del trailing; y al final refresca las métricas vivas."""
    if fd.control:
        if rt.r1_active or rt.r2_active:
            await handle_take_profit(symbol, ps, fd, rt)
        if rt.r_1_active:
            await handle_close(symbol, ps, fd, rt)

        if fd.type_pos == 'LONG':
            if rt.current_price >= rt.r_ts:
                await advance_trailing_stop(symbol, ps, fd, rt)
        elif fd.type_pos == 'SHORT':
            if rt.current_price <= rt.r_ts:
                await advance_trailing_stop(symbol, ps, fd, rt)

        await metrics(symbol, ps, fd, rt)
    
    return


    


