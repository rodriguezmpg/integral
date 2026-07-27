"""Blueprint de trading: sirve las dos vistas principales (dashboard y monitor en vivo)
y los endpoints que abren/cierran posiciones y publican el estado en vivo de cada símbolo."""
import json
from flask import Blueprint, jsonify, render_template, request, Response
from core.classes import gl
from core.utils import crypto_list, forex_list, cfd_list, symbols, var_restart
from core.exchanges.binance_ext import binance
from core.exchanges.ctrader_ext import ctrader
from core.dbfunc import update_trade_comment

import logging
logger = logging.getLogger('reg')

bp = Blueprint('trading', __name__)


# Dashboard: posiciones activas + las tres secciones de instrumentos (crypto, forex, CFD).
@bp.route('/')
def index():
    return render_template('index.html',
                           crypto_list=crypto_list,
                           forex_list=forex_list,
                           cfd_list=cfd_list)


# Monitor en vivo de una posición (se abre con ?ticker=X).
@bp.route('/main')
def index_main():
    return render_template('main_loop.html')


# Precio actual de un símbolo (0 si no está registrado). Lo consulta la UI cada segundo.
@bp.route('/precio')
def precio():
    ticker = request.args.get("ticker", "").lower()
    container = symbols.get(ticker)
    precio_actual = container.rt.current_price if container else 0.0
    return jsonify({'current_price': precio_actual})


# Datos fijos de la posición (fd) + inputs del usuario (ps): niveles R, cantidades, precisión, etc.
@bp.route('/dat_fixed')
def dat_fixed():
    ticker = request.args.get("ticker", "").lower()
    container = symbols.get(ticker)
    if not container:
        return jsonify({"error": f"Ticker {ticker} no encontrado"}), 404
    return jsonify({**container.fd.to_dict(), **container.ps.to_dict()})


# Estado en vivo de la posición (rt): precio, flags activos, P&L, etapa de breakeven.
@bp.route('/datos')
def datos():
    ticker = request.args.get("ticker", "").lower()
    container = symbols.get(ticker)
    if not container:
        return jsonify({"error": f"Ticker {ticker} no encontrado"}), 404
    return jsonify(container.rt.to_dict())



# Métricas de portfolio agregadas (global + por broker). Se recalculan en cada consulta.
@bp.route('/datos_analytics')
def datos_analytics():
    gl.aggregate(crypto_list, forex_list, cfd_list, symbols)
    return jsonify(gl.to_dict())


# Estado combinado de TODOS los símbolos para el panel de posiciones activas del dashboard.
@bp.route('/datos_PControl')
def datos_PControl():
    resultado = {}
    for ticker, container in symbols.items():
        data = container.get_combined()
        # Estado de mercado para la columna "Market": solo símbolos activos (inactivos -> None -> celda vacía).
        if container.ps.socket_active:
            if container.fd.type_ins in ("FOREX", "CFD"):
                name = ticker.upper()
                sched = ctrader._format_schedule(ctrader._schedule_map.get(name, []), ctrader._schedule_tz_map.get(name))
                data['market_open'] = sched['is_open']
            else:
                data['market_open'] = True   # crypto: 24/7
        else:
            data['market_open'] = None
        resultado[ticker] = data
    return jsonify(resultado)


# Abre una posición: valida risk/target, marca el símbolo activo y arranca el feed del broker
# que corresponda (Binance para crypto, cTrader para forex/CFD). Rechaza con 409 si ya hay una
# posición activa y hace rollback si el feed no llega a arrancar.
@bp.route('/mainstart', methods=['POST'])
def start_trading():
    ticker = request.values.get("ticker", "").lower()
    risk   = request.values.get('risk_usdt')
    target = request.values.get('target_price')

    container = symbols.get(ticker)
    if not container:
        return jsonify({"error": f"Ticker {ticker} no encontrado"}), 404

    if container.ps.socket_active:
        from core.utils import pending_warnings
        logger.warning(f"[START][{ticker.upper()}] Rejected: position already active")
        pending_warnings[ticker] = f'Start rejected: {ticker} already has an active position'
        return jsonify({"error": f"{ticker} ya tiene una posición activa"}), 409


    def _parse_pos(valor):
        try:
            n = float(str(valor).strip().replace(',', '.'))
        except (TypeError, ValueError):
            return None
        return n if n > 0 else None

    risk_val   = _parse_pos(risk)
    target_val = _parse_pos(target)
    if risk_val is None or target_val is None:
        from core.utils import pending_warnings
        logger.warning(f"[START][{ticker.upper()}] Rejected: invalid risk/target (risk={risk!r}, target={target!r})")
        pending_warnings[ticker] = 'Start rejected: risk and target price must be positive numbers'
        return jsonify({"error": "risk_usdt y target_price deben ser números positivos"}), 400

    container.ps.risk_usdt    = risk_val
    container.ps.target_price = target_val
    # En 2:1 el modal manda tp1 y r_1 vacíos (los deriva la lógica): _parse_pos devuelve None y quedan en 0.
    container.ps.tp1          = _parse_pos(request.values.get('tp1')) or 0.0
    container.ps.r_1          = _parse_pos(request.values.get('r_1')) or 0.0
    # Los checkboxes llegan como TEXTO ("true"/"false", en minúscula porque los manda JS);
    # la comparación es la que produce el True/False de Python que se guarda.
    container.ps.check_2_1        = request.values.get('check_2_1') == 'true'
    container.ps.check_BE_R0      = request.values.get('check_BE_R0') == 'true'
    container.ps.check_BE_percR_1 = request.values.get('check_BE_percR_1') == 'true'
    container.ps.comment      = request.values.get('comment', '')
    container.ps.socket_active = True

    if ticker in [s.lower() for s in forex_list]: 
        container.fd.type_ins = "FOREX"
        started = ctrader.start_price_feed(ticker.upper())

    elif ticker in [s.lower() for s in cfd_list]:
        container.fd.type_ins = "CFD"
        started = ctrader.start_price_feed(ticker.upper())

    else:
        container.fd.type_ins = "CRYPTO"
        started = binance.start_socket_task(ticker)

    if not started:
        from core.utils import pending_warnings
        container.ps.socket_active = False   
        logger.error(f"[START][{ticker.upper()}] Feed could not start -> rollback of socket_active")
        pending_warnings[ticker] = f'Start failed for {ticker}: price feed could not start (see log)'
        return jsonify({"error": f"El feed de {ticker} no pudo arrancar"}), 502

    return Response(status=204)


# Guarda el comentario de una operación: si viene pk es un trade ya cerrado (va a la DB);
# si viene ticker es una posición en vivo (queda en memoria hasta el cierre).
@bp.route('/comment_save', methods=['POST'])
def comment_save():
    comment = request.values.get('comment', '')
    pk      = request.values.get('pk')
    ticker  = (request.values.get('ticker') or '').lower()

    if pk:
        update_trade_comment(int(pk), comment)
    else:
        container = symbols.get(ticker)
        if not container:
            return jsonify({"error": f"Ticker {ticker} no encontrado"}), 404
        container.ps.comment = comment

    return Response(status=204)


# Cierre manual de una posición: cierra en el broker y resetea el container. 409 si no está activa.
@bp.route('/stopsocket', methods=['POST'])
async def stop_socket():
    from core.utils import pending_warnings
    ticker = request.values.get("ticker", "").lower()
    container = symbols.get(ticker)

    # Guard: sin posición activa no hay nada que detener. Evita que un stop espurio ejecute
    # handle_close sobre estado vacío (fila CM en cero en la DB) o deje stop_manual=True colgado.
    if not container or not container.ps.socket_active:
        logger.warning(f"[STOP][{ticker.upper()}] Rejected: no active position")
        pending_warnings[ticker] = f'Stop rejected: no active position for {ticker}'
        return jsonify({"error": f"{ticker} no tiene una posición activa"}), 409

    container.rt.stop_manual = True

    if ticker not in [s.lower() for s in forex_list] and ticker not in [s.lower() for s in cfd_list]:
        await binance.stop_socket(ticker, container.ps, container.fd, container.rt)
        var_restart([ticker])
    else:
        ctrader.close_position(ticker.upper(), container.fd.id_order)

    return Response(status=204)





# Página del gráfico de un símbolo (lightweight-charts).
@bp.route("/chart/<symbol>")
def chart_page(symbol):
    return render_template("chart.html", symbol=symbol.upper())


# Velas para el gráfico: forex/CFD desde cTrader, crypto desde Binance (mismo formato de salida).
@bp.route("/api/chart/<symbol>")
def get_chart(symbol):
    try:
        name = symbol.upper()
        # Forex/CFD -> velas de cTrader; el resto (crypto) -> velas de Binance. Mismo formato de salida.
        if name in forex_list or name in cfd_list:
            data = ctrader.get_chart_data(name)
        else:
            data = binance.get_chart_data(symbol)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route('/api/spread')
def api_spread():
    # Spread en vivo para el modal de Start. SOLO cTrader (forex/CFD)
    ticker = (request.args.get("ticker") or "").lower()
    name   = ticker.upper()
    if name not in forex_list and name not in cfd_list:
        return jsonify({"status": "n/a"})
    snap = ctrader.get_spread_snapshot(name)
    if "error" in snap:
        return jsonify({"status": "error", "message": snap["error"]})
    bid, ask = snap["bid"], snap["ask"]
    return jsonify({
        "status": "ok",
        "bid":    bid,
        "ask":    ask,
        "spread": ask - bid,
        "price":  (bid + ask) / 2,   # mid = precio de referencia (r0 pre-fill) para el % contra 1R
        "digits": snap.get("digits") or 5,
    })


@bp.route('/market_hours')
def market_hours():
    # Horario de mercado del símbolo para el recuadro de main_loop. Sale de la MISMA función _format_schedule.
    # Solo forex/CFD tienen horario (vienen de cTrader); crypto es 24/7 -> "Market Open".
    ticker = (request.args.get("ticker") or "").lower()
    name   = ticker.upper()
    if name not in forex_list and name not in cfd_list:
        return jsonify({'status': 'crypto', 'rows': [], 'is_open': True, 'opens_in': None, 'next_open_idx': None})
    if name not in ctrader._schedule_map:
        ctrader.request_symbol_details(name)
        return jsonify({'status': 'loading', 'rows': [], 'is_open': True, 'opens_in': None, 'next_open_idx': None})
    sched = ctrader._format_schedule(ctrader._schedule_map[name], ctrader._schedule_tz_map.get(name))
    sched['status'] = 'ok'
    return jsonify(sched)



# Volcado completo del estado interno (ps/fd/rt de cada símbolo + gl). Para diagnóstico.
@bp.route('/debug')
def debug():
    ticker = (request.args.get("ticker") or "").lower()
    if ticker and ticker not in symbols:
        return jsonify({"error": f"Ticker {ticker} no encontrado"}), 404
    items = {ticker: symbols[ticker]} if ticker else symbols
    data = {tk: {'ps': vars(c.ps), 'fd': vars(c.fd), 'rt': vars(c.rt)} for tk, c in items.items()}
    data['gl'] = vars(gl)
    return Response(json.dumps(data, default=str, indent=2), mimetype='application/json')
