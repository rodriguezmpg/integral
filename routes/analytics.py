"""Blueprint de analítica: sirve las vistas de análisis y de cashflow, y los endpoints JSON
que las alimentan (historial de trades, movimientos por posición, cashflow). Las métricas de
calidad se calculan en el front a partir de estos datos; acá solo se leen de la DB."""
import os
import psycopg2

from flask import Blueprint, jsonify, render_template, request

from core.dbfunc import write_cashflow
from core.classes import gl

bp = Blueprint('analytics', __name__)

EXCHANGES = ("BINANCE", "CTRADER")   # valores válidos para el select de cashflow


# Helpers de solo lectura contra la DB: abrir conexión, correr una query y normalizar los None a "".
def _get_conn():
    return psycopg2.connect(os.environ.get("DATABASE_URL"))

def _query(sql, params=()):
    conn  = _get_conn()
    cur   = conn.cursor()
    cur.execute(sql, params)
    filas = cur.fetchall()
    cur.close()
    conn.close()
    return filas

def _serialize(filas):
    return [[("" if v is None else v) for v in fila] for fila in filas]


# ── Vistas HTML ───────────────────────────────────────────────────────────────

@bp.route('/analytics')
def analytics_open():
    return render_template('analytics.html')

@bp.route('/analytics_symbol')
def analytics_symbol():
    return render_template('analytics_symbol.html')

@bp.route('/cashflow')
def cashflow_open():
    return render_template('cashflow.html')


# ── Cashflow (carga y consulta de movimientos de capital) ──────────────────────

@bp.route('/cashflow_add', methods=['POST'])
def cashflow_add():
    # value acepta negativos/decimales (coma o punto); exchange se valida contra EXCHANGES;
    # el datetime lo genera la DB en UTC. comments es opcional.
    value    = (request.values.get("value") or "").strip().replace(",", ".")
    exchange = (request.values.get("exchange") or "").strip().upper()
    comments = (request.values.get("comments") or "").strip()

    try:
        value = float(value)
    except ValueError:
        return jsonify({"error": "Valor inválido"}), 400
    if exchange not in EXCHANGES:
        return jsonify({"error": "Exchange inválido"}), 400

    write_cashflow(value, exchange, comments)

    # Refleja el ingreso/egreso en el capital en vivo (incremental, no resetea el estado).
    # load_state_from_db lo recalcula igual al arrancar/cerrar trades (asigna, no duplica).
    sfx = "binance" if exchange == "BINANCE" else "ctrader"
    setattr(gl, f"capital_{sfx}", getattr(gl, f"capital_{sfx}") + value)
    gl.capital = gl.capital_binance + gl.capital_ctrader

    return jsonify({"status": "ok"})


@bp.route('/cashflow_data')
def cashflow_data():
    filas = _query("""
        SELECT TO_CHAR(datetime, 'DD/MM/YYYY HH24:MI') as datetime,
               exchange, value, comments
        FROM cashflow
        ORDER BY pk DESC
    """)
    return jsonify(_serialize(filas))


# ── Endpoints JSON ────────────────────────────────────────────────────────────

# Eventos de orden (R0/R1/R2/SL/BE/TS/etc.) de la ÚLTIMA posición de un símbolo.
@bp.route('/movimientos')
def movimientos():
    ticker = (request.args.get("ticker") or "").lower().strip()
    filas  = _query("""
        SELECT id_order, type, pos, pe, sl, r1, r2, qty,
               v1r, pnl, balance, commission,
               TO_CHAR(time, 'DD/MM/YYYY HH24:MI') as time
        FROM order_event
        WHERE lower(trim(symbol)) = lower(trim(%s))
          AND id_pos = (
              SELECT MAX(id_pos)
              FROM order_event
              WHERE lower(trim(symbol)) = lower(trim(%s))
          )
        ORDER BY order_event.time ASC
    """, (ticker, ticker))
    return jsonify(_serialize(filas))


# Igual que /movimientos pero con TODAS las posiciones históricas del símbolo (incluye id_pos).
@bp.route('/movimientos_all')
def movimientos_all():
    ticker = (request.args.get("ticker") or "").lower().strip()
    filas  = _query("""
        SELECT id_order, id_pos, type, pos, pe, sl, r1, r2, qty,
               v1r, pnl, balance, commission,
               TO_CHAR(time, 'DD/MM/YYYY HH24:MI') as time
        FROM order_event
        WHERE lower(trim(symbol)) = %s
        ORDER BY order_event.time ASC
    """, (ticker,))
    return jsonify(_serialize(filas))


# Historial completo de trades cerrados (una fila por trade). Base de todas las métricas del front.
@bp.route('/analytics_data')
def analytics_data():
    filas = _query("""
        SELECT symbol, id_pos, type, pos,
               TO_CHAR(time_open, 'DD/MM/YYYY HH24:MI') as time_open,
               TO_CHAR(time_close, 'DD/MM/YYYY HH24:MI') as time_close,
               pe, ps, v1r, resultado, trade_sequence, inst, dd_index, pk, comments
        FROM trade_history
        ORDER BY trade_history.time_open DESC
    """)
    return jsonify(_serialize(filas))


# Igual que /analytics_data pero filtrado a un solo símbolo (para la vista analytics_symbol).
@bp.route('/analytics_data_symbol')
def analytics_data_symbol():
    ticker = (request.args.get("ticker") or "").lower().strip()
    filas  = _query("""
        SELECT symbol, id_pos, type, pos,
               TO_CHAR(time_open, 'DD/MM/YYYY HH24:MI') as time_open,
               TO_CHAR(time_close, 'DD/MM/YYYY HH24:MI') as time_close,
               pe, ps, v1r, resultado, trade_sequence, inst
        FROM trade_history
        WHERE lower(trim(symbol)) = %s
        ORDER BY trade_history.time_open DESC
    """, (ticker,))
    return jsonify(_serialize(filas))