import os
import psycopg2
from psycopg2.extras import RealDictCursor


def _get_conn():
    return psycopg2.connect(os.environ.get("DATABASE_URL"))


def _to_num(v):
    """Convierte '' o None a NULL; deja números como están."""
    if v == "" or v is None:
        return None
    return v


def order_event_row(id_order=None, id_pos=None, type_pos=None, pos=None,
                    pe=None, sl=None, r1=None, r2=None, qty=None,
                    v1r=None, pnl=None, balance=None, commission=None, time=None):
    return [id_order, id_pos, type_pos, pos, pe, sl, r1, r2, qty,
            v1r, pnl, balance, commission, time]


def write_db(Data_csv, symbol):
    conn = _get_conn()
    cur = conn.cursor()

    filas = []
    for row in Data_csv:
        filas.append((
            symbol,
            row[0],             # id_order
            row[1],             # id_pos
            row[2],             # type
            row[3],             # pos
            _to_num(row[4]),    # pe
            _to_num(row[5]),    # sl
            _to_num(row[6]),    # r1
            _to_num(row[7]),    # r2
            _to_num(row[8]),    # qty
            _to_num(row[9]),    # v1r
            _to_num(row[10]),   # pnl
            _to_num(row[11]),   # balance
            _to_num(row[12]),   # commission
            row[13],            # time
        ))

    cur.executemany("""
        INSERT INTO order_event
            (symbol, id_order, id_pos, type, pos, pe, sl, r1, r2, qty,
             v1r, pnl, balance, commission, time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, filas)

    conn.commit()
    cur.close()
    conn.close()


def write_tradehistory_db(symbol, id_pos, type_pos, pos, time_open, time_close,
                      pe, ps, v1r, resultado, trade_sequence, dd_index, inst, comments,
                      perc_spread=None):
    # dd_index = índice de drawdown (base 100) ; inst = FOREX/CFD/CRYPTO ; comments = nota de la posición (opcional).
    # perc_spread = spread de apertura como % de 1R (solo cTrader; NULL en Binance).
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO trade_history
            (symbol, id_pos, type, pos, time_open, time_close,
             pe, ps, v1r, resultado, trade_sequence, dd_index, inst, comments, perc_spread)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        symbol,
        id_pos,
        type_pos,
        pos,
        time_open,
        time_close,
        _to_num(pe),
        _to_num(ps),
        _to_num(v1r),
        _to_num(resultado),
        trade_sequence,
        _to_num(dd_index),
        inst,
        comments or None,
        _to_num(perc_spread),
    ))

    conn.commit()
    cur.close()
    conn.close()


def update_trade_comment(pk, comments):
    """Actualiza el comentario de un trade ya cerrado (fila de trade_history por pk)."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE trade_history SET comments = %s WHERE pk = %s", (comments or None, pk))
    conn.commit()
    cur.close()
    conn.close()


def read_analytics():
    """Lee toda la tabla trade_history y devuelve una lista por cada columna.

    Devuelve, en orden: symbol, id_pos, type, pos, time_open, time_close,
    pe, ps, v1r, resultado, trade_sequence, inst. Cada variable es una lista con
    el dato de esa columna para todas las filas (listas vacías si no hay datos).
    """
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT symbol, id_pos, type, pos, time_open, time_close,
               pe, ps, v1r, resultado, trade_sequence, inst
        FROM trade_history
        ORDER BY time_open ASC
    """)
    filas = cur.fetchall()
    cur.close()
    conn.close()

    # Sin datos: una lista vacía por cada una de las 12 columnas.
    if not filas:
        return ([], [], [], [], [], [], [], [], [], [], [], [])

    # zip(*filas) transpone filas -> columnas; map(list, ...) las hace listas mutables.
    (symbol, id_pos, type_pos, pos, time_open, time_close,
     pe, ps, v1r, resultado, trade_sequence, inst) = map(list, zip(*filas))

    return (symbol, id_pos, type_pos, pos, time_open, time_close,
            pe, ps, v1r, resultado, trade_sequence, inst)


def read_last_dd_index():
    """Devuelve el último índice de drawdown (dd_index) guardado en trade_history.

    Si la tabla está vacía (o el último dd_index es NULL) devuelve 100.0 (base inicial).
    Ordena por pk DESC = última fila insertada = último trade cerrado.
    """
    conn = _get_conn()
    cur = conn.cursor()
    # WHERE dd_index IS NOT NULL: saltea filas sin índice (ej. trades de cTrader mientras no lo escribimos),
    # así el índice no se resetea a 100 por una fila con dd_index nulo.
    cur.execute("SELECT dd_index FROM trade_history WHERE dd_index IS NOT NULL ORDER BY pk DESC LIMIT 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row is None or row[0] is None:
        return 100.0
    return row[0]


def read_max_drawdown_pct():
    """Calcula el Max Drawdown histórico (%): la peor caída de toda la serie de dd_index.

    Recorre la serie en orden cronológico llevando un pico corriente, calcula el
    drawdown en cada punto (mismo criterio que en logic.py/logic_c.py) y devuelve
    el más negativo encontrado. Devuelve 0.0 si no hay datos.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT dd_index FROM trade_history
        WHERE dd_index IS NOT NULL
        ORDER BY pk ASC
    """)
    filas = cur.fetchall()
    cur.close()
    conn.close()

    if not filas:
        return 0.0

    peak = filas[0][0]
    max_drawdown = 0.0
    for (indice,) in filas:
        peak = max(peak, indice)
        drawdown = min(0.0, (indice - peak) / peak * 100)
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown


def read_max_dd_index():
    """Devuelve el pico histórico (máximo) del índice dd_index en trade_history.

    Si la tabla está vacía (o todos los dd_index son NULL) devuelve 100.0 (pico inicial).
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(dd_index) FROM trade_history WHERE dd_index IS NOT NULL")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row is None or row[0] is None:
        return 100.0
    return row[0]


def write_cashflow(value, exchange, comments):
    """Inserta un movimiento de capital (depósito/retiro/ajuste) en cashflow.
    El datetime lo pone la DB en UTC (NOW() AT TIME ZONE 'UTC' = hora de pared UTC como
    TIMESTAMP naive), así no depende de la zona horaria del proceso. exchange = BINANCE/CTRADER.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cashflow (value, exchange, datetime, comments)
        VALUES (%s, %s, NOW() AT TIME ZONE 'UTC', %s)
    """, (_to_num(value), exchange, comments or None))
    conn.commit()
    cur.close()
    conn.close()


def read_cashflow_totals():
    """Suma el neto de cashflow por exchange (ingresos +, egresos -).

    Devuelve (total_binance, total_ctrader). Estos montos se agregan al capital base de
    cada broker para obtener el capital real (junto con el P&L realizado de trade_history).
    Devuelve 0.0 por broker si no hay filas.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT UPPER(TRIM(exchange)) AS ex, COALESCE(SUM(value), 0)
        FROM cashflow
        GROUP BY UPPER(TRIM(exchange))
    """)
    filas = cur.fetchall()
    cur.close()
    conn.close()

    tot = {"BINANCE": 0.0, "CTRADER": 0.0}
    for ex, s in filas:
        if ex in tot:
            tot[ex] = float(s or 0.0)
    return tot["BINANCE"], tot["CTRADER"]