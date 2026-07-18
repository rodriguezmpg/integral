import os
import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ.get("DATABASE_URL")


def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_event (
        pk          SERIAL PRIMARY KEY,
        symbol      TEXT    NOT NULL,
        id_order    BIGINT,
        id_pos      INTEGER,
        type        TEXT,
        pos         TEXT,
        pe          DOUBLE PRECISION,
        sl          DOUBLE PRECISION,
        r1          DOUBLE PRECISION,
        r2          DOUBLE PRECISION,
        qty         DOUBLE PRECISION,
        v1r         DOUBLE PRECISION,
        pnl         DOUBLE PRECISION,
        balance     DOUBLE PRECISION,
        commission  DOUBLE PRECISION,
        time        TIMESTAMP
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_event_symbol ON order_event(symbol)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_event_time   ON order_event(time)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trade_history (
        pk         SERIAL PRIMARY KEY,
        symbol     TEXT    NOT NULL,
        id_pos     INTEGER,
        type       TEXT,
        pos        TEXT,
        time_open  TIMESTAMP,
        time_close TIMESTAMP,
        pe         DOUBLE PRECISION,
        ps         DOUBLE PRECISION,
        v1r        DOUBLE PRECISION,
        resultado  DOUBLE PRECISION,
        trade_sequence  TEXT,
        dd_index   DOUBLE PRECISION,
        inst       TEXT,
        comments   TEXT,
        perc_spread DOUBLE PRECISION
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_history_symbol ON trade_history(symbol)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS cashflow (
        pk        SERIAL PRIMARY KEY,
        value     DOUBLE PRECISION,
        exchange  TEXT,
        datetime  TIMESTAMP,
        comments  TEXT
    )
    """)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("DB inicializada correctamente.")