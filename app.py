import os
import threading

from flask import Flask

from dotenv import load_dotenv
load_dotenv()

import core.logger 
from core.auth import init_auth
from core.dbinit import init_db
from core.classes import gl
from core.exchanges.binance_ext import binance
from core.exchanges.ctrader_ext import ctrader




def create_app():
    app = Flask(__name__)

    # Autenticación: protege TODAS las rutas (login/logout + guardián global).
    init_auth(app)

    init_db() # Reconstruye balance/capital REALIZADO desde trade_history (sobrevive reinicios).
    gl.load_state_from_db()
    binance.preload_exchange_info()

    from routes.trading   import bp as trading_bp
    from routes.analytics import bp as analytics_bp
    from routes.admin     import bp as admin_bp

    app.register_blueprint(trading_bp)
    app.register_blueprint(analytics_bp)
    app.register_blueprint(admin_bp)

    return app


app = create_app()


if __name__ == '__main__':   
    import socket
    socket.getfqdn = lambda name='': name or socket.gethostname() # Evita que Werkzeug se cuelgue resolviendo el hostname via DNS/mDNS al arrancar.

    # Thread: escucha ordenes de binance
    threading.Thread(target=binance.start_event_loop, daemon=True).start()
    # Thread: conexion cTrader (Twisted reactor, bloqueante) - precarga listas forex/cfd
    threading.Thread(target=ctrader.start, daemon=True).start()

    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5002)), debug=False, use_reloader=False)

