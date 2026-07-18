"""Blueprint de administración: utilidades operativas que no son de trading en sí -
ver y limpiar el log, chequear la IP saliente, refrescar el token de cTrader, exponer los
catálogos de símbolos disponibles, dar de alta/baja instrumentos y el buzón de avisos de la UI."""
import requests
from flask import Blueprint, jsonify, render_template, request

from core.logger import log_handler
from core.exchanges.binance_ext import binance
from core.exchanges.ctrader_ext import ctrader

bp = Blueprint('admin', __name__)


# IP saliente del servidor + si Binance responde. Sirve para saber qué IP hay que whitelistear.
@bp.route('/getip')
def getip():
    ip = requests.get('https://api.ipify.org').text.strip()
    return jsonify({'ip': ip, 'ip_check': binance.check_connection(), 'testnet': binance.testnet})


# Muestra el log en memoria (el buffer del DequeHandler, nada se escribe a disco).
@bp.route('/log')
def view_log():
    return render_template('log.html', lineas=list(log_handler.buffer))


# Vacía el buffer del log y vuelve a la vista.
@bp.route('/log/limpiar')
def clear_log():
    log_handler.buffer.clear()
    return '<script>window.location="/log"</script>'


@bp.route('/ctrader/refresh_token', methods=['POST'])
def ctrader_refresh_token():
    # Fuerza un refresh del token de cTrader a demanda (para probar o renovar manualmente).
    import time
    if not ctrader.tokens.get("refresh_token"):
        return jsonify({"status": "error", "message": "No hay refresh token configurado"}), 400
    try:
        ok = ctrader.refresh_access_token()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    if not ok:
        return jsonify({"status": "error", "message": "El refresh falló (ver /log)"}), 502
    days = int((ctrader.tokens.get("expires_at", 0) - time.time()) / 86400)
    return jsonify({"status": "ok", "expires_in_days": days})


# ── Catálogos de símbolos disponibles ────────────────────────────────────────

# Devuelve todos los símbolos de Binance Futures (desde cache).
@bp.route('/api/binance-symbols')
def get_binance_symbols():
    return jsonify(binance.get_futures_symbols())

@bp.route('/api/forex-symbols')
def get_forex_symbols():
    return jsonify(ctrader._forex_cache)

@bp.route('/api/cfd-symbols')
def get_cfd_symbols():
    return jsonify(ctrader._cfd_cache)


# ── Agregar símbolos ──────────────────────────────────────────────────────────

# Agrega un símbolo crypto. Espera JSON {"ticker": "BTCUSDT"}.
@bp.route('/api/symbols/add', methods=['POST'])
def add_symbol_route():
    from core.utils import add_symbol
    data   = request.get_json(silent=True) or {}
    ticker = (data.get('ticker') or '').strip()
    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400
    ok, msg = add_symbol(ticker)
    return jsonify({'ok': True}) if ok else (jsonify({'error': msg}), 409)

# Agrega un par Forex. Espera JSON {"ticker": "EURUSD"}.
@bp.route('/api/symbols/add-forex', methods=['POST'])
def add_forex_route():
    from core.utils import add_forex_symbol
    data   = request.get_json(silent=True) or {}
    ticker = (data.get('ticker') or '').strip()
    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400
    ok, msg = add_forex_symbol(ticker)
    return jsonify({'ok': True}) if ok else (jsonify({'error': msg}), 409)

# Agrega un CFD. Espera JSON {"ticker": "XAUUSD"}.
@bp.route('/api/symbols/add-cfd', methods=['POST'])
def add_cfd_route():
    from core.utils import add_cfd_symbol
    data   = request.get_json(silent=True) or {}
    ticker = (data.get('ticker') or '').strip()
    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400
    ok, msg = add_cfd_symbol(ticker)
    return jsonify({'ok': True}) if ok else (jsonify({'error': msg}), 409)


# ── Eliminar símbolos ─────────────────────────────────────────────────────────

# Elimina cualquier símbolo. Rechaza si el socket está activo.
@bp.route('/api/symbols/remove', methods=['DELETE'])
def remove_symbol_route():
    from core.utils import remove_symbol
    ticker = request.args.get('ticker', '').strip()
    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400
    ok, msg = remove_symbol(ticker)
    return jsonify({'ok': True}) if ok else (jsonify({'error': msg}), 409)


# Buzón de avisos para la UI: entrega los warnings pendientes y los limpia (se leen una sola vez).
@bp.route('/api/warnings')
def get_warnings():
    from core.utils import pending_warnings
    warnings = dict(pending_warnings)
    pending_warnings.clear()
    return jsonify(warnings)
