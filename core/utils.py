"""Aca esta el dict `symbols`, que mapea cada ticker a su
SymbolContainer: todos los módulos (rutas, lógica, conectores) leen y escriben el estado
de un instrumento a través de este dict. También maneja el alta y baja de instrumentos,
que quedan persistidas en symbols.json."""
import json
import os
from core.classes import SymbolContainer
import logging
logger = logging.getLogger('reg')

_SYMBOLS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'symbols.json')


def _load_config() -> dict:
    with open(_SYMBOLS_PATH, 'r') as f:
        return json.load(f)


def _save_config(config: dict):
    with open(_SYMBOLS_PATH, 'w') as f:
        json.dump(config, f, indent=2)


# symbols.json se carga una vez al arrancar; las tres listas quedan compartidas y las
# altas/bajas las modifican en el lugar, así todos los módulos ven el cambio al instante.
_config     = _load_config()
crypto_list = _config['crypto']
forex_list  = _config['forex']
cfd_list    = _config['cfd']


pending_warnings = {}  # ticker -> mensaje de error; la UI los levanta por /api/warnings y los muestra como popup


def get_vars(symbol):
    """Puerta de entrada al estado de un símbolo: devuelve la tupla (ps, fd, rt) de su
    container. Todos los módulos leen y modifican un instrumento a través de esto."""
    container = symbols.get(symbol.lower())
    if not container:
        raise ValueError(f"El símbolo {symbol} no está registrado en el sistema central.")
    return container.ps, container.fd, container.rt


def var_restart(symbols_to_restart):
    """Vuelve los containers indicados a sus valores iniciales, dejando cada símbolo
    listo para un trade nuevo."""
    for ticker in symbols_to_restart:
        tk = ticker.lower()
        if tk in symbols:
            symbols[tk].reset()
            logger.info(f"[RESET][{tk.upper()}] Done")


def margin_precheck(symbol, new_risk_usdt, broker):
    """Control de riesgo agregado antes de abrir, común a los dos brokers. Simula el peor
    escenario: todas las posiciones abiertas que todavía pueden perder tocan su SL a la
    vez, más la que se quiere abrir. Si esa pérdida potencial (neta del locked profit,
    que hace de colchón) supera max_risk_pct del balance real del broker, la apertura se
    bloquea. Devuelve (ok, motivo)."""
    from core.classes import gl
    tk = symbol.lower()

    if broker == 'binance':
        wallet = gl.wallet_balance_binance
        insts  = ('CRYPTO',)
        locked = gl.locked_profit_binance
    else:  # ctrader
        wallet = gl.wallet_balance_ctrader
        insts  = ('FOREX', 'CFD')
        locked = gl.locked_profit_ctrader

    if wallet <= 0:
        return True, ""   # balance todavía no cargado: no se bloquea, si de verdad falta margen la orden la rechaza el broker

    # Riesgo vivo del resto de las posiciones del broker: solo cuentan las que siguen
    # con el SL en zona de pérdida; en breakeven o trailing ya no pueden perder.
    existing_risk = 0.0
    for key, c in symbols.items():
        if key == tk:
            continue
        if c.fd.type_ins in insts and c.ps.socket_active and c.rt.breakeven_stage == -1:
            existing_risk += c.ps.risk_usdt

    net_risk = existing_risk + new_risk_usdt - locked
    limit = gl.max_risk_pct * wallet
    if net_risk > limit:
        inject = net_risk / gl.max_risk_pct - wallet     # depósito aproximado que haría falta para habilitar la operación
        return False, (f"riesgo neto {net_risk:.2f} USD (todas a SL, menos locked profit) supera el "
                       f"{gl.max_risk_pct:.0%} del balance ({wallet:.2f} USD). Inyectá ~{inject:.2f} USD.")
    return True, ""


def add_symbol(ticker: str) -> tuple:
    """Alta de un crypto: lo suma a symbols.json, a la lista en memoria y le crea el container."""
    tk = ticker.lower()
    if tk in symbols:
        return False, "Symbol already exists"
    config = _load_config()
    if tk in [s.lower() for s in config['crypto']]:
        return False, "Symbol already exists"
    config['crypto'].append(tk)
    _save_config(config)
    crypto_list.append(tk)
    symbols[tk] = SymbolContainer(tk)
    logger.info(f"[SYMBOLS][{tk.upper()}] Crypto symbol added")
    return True, "OK"


def add_forex_symbol(ticker: str) -> tuple:
    """Alta de un par forex de cTrader. El nombre se guarda en mayúsculas (EURUSD) como
    lo usa el broker; la clave del container va en minúsculas como todo el dict symbols."""
    tk_upper = ticker.upper()
    tk_lower = ticker.lower()
    if tk_lower in symbols:
        return False, "Symbol already exists"
    config = _load_config()
    if any(s.upper() == tk_upper for s in config['forex']):
        return False, "Symbol already exists"
    config['forex'].append(tk_upper)
    _save_config(config)
    forex_list.append(tk_upper)
    symbols[tk_lower] = SymbolContainer(tk_lower)
    logger.info(f"[SYMBOLS][{tk_upper}] Forex symbol added")
    return True, "OK"


def add_cfd_symbol(ticker: str) -> tuple:
    """Alta de un CFD de cTrader, mismo mecanismo que forex."""
    tk_upper = ticker.upper()
    tk_lower = ticker.lower()
    if tk_lower in symbols:
        return False, "Symbol already exists"
    config = _load_config()
    if any(s.upper() == tk_upper for s in config['cfd']):
        return False, "Symbol already exists"
    config['cfd'].append(tk_upper)
    _save_config(config)
    cfd_list.append(tk_upper)
    symbols[tk_lower] = SymbolContainer(tk_lower)
    logger.info(f"[SYMBOLS][{tk_upper}] CFD symbol added")
    return True, "OK"


def remove_symbol(ticker: str) -> tuple:
    """Baja de un símbolo, buscándolo en las tres listas. Si tiene una posición activa
    no deja borrarlo."""
    tk = ticker.lower()
    container = symbols.get(tk)
    if container and container.ps.socket_active:
        return False, "Symbol is active, stop it before removing"

    config = _load_config()

    if any(s.lower() == tk for s in config.get('crypto', [])):
        config['crypto'] = [s for s in config['crypto'] if s.lower() != tk]
        try: crypto_list.remove(tk)
        except ValueError: pass

    elif any(s.lower() == tk for s in config.get('forex', [])):
        config['forex'] = [s for s in config['forex'] if s.lower() != tk]
        orig = next((s for s in forex_list if s.lower() == tk), None)
        if orig: forex_list.remove(orig)

    elif any(s.lower() == tk for s in config.get('cfd', [])):
        config['cfd'] = [s for s in config['cfd'] if s.lower() != tk]
        orig = next((s for s in cfd_list if s.lower() == tk), None)
        if orig: cfd_list.remove(orig)

    else:
        return False, "Symbol not found"

    _save_config(config)
    if tk in symbols:
        del symbols[tk]

    logger.info(f"[SYMBOLS][{tk.upper()}] Symbol removed")
    return True, "OK"


# Al arrancar se crea un container por cada instrumento presente en symbols.json.
all_tickers = [tk.lower() for tk in (crypto_list + forex_list + cfd_list)]
symbols = {ticker: SymbolContainer(ticker) for ticker in all_tickers}
