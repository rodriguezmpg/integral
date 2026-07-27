"""Modelo de estado por instrumento. Cada símbolo tiene un SymbolContainer con tres piezas
que se llenan en momentos distintos del trade: DataPost (lo que carga el usuario), FixedData
(lo que se fija al abrir) y RealTime (lo que se mueve tick a tick). Globales (`gl`) agrega
las métricas de cartera sobre todos los containers."""


class DataPost:
    """Inputs del usuario en el cuadro de apertura."""
    def reset(self):
        self.__init__()
    def __init__(self):        
        self.risk_usdt = 0
        self.socket_active = False
        self.comment = ''   # nota opcional; va a trade_history al cerrar

        self.target_price = 0.00
        self.tp1 = 0.00
        self.r_1 = 0.00
        self.check_2_1 = False
        self.check_BE_R0 = False
        self.check_BE_percR_1 = False
    

    def to_dict(self):
        return {
            'ps_target_price':          self.target_price,
            'ps_risk_usdt':       self.risk_usdt,
            'ps_socket_active': self.socket_active,
            'ps_comment':       self.comment
        }

        
class FixedData:
    """Se calcula al abrir y queda fijo hasta el cierre: niveles R, cantidades, precisión."""
    def reset(self):
        self.__init__()
    def __init__(self):
        self.restart = True
        self.open_time = None       
        self.type_pos = ''
        self.control = True
        self.validation_message = ''
        self.id_order = 0
        self.side_close = ''
        self.id_pos = 0
        self.type_ins = '' #FOREX, CRYPTO, CFD

        self.steps_Qty_var = 0.00

        self.Qty_min = 0
        self.price_decimals = 0
        self.dec_qty = 0

        self.perc1r = 0.00
        self.perc2r = 0.00
        self.perc1_r = 0.00

        self.r_1 = 0.00
        self.r0 = 0.00
        self.r1 = 0.00
        self.r2 = 0.00
       
        self.dist_1r = 0.00
        self.mid_dist = 0.00

        self.Qty_r1 = 0.00
        self.Qty_r2 = 0.00
        self.Qty_ts = 0.00

        self.pnl1_r = 0.00
        self.pnl1r = 0.00
        self.pnl2r = 0.00

        self.perc_spread = 0.00   # spread de apertura como % de 1R (solo cTrader)

        self.Qty_mVar = 0.00
        
    def to_dict(self):
        return {
            'fd_open_time':   self.open_time.strftime("%d/%m/%Y %H:%M") if self.open_time else None,
            'fd_type_pos':      self.type_pos,
            'fd_control':       self.control,
            'fd_validation_message':       self.validation_message,
            'fd_id_order':      self.id_order,
            'fd_side_close':    self.side_close,
            'fd_id_pos':        self.id_pos,
            'fd_Qty_min':       self.Qty_min,
            'fd_price_decimals':    self.price_decimals,
            'fd_dec_qty':       self.dec_qty,
            'fd_perc1r':        self.perc1r,
            'fd_perc2r':        self.perc2r,
            'fd_perc1_r':       self.perc1_r,
            'fd_r_1':           self.r_1,
            'fd_r0':            self.r0,
            'fd_r1':            self.r1,
            'fd_r2':            self.r2,
            'fd_dist_1r':       self.dist_1r,
            'fd_Qty_r1':        self.Qty_r1,
            'fd_Qty_r2':        self.Qty_r2,
            'fd_Qty_ts':        self.Qty_ts,
            'fd_pnl1_r':        self.pnl1_r,
            'fd_pnl1r':         self.pnl1r,
            'fd_pnl2r':         self.pnl2r,
            'fd_perc_spread':   self.perc_spread,
            'fd_Qty_mVar':      self.Qty_mVar,
            'fd_type_ins':      self.type_ins
        }


class RealTime:
    """Estado vivo de la posición: precio, flags de niveles, P&L no realizado."""
    def reset(self):
        self.__init__()
    def __init__(self):
        self.current_price = 0.00
        self.previous_price = 0.00
        self.current_datetime = None
        self.trade_sequence = ''

        self.DD_index = 0.00
        self.DD = 0.00
        self.max_DD = 0.00

        self.stop_manual = False   # cierre manual pedido desde la UI

        self.balance = 0.00

        self.commission = 0.00

        self.breakeven_stage = 0   # -1 = SL original, 0 = breakeven, 1+ = trailing

        self.r_1 = 0.00

        self.r1_active = False
        self.r2_active = False
        self.r_1_active = False
        self.r_ts = 0.00

        self.Qty_r1 = 0.00
        self.Qty_r2 = 0.00
        self.Qty_ts = 0.00

        self.ALGO_orderid = 0
        self.ALGO_pnl = 0.00
        self.ALGO_commission = 0.00
        self.ALGO_QtymVar = 0.00
        self.ALGO_PE = 0.00        

        self.id_order_r1 = None
        self.id_order_r2 = None
        self.id_order_r_1 = None

        #Variables financieras
        self.position_pct = 0.00
        self.unrealized_pnl = 0.00
        self.ALGO_pos = ''
        self.equity = 0.00
        self.funding_fee = 0.00
        self.swap = 0.00

        # datos de margin que sirve el broker (Binance, cuenta cruzada); los refresca el poll
        self.liquid_price = 0.00   # precio de liquidación que informa Binance
        self.maint_margin = 0.00   # aporte de esta posición al maintenance margin
        self.leverage     = 0.00   # apalancamiento configurado del símbolo
        self.used_margin  = 0.00   # cTrader: margin usado por esta posición, en USD

    def to_dict(self):
        return {
            'current_price':     self.current_price,
            'rt_previous_price': self.previous_price,
            'rt_current_datetime':     self.current_datetime,
            'rt_trade_sequence':      self.trade_sequence,
            'rt_stop_manual':     self.stop_manual,
            'rt_balance':        round(self.balance,4),
            'rt_commission':       self.commission,
            'rt_breakeven_stage':         self.breakeven_stage,
            'rt_r_1':            self.r_1,
            'rt_r1_active':      self.r1_active,
            'rt_r2_active':      self.r2_active,
            'rt_r_1_active':     self.r_1_active,
            'rt_r_ts':           self.r_ts,
            'rt_Qty_r1':         self.Qty_r1,
            'rt_Qty_r2':         self.Qty_r2,
            'rt_Qty_ts':         self.Qty_ts,
            'rt_ALGO_orderid':   self.ALGO_orderid,
            'rt_ALGO_pnl':       self.ALGO_pnl,
            'rt_ALGO_commission':  self.ALGO_commission,
            'rt_ALGO_QtymVar':   self.ALGO_QtymVar,
            'rt_ALGO_PE':        self.ALGO_PE,
            'rt_id_order_r1':    self.id_order_r1,
            'rt_id_order_r2':    self.id_order_r2,
            'rt_id_order_r_1':   self.id_order_r_1,
            'rt_position_pct':   self.position_pct,
            'rt_unrealized_pnl':       self.unrealized_pnl,
            'rt_ALGO_pos':       self.ALGO_pos,
            'rt_equity':   self.equity,
            'rt_funding_fee':    self.funding_fee,
            'rt_swap':           self.swap,
            'rt_DD' :    self.DD,
            'rt_DD_index' :    self.DD_index,
            'rt_max_DD' :    self.max_DD,
            'rt_liquid_price':   self.liquid_price,
            'rt_maint_margin':   self.maint_margin,
            'rt_leverage':       self.leverage,
            'rt_used_margin':    self.used_margin
        }


class Globales:
    # métricas vivas: aggregate() las recalcula en cada corrida; cada una existe en tres
    # versiones (global, _binance, _ctrader)
    _METRICS = (
        "risk_usdt",
        "active_sockets",
        "active_sockets_long",
        "active_sockets_short",
        "unrealized_pnl",
        "unrealized_pnl_long",
        "unrealized_pnl_short",
        "equity",
        "equity_long",
        "equity_short",
        "capital_at_risk",
        "available_capital",
        "locked_profit",
        "locked_equity"
    )

    # P&L realizado: se obtiene desde la DB y aggregate() no lo resetea
    _PERSISTENT = (
        "balance",
        "balance_long",
        "balance_short",
    )

    def __init__(self):
        # capital real por broker = base fija + P&L realizado; se reconstruye desde la DB
        self.capital_binance_base = 0.0
        self.capital_ctrader_base = 0.0
        self.capital_binance = self.capital_binance_base
        self.capital_ctrader = self.capital_ctrader_base
        self.capital = self.capital_binance + self.capital_ctrader
        self.perc_f = 0.005  # 0,5%
        self.risk_max       = 0.00
        self.risk_limit     = 0.00
        self.available_risk = 0.00
        # estado de margin de la cuenta Binance (cruzada); lo llena el poll, aggregate() no lo toca
        self.margin_ratio_binance      = 0.00   # maintMargin / marginBalance * 100; al 100% Binance liquida
        self.margin_balance_binance    = 0.00   # wallet + uPnL: equity real de futures
        self.wallet_balance_binance    = 0.00   # capital sin uPnL transitorio
        self.maint_margin_binance      = 0.00   # maintenance margin total de las abiertas
        self.available_balance_binance = 0.00   # disponible para abrir nuevas posiciones
        # cTrader: mismo control con el balance real del broker
        self.wallet_balance_ctrader    = 0.00
        self.used_margin_ctrader       = 0.00
        # fracción máxima del balance que puede estar en riesgo a la vez (todas las posiciones a SL)
        self.max_risk_pct = 0.70
        self._init_persistent()
        self._reset_accumulators()

    def _reset_accumulators(self):
        # métricas vivas en 0, en sus tres versiones; la llama aggregate()
        for m in self._METRICS:
            setattr(self, m, 0)
            setattr(self, f"{m}_binance", 0)
            setattr(self, f"{m}_ctrader", 0)

    def _init_persistent(self):
        # el realizado arranca en 0; lo siembra load_state_from_db
        for m in self._PERSISTENT:
            setattr(self, m, 0.0)
            setattr(self, f"{m}_binance", 0.0)
            setattr(self, f"{m}_ctrader", 0.0)

    def load_state_from_db(self):
        """Reconstruye el realizado desde trade_history (sobrevive reinicios): balance por
        exchange y por lado, y el capital real (base + realizado + cashflow)."""
        from core.dbfunc import read_analytics, read_cashflow_totals
        _, _, tipo, _, _, _, _, _, _, resultado, _, inst = read_analytics()
        cf_binance, cf_ctrader = read_cashflow_totals()   # neto de depósitos/retiros por broker

        for a in ("balance_binance", "balance_ctrader",
                  "balance_long_binance", "balance_long_ctrader",
                  "balance_short_binance", "balance_short_ctrader"):
            setattr(self, a, 0.0)

        for t, r, ins in zip(tipo, resultado, inst):
            if r is None:
                continue
            sfx  = "binance" if (ins or "").upper() == "CRYPTO" else "ctrader"   # crypto -> binance, forex/cfd -> ctrader
            side = "long" if (t or "").upper() == "LONG" else "short"
            setattr(self, f"balance_{side}_{sfx}", getattr(self, f"balance_{side}_{sfx}") + r)
            setattr(self, f"balance_{sfx}",        getattr(self, f"balance_{sfx}")        + r)

        self.balance_long  = self.balance_long_binance  + self.balance_long_ctrader
        self.balance_short = self.balance_short_binance + self.balance_short_ctrader
        self.balance       = self.balance_binance       + self.balance_ctrader

        # el cashflow no es P&L pero sí mueve el capital disponible
        self.capital_binance = self.capital_binance_base + self.balance_binance + cf_binance
        self.capital_ctrader = self.capital_ctrader_base + self.balance_ctrader + cf_ctrader
        self.capital = self.capital_binance + self.capital_ctrader

    def aggregate(self, crypto_list, forex_list, cfd_list, symbols):
        from core.exchanges.ctrader_ext import ctrader   
        self._reset_accumulators()
        self.used_margin_ctrader = 0.0   

        def bump(attr, val):                     
            setattr(self, attr, getattr(self, attr) + val)

        for symbol in crypto_list + [s.lower() for s in forex_list] + [s.lower() for s in cfd_list]:
            container = symbols.get(symbol)
            if not container:
                continue
            ps, rt, fd = container.ps, container.rt, container.fd
            if not ps.socket_active:
                continue

            sfx = "binance" if fd.type_ins == "CRYPTO" else "ctrader"  

            if sfx == "ctrader":
                self.used_margin_ctrader += rt.used_margin 

            if rt.breakeven_stage == -1:
                bump(f"risk_usdt_{sfx}", ps.risk_usdt)
            elif rt.breakeven_stage > 0:
                qty_lp = rt.Qty_ts + rt.Qty_r1 + rt.Qty_r2
                if fd.type_ins == "CRYPTO":
                    lp = abs((fd.r0 - rt.r_1) * qty_lp)                             
                else:
                    lp = abs(ctrader.pnl_usd(symbol.upper(), fd.r0 - rt.r_1, qty_lp))  # forex/CFD: lotes a unidades×conv
                bump(f"locked_profit_{sfx}", lp)

           
            if fd.type_pos == 'LONG':
                bump(f"active_sockets_long_{sfx}",  1)
                bump(f"unrealized_pnl_long_{sfx}",  rt.unrealized_pnl)
            else:
                bump(f"active_sockets_short_{sfx}", 1)
                bump(f"unrealized_pnl_short_{sfx}", rt.unrealized_pnl)

        # Derivados por broker (mismas fórmulas para binance y ctrader).
        for sfx in ("binance", "ctrader"):
            g = lambda a: getattr(self, f"{a}_{sfx}")
            s = lambda a, v: setattr(self, f"{a}_{sfx}", v)
            cap = getattr(self, f"capital_{sfx}")
            s("active_sockets",    g("active_sockets_long") + g("active_sockets_short"))
            s("balance",           g("balance_long")        + g("balance_short"))
            s("unrealized_pnl",    g("unrealized_pnl_long") + g("unrealized_pnl_short"))
            s("equity_long",       g("balance_long")        + g("unrealized_pnl_long"))
            s("equity_short",      g("balance_short")       + g("unrealized_pnl_short"))
            s("equity",            g("equity_long")         + g("equity_short"))
            s("locked_equity",     g("balance")             + g("locked_profit"))
            s("capital_at_risk",   (g("risk_usdt") / cap * 100) if cap else 0)
            s("available_capital", cap - g("risk_usdt"))

      
        for m in self._METRICS + self._PERSISTENT:
            if m == "capital_at_risk":
                continue
            setattr(self, m, getattr(self, f"{m}_binance") + getattr(self, f"{m}_ctrader"))
        self.capital_at_risk = (self.risk_usdt / self.capital * 100) if self.capital else 0
        self.risk_max       = (self.locked_profit + self.capital) * self.perc_f 
        self.risk_limit     = self.capital * 0.06
        self.available_risk = self.risk_limit - self.risk_usdt

    def to_dict(self):
        d = {
            'gl_capital':         round(self.capital, 4),
            'gl_capital_binance': round(self.capital_binance, 4),
            'gl_capital_ctrader': round(self.capital_ctrader, 4),
            'gl_perc_f':          self.perc_f,
            'gl_risk_max':        round(self.risk_max, 4),
            'gl_risk_limit':      round(self.risk_limit, 4),
            'gl_available_risk':  round(self.available_risk, 4),
            # Margin de Binance Futures (solo _binance; cTrader no tiene equivalente acá).
            'gl_margin_ratio_binance':      round(self.margin_ratio_binance, 2),
            'gl_margin_balance_binance':    round(self.margin_balance_binance, 4),
            'gl_wallet_balance_binance':    round(self.wallet_balance_binance, 4),
            'gl_maint_margin_binance':      round(self.maint_margin_binance, 4),
            'gl_available_balance_binance': round(self.available_balance_binance, 4),
            'gl_wallet_balance_ctrader':    round(self.wallet_balance_ctrader, 4),
            'gl_used_margin_ctrader':       round(self.used_margin_ctrader, 4),
            'gl_max_risk_pct':              self.max_risk_pct,
        }
        # Cada métrica en sus 3 versiones: gl_<m>, gl_<m>_binance, gl_<m>_ctrader.
        # Incluye _PERSISTENT (balance*) para que el frontend reciba gl_balance / gl_balance_long / etc.
        for m in self._METRICS + self._PERSISTENT:
            d[f'gl_{m}']         = round(getattr(self, m), 4)
            d[f'gl_{m}_binance'] = round(getattr(self, f'{m}_binance'), 4)
            d[f'gl_{m}_ctrader'] = round(getattr(self, f'{m}_ctrader'), 4)
        return d


class SymbolContainer:
    """El contenedor de un instrumento: agrupa sus tres estados (ps/fd/rt) bajo un mismo ticker."""
    def __init__(self, symbol):
        self.symbol = symbol.lower()
        # Cada símbolo nace con sus tres estados independientes
        self.ps = DataPost()
        self.fd = FixedData()
        self.rt = RealTime()

    def reset(self):
        """Llama al reset de cada clase interna de forma aislada."""
        self.ps.reset()
        self.fd.reset()
        self.rt.reset()


    def get_combined(self):
        """Une los 3 estados para el Panel de Control."""
        return {
            **self.fd.to_dict(),
            **self.rt.to_dict(),
            **self.ps.to_dict(),
        }



gl = Globales() 

class OrderError(Exception): #Para que detenga el socket si hay error en la orden.
    pass