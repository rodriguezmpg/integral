"""Autenticación básica por sesión para toda la app.
Como el proyecto usa blueprints, en vez de decorar ruta por ruta protegemos todo
con un guardián global (`before_request`): cualquier request sin sesión iniciada
se redirige a /login. Basta con llamar `init_auth(app)` una vez en create_app().
"""
import os
import time
import secrets
import logging

from flask import request, session, redirect, url_for, render_template

logger = logging.getLogger('reg')

# Endpoints accesibles SIN estar logueado: la propia pantalla de login y los
# archivos estáticos (css/iconos). Todo lo demás queda detrás del login.
PUBLIC_ENDPOINTS = {'login', 'logout', 'static'}

# Rate limit de login por IP (en memoria): {ip: (intentos_fallidos, timestamp_ultimo_fallo)}.
# Tras cada fallo se exige una espera creciente (2^intentos seg, tope 60) antes del próximo
# intento. Se limpia al loguear bien.
_failed_logins = {}
_MAX_WAIT = 60


def init_auth(app):
    app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

    login_user = os.getenv("LOGIN_USER", "admin")
    login_pass = os.getenv("LOGIN_PASS", "changeme")

    @app.before_request
    def require_login():
        if request.endpoint in PUBLIC_ENDPOINTS:
            return None
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return None

    @app.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            ip = request.remote_addr or 'unknown'

            intentos, ultimo = _failed_logins.get(ip, (0, 0))
            espera = min(2 ** intentos, _MAX_WAIT) if intentos else 0
            restante = espera - (time.time() - ultimo)
            if restante > 0:
                logger.warning(f"[LOGIN] Attempt blocked by rate-limit from {ip} (wait {restante:.0f}s)")
                error = f"Demasiados intentos. Esperá {restante:.0f} segundos."
                return render_template("login.html", error=error)

            
            user_ok = secrets.compare_digest(request.form.get("username", ""), login_user)
            pass_ok = secrets.compare_digest(request.form.get("password", ""), login_pass)
            if user_ok and pass_ok:
                _failed_logins.pop(ip, None)   # login correcto: se resetea el contador de la IP
                session["logged_in"] = True
                return redirect(url_for("trading.index"))

            _failed_logins[ip] = (intentos + 1, time.time())
            logger.warning(f"[LOGIN] Failed attempt from {ip} (attempt #{intentos + 1})")
            error = "Usuario o contraseña incorrectos"
        return render_template("login.html", error=error)

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))
