# Capturas del README

Imágenes que referencia el README principal.

Ya generada: `estrategia.svg` (diagrama de niveles R, hecho a mano; colores elegidos para
verse bien en tema claro y oscuro de GitHub).

Faltan estas tres:

| Archivo | Pantalla | Qué tiene que mostrar |
|---|---|---|
| `dashboard.png` | `/` (index.html) | Panel de Control con posiciones activas. Ideal: una de Binance y una de cTrader a la vez (demuestra los dos brokers en una imagen), una en verde y otra en rojo, y alguna con el stop ya movido. |
| `monitor.png` | `/main?ticker=X` (main_loop.html) | Posición activa con el chart cargado y las líneas Entry/TP1/TP2/SL dibujadas sobre las velas. |
| `analytics.png` | `/analytics` | Métricas de calidad y el gráfico de P&L acumulado. |

## Cómo sacarlas

Con DevTools de Chrome, que captura sin la barra del navegador:

1. F12 → `Ctrl+Shift+M` (modo dispositivo)
2. Elegir "Responsive" y poner **1400** de ancho (el `.container` tiene `max-width: 1400px`;
   más ancho solo agrega espacio blanco, y GitHub reescala a ~850px, así que de 1920 el texto
   queda ilegible). DPR en 2 para que salga nítida.
3. `Ctrl+Shift+P` → `screenshot` → "Capture screenshot" (pantalla visible) o
   "Capture full size screenshot" (página entera).

Para `dashboard.png` conviene la de pantalla visible: la página completa sale como una tira
vertical larguísima. Para `monitor.png`, full size entra bien.

Sacarlas en modo demo y sin datos de cuenta reales a la vista.
