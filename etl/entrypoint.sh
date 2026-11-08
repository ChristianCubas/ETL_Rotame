#!/bin/bash
# ==============================================================================
# entrypoint.sh - Pipeline completo del Data Warehouse
# ==============================================================================
#   1. Crea la base de datos y aplica el esquema (tablas operativas, dimensiones
#      y hechos).
#   2. Carga los CSV de 'datos_clinica' en sus tablas operativas.
#   3. Construye las dimensiones y los hechos a partir de esas tablas.
#
# Para ejecutar un paso suelto:
#   docker compose run --rm --entrypoint python etl llenado.py --solo-esquema
# ==============================================================================
set -euo pipefail

echo "=========================================================="
echo "  ETL CLINICA PACIFICO - PIPELINE COMPLETO"
echo "=========================================================="
echo "  SQL Server      : ${SQLSERVER_HOST:-localhost}:${SQLSERVER_PORT:-1433}"
echo "  Base de datos   : ${SQLSERVER_DATABASE:-clinica_pacifico}"
echo "  Recarga forzada : ${FORCE_RELOAD:-0}"
echo "  Ventana (anios) : ${ANIOS_HISTORICO:-3}"
echo "=========================================================="

echo ""
echo "[PASO 1/3] Creando la base de datos y aplicando el esquema..."
python llenado.py --solo-esquema

echo ""
echo "[PASO 2/3] Cargando los CSV en las tablas operativas..."
python llenado.py --solo-carga

echo ""
echo "[PASO 3/3] Construyendo dimensiones y hechos..."
if [ "$#" -gt 0 ]; then
    "$@"
else
    python etl.py
fi
