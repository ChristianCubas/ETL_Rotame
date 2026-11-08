#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
llenado.py - Creacion del esquema y carga inicial de los CSV
==============================================================================
Flujo:
    1. Espera a que SQL Server este disponible (reintentos).
    2. Crea la base de datos si no existe.
    3. Aplica 'estructura/esquema_tablas_farma.sql' (tablas operativas,
       dimensiones y hechos). La ejecucion es idempotente: los errores de
       "ya existe" se ignoran y los errores reales se reportan.
    4. Carga cada CSV de 'datos_clinica' en su tabla, en lotes y con
       fast_executemany. Las filas que SQL Server rechaza se aíslan y se
       cuentan en lugar de abortar toda la carga.
    5. Siembra 'pa_acto_medico' con los actos medicos referenciados por las
       recetas y los diagnosticos (la tabla no viene en los CSV).

Uso:
    python llenado.py                     # esquema + carga (por defecto)
    python llenado.py --solo-esquema      # solo crea base de datos y esquema
    python llenado.py --solo-carga        # solo carga (asume esquema ya creado)
    python llenado.py --force             # recarga aunque las tablas tengan datos

Variables de entorno:
    SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER, SQLSERVER_PASSWORD,
    SQLSERVER_DATABASE, SQL_DRIVER, BATCH_SIZE, FORCE_RELOAD
==============================================================================
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, date

import pyodbc

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv es opcional
    pass


# ==============================================================================
# CONFIGURACION
# ==============================================================================
SQL_HOST     = os.getenv("SQLSERVER_HOST", "localhost")
SQL_PORT     = os.getenv("SQLSERVER_PORT", "1433")
SQL_USER     = os.getenv("SQLSERVER_USER", "sa")
SQL_PASSWORD = os.getenv("SQLSERVER_PASSWORD", os.getenv("MSSQL_SA_PASSWORD", ""))
SQL_DATABASE = os.getenv("SQLSERVER_DATABASE", "clinica_pacifico")
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "5000"))
FORCE_RELOAD = os.getenv("FORCE_RELOAD", "0").lower() in ("1", "true", "yes")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POSSIBLE_ROOTS = [
    BASE_DIR,
    os.path.abspath(os.path.join(BASE_DIR, "..")),
    "/app",
    ".",
]


def find_path(rel_path: str, is_dir: bool = False) -> str:
    """Busca una ruta relativa en las ubicaciones raiz posibles."""
    for root in POSSIBLE_ROOTS:
        candidate = os.path.join(root, rel_path)
        if (is_dir and os.path.isdir(candidate)) or (not is_dir and os.path.isfile(candidate)):
            return os.path.abspath(candidate)
    return os.path.abspath(os.path.join(BASE_DIR, rel_path))


DIR_DATOS_CLINICA = find_path("datos_clinica", is_dir=True)
FILE_ESQUEMA_SQL  = find_path(os.path.join("estructura", "esquema_tablas_farma.sql"))


# Columnas de los CSV que vienen SIN cabecera (el orden del archivo es el orden
# de columnas de la tabla, salvo las columnas que no existen en el CSV).
FA_PRODUCTO_COLUMNS = [
    'id', 'codpro', 'despro', 'codlab', 'codgen', 'codfam', 'codtip',
    'pventatipo', 'pventatipo_part', 'codiunidad', 'codunidad_frac',
    'preventa', 'pventa_particular', 'cospromedio', 'existencia_valor',
    'costocompra', 'desctocomp_uno', 'desctocomp_dos', 'costocom_parti',
    'dateregistro', 'datatevence', 'dateanticipo', 'desgeneral', 'desespecial',
    'igvpro', 'profrac', 'fracpro', 'preciofrac', 'precio_fracpart',
    'costo_compra_frac', 'costocom_partfrac', 'costo_pro_frac', 'existencia_frac_val',
    'utilipro', 'utilipro_lista', 'stkcompactual', 'stkcompfra', 'stkmin',
    'st_max', 'estado', 'reusable', 'almacenado', 'ajustado',
    'stk_anterior_ajustado_n', 'stk_anterior_ajustado_f', 'stk_ajustado_normal',
    'stk_ajustado_frac', 'reingreado_ajuste', 'stk_reingresado_normal',
    'stk_reingresado_frac', 'fec_actualiza', 'crea_user', 'user_mod', 'fecha_mod',
    'user_name', 'host_name', 'COST_P_ENT', 'COST_P_FRA', 'caso',
    'costo_pro_frac_1', 'cospromedio_1', 'stkcompactual_1', 'stkcompfra_1',
    'mod', 'fecha_crea', 'cost_comp_part', 'utilidad_part', 'utilidad_paq',
    'gen_receta', 'usu_cam_cost_comp_part', 'ina_por_petitorio', 'pro_no_req',
    'marca', 'cod_seps', 'controlado', 'cod_sunat', 'cod_minsa', 'uni_compra',
    'pre_compra', 'cod_ean', 'para_contraste', 'unidad_minima_sop', 'preventa_bk',
    'cod_uni_ant', 'especial', 'precio_particular', 'icbper', 'preciofrac_ant',
    'codbarra', 'lote', 'fraccionable', 'insumo', 'disponibleventa', 'id_ubicacion',
    'manejapreciopaquete', 'escomisionado', 'presentacion', 'idlogfamilia',
    'linea', 'bien_activo', 'area_destino', 'almacen_distrib', 'especialidad',
    'salida_kardex_almacen', 'kardex', 'solicitar_doc', 'refrigerado',
    'created_by', 'updated_by', 'created_at', 'updated_at', 'deleted_at',
    'nocoberturado'
]

CE_DIAGNOSTICO_COLUMNS = [
    'id', 'dx_codigo', 'dx_desc', 'dx_raiz', 'dx_nivel', 'dx_sexo',
    'codigo_familia', 'nivel', 'estado', 'sinonimos'
]

# Configuracion de mapeo CSV -> tabla.
#   columnas_ignoradas : columnas del CSV que NO se insertan (por nombre del CSV),
#                    porque su valor no es un identificador valido (viene
#                    repetido o en cero).
#   renombrar      : columna del CSV -> columna de la tabla. Se usa cuando el
#                    CSV trae el dato en una columna con el nombre cambiado.
#   derivadas      : columna de la tabla que no viene en el CSV y se calcula a
#                    partir de otra columna del CSV.
TABLAS_CONFIG = [
    {
        "csv": "fa_producto.csv",
        "tabla": "fa_producto",
        "con_cabecera": False,
        "columnas": FA_PRODUCTO_COLUMNS,
        "columnas_ignoradas": (),
    },
    {
        "csv": "ce_dx.csv",
        "tabla": "ce_diagnostico",
        "con_cabecera": False,
        "columnas": CE_DIAGNOSTICO_COLUMNS,
        "columnas_ignoradas": (),
    },
    {
        "csv": "ce_recetas.csv",
        "tabla": "ce_recetas",
        "con_cabecera": True,
        "columnas": None,
        "columnas_ignoradas": (),
    },
    {
        "csv": "ce_detalle_receta.csv",
        "tabla": "ce_detalle_receta",
        "con_cabecera": True,
        "columnas": None,
        "columnas_ignoradas": (),
    },
    {
        "csv": "ce_dx_pac.csv",
        "tabla": "ce_diagnostico_pacientes",
        "con_cabecera": True,
        "columnas": None,
        "columnas_ignoradas": (),
    },
    {
        # La PK es (nro_doc, nro_documento). En el CSV la columna 'nro_doc' trae
        # el CODIGO COMPLETO del documento (11 caracteres, unico por documento),
        # asi que va a nro_documento; nro_doc (char(2)) se deriva del prefijo
        # ('BF' boleta / 'FF' factura). El 'id' del CSV solo tiene 2 valores
        # distintos y la columna 'nro_documento' del CSV es un correlativo
        # repetido: ambos se descartan.
        "csv": "fa_documentos_farma.csv",
        "tabla": "fa_documentos_farma",
        "con_cabecera": True,
        "columnas": None,
        "columnas_ignoradas": ("id", "nro_documento"),
        "renombrar": {"nro_doc": "nro_documento"},
        "derivadas": {"nro_doc": ("nro_doc", "prefijo_2")},
    },
    {
        # 'nro_registro' viene repetido (~4 lineas por documento): no sirve como
        # clave, SQL Server lo genera. Mismo criterio que en la cabecera para el
        # codigo de documento.
        "csv": "fa_detalle_documento.csv",
        "tabla": "fa_detalle_documento",
        "con_cabecera": True,
        "columnas": None,
        "columnas_ignoradas": ("nro_registro", "nro_documento"),
        "renombrar": {"nro_doc": "nro_documento"},
        "derivadas": {"nro_doc": ("nro_doc", "prefijo_2")},
    },
    {
        "csv": "movimiento_kardex.csv",
        "tabla": "movimiento_kardex",
        "con_cabecera": True,
        "columnas": None,
        "columnas_ignoradas": (),
    },
]

TABLAS_REQUERIDAS = [
    # Operativas (se llenan desde los CSV)
    "ce_recetas", "ce_detalle_receta", "ce_diagnostico", "ce_diagnostico_pacientes",
    "fa_documentos_farma", "fa_detalle_documento", "fa_producto",
    "fa_pedidos_farma", "fa_detalle_pedidos_farma", "pa_acto_medico",
    "movimiento_kardex",
    # Data Warehouse (las llena etl.py)
    "dim_tiempo", "dim_medicamento", "dim_lote", "dim_proveedor", "dim_enfermedad",
    "hecho_ventas", "hecho_prescripciones", "hecho_inventario",
]

# Valores de texto que se consideran "sin dato"
NULOS = {"", "NULL", "NAN", "<NA>", "NONE"}

# Transformaciones para columnas derivadas
TRANSFORMACIONES = {
    "completo": lambda v: v,
    "prefijo_2": lambda v: str(v).strip().upper()[:2],
    "prefijo_3": lambda v: str(v).strip().upper()[:3],
}

# Si una tabla descarta mas de este numero de filas, se deja de aislar fila por
# fila (seria lentisimo) y se descartan los lotes completos que fallan.
MAX_FILAS_DESCARTADAS_AISLADAS = 5000

FORMATOS_FECHA = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
)


# ==============================================================================
# CONEXION
# ==============================================================================
def get_best_odbc_driver() -> str:
    """Devuelve el driver ODBC de SQL Server disponible en el sistema."""
    driver_env = os.getenv("SQL_DRIVER")
    if driver_env:
        return driver_env
    try:
        disponibles = pyodbc.drivers()
    except Exception:
        disponibles = []
    for candidato in ("ODBC Driver 18 for SQL Server",
                      "ODBC Driver 17 for SQL Server",
                      "SQL Server"):
        if candidato in disponibles:
            return candidato
    if disponibles:
        return disponibles[0]
    return "ODBC Driver 18 for SQL Server"


def get_connection_string(database: str = None) -> str:
    """Construye la cadena de conexion ODBC a SQL Server."""
    driver = get_best_odbc_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SQL_HOST},{SQL_PORT};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=no;"
    )
    if database:
        conn_str += f"DATABASE={database};"
    return conn_str


def wait_and_connect(database: str = None, max_retries: int = 30,
                     delay: int = 3) -> pyodbc.Connection:
    """Conecta a SQL Server reintentando mientras el contenedor arranca."""
    conn_str = get_connection_string(database)
    destino = database if database else "master"
    print(f"[conexion] SQL Server {SQL_HOST}:{SQL_PORT} base '{destino}' "
          f"(driver: {get_best_odbc_driver()})")

    ultimo_error = None
    for intento in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_str, autocommit=True)
            print(f"[conexion] OK en el intento {intento}.")
            return conn
        except pyodbc.Error as e:
            ultimo_error = e
            if intento == 1:
                print(f"[conexion] Esperando a SQL Server... ({e})")
            if intento < max_retries:
                time.sleep(delay)
    raise RuntimeError(f"No se pudo conectar a SQL Server tras {max_retries} "
                       f"intentos: {ultimo_error}")


# ==============================================================================
# BASE DE DATOS Y ESQUEMA
# ==============================================================================
def ensure_database_exists():
    """Crea la base de datos de trabajo si todavia no existe."""
    conn = wait_and_connect(database="master")
    cursor = conn.cursor()
    cursor.execute("SELECT database_id FROM sys.databases WHERE name = ?", SQL_DATABASE)
    existe = cursor.fetchone()
    if existe:
        print(f"[esquema] La base '{SQL_DATABASE}' ya existe.")
    else:
        print(f"[esquema] Creando la base '{SQL_DATABASE}'...")
        cursor.execute(f"CREATE DATABASE [{SQL_DATABASE}]")
        print(f"[esquema] Base '{SQL_DATABASE}' creada.")
    cursor.close()
    conn.close()


def read_sql_file(file_path: str) -> str:
    """Lee el script SQL probando las codificaciones habituales (UTF-16/UTF-8)."""
    for enc in ("utf-16", "utf-8-sig", "utf-8", "latin1"):
        try:
            with open(file_path, "r", encoding=enc) as f:
                contenido = f.read()
            if "CREATE TABLE" in contenido:
                return contenido
        except (UnicodeError, LookupError):
            continue
    raise FileNotFoundError(f"No se pudo leer el archivo de esquema: {file_path}")


def limpiar_script(sql: str, base: str) -> str:
    """
    Quita del script las instrucciones propias de sqlcmd que pyodbc no entiende
    y evita que se cree una base con el nombre literal '$(DB_NAME)'.
    """
    sql = sql.replace("\r\n", "\n").replace("\r", "\n")

    # Comandos de sqlcmd (:setvar, :r, :on error exit, ...)
    sql = re.sub(r"^\s*:.*$", "", sql, flags=re.MULTILINE)

    # Nombre de base parametrizado -> base real
    sql = sql.replace("$(DB_NAME)", base).replace("[$(DB_NAME)]", f"[{base}]")

    # Bloque que crea la base (ya la crea ensure_database_exists)
    sql = re.sub(
        r"IF\s+DB_ID\([^)]*\)\s+IS\s+NULL\s*\n?\s*CREATE\s+DATABASE\s+\[?[^\];\s]*\]?\s*;?",
        "", sql, flags=re.IGNORECASE)

    # USE [base]; -> la conexion ya apunta a la base correcta
    sql = re.sub(r"^\s*USE\s+\[[^\]]*\]\s*;?\s*$", "", sql, flags=re.MULTILINE | re.IGNORECASE)

    return sql


def _es_error_ya_existe(mensaje: str) -> bool:
    """True si el error es 'el objeto ya existe' (ejecucion idempotente)."""
    patrones = (
        "already an object named",
        "There is already an object",
        "already exists",
        "already has a DEFAULT",
        "already been added",
        "ya existe",
    )
    return any(p.lower() in mensaje.lower() for p in patrones)


def tablas_faltantes(conn: pyodbc.Connection) -> list:
    """Devuelve las tablas requeridas que todavia no existen."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sys.tables WHERE is_ms_shipped = 0")
    existentes = {fila[0].lower() for fila in cursor.fetchall()}
    cursor.close()
    return [t for t in TABLAS_REQUERIDAS if t.lower() not in existentes]


def apply_schema(conn: pyodbc.Connection):
    """Ejecuta el script de esquema completo, bloque por bloque."""
    if not os.path.isfile(FILE_ESQUEMA_SQL):
        raise FileNotFoundError(f"No existe el archivo de esquema: {FILE_ESQUEMA_SQL}")

    print(f"[esquema] Aplicando '{FILE_ESQUEMA_SQL}'...")
    sql = limpiar_script(read_sql_file(FILE_ESQUEMA_SQL), SQL_DATABASE)
    bloques = [b.strip() for b in re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)
               if b.strip()]

    cursor = conn.cursor()
    ya_existian = 0
    errores = []
    for bloque in bloques:
        try:
            cursor.execute(bloque)
            while cursor.nextset():
                pass
        except pyodbc.Error as e:
            mensaje = " ".join(str(x) for x in e.args if x is not None)
            if _es_error_ya_existe(mensaje):
                ya_existian += 1
            else:
                errores.append((bloque.splitlines()[0][:90], mensaje[:300]))
    cursor.close()

    print(f"[esquema] {len(bloques)} bloques procesados "
          f"({ya_existian} omitidos por existir ya, {len(errores)} con error).")
    if errores:
        print("[esquema] Errores reales (no bloquean si las tablas se crearon):")
        for encabezado, mensaje in errores[:8]:
            print(f"          - {encabezado}\n            {mensaje}")
        if len(errores) > 8:
            print(f"          ... y {len(errores) - 8} errores mas.")


def verificar_tablas(conn: pyodbc.Connection):
    """Comprueba que existan todas las tablas que necesita el pipeline."""
    faltantes = tablas_faltantes(conn)
    if faltantes:
        raise RuntimeError(
            "Faltan tablas en la base '" + SQL_DATABASE + "': "
            + ", ".join(faltantes)
            + "\nRevisa los errores del esquema mostrados arriba.")

    print(f"[esquema] OK: las {len(TABLAS_REQUERIDAS)} tablas requeridas existen.")


# ==============================================================================
# SANEAMIENTO DE VALORES
# ==============================================================================
def _es_nulo(valor: str) -> bool:
    return valor.strip().upper() in NULOS


def _parse_fecha(valor: str):
    """Convierte texto a datetime; devuelve None si no es una fecha valida."""
    texto = valor.strip()
    if _es_nulo(texto) or texto.startswith("0000-00-00"):
        return None
    for formato in FORMATOS_FECHA:
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(texto)
    except ValueError:
        return None


def _valor_por_defecto(tipo_sql: str):
    """Valor a usar cuando la columna NO admite nulos y el dato esta vacio."""
    if tipo_sql in ("bigint", "int", "smallint", "tinyint"):
        return 0
    if tipo_sql in ("decimal", "numeric", "float", "real", "money", "smallmoney"):
        return 0.0
    if tipo_sql == "bit":
        return 0
    if tipo_sql in ("datetime", "smalldatetime", "datetime2"):
        return datetime(1900, 1, 1)
    if tipo_sql == "date":
        return date(1900, 1, 1)
    return ""


def clean_val(valor, meta_col: dict):
    """
    Convierte el valor de texto del CSV al tipo de la columna SQL Server.
    Nunca lanza excepcion: si el dato es invalido devuelve NULL (columna
    nullable) o un valor por defecto (columna NOT NULL), de modo que una fila
    corrupta no tumbe el lote completo.
    """
    tipo_sql = meta_col["type"]
    nullable = meta_col["nullable"]

    if valor is None:
        return None if nullable else _valor_por_defecto(tipo_sql)

    texto = str(valor).strip().replace("\x00", "")
    if _es_nulo(texto):
        return None if nullable else _valor_por_defecto(tipo_sql)

    if tipo_sql in ("bigint", "int", "smallint", "tinyint"):
        try:
            return int(float(texto))
        except (ValueError, OverflowError):
            return None if nullable else 0

    if tipo_sql in ("decimal", "numeric", "float", "real", "money", "smallmoney"):
        try:
            numero = float(texto)
        except (ValueError, OverflowError):
            return None if nullable else 0.0
        if numero != numero or numero in (float("inf"), float("-inf")):
            return None if nullable else 0.0
        return numero

    if tipo_sql == "bit":
        bajo = texto.lower()
        if bajo in ("1", "true", "t", "s", "si", "yes"):
            return 1
        if bajo in ("0", "false", "f", "n", "no"):
            return 0
        try:
            return 1 if int(float(texto)) != 0 else 0
        except ValueError:
            return 0

    if tipo_sql in ("datetime", "smalldatetime", "datetime2", "date"):
        fecha = _parse_fecha(texto)
        if fecha is None:
            return None if nullable else _valor_por_defecto(tipo_sql)
        return fecha.date() if tipo_sql == "date" else fecha

    # Texto
    max_len = meta_col["max_len"]
    if max_len and max_len > 0 and len(texto) > max_len:
        return texto[:max_len]
    return texto


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================
def get_table_metadata(conn: pyodbc.Connection, tabla: str) -> dict:
    """Metadatos (tipo, nulabilidad y longitud) de las columnas de una tabla."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COLUMN_NAME, LOWER(DATA_TYPE), IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, tabla)
    meta = {}
    for nombre, tipo, nullable, max_len in cursor.fetchall():
        meta[nombre.lower()] = {
            "name": nombre,
            "type": tipo,
            "nullable": (nullable.upper() == "YES"),
            "max_len": max_len,
        }
    cursor.close()
    return meta


def get_identity_columns(conn: pyodbc.Connection, tabla: str) -> set:
    """Columnas IDENTITY de una tabla (en minusculas)."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sys.identity_columns WHERE object_id = OBJECT_ID(?)
    """, f"dbo.{tabla}")
    columnas = {fila[0].lower() for fila in cursor.fetchall()}
    cursor.close()
    return columnas


def _insertar_lote(cursor, sql: str, filas: list, incidencias: dict) -> int:
    """
    Inserta un lote. Si el lote falla (una fila invalida hace fallar el
    executemany completo) lo divide a la mitad hasta aislar las filas malas,
    que se descartan y se contabilizan.
    """
    if not filas:
        return 0
    try:
        cursor.executemany(sql, filas)
        return len(filas)
    except pyodbc.Error as e:
        if incidencias["filas"] >= MAX_FILAS_DESCARTADAS_AISLADAS:
            # Ya hay demasiadas filas malas: descartar el lote completo en vez
            # de seguir aislando fila por fila (haria el proceso eterno).
            incidencias["filas"] += len(filas)
            if "detalle" not in incidencias:
                incidencias["detalle"] = " ".join(str(x) for x in e.args if x)[:300]
            return 0
        if len(filas) == 1:
            incidencias["filas"] += 1
            if "detalle" not in incidencias:
                incidencias["detalle"] = " ".join(str(x) for x in e.args if x)[:300]
            return 0
        mitad = len(filas) // 2
        return (_insertar_lote(cursor, sql, filas[:mitad], incidencias)
                + _insertar_lote(cursor, sql, filas[mitad:], incidencias))


def cargar_csv(conn: pyodbc.Connection, config: dict) -> None:
    """Carga un archivo CSV en su tabla correspondiente."""
    csv_file = os.path.join(DIR_DATOS_CLINICA, config["csv"])
    tabla = config["tabla"]

    if not os.path.isfile(csv_file):
        print(f"[carga] AVISO: no existe {csv_file}; se omite '{tabla}'.")
        return

    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.[{tabla}]")
    filas_actuales = cursor.fetchone()[0]

    if filas_actuales > 0 and not FORCE_RELOAD:
        print(f"[carga] '{tabla}' ya tiene {filas_actuales:,} filas (se omite; "
              f"usa --force o FORCE_RELOAD=1 para recargar).")
        cursor.close()
        return

    if filas_actuales > 0 and FORCE_RELOAD:
        print(f"[carga] '{tabla}': borrando {filas_actuales:,} filas previas...")
        try:
            cursor.execute(f"TRUNCATE TABLE dbo.[{tabla}]")
        except pyodbc.Error:
            cursor.execute(f"DELETE FROM dbo.[{tabla}]")

    meta = get_table_metadata(conn, tabla)
    if not meta:
        print(f"[carga] ERROR: la tabla '{tabla}' no tiene columnas; se omite.")
        cursor.close()
        return

    print(f"\n[carga] '{config['csv']}' -> dbo.[{tabla}]")
    ignoradas = {c.lower() for c in config.get("columnas_ignoradas", ())}
    renombrar = {k.lower(): v.lower() for k, v in config.get("renombrar", {}).items()}

    with open(csv_file, "r", encoding="utf-8-sig", errors="replace", newline="") as fp:
        lector = csv.reader(fp)
        if config["con_cabecera"]:
            cabecera = next(lector)
            columnas_csv = [c.strip() for c in cabecera]
        else:
            columnas_csv = list(config["columnas"])

        columnas_sql = []
        indices = []
        for idx, columna in enumerate(columnas_csv):
            clave_csv = columna.strip().lower()
            if clave_csv in ignoradas:
                continue
            clave = renombrar.get(clave_csv, clave_csv)
            if clave not in meta:
                continue
            columnas_sql.append(meta[clave]["name"])
            indices.append((idx, meta[clave], None))

        # Columnas que no vienen en el CSV pero se calculan a partir de otra
        for destino, (origen, transformacion) in config.get("derivadas", {}).items():
            destino = destino.lower()
            if destino not in meta or destino in (c.lower() for c in columnas_sql):
                continue
            try:
                idx_origen = [c.strip().lower() for c in columnas_csv].index(origen.lower())
            except ValueError:
                continue
            columnas_sql.append(meta[destino]["name"])
            indices.append((idx_origen, meta[destino], TRANSFORMACIONES[transformacion]))

        if not columnas_sql:
            print(f"[carga] ERROR: ninguna columna del CSV coincide con '{tabla}'.")
            cursor.close()
            return

        print(f"[carga]   columnas mapeadas: {len(columnas_sql)} de {len(meta)} en la tabla")
        sql_insert = (
            f"INSERT INTO dbo.[{tabla}] "
            f"({', '.join('[' + c + ']' for c in columnas_sql)}) "
            f"VALUES ({', '.join(['?'] * len(columnas_sql))})"
        )

        try:
            cursor.execute(f"ALTER TABLE dbo.[{tabla}] NOCHECK CONSTRAINT ALL;")
        except pyodbc.Error:
            pass

        # Si el CSV aporta la columna IDENTITY, hay que conservar sus valores.
        # Se detecta en la base para no tener que mantenerlo a mano por tabla.
        identidad = get_identity_columns(conn, tabla)
        usar_identity_insert = bool(identidad & {c.lower() for c in columnas_sql})
        if usar_identity_insert:
            try:
                cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabla}] ON;")
            except pyodbc.Error as e:
                print(f"[carga]   AVISO: no se pudo activar IDENTITY_INSERT ({e}).")

        cursor.fast_executemany = True

        inicio = time.time()
        total = 0
        lote = []
        incidencias = {"filas": 0}

        for fila in lector:
            if not fila:
                continue
            registro = []
            for idx, meta_col, transformacion in indices:
                valor = fila[idx] if idx < len(fila) else None
                if transformacion is not None and valor is not None:
                    valor = transformacion(valor)
                registro.append(clean_val(valor, meta_col))
            lote.append(tuple(registro))

            if len(lote) >= BATCH_SIZE:
                total += _insertar_lote(cursor, sql_insert, lote, incidencias)
                lote.clear()
                if total and total % 100000 == 0:
                    print(f"[carga]   ... {total:,} filas insertadas")

        if lote:
            total += _insertar_lote(cursor, sql_insert, lote, incidencias)

        if usar_identity_insert:
            try:
                cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabla}] OFF;")
            except pyodbc.Error:
                pass

        try:
            cursor.execute(f"ALTER TABLE dbo.[{tabla}] WITH NOCHECK CHECK CONSTRAINT ALL;")
        except pyodbc.Error:
            pass

    cursor.close()
    segundos = max(time.time() - inicio, 0.001)
    print(f"[carga]   OK: {total:,} filas en {segundos:.1f}s "
          f"({total / segundos:,.0f} filas/s)")
    if incidencias["filas"]:
        print(f"[carga]   AVISO: {incidencias['filas']:,} filas descartadas por datos "
              f"invalidos. Primer error: {incidencias.get('detalle', 'n/d')}")


def seed_acto_medico(conn: pyodbc.Connection) -> None:
    """
    La tabla pa_acto_medico no viene en los CSV: se generan los actos medicos
    referenciados por las recetas y los diagnosticos para no dejar claves
    foraneas huerfanas.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dbo.pa_acto_medico")
    actuales = cursor.fetchone()[0]

    if actuales > 0 and not FORCE_RELOAD:
        print(f"\n[seed] pa_acto_medico ya tiene {actuales:,} filas (se omite).")
        cursor.close()
        return

    if actuales > 0:
        cursor.execute("DELETE FROM dbo.pa_acto_medico")

    print("\n[seed] Generando actos medicos a partir de recetas y diagnosticos...")
    cursor.execute("""
        INSERT INTO dbo.pa_acto_medico (
            nro_acto, nro_historia, edad, pac_servicio, atendido, facturado,
            facturado_varios, deducible, coaseguro, cod_aseg, cod_emp, triaje,
            tratamiento_hospitalario, grupal, entregado, aprobacion_costos, fec_crea
        )
        SELECT DISTINCT
            act.nro_acto, '000000', 0, 'A', 1, 0,
            0, 0.0, 0.0, '0000', '0000000', 0,
            0, 0, 0, 0, GETDATE()
        FROM (
            SELECT DISTINCT nro_acto FROM dbo.ce_recetas WHERE nro_acto IS NOT NULL
            UNION
            SELECT DISTINCT nro_acto FROM dbo.ce_diagnostico_pacientes WHERE nro_acto IS NOT NULL
        ) act
        WHERE act.nro_acto > 0
          AND NOT EXISTS (SELECT 1 FROM dbo.pa_acto_medico p WHERE p.nro_acto = act.nro_acto);
    """)
    cursor.execute("SELECT COUNT(*) FROM dbo.pa_acto_medico")
    print(f"[seed] OK: pa_acto_medico tiene {cursor.fetchone()[0]:,} filas.")
    cursor.close()


def resumen_tablas(conn: pyodbc.Connection) -> None:
    """Imprime el conteo de filas de cada tabla del modelo."""
    cursor = conn.cursor()
    print("\n" + "=" * 70)
    print(" RESUMEN DE CARGA")
    print("=" * 70)
    for tabla in TABLAS_REQUERIDAS:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM dbo.[{tabla}]")
            print(f"  {tabla:<28} {cursor.fetchone()[0]:>12,} filas")
        except pyodbc.Error:
            print(f"  {tabla:<28} {'(no existe)':>12}")
    cursor.close()


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================
def main(argv=None) -> int:
    global FORCE_RELOAD

    parser = argparse.ArgumentParser(
        description="Crea el esquema y carga los CSV de datos_clinica en SQL Server.")
    parser.add_argument("--solo-esquema", action="store_true",
                        help="Solo crea la base de datos y aplica el esquema.")
    parser.add_argument("--solo-carga", action="store_true",
                        help="Solo carga los CSV (asume que el esquema ya existe).")
    parser.add_argument("--force", action="store_true",
                        help="Recarga las tablas aunque ya tengan datos.")
    args = parser.parse_args(argv)

    if args.force:
        FORCE_RELOAD = True

    print("=" * 70)
    print(" LLENADO DE DATOS - CLINICA PACIFICO")
    print("=" * 70)
    print(f"  Fecha           : {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Datos CSV       : {DIR_DATOS_CLINICA}")
    print(f"  Esquema         : {FILE_ESQUEMA_SQL}")
    print(f"  Base de datos   : {SQL_DATABASE}")
    print(f"  Recargar (force): {FORCE_RELOAD}")

    if not args.solo_carga:
        ensure_database_exists()

    conn = wait_and_connect(database=SQL_DATABASE)

    if not args.solo_carga:
        if tablas_faltantes(conn):
            apply_schema(conn)
        else:
            print(f"[esquema] Las tablas ya existen en '{SQL_DATABASE}'; "
                  f"no se reaplica el esquema.")
    verificar_tablas(conn)

    if args.solo_esquema:
        conn.close()
        print("\n[esquema] Listo (modo --solo-esquema; no se cargaron CSV).")
        return 0

    inicio = time.time()
    for config in TABLAS_CONFIG:
        cargar_csv(conn, config)
    seed_acto_medico(conn)
    resumen_tablas(conn)
    conn.close()

    print("\n" + "=" * 70)
    print(f" CARGA COMPLETADA en {time.time() - inicio:,.1f}s")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:  # noqa: BLE001 - se reporta y se sale con error
        print(f"\n[ERROR] {error}", file=sys.stderr)
        sys.exit(1)
