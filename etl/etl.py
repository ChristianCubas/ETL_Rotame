#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
etl.py - Data Warehouse Clinica Pacifico
==============================================================================
Lee las tablas operativas que lleno 'llenado.py' y construye (en SQL Server)
las dimensiones y los hechos del Data Warehouse:

    Origen (llenado.py)                 Destino (etl.py)
    ----------------------------------  ------------------------------------
    ce_recetas                          dim_tiempo
    ce_detalle_receta                   dim_medicamento
    ce_diagnostico_pacientes            dim_lote
    ce_diagnostico                      dim_proveedor
    fa_producto                         dim_enfermedad
    movimiento_kardex                   hecho_ventas
                                        hecho_prescripciones
                                        hecho_inventario

Ventana temporal: los ultimos N anios calendario (por defecto 3), configurable
con la variable ANIOS_HISTORICO.

Cada tabla destino se trunca antes de cargarse, por lo que el proceso es
idempotente: se puede ejecutar tantas veces como haga falta.

Uso:
    python etl.py

Variables de entorno:
    SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER, SQLSERVER_PASSWORD,
    SQLSERVER_DATABASE, SQL_DRIVER, BATCH_SIZE, ANIOS_HISTORICO
==============================================================================
"""

import os
import sys
import time
import warnings
from datetime import date, datetime
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv es opcional
    pass


# ==============================================================================
# 0. CONFIGURACION
# ==============================================================================
SQL_HOST     = os.getenv("SQLSERVER_HOST", "localhost")
SQL_PORT     = os.getenv("SQLSERVER_PORT", "1433")
SQL_USER     = os.getenv("SQLSERVER_USER", "sa")
SQL_PASSWORD = os.getenv("SQLSERVER_PASSWORD", os.getenv("MSSQL_SA_PASSWORD", ""))
SQL_DATABASE = os.getenv("SQLSERVER_DATABASE", "clinica_pacifico")
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "5000"))
ANIOS_HISTORICO = int(os.getenv("ANIOS_HISTORICO", "3"))

ANIO_ACTUAL = datetime.now().year
ANIO_CORTE  = ANIO_ACTUAL - (ANIOS_HISTORICO - 1)
INICIO      = date(ANIO_CORTE, 1, 1)

# Valores que se consideran "sin informacion" en el origen
LOTES_INVALIDOS = {"", "SIN DATO", "-", ".", "SNL", "NAN", "NULL", "NONE"}
PROVEEDORES_INVALIDOS = {"", "SIN DATO", "SIN_PROV", "-", ".", "NAN", "NULL", "NONE", "0"}
NOMBRE_SIN_DATO = "SIN DATO"
NOMBRE_SIN_DESCRIPCION = "SIN DESCRIPCION"


def _driver_odbc() -> str:
    """Detecta el driver ODBC de SQL Server disponible."""
    driver = os.getenv("SQL_DRIVER")
    if driver:
        return driver
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
    raise RuntimeError("No hay ningun driver ODBC de SQL Server instalado.")


CONN_STR = (
    f"DRIVER={{{_driver_odbc()}}};"
    f"SERVER={SQL_HOST},{SQL_PORT};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD};"
    f"TrustServerCertificate=yes;"
    f"Encrypt=no;"
)

# quote_plus evita que caracteres como '#', '$' o '!' rompan la URL de conexion
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={quote_plus(CONN_STR)}",
    fast_executemany=True,
    pool_pre_ping=True,
)


# ==============================================================================
# 1. UTILIDADES
# ==============================================================================
def extraer(tabla: str, columnas: list, col_fecha: str = None) -> pd.DataFrame:
    """Lee las columnas indicadas de una tabla operativa, filtrando la ventana."""
    sql = (f"SELECT {', '.join('[' + c + ']' for c in columnas)} "
           f"FROM dbo.[{tabla}]")
    if col_fecha:
        sql += f" WHERE [{col_fecha}] >= '{INICIO.isoformat()}'"

    partes = []
    leidas = 0
    interactivo = sys.stdout.isatty()
    for trozo in pd.read_sql(text(sql), engine, chunksize=200_000):
        partes.append(trozo)
        leidas += len(trozo)
        if interactivo:
            print(f"  {tabla:<26} {leidas:>10,} filas leidas", end="\r", flush=True)
    if interactivo:
        print(" " * 60, end="\r")

    if not partes:
        print(f"  {tabla:<26} {'0':>10,} filas leidas")
        return pd.DataFrame(columns=columnas)

    df = pd.concat(partes, ignore_index=True)
    print(f"  {tabla:<26} {len(df):>10,} filas leidas")
    return df


def normalizar_codigo(serie: pd.Series) -> pd.Series:
    """
    Normaliza codigos de medicamento/proveedor: quita espacios, el sufijo '.0'
    que agrega el origen y los deja como texto. Asi '00001', '1' y '1.0'
    terminan representando el mismo codigo.
    """
    s = serie.astype("string").str.strip()
    s = s.str.replace(r"\.0+$", "", regex=True)
    numero = pd.to_numeric(s, errors="coerce")
    valido = numero.notna()
    salida = s.copy()
    salida.loc[valido] = numero.loc[valido].astype("int64").astype("string")
    return salida.fillna("").astype(str)


def limpiar_texto(serie: pd.Series, por_defecto: str = None) -> pd.Series:
    """Trim + None (o valor por defecto) para los valores vacios o 'nan'."""
    s = serie.astype("string").str.strip()
    s = s.mask(s.str.upper().isin(["", "NAN", "NULL", "NONE", "<NA>"]))
    if por_defecto is not None:
        s = s.fillna(por_defecto)
    return s.astype(object).where(s.notna(), None)


def recortar(df: pd.DataFrame, columna: str, largo: int) -> None:
    """Recorta el texto al ancho de la columna destino (evita truncation error)."""
    df[columna] = df[columna].apply(
        lambda v: v if v is None or len(str(v)) <= largo else str(v)[:largo])


def enteros_opcionales(df: pd.DataFrame, columna: str) -> None:
    """Deja la columna como enteros con None (para columnas INT anulables)."""
    valores = pd.to_numeric(df[columna], errors="coerce")
    df[columna] = [None if pd.isna(v) else int(v) for v in valores]


def cargar(tabla: str, df: pd.DataFrame) -> None:
    """Reemplaza el contenido de una tabla del DW con el DataFrame indicado."""
    if df is None or df.empty:
        print(f"  {tabla:<24} sin filas que cargar (se deja vacia)")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE dbo.[{tabla}]"))
        return

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE dbo.[{tabla}]"))

    df.to_sql(tabla, engine, schema="dbo", if_exists="append",
              index=False, chunksize=BATCH_SIZE, method=None)
    print(f"  {tabla:<24} {len(df):>10,} filas cargadas")


def contar(tabla: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM dbo.[{tabla}]")).scalar())


# ==============================================================================
# 2. EXTRACCION
# ==============================================================================
def main() -> int:
    inicio_proceso = time.time()

    print("=" * 70)
    print(" ETL CLINICA PACIFICO - CONSTRUCCION DEL DATA WAREHOUSE")
    print("=" * 70)
    print(f"  Base de datos : {SQL_DATABASE} ({SQL_HOST}:{SQL_PORT})")
    print(f"  Driver ODBC   : {_driver_odbc()}")
    print(f"  Ventana       : {INICIO.isoformat()} .. hoy "
          f"({ANIOS_HISTORICO} anios calendario)")
    print("=" * 70)

    # --------------------------------------------------------------------
    print("\n[1/5] Extrayendo tablas operativas...")
    # --------------------------------------------------------------------
    kardex = extraer("movimiento_kardex", [
        "secuencia", "codpro", "dateregistro", "tipoopera", "nLote",
        "fec_vence", "cod_prov", "id_almacen", "canentradas", "fraingreso",
        "cansalida", "fracsalida", "precio_salida", "frac_precio_salida",
        "stock_prom", "costo_promedio",
    ], col_fecha="dateregistro")

    recetas = extraer("ce_recetas", [
        "nro_receta", "nro_acto", "fec_receta",
    ], col_fecha="fec_receta")

    detalle_receta = extraer("ce_detalle_receta", [
        "nro_receta", "codpro", "cant_pedido", "pre_venta", "costo_salida",
    ])

    dx_pac = extraer("ce_diagnostico_pacientes", [
        "nro_acto", "dx_codigo", "cod_tipo_diag", "fecha",
    ], col_fecha="fecha")

    diagnostico = extraer("ce_diagnostico", [
        "dx_codigo", "dx_desc", "dx_raiz", "dx_nivel",
    ])

    producto = extraer("fa_producto", [
        "codpro", "despro", "codlab", "codgen", "codfam", "marca", "presentacion",
    ])

    if kardex.empty and recetas.empty:
        print("\n[ERROR] No hay datos operativos en la ventana temporal. "
              "Ejecuta primero 'python llenado.py'.")
        return 1

    # ==================================================================
    print("\n[2/5] Limpieza y normalizacion...")
    # ==================================================================
    # ---------- Kardex ----------
    if not kardex.empty:
        kardex["fecha"] = pd.to_datetime(kardex["dateregistro"], errors="coerce")
        kardex["fecha"] = kardex["fecha"].dt.normalize()
        kardex = kardex[kardex["fecha"].notna()].copy()

        kardex["codigo_medicamento"] = normalizar_codigo(kardex["codpro"])
        kardex["numero_lote"] = (kardex["nLote"].astype("string").str.strip()
                                 .fillna("").astype(str))
        kardex["codigo_proveedor"] = normalizar_codigo(kardex["cod_prov"])

        for columna in ("canentradas", "fraingreso", "cansalida", "fracsalida",
                        "precio_salida", "frac_precio_salida", "stock_prom",
                        "costo_promedio"):
            kardex[columna] = pd.to_numeric(kardex[columna], errors="coerce").fillna(0.0)

        kardex["id_almacen"] = (pd.to_numeric(kardex["id_almacen"], errors="coerce")
                                .fillna(-1).astype("int64"))
        kardex["tipoopera"] = pd.to_numeric(kardex["tipoopera"], errors="coerce").fillna(0)

        # Fechas de vencimiento: 1900-01-01 significa "sin dato" en el origen
        vence = pd.to_datetime(kardex["fec_vence"], errors="coerce")
        kardex["fecha_vencimiento"] = vence.where(vence >= pd.Timestamp("2000-01-01"))

        kardex["es_entrada"] = kardex["tipoopera"] == 1
        kardex["lote_valido"] = ~kardex["numero_lote"].str.upper().isin(LOTES_INVALIDOS)
        kardex["proveedor_valido"] = ~kardex["codigo_proveedor"].str.upper().isin(
            PROVEEDORES_INVALIDOS)

        print(f"  kardex: {len(kardex):,} movimientos utiles, "
              f"{kardex['numero_lote'][kardex['lote_valido']].nunique():,} lotes")

    # ---------- Recetas y detalle ----------
    if not recetas.empty:
        recetas["fecha"] = pd.to_datetime(recetas["fec_receta"], errors="coerce").dt.normalize()
        recetas = recetas[recetas["fecha"].notna()].copy()
        recetas["nro_receta"] = recetas["nro_receta"].astype("string").str.strip()
        recetas["nro_acto"] = pd.to_numeric(recetas["nro_acto"], errors="coerce").fillna(0).astype("int64")

    if not detalle_receta.empty:
        detalle_receta["nro_receta"] = detalle_receta["nro_receta"].astype("string").str.strip()
        detalle_receta["codigo_medicamento"] = normalizar_codigo(detalle_receta["codpro"])
        for columna in ("cant_pedido", "pre_venta", "costo_salida"):
            detalle_receta[columna] = pd.to_numeric(
                detalle_receta[columna], errors="coerce").fillna(0.0)
        if not recetas.empty:
            detalle_receta = detalle_receta[
                detalle_receta["nro_receta"].isin(set(recetas["nro_receta"]))].copy()
        print(f"  ce_detalle_receta: {len(detalle_receta):,} lineas en la ventana")

    # ---------- Diagnosticos de paciente ----------
    if not dx_pac.empty:
        dx_pac["fecha"] = pd.to_datetime(dx_pac["fecha"], errors="coerce").dt.normalize()
        dx_pac = dx_pac[dx_pac["fecha"].notna()].copy()
        dx_pac["nro_acto"] = pd.to_numeric(dx_pac["nro_acto"], errors="coerce").fillna(0).astype("int64")
        dx_pac["codigo_diagnostico"] = dx_pac["dx_codigo"].astype("string").str.strip()
        dx_pac["cod_tipo_diag"] = pd.to_numeric(dx_pac["cod_tipo_diag"], errors="coerce").fillna(99)
        # Un solo diagnostico (el principal) por acto medico
        dx_pac = (dx_pac.sort_values(["nro_acto", "cod_tipo_diag"])
                  .drop_duplicates(subset=["nro_acto"], keep="first"))
        dx_pac["codigo_diagnostico"] = dx_pac["codigo_diagnostico"].fillna("")

    # ---------- Catalogo de productos ----------
    if not producto.empty:
        producto["codigo_medicamento"] = normalizar_codigo(producto["codpro"])
        producto = producto[producto["codigo_medicamento"] != ""].copy()
        producto = producto.sort_values("codigo_medicamento").drop_duplicates(
            subset=["codigo_medicamento"], keep="first")

    # ==================================================================
    print("\n[3/5] Construyendo dimensiones...")
    # ==================================================================

    # ---------------- dim_tiempo ----------------
    fechas = []
    if not kardex.empty:
        fechas.append(kardex["fecha"])
    if not recetas.empty:
        fechas.append(recetas["fecha"])
    if not dx_pac.empty:
        fechas.append(dx_pac["fecha"])

    if fechas:
        unicas = pd.concat(fechas, ignore_index=True).dropna().dt.normalize().unique()
        unicas = np.sort(unicas)
        dim_tiempo = pd.DataFrame({"fecha": pd.to_datetime(unicas)})
    else:
        dim_tiempo = pd.DataFrame({"fecha": pd.to_datetime([])})

    dim_tiempo["anio"] = dim_tiempo["fecha"].dt.year.astype("int64")
    dim_tiempo["mes"] = dim_tiempo["fecha"].dt.month.astype("int64")
    dim_tiempo["dia"] = dim_tiempo["fecha"].dt.day.astype("int64")
    dim_tiempo["dia_semana"] = dim_tiempo["fecha"].dt.day_name()
    dim_tiempo["trimestre"] = dim_tiempo["fecha"].dt.quarter.astype("int64")
    dim_tiempo["nombre_mes"] = dim_tiempo["fecha"].dt.month_name()
    dim_tiempo.reset_index(drop=True, inplace=True)
    dim_tiempo["id_tiempo_sk"] = (dim_tiempo.index + 1).astype("int64")
    dim_tiempo = dim_tiempo[["id_tiempo_sk", "fecha", "anio", "mes", "dia",
                             "dia_semana", "trimestre", "nombre_mes"]]
    recortar(dim_tiempo, "dia_semana", 15)
    recortar(dim_tiempo, "nombre_mes", 15)

    mapa_tiempo = {pd.Timestamp(f).strftime("%Y-%m-%d"): sk
                   for f, sk in zip(dim_tiempo["fecha"], dim_tiempo["id_tiempo_sk"])}
    print(f"  dim_tiempo: {len(dim_tiempo):,} dias")

    # ---------------- dim_medicamento ----------------
    if not producto.empty:
        dim_medicamento = pd.DataFrame({
            "codigo_medicamento": producto["codigo_medicamento"].values,
            "nombre_medicamento": limpiar_texto(producto["despro"], NOMBRE_SIN_DESCRIPCION).values,
            "codlab": limpiar_texto(producto["codlab"]).values,
            "codgen": limpiar_texto(producto["codgen"]).values,
            "codfam": limpiar_texto(producto["codfam"]).values,
            "marca": pd.to_numeric(producto["marca"], errors="coerce").values,
            "presentacion": limpiar_texto(producto["presentacion"]).values,
        })
    else:
        dim_medicamento = pd.DataFrame(columns=[
            "codigo_medicamento", "nombre_medicamento", "codlab", "codgen",
            "codfam", "marca", "presentacion"])

    # Codigos que aparecen en los hechos pero no en el catalogo: se agregan
    # para que ninguna fila de hecho quede sin id_medicamento_sk.
    codigos_hechos = set()
    if not kardex.empty:
        codigos_hechos |= set(kardex["codigo_medicamento"])
    if not detalle_receta.empty:
        codigos_hechos |= set(detalle_receta["codigo_medicamento"])
    codigos_hechos.discard("")
    faltantes = codigos_hechos - set(dim_medicamento["codigo_medicamento"])
    if faltantes:
        with warnings.catch_warnings():
            # Las columnas nuevas van vacias (None): el aviso de pandas sobre
            # concatenar columnas all-NA no aplica aqui.
            warnings.simplefilter("ignore", FutureWarning)
            dim_medicamento = pd.concat([
                dim_medicamento,
                pd.DataFrame({
                    "codigo_medicamento": sorted(faltantes),
                    "nombre_medicamento": NOMBRE_SIN_DESCRIPCION,
                    "codlab": None, "codgen": None, "codfam": None,
                    "marca": None, "presentacion": None,
                }),
            ], ignore_index=True)
        print(f"  dim_medicamento: {len(faltantes):,} codigos agregados sin catalogo")

    dim_medicamento = dim_medicamento.sort_values(
        "codigo_medicamento").reset_index(drop=True)
    dim_medicamento["id_medicamento_sk"] = (dim_medicamento.index + 1).astype("int64")
    # 'marca' es INT anulable en el DW
    dim_medicamento["marca"] = [
        None if pd.isna(v) else int(v) for v in dim_medicamento["marca"]]
    for columna, largo in (("codigo_medicamento", 20), ("nombre_medicamento", 200),
                           ("codlab", 20), ("codgen", 20), ("codfam", 20),
                           ("presentacion", 40)):
        recortar(dim_medicamento, columna, largo)
    dim_medicamento = dim_medicamento[["id_medicamento_sk", "codigo_medicamento",
                                       "nombre_medicamento", "codlab", "codgen",
                                       "codfam", "marca", "presentacion"]]
    mapa_medicamento = dict(zip(dim_medicamento["codigo_medicamento"],
                                dim_medicamento["id_medicamento_sk"]))
    print(f"  dim_medicamento: {len(dim_medicamento):,} medicamentos")

    # ---------------- dim_proveedor ----------------
    if not kardex.empty:
        codigos_prov = sorted(set(kardex.loc[kardex["proveedor_valido"],
                                             "codigo_proveedor"]))
    else:
        codigos_prov = []

    dim_proveedor = pd.DataFrame({
        "codigo_proveedor": codigos_prov,
        "nombre_proveedor": [f"PROVEEDOR_{c}" for c in codigos_prov],
    })
    dim_proveedor["id_proveedor_sk"] = (dim_proveedor.index + 1).astype("int64")
    recortar(dim_proveedor, "codigo_proveedor", 20)
    recortar(dim_proveedor, "nombre_proveedor", 80)
    dim_proveedor = dim_proveedor[["id_proveedor_sk", "codigo_proveedor", "nombre_proveedor"]]
    mapa_proveedor = dict(zip(dim_proveedor["codigo_proveedor"],
                              dim_proveedor["id_proveedor_sk"]))
    print(f"  dim_proveedor: {len(dim_proveedor):,} proveedores")

    # ---------------- dim_lote ----------------
    if not kardex.empty:
        lotes = kardex[kardex["lote_valido"]].copy()

        base_lote = lotes.groupby("numero_lote", as_index=False).agg(
            fecha_vencimiento=("fecha_vencimiento", "max"),
            fecha_adquisicion=("fecha", "min"),
        )

        # Medicamento del lote: el del ultimo movimiento del lote
        medicamento_lote = (lotes.sort_values("fecha")
                            .groupby("numero_lote")["codigo_medicamento"].last())

        # Proveedor del lote: el de la ultima entrada con proveedor valido
        entradas = lotes[lotes["es_entrada"] & lotes["proveedor_valido"]]
        proveedor_lote = (entradas.sort_values("fecha")
                          .groupby("numero_lote")["codigo_proveedor"].last())

        dim_lote = base_lote
        dim_lote["codigo_medicamento"] = dim_lote["numero_lote"].map(medicamento_lote)
        dim_lote["codigo_proveedor"] = dim_lote["numero_lote"].map(proveedor_lote)
        dim_lote["id_medicamento_sk"] = dim_lote["codigo_medicamento"].map(mapa_medicamento)
        dim_lote["id_proveedor_sk"] = dim_lote["codigo_proveedor"].map(mapa_proveedor)
        dim_lote["proveedor"] = dim_lote["codigo_proveedor"].map(
            dict(zip(dim_proveedor["codigo_proveedor"],
                     dim_proveedor["nombre_proveedor"]))).fillna(NOMBRE_SIN_DATO)
        # Regla de canje: el dia 10 del mes de adquisicion
        dim_lote["fecha_canje"] = dim_lote["fecha_adquisicion"].apply(
            lambda f: date(f.year, f.month, 10) if pd.notna(f) else None)

        dim_lote = dim_lote.sort_values("numero_lote").reset_index(drop=True)
    else:
        dim_lote = pd.DataFrame(columns=[
            "numero_lote", "fecha_vencimiento", "fecha_adquisicion", "id_medicamento_sk",
            "id_proveedor_sk", "proveedor", "fecha_canje"])

    dim_lote["id_lote_sk"] = (dim_lote.index + 1).astype("int64")
    enteros_opcionales(dim_lote, "id_medicamento_sk")
    enteros_opcionales(dim_lote, "id_proveedor_sk")
    recortar(dim_lote, "numero_lote", 25)
    recortar(dim_lote, "proveedor", 80)
    dim_lote = dim_lote[["id_lote_sk", "numero_lote", "fecha_vencimiento",
                         "id_medicamento_sk", "id_proveedor_sk", "proveedor",
                         "fecha_adquisicion", "fecha_canje"]]
    mapa_lote = dict(zip(dim_lote["numero_lote"], dim_lote["id_lote_sk"]))
    mapa_prov_lote = dict(zip(dim_lote["numero_lote"], dim_lote["proveedor"]))
    print(f"  dim_lote: {len(dim_lote):,} lotes")

    # ---------------- dim_enfermedad ----------------
    if not diagnostico.empty:
        dim_enfermedad = pd.DataFrame({
            "codigo_diagnostico": diagnostico["dx_codigo"].astype("string").str.strip().values,
            "descripcion_diagnostico": limpiar_texto(
                diagnostico["dx_desc"], NOMBRE_SIN_DATO).values,
            "dx_raiz": pd.to_numeric(diagnostico["dx_raiz"], errors="coerce").values,
            "dx_nivel": pd.to_numeric(diagnostico["dx_nivel"], errors="coerce").values,
        })
        dim_enfermedad = (dim_enfermedad[dim_enfermedad["codigo_diagnostico"].notna()]
                          .drop_duplicates(subset=["codigo_diagnostico"], keep="first")
                          .sort_values("codigo_diagnostico").reset_index(drop=True))
    else:
        dim_enfermedad = pd.DataFrame(columns=[
            "codigo_diagnostico", "descripcion_diagnostico", "dx_raiz", "dx_nivel"])

    dim_enfermedad["id_enfermedad_sk"] = (dim_enfermedad.index + 1).astype("int64")
    for columna in ("dx_raiz", "dx_nivel"):
        dim_enfermedad[columna] = [
            None if pd.isna(v) else int(v) for v in dim_enfermedad[columna]]
    recortar(dim_enfermedad, "codigo_diagnostico", 10)
    recortar(dim_enfermedad, "descripcion_diagnostico", 260)
    dim_enfermedad = dim_enfermedad[["id_enfermedad_sk", "codigo_diagnostico",
                                     "descripcion_diagnostico", "dx_raiz", "dx_nivel"]]
    mapa_enfermedad = dict(zip(dim_enfermedad["codigo_diagnostico"],
                               dim_enfermedad["id_enfermedad_sk"]))
    print(f"  dim_enfermedad: {len(dim_enfermedad):,} diagnosticos")

    # ==================================================================
    print("\n[4/5] Construyendo hechos...")
    # ==================================================================

    # ---------------- hecho_ventas ----------------
    if not kardex.empty:
        k = kardex.copy()
        k["unidades_entrada"] = np.where(
            k["es_entrada"], k["canentradas"] + k["fraingreso"], 0.0)
        k["unidades_salida"] = np.where(
            ~k["es_entrada"], k["cansalida"] + k["fracsalida"], 0.0)
        k["monto_venta"] = np.where(
            ~k["es_entrada"],
            k["cansalida"] * k["precio_salida"] + k["fracsalida"] * k["frac_precio_salida"],
            0.0)

        ventas = k.groupby(["fecha", "codigo_medicamento", "numero_lote"],
                           as_index=False).agg(
            cantidad_entradas_dia=("unidades_entrada", "sum"),
            cantidad_salidas_dia=("unidades_salida", "sum"),
            total_venta_dia=("monto_venta", "sum"),
        )

        ventas["id_tiempo_sk"] = ventas["fecha"].dt.strftime("%Y-%m-%d").map(mapa_tiempo)
        ventas = ventas[ventas["id_tiempo_sk"].notna()].copy()
        ventas["id_tiempo_sk"] = ventas["id_tiempo_sk"].astype("int64")

        ventas["id_medicamento_sk"] = ventas["codigo_medicamento"].map(mapa_medicamento)
        enteros_opcionales(ventas, "id_medicamento_sk")

        ventas["id_lote_sk"] = ventas["numero_lote"].map(mapa_lote)
        enteros_opcionales(ventas, "id_lote_sk")

        ventas["lote"] = ventas["numero_lote"].map(
            lambda v: v if v and v.upper() not in LOTES_INVALIDOS else NOMBRE_SIN_DATO)
        ventas["proveedor"] = ventas["numero_lote"].map(mapa_prov_lote).fillna(NOMBRE_SIN_DATO)
        ventas["id_proveedor_sk"] = ventas["numero_lote"].map(
            dict(zip(dim_lote["numero_lote"], dim_lote["id_proveedor_sk"])))
        enteros_opcionales(ventas, "id_proveedor_sk")

        hecho_ventas = pd.DataFrame({
            "id_tiempo_sk": ventas["id_tiempo_sk"],
            "id_medicamento_sk": ventas["id_medicamento_sk"],
            "id_lote_sk": ventas["id_lote_sk"],
            "id_proveedor_sk": ventas["id_proveedor_sk"],
            "fecha_venta": ventas["fecha"],
            "lote": ventas["lote"],
            "proveedor": ventas["proveedor"],
            "codigo_medicamento": ventas["codigo_medicamento"],
            "cantidad_entradas_dia": ventas["cantidad_entradas_dia"],
            "cantidad_salidas_dia": ventas["cantidad_salidas_dia"],
            "total_venta_dia": ventas["total_venta_dia"],
        })
        recortar(hecho_ventas, "lote", 25)
        recortar(hecho_ventas, "proveedor", 80)
        recortar(hecho_ventas, "codigo_medicamento", 20)
        print(f"  hecho_ventas: {len(hecho_ventas):,} filas")
    else:
        hecho_ventas = pd.DataFrame()

    # ---------------- hecho_prescripciones ----------------
    if not detalle_receta.empty and not recetas.empty:
        presc = detalle_receta.merge(
            recetas[["nro_receta", "nro_acto", "fecha"]], on="nro_receta", how="inner")
        presc = presc.merge(
            dx_pac[["nro_acto", "codigo_diagnostico"]], on="nro_acto", how="left")
        # Las recetas sin diagnostico asociado se conservan (sin id_enfermedad_sk)
        presc["codigo_diagnostico"] = presc["codigo_diagnostico"].fillna("")

        presc = presc.groupby(["fecha", "codigo_medicamento", "codigo_diagnostico"],
                              as_index=False).agg(
            cant_pedido=("cant_pedido", "sum"),
            pre_venta=("pre_venta", "sum"),
            costo_salida=("costo_salida", "sum"),
        )

        presc["id_tiempo_sk"] = presc["fecha"].dt.strftime("%Y-%m-%d").map(mapa_tiempo)
        presc = presc[presc["id_tiempo_sk"].notna()].copy()
        presc["id_tiempo_sk"] = presc["id_tiempo_sk"].astype("int64")

        presc["id_medicamento_sk"] = presc["codigo_medicamento"].map(mapa_medicamento)
        enteros_opcionales(presc, "id_medicamento_sk")
        presc["id_enfermedad_sk"] = presc["codigo_diagnostico"].map(mapa_enfermedad)
        enteros_opcionales(presc, "id_enfermedad_sk")

        hecho_prescripciones = pd.DataFrame({
            "id_tiempo_sk": presc["id_tiempo_sk"],
            "id_medicamento_sk": presc["id_medicamento_sk"],
            "id_enfermedad_sk": presc["id_enfermedad_sk"],
            "cant_pedido": presc["cant_pedido"].round().astype("int64"),
            "pre_venta": presc["pre_venta"].astype(float),
            "costo_salida": presc["costo_salida"].astype(float),
        })
        print(f"  hecho_prescripciones: {len(hecho_prescripciones):,} filas")
    else:
        hecho_prescripciones = pd.DataFrame()

    # ---------------- hecho_inventario ----------------
    if not kardex.empty:
        inv = kardex.groupby(
            ["fecha", "codigo_medicamento", "numero_lote", "codigo_proveedor", "id_almacen"],
            as_index=False).agg(
            canentradas=("canentradas", "sum"),
            cansalida=("cansalida", "sum"),
            stock_prom=("stock_prom", "mean"),
            costo_promedio=("costo_promedio", "mean"),
        )

        inv["id_tiempo_sk"] = inv["fecha"].dt.strftime("%Y-%m-%d").map(mapa_tiempo)
        inv = inv[inv["id_tiempo_sk"].notna()].copy()
        inv["id_tiempo_sk"] = inv["id_tiempo_sk"].astype("int64")

        inv["id_medicamento_sk"] = inv["codigo_medicamento"].map(mapa_medicamento)
        enteros_opcionales(inv, "id_medicamento_sk")
        inv["id_lote_sk"] = inv["numero_lote"].map(mapa_lote)
        enteros_opcionales(inv, "id_lote_sk")
        inv["id_proveedor_sk"] = inv["codigo_proveedor"].map(mapa_proveedor)
        enteros_opcionales(inv, "id_proveedor_sk")

        inv["lote"] = inv["numero_lote"].map(
            lambda v: v if v and v.upper() not in LOTES_INVALIDOS else NOMBRE_SIN_DATO)
        inv["proveedor"] = inv["codigo_proveedor"].map(
            dict(zip(dim_proveedor["codigo_proveedor"],
                     dim_proveedor["nombre_proveedor"]))).fillna(NOMBRE_SIN_DATO)
        inv["id_almacen"] = [None if v < 0 else int(v) for v in inv["id_almacen"]]

        hecho_inventario = pd.DataFrame({
            "id_tiempo_sk": inv["id_tiempo_sk"],
            "id_medicamento_sk": inv["id_medicamento_sk"],
            "id_lote_sk": inv["id_lote_sk"],
            "id_proveedor_sk": inv["id_proveedor_sk"],
            "id_almacen": inv["id_almacen"],
            "lote": inv["lote"],
            "proveedor": inv["proveedor"],
            "canentradas": inv["canentradas"].astype(float),
            "cansalida": inv["cansalida"].astype(float),
            "stock_prom": inv["stock_prom"].astype(float),
            "costo_promedio": inv["costo_promedio"].astype(float),
        })
        recortar(hecho_inventario, "lote", 25)
        recortar(hecho_inventario, "proveedor", 80)
        print(f"  hecho_inventario: {len(hecho_inventario):,} filas")
    else:
        hecho_inventario = pd.DataFrame()

    # ==================================================================
    print("\n[5/5] Cargando dimensiones y hechos en SQL Server...")
    # ==================================================================
    # Primero dimensiones (los hechos referencian sus claves subrogadas)
    cargar("dim_tiempo", dim_tiempo)
    cargar("dim_medicamento", dim_medicamento)
    cargar("dim_proveedor", dim_proveedor)
    cargar("dim_lote", dim_lote)
    cargar("dim_enfermedad", dim_enfermedad)
    cargar("hecho_ventas", hecho_ventas)
    cargar("hecho_prescripciones", hecho_prescripciones)
    cargar("hecho_inventario", hecho_inventario)

    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(" RESUMEN DEL DATA WAREHOUSE")
    print("=" * 70)
    for tabla in ("dim_tiempo", "dim_medicamento", "dim_lote", "dim_proveedor",
                  "dim_enfermedad", "hecho_ventas", "hecho_prescripciones",
                  "hecho_inventario"):
        print(f"  {tabla:<24} {contar(tabla):>12,} filas")

    print("\n" + "=" * 70)
    print(f" ETL COMPLETADO en {time.time() - inicio_proceso:,.1f}s")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:  # noqa: BLE001 - se reporta y se sale con error
        print(f"\n[ERROR] {error}", file=sys.stderr)
        sys.exit(1)
