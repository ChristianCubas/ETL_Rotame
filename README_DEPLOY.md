# ETL Clínica Pacífico — Despliegue con Docker

Data Warehouse de farmacia sobre **SQL Server 2022** (contenedor) + **Python 3.12**
(contenedor). El pipeline completo son tres pasos:

| Paso | Quién lo hace | Qué hace |
|------|---------------|----------|
| 1 | `llenado.py --solo-esquema` | Crea la base de datos y aplica `estructura/esquema_tablas_farma.sql` (tablas operativas, dimensiones y hechos). |
| 2 | `llenado.py --solo-carga` | Carga cada CSV de `datos_clinica/` en su tabla operativa. |
| 3 | `etl.py` | Lee esas tablas y llena las dimensiones y los hechos del Data Warehouse. |

El contenedor `python_etl` ejecuta los tres pasos en orden y termina.

---

## 1. Requisitos

- Docker Engine / Docker Desktop con **Docker Compose v2** (`docker compose ...`).
- ~6 GB de espacio libre (imagen de SQL Server + volumen de datos).
- Los CSV dentro de `datos_clinica/`.

Verificar:

```bash
docker --version
docker compose version
```

---

## 2. Estructura del proyecto

```
ETL_Deploy/
├── docker-compose.yml                 # SQL Server + contenedor del ETL
├── .env                               # Clave de 'sa' y parámetros del pipeline
├── datos_clinica/                     # CSVs de entrada (se montan como volumen)
│   ├── ce_recetas.csv
│   ├── ce_detalle_receta.csv
│   ├── ce_dx.csv
│   ├── ce_dx_pac.csv
│   ├── fa_producto.csv
│   ├── fa_documentos_farma.csv
│   ├── fa_detalle_documento.csv
│   └── movimiento_kardex.csv
├── estructura/
│   └── esquema_tablas_farma.sql       # Esquema completo (no se modifica)
└── etl/
    ├── Dockerfile
    ├── entrypoint.sh                  # Orquesta los 3 pasos
    ├── requirements.txt
    ├── llenado.py                     # Esquema + carga de CSV
    └── etl.py                         # Dimensiones y hechos
```

---

## 3. Configuración (`.env`)

```ini
MSSQL_SA_PASSWORD=CambiaEstaClave123!   # clave del usuario sa
SQLSERVER_USER=sa
SQLSERVER_DATABASE=clinica_pacifico
SQL_DRIVER=ODBC Driver 18 for SQL Server
BATCH_SIZE=5000
FORCE_RELOAD=0                          # 1 = borra y recarga las tablas operativas
ANIOS_HISTORICO=3                       # años calendario a procesar
```

> Cambia `MSSQL_SA_PASSWORD` antes de desplegar en un servidor real.
> Si la cambias **después** de crear el volumen, hay que recrearlo (ver flujo limpio),
> porque SQL Server solo aplica esa clave al inicializar el volumen.

Puertos:

- `localhost:1435` → SQL Server del contenedor (1433 se deja libre por si hay un
  SQL Server instalado en el host).
- Dentro de la red de Docker el ETL se conecta a `sqlserver:1433`.

---

## 4. Flujo completo desde cero (prueba limpia)

```bash
# 1) Borrar contenedores, red y el volumen de datos de SQL Server
docker compose down -v

# 2) Construir la imagen del ETL
docker compose build --no-cache

# 3) Levantar SQL Server y esperar a que esté healthy
docker compose up -d sqlserver
docker compose ps

# 4) Ejecutar el pipeline completo (esquema -> carga CSV -> dimensiones y hechos)
docker compose run --rm etl
```

El paso 4 muestra en pantalla los tres pasos, el conteo de filas por tabla y
termina con:

```
 ETL COMPLETADO en ...s
```

> `docker compose run --rm etl` es la forma recomendada: ejecuta el pipeline en
> primer plano y borra el contenedor al terminar. Si prefieres levantarlo como
> servicio: `docker compose up etl` y luego `docker compose logs -f etl`.

Cuando SQL Server ya está healthy el pipeline tarda **~15-20 minutos**: la carga
de los CSV (unos 3.2 millones de filas, incluido el kardex con 1.74 M) es la
parte más lenta; el ETL de dimensiones y hechos tarda ~3 minutos.

---

## 5. Ejecutar los pasos por separado

Cada paso se puede repetir sin rehacer los demás:

```bash
# Paso 1: solo base de datos + esquema
docker compose run --rm --entrypoint python etl llenado.py --solo-esquema

# Paso 2: solo carga de los CSV (omite las tablas que ya tienen datos)
docker compose run --rm --entrypoint python etl llenado.py --solo-carga

# Paso 3: solo dimensiones y hechos (lee las tablas operativas ya cargadas)
docker compose run --rm --entrypoint python etl etl.py
```

`--entrypoint python` evita que el `entrypoint.sh` vuelva a lanzar los tres pasos.

---

## 6. Recargar los CSV desde cero

Borra el contenido de las tablas operativas y las vuelve a llenar. `etl.py`
trunca las tablas del DW en cada ejecución, así que no quedan datos duplicados.

```bash
# Opción A (Linux/macOS/Git Bash): variable en la misma línea
FORCE_RELOAD=1 docker compose run --rm etl

# Opción B (cualquier sistema): poner FORCE_RELOAD=1 en .env y ejecutar
docker compose run --rm etl
```

Para volver a `FORCE_RELOAD=0` después de la recarga.

También acepta el flag directamente:

```bash
docker compose run --rm --entrypoint python etl llenado.py --solo-carga --force
```

---

## 7. Verificar que todo quedó bien

### 7.1 Conteo de tablas operativas (origen)

```bash
docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "CambiaEstaClave123!" -C -d clinica_pacifico \
  -Q "SET NOCOUNT ON;
      SELECT t.name AS tabla, SUM(p.rows) AS filas
      FROM sys.tables t
      JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
      WHERE t.name IN ('ce_recetas','ce_detalle_receta','ce_diagnostico',
                       'ce_diagnostico_pacientes','fa_producto','fa_documentos_farma',
                       'fa_detalle_documento','pa_acto_medico','movimiento_kardex')
      GROUP BY t.name ORDER BY t.name;"
```

### 7.2 Conteo del Data Warehouse (dimensiones y hechos)

```bash
docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "CambiaEstaClave123!" -C -d clinica_pacifico \
  -Q "SET NOCOUNT ON;
      SELECT 'dim_tiempo' AS tabla, COUNT(*) AS filas FROM dbo.dim_tiempo
      UNION ALL SELECT 'dim_medicamento', COUNT(*) FROM dbo.dim_medicamento
      UNION ALL SELECT 'dim_proveedor',   COUNT(*) FROM dbo.dim_proveedor
      UNION ALL SELECT 'dim_lote',        COUNT(*) FROM dbo.dim_lote
      UNION ALL SELECT 'dim_enfermedad',  COUNT(*) FROM dbo.dim_enfermedad
      UNION ALL SELECT 'hecho_ventas',    COUNT(*) FROM dbo.hecho_ventas
      UNION ALL SELECT 'hecho_prescripciones', COUNT(*) FROM dbo.hecho_prescripciones
      UNION ALL SELECT 'hecho_inventario',     COUNT(*) FROM dbo.hecho_inventario;"
```

### 7.3 Consulta de control (ventas por mes)

```bash
docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "CambiaEstaClave123!" -C -d clinica_pacifico \
  -Q "SET NOCOUNT ON;
      SELECT dt.anio, dt.mes,
             SUM(hv.cantidad_salidas_dia) AS unidades_vendidas,
             SUM(hv.total_venta_dia)      AS importe_venta
      FROM dbo.hecho_ventas hv
      JOIN dbo.dim_tiempo dt ON dt.id_tiempo_sk = hv.id_tiempo_sk
      GROUP BY dt.anio, dt.mes
      ORDER BY dt.anio, dt.mes;"
```

---

## 8. Conectarse con SSMS / Azure Data Studio

| Campo | Valor |
|-------|-------|
| Servidor | `localhost,1435` |
| Autenticación | SQL Server Authentication |
| Usuario | `sa` |
| Contraseña | la de `MSSQL_SA_PASSWORD` en `.env` |
| Base de datos | `clinica_pacifico` |

---

## 9. Ejecutar los scripts fuera de Docker (opcional)

Requiere Python 3.12, `pip install -r etl/requirements.txt` y un driver ODBC de
SQL Server instalado.

```bash
# Linux/macOS
export SQLSERVER_HOST=localhost SQLSERVER_PORT=1435 SQLSERVER_USER=sa
export SQLSERVER_PASSWORD='CambiaEstaClave123!' SQLSERVER_DATABASE=clinica_pacifico
python etl/llenado.py
python etl/etl.py
```

---

## 10. Notas de implementación

- **El esquema no se modificó.** Se aplica tal cual desde
  `estructura/esquema_tablas_farma.sql`. `llenado.py` solo neutraliza las
  instrucciones que son de `sqlcmd` (`:setvar`, `USE [$(DB_NAME)]` y el bloque
  `CREATE DATABASE`) para poder ejecutarlo por ODBC, y siempre apunta a
  `SQLSERVER_DATABASE`.
- **Errores esperados del esquema.** Algunas restricciones `FOREIGN KEY` apuntan a
  tablas que no vienen en el script (`web_usuarios`, `pa_doctores`, `pa_pacientes`,
  `fa_tipos`, …). No se pueden crear y el script las reporta, pero **no afectan**
  al pipeline: las 19 tablas necesarias se crean y se verifican al final del paso 1.
- **Identificadores.** `fa_documentos_farma.id` y `fa_detalle_documento.nro_registro`
  vienen repetidos en los CSV (no son claves válidas), así que SQL Server los genera.
  En `fa_documentos_farma`, la columna `nro_doc` del CSV trae el código completo del
  documento (`BF…`/`FF…`, único por documento) y se carga en `nro_documento`; la
  columna `nro_doc` de la tabla (`char(2)`) se llena con el prefijo del tipo de
  documento.
- **`pa_acto_medico`** no viene en los CSV. `llenado.py` la siembra con los actos
  médicos referenciados por recetas y diagnósticos para no dejar claves huérfanas.
- **Ventana temporal.** `etl.py` procesa los últimos `ANIOS_HISTORICO` años
  calendario (por defecto 3), igual que el diseño original.
- **Filas inválidas.** Si una fila no se puede insertar (dato fuera de rango o
  incompatible), el lote se subdivide hasta aislarla: la fila se descarta y se
  informa el total descartado, en lugar de abortar la carga completa.
- **Fuentes de los hechos.** `hecho_ventas` y `hecho_inventario` salen de
  `movimiento_kardex`; `hecho_prescripciones` de `ce_recetas` + `ce_detalle_receta`
  + `ce_diagnostico_pacientes`. `fa_documentos_farma` y `fa_detalle_documento` se
  cargan (forman parte del esquema) pero no alimentan hechos: sus importes vienen
  desalineados en el CSV exportado y el kardex ya contiene esos movimientos.
- **Idempotencia.** `etl.py` trunca `dim_*` y `hecho_*` antes de cargar, así que se
  puede ejecutar tantas veces como haga falta.

---

## 11. Problemas frecuentes

| Síntoma | Causa / solución |
|---------|------------------|
| `Define MSSQL_SA_PASSWORD en el archivo .env` | Falta `.env` o la variable está vacía. |
| `No se pudo conectar a SQL Server tras 30 intentos` | SQL Server sigue arrancando o no está healthy: `docker compose ps` y `docker compose logs sqlserver`. |
| Login fallido con `sa` | La contraseña del volumen es la de la primera inicialización. Recrear: `docker compose down -v`. |
| `puerto 1435 en uso` | Cambiar el mapeo a `1436:1433` en `docker-compose.yml` y ajustar `SQLSERVER_PORT` al ejecutar desde el host. |
| Reejecutar `docker compose run --rm etl` no recarga nada | Es lo esperado: las tablas ya tienen datos. Usar `FORCE_RELOAD=1`. |
