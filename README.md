# 🏥 ETL Pipeline — CENABAST → BigQuery

Pipeline de extracción, transformación y carga de datos de precios de medicamentos hacia Google BigQuery. Diseñado para automatizar el procesamiento de datos del sistema de salud chileno con trazabilidad completa y escalabilidad en la nube.

## 🛠 Stack Tecnológico

- **Python 3.10+**
- **Pandas** — transformación y limpieza de datos
- **Google Cloud BigQuery** — data warehouse en la nube
- **google-cloud-bigquery** — cliente oficial de Google para Python

## 📁 Estructura del Proyecto

```
etl-cenabast-bigquery/
├── etl_cenabast_bigquery.py   # Script principal del pipeline
├── requirements.txt           # Dependencias
├── credentials/
│   └── service_account.json   # Credenciales Google Cloud (no subir a Git)
└── .gitignore
```

## 🚀 Cómo ejecutar

```bash
# 1. Clonar el repositorio
git clone https://github.com/tu-usuario/etl-cenabast-bigquery.git
cd etl-cenabast-bigquery

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar credenciales de Google Cloud
export GOOGLE_APPLICATION_CREDENTIALS="credentials/service_account.json"

# 4. Ejecutar el pipeline
python etl_cenabast_bigquery.py
```

## ⚙️ Configuración

En el bloque `CONFIG` del script define tu proyecto y destino en BigQuery:

```python
PROJECT_ID  = "pipeline-cenabast"   # ID de tu proyecto en Google Cloud
DATASET_ID  = "cenabast_data"       # Dataset de destino
TABLE_ID    = "precios_remedios"    # Tabla de destino
```

## 📊 Funcionalidades

| Etapa | Descripción |
|---|---|
| **Extract** | Carga datos de medicamentos (CSV, API o hardcoded para prototipo) |
| **Transform** | Elimina duplicados, calcula precio total, agrega fecha de carga |
| **Load** | Carga el DataFrame directo a BigQuery con `WRITE_TRUNCATE` |
| **Verificación** | Query SQL post-carga para validar integridad de datos |

## 🗄️ Esquema de la tabla en BigQuery

| Columna | Tipo | Descripción |
|---|---|---|
| `producto` | STRING | Nombre del medicamento |
| `laboratorio` | STRING | Laboratorio fabricante |
| `categoria` | STRING | Categoría terapéutica |
| `precio_unit` | INTEGER | Precio unitario en CLP |
| `unidades` | INTEGER | Unidades disponibles |
| `precio_total` | INTEGER | precio_unit × unidades |
| `region` | STRING | Región de Chile |
| `anio` | INTEGER | Año del registro |
| `fecha_carga` | DATE | Fecha de ejecución del pipeline |

## 🌎 Región de almacenamiento

Los datos se almacenan en **`southamerica-west1` (Santiago, Chile)**, cumpliendo con regulaciones de datos del sistema de salud chileno y minimizando latencia.

## 📈 Decisiones técnicas

**`WRITE_TRUNCATE`** — El pipeline reemplaza la tabla completa en cada ejecución. Esto garantiza que los precios siempre estén actualizados y que el pipeline sea **idempotente** (correrlo dos veces produce el mismo resultado, sin duplicados).

**`autodetect=True`** — BigQuery infiere automáticamente el tipo de cada columna desde el DataFrame, eliminando la necesidad de definir el esquema manualmente.

**Dataset con `try/except`** — El pipeline intenta crear el dataset y si ya existe continúa sin interrumpirse. Permite ejecutarlo en cualquier entorno sin configuración previa.

## 🔐 Seguridad

```gitignore
# .gitignore — nunca subas tus credenciales
credentials/
*.json
.env
```

Las credenciales de Google Cloud **nunca deben subirse a Git**. Usa variables de entorno o Secret Manager en producción.

## 📦 requirements.txt

```
pandas>=2.0.0
google-cloud-bigquery>=3.0.0
google-cloud-bigquery-storage>=2.0.0
pyarrow>=12.0.0
db-dtypes>=1.0.0
```

## 🔮 Próximas mejoras

- [ ] Conexión a API oficial de CENABAST
- [ ] Orquestación con Apache Airflow o Cloud Scheduler
- [ ] Tests unitarios por etapa
- [ ] Alertas por email en caso de error
- [ ] Dashboard en Looker Studio conectado a BigQuery

---

**Autor:** Julio Carvallo | www.linkedin.com/in/julio-carvalo
