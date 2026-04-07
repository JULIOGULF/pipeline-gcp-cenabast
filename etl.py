# ============================================================
# ETL Pipeline - Datos CENABAST → BigQuery (sin Cloud Storage)
# Autor: Julio Carvallo
# Descripción: Pipeline que extrae datos de precios de
# medicamentos, los limpia y los carga directo en BigQuery
# ============================================================

import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# ── CONFIGURACIÓN ──────────────────────────────────────────
PROJECT_ID  = "pipeline-cenabast"
DATASET_ID  = "cenabast_data"
TABLE_ID    = "precios_remedios"

# ── PASO 1: EXTRAER DATOS ──────────────────────────────────
print("📥 Paso 1: Cargando datos de medicamentos...")

df = pd.DataFrame({
    "producto":    [
        "Paracetamol 500mg", "Ibuprofeno 400mg", "Amoxicilina 500mg",
        "Metformina 850mg",  "Atorvastatina 20mg", "Omeprazol 20mg",
        "Losartan 50mg", "Enalapril 10mg", "Aspirina 100mg",
        "Clonazepam 0.5mg", "Sertralina 50mg", "Alprazolam 0.25mg"
    ],
    "laboratorio": [
        "Lab Chile", "Lab Maver", "Lab Bestpharma",
        "Lab Saval", "Lab Mintlab", "Lab Recalcine",
        "Lab Chile", "Lab Maver", "Lab Bestpharma",
        "Lab Saval", "Lab Mintlab", "Lab Recalcine"
    ],
    "categoria": [
        "Analgésico", "Analgésico", "Antibiótico",
        "Diabetes", "Cardiovascular", "Gastrointestinal",
        "Cardiovascular", "Cardiovascular", "Analgésico",
        "Sistema Nervioso", "Sistema Nervioso", "Sistema Nervioso"
    ],
    "precio_unit": [150, 280, 420, 190, 310, 230, 195, 175, 120, 450, 380, 520],
    "unidades":    [100, 50,  30,  60,  45,  90,  30,  60,  100, 30,  30,  30],
    "region":      [
        "RM", "RM", "Valparaíso",
        "Biobío", "RM", "Maule",
        "Valparaíso", "RM", "Biobío",
        "RM", "Valparaíso", "RM"
    ],
    "anio": [2023] * 12
})

print(f"   ✅ {len(df)} productos cargados")

# ── PASO 2: TRANSFORMAR / LIMPIAR DATOS ───────────────────
print("\n🔧 Paso 2: Limpiando y transformando datos...")

# Eliminar duplicados
df.drop_duplicates(inplace=True)

# Calcular precio total por producto
df["precio_total"] = df["precio_unit"] * df["unidades"]

# Agregar fecha de carga
df["fecha_carga"] = datetime.today().strftime("%Y-%m-%d")

print(f"   ✅ Datos listos: {len(df)} filas, {len(df.columns)} columnas")
print(f"   Columnas: {list(df.columns)}")

# ── PASO 3: CARGAR A BIGQUERY ──────────────────────────────
print("\n🗄️  Paso 3: Cargando datos en BigQuery...")

bq_client = bigquery.Client(project=PROJECT_ID)

# Crear dataset si no existe
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
dataset_ref.location = "US"
try:
    bq_client.create_dataset(dataset_ref)
    print(f"   Dataset '{DATASET_ID}' creado")
except Exception:
    print(f"   Dataset '{DATASET_ID}' ya existe")

# Cargar dataframe directo a BigQuery
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # Reemplaza si ya existe
    autodetect=True,
)

job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()  # Esperar que termine

tabla = bq_client.get_table(table_ref)
print(f"   ✅ {tabla.num_rows} filas cargadas en BigQuery")

# ── PASO 4: VERIFICAR CON QUERIES SQL