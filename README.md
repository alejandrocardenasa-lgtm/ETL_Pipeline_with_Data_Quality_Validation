```md
# ETL Pipeline con Validación de Calidad de Datos

## Descripción del Proyecto
Este proyecto implementa un pipeline ETL (Extract, Transform, Load) con validación de calidad de datos utilizando Great Expectations.

El pipeline incluye profiling de datos, validación de datos de entrada, limpieza, transformación, carga a base de datos y validación de datos de salida. Además, se generan reportes de calidad de datos y documentación automática de validaciones.

El flujo del pipeline es el siguiente:

Extract → Profiling → Validate Input → Clean → Transform → Validate Output → Load

---

## Estructura del Proyecto
El proyecto está organizado de la siguiente manera:

```

Data/
raw/
processed/

gx/                         # Configuración de Great Expectations
reports/                    # Reportes PDF de calidad de datos
src/
extract.py
clean.py
transform.py
load.py
validate_input.py
validate_output.py
main.py

requirements.txt
README.md

````

---

## Requisitos del Sistema
Este proyecto fue desarrollado y probado con las siguientes versiones:

- Python 3.11
- Great Expectations 0.18.21
- pandas
- numpy
- matplotlib
- SQLAlchemy

Se recomienda usar estas mismas versiones para evitar problemas de compatibilidad.

---

## Cómo Ejecutar el Proyecto

### 1. Clonar el repositorio
```bash
git clone https://github.com/alejandrocardenasa-lgtm/ETL_Pipeline_with_Data_Quality_Validation.git

cd Great_expectations_ETL
````
--- 

### 2. Crear el entorno virtual

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Si python3.11 no funciona:

```bash
python3 -m venv .venv
source .venv/bin/activate
python --version
```

Verificar que la versión sea Python 3.11.

### 3. Instalar dependencias

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Ejecutar el pipeline ETL

```bash
python src/main.py
```

Esto ejecutará:

* Extract
* Profiling
* Validación de datos de entrada
* Limpieza
* Transformación
* Carga a base de datos
* Validación de datos de salida
* Generación de reportes

### 5. Construir la documentación de Great Expectations

```bash
great_expectations docs build
```

Luego abrir:

```
gx/uncommitted/data_docs/local_site/index.html
```

Ahí se pueden ver los resultados de las validaciones.

---

## Salidas Esperadas del Proyecto

Después de ejecutar el pipeline, se generan:

| Archivo                               | Descripción                        |
| ------------------------------------- | ---------------------------------- |
| Data/processed/retail_transformed.csv | Dataset transformado               |
| reports/failure_rate_summary.pdf      | Tabla de tasa de fallos            |
| reports/dq_scores.pdf                 | Puntajes de calidad de datos       |
| gx/uncommitted/data_docs/             | Documentación HTML de validaciones |
| dq_raw.txt                            | Puntaje de calidad de datos crudos |

---

## Dimensiones de Calidad de Datos Validadas

El proyecto valida las siguientes dimensiones de Data Quality:

| Dimensión    | Validación                       |
| ------------ | -------------------------------- |
| Completeness | Valores no nulos                 |
| Uniqueness   | invoice_id + product únicos      |
| Validity     | Rangos de quantity y price       |
| Consistency  | Valores válidos de country       |
| Timeliness   | Fechas dentro de 2023            |
| Accuracy     | total_revenue = quantity × price |

---

## Tabla Failure Rate

El proyecto genera una tabla de Failure Rate, que muestra el porcentaje de filas que fallan cada regla de calidad de datos.

Esta tabla se genera después de la validación de los datos de entrada para medir la calidad del dataset antes del proceso de limpieza.

La fórmula utilizada es:

Failure Rate = (Filas que fallan / Total de filas) × 100

Esto permite identificar qué columnas tienen más problemas de calidad de datos.

---

## Notas Importantes

* Este proyecto utiliza Great Expectations 0.18.21, que pertenece a una versión antigua de la librería.
* Versiones más nuevas de Great Expectations pueden no ser compatibles con este código.
* Se recomienda usar Python 3.11 y las versiones del archivo requirements.txt.

---

## Solución de Problemas

Si el proyecto no funciona:

1. Verificar versión de Python:

```bash
python --version
```

2. Crear nuevamente el entorno virtual:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

3. Reinstalar dependencias:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4. Verificar Great Expectations:

```bash
pip show great-expectations
```

Debe mostrar versión 0.18.21.

---

# Autor

GonoAlejo
