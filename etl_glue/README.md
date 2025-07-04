# Proyecto de Ingesta y Gestión de Datos con AWS Glue y Redshift Serverless

Este script de AWS Glue permite realizar un flujo completo de ingestión, validación, carga, respaldo y restauración de datos hacia/desde Redshift Serverless utilizando archivos almacenados en Amazon S3. Los datos son procesados desde archivos CSV y respaldados en formato AVRO.

## Estructura de Carpetas Esperadas en S3

- `entel-csv/`: carpeta que contiene los archivos CSV originales (`jobs.csv`, `departments.csv`, `hired_employees.csv`).
- `entel-csv-filtrados/`: CSV limpios después de aplicar validaciones.
- `entel-csv-omitidos/`: registros omitidos por problemas de validación.
- `backup-entel/[tabla]/[tabla].avro`: ubicación de respaldo en formato AVRO.

## Parámetros de entrada

El script utiliza los siguientes argumentos al momento de ejecución:

| Parámetro         | Descripción                                                  |
|-------------------|--------------------------------------------------------------|
| `--ACCION`         | Acción a ejecutar: `LOAD`, `BACKUP` o `RESTORE`.             |
| `--ARCHIVOS`       | Lista de tablas a restaurar (solo para `RESTORE`). Ej: `['jobs']`. |
| `--BUCKET_INGESTA` | Nombre del bucket S3 de origen para ingestión.              |
| `--BUCKET_BACKUP`  | Nombre del bucket S3 para respaldo/restauración.            |
| `--ROLE_REDSHIFT`  | ARN del rol IAM con permisos para Redshift y S3.            |

## Funcionalidades

### `LOAD`

1. **Crea las tablas si no existen(`jobs`, `departments`, `hired_employees`).
2. **Limpia** los archivos CSV, eliminando registros inválidos (nulos en campos clave).
3. **Guarda los registros válidos** en `entel-csv-filtrados/` y los inválidos en `entel-csv-omitidos/`.
4. **Carga los datos** limpios a Redshift usando `COPY`.

### `BACKUP`

- Realiza respaldo de las tablas en formato **AVRO** compatible con Redshift Serverless.
- Guarda los archivos en el bucket definido por `BUCKET_BACKUP`.

### `RESTORE`

- Verifica existencia del archivo AVRO en S3.
- Elimina la tabla si existe, la vuelve a crear y **carga datos desde el backup**.

## Tablas utilizadas

### `jobs`

```sql
CREATE TABLE public.jobs (
  id INT NOT NULL,
  job VARCHAR(255) NOT NULL,
  PRIMARY KEY(id)
);
```

### `departments`

```sql
CREATE TABLE public.departments (
  id INT NOT NULL,
  departament VARCHAR(255) NOT NULL,
  PRIMARY KEY(id)
);
```

### `hired_employees`

```sql
CREATE TABLE public.hired_employees (
  id INT NOT NULL,
  name VARCHAR(255) NOT NULL,
  fecha DATETIME NOT NULL,
  departament_id INT NOT NULL,
  job_id INT NOT NULL,
  PRIMARY KEY(id)
);
```

## Seguridad

Este script asume que se proporciona el **ARN del rol IAM** con permisos suficientes para:

- Acceder a los buckets S3 involucrados.
- Ejecutar `COPY` y consultas SQL en Redshift.

## Requisitos

- Redshift Serverless habilitado y configurado.
- Buckets de S3 creados previamente.
- Glue Job con Python 3 y librerías externas como `psycopg2`, `fastavro`, `pandas`.

## Librerías utilizadas

- `psycopg2`: conexión a Redshift.
- `boto3`: acceso a S3.
- `pandas`: manejo y limpieza de CSV.
- `fastavro`: generación de archivos AVRO.

## Ejemplo de ejecución

```bash
# Ejemplo de carga de datos
--ACCION LOAD --BUCKET_INGESTA mi-bucket-origen --ROLE_REDSHIFT arn:aws:iam::123456789:role/redshift-role

# Ejemplo de backup
--ACCION BACKUP --BUCKET_BACKUP mi-bucket-backup --ROLE_REDSHIFT arn:aws:iam::123456789:role/redshift-role

# Ejemplo de restore
--ACCION RESTORE --ARCHIVOS "['jobs', 'departments']" --BUCKET_BACKUP mi-bucket-backup --ROLE_REDSHIFT arn:aws:iam::123456789:role/redshift-role
```

## Mantenimiento

- Asegúrate de revisar periódicamente los datos omitidos en `entel-csv-omitidos/` para detectar errores de origen.
- Los backups se generan en formato AVRO para compatibilidad con Glue y Redshift Serverless.

## Autor

Eduardo Acevedo  

