import sys
import psycopg2
import traceback
import boto3
import botocore
import io
import csv
import pandas as pd
from awsglue.utils import getResolvedOptions
from fastavro import writer, parse_schema
from io import StringIO
import ast

#funcion para crear tablas
def create_table(tabla):
    queries = {
        'jobs': '''CREATE TABLE IF NOT EXISTS public.jobs (id int not null, job varchar(255) not null, primary key(id))''',
        'departments': '''CREATE TABLE IF NOT EXISTS public.departments (id int not null, departament varchar(255) not null, primary key(id))''',
        'hired_employees': '''CREATE TABLE IF NOT EXISTS public.hired_employees (
                                id int not null,
                                name varchar(255) not null,
                                fecha datetime not null,
                                departament_id int not null,
                                job_id int not null,
                                primary key(id)
                              )'''
    }
    if tabla not in queries:
        raise Exception(f"No existe creación para tabla {tabla}")
    return ejecutar_query(queries[tabla])

#funcion para cargar datos csv
def carga_datos(bucket, ruta, tabla):
    campos_obligatorios = {
        "jobs": ['id', 'job'],
        "departments": ['id', 'department'],
        "hired_employees": ['id', 'name', 'datetime', 'deparment_id', 'job_id']
    }
    if create_table(tabla):
        print(f"Tabla {tabla} creada correctamente")
        #limpiar el csv antes de exportar a redshift
        limpia_csv(bucket, ruta, campos_obligatorios[tabla], tabla)
        #en caso de ejecutar correctamente llamo a metodo para hacer copy de los datos
        if carga_insert_con_copy(bucket, f"entel-csv-filtrados/{tabla}.csv", tabla, 'CSV'):
            print("Datos agregados con exito")
        
#funcion para limpiar los csv antes de cargar
def limpia_csv(bucket, ruta, columnas_requeridas, tabla):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=ruta)
    df = pd.read_csv(obj['Body'], header=None, names=columnas_requeridas)
    
    # Separar los registros válidos (sin nulls en columnas requeridas)
    df_limpio = df.dropna(subset=columnas_requeridas)

    # Filas con errores (con nulls en columnas requeridas)
    df_con_errores = df[df.isnull().any(axis=1) & df[columnas_requeridas].isnull().any(axis=1)]
    
    if not df_con_errores.empty:
        print(f"El archivo {tabla} contiene: {len(df_con_errores)} filas con errores se guardara en la ruta entel-csv-omitidos/{tabla}.csv")
        csv_buffer = StringIO()
        df_con_errores.to_csv(csv_buffer, index=False, sep = "|")
        s3.put_object(Bucket=bucket, Key=f"entel-csv-omitidos/{tabla}.csv", Body=csv_buffer.getvalue())
    else:
        print(f"0 filas del archivo {tabla} fueron omitidas")

    # Guardar nuevo CSV en memoria
    csv_buffer = StringIO()
    if tabla == 'hired_employees':
        df_limpio['deparment_id'] = df_limpio['deparment_id'].astype(int) 
        df_limpio['job_id'] = df_limpio['job_id'].astype(int)
    df_limpio.to_csv(csv_buffer, index=False, sep = "|")
    
    # Subir CSV limpio a S3
    s3.put_object(Bucket=bucket, Key=f"entel-csv-filtrados/{tabla}.csv", Body=csv_buffer.getvalue())
    print(f"Archivo limpio guardado en s3://{bucket}/entel-csv-filtrados/{tabla}.csv")

#funcion que se encarga de hacer el copy de las tablas pasadas por parametro
def carga_insert_con_copy(bucket, ruta, tabla, archivo):
    global iam_role
    
    try:
        if archivo == 'CSV':
            query = f"""
                COPY public.{tabla}
                FROM 's3://{bucket}/{ruta}'
                IAM_ROLE '{iam_role}'
                FORMAT AS CSV
                DELIMITER '|'
                TIMEFORMAT 'auto'
                TRUNCATECOLUMNS
                IGNOREHEADER 1;
            """
        else:
            query = f"""
                COPY public.{tabla}
                FROM 's3://{bucket}/{ruta}'
                IAM_ROLE '{iam_role}'
                FORMAT AS AVRO 'auto';"""
        print(f"Ejecutando COPY para tabla: {tabla}")
        return ejecutar_query(query)
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error al ejecutar COPY: {e}")

#funcion que se encarga de realizar un unload desde reshift lo guarda en s3 en formato AVRO (no funciona esta en redshift serverless)
def backup_tabla_redshift(bucket, ruta, tabla):
    if existe_tabla(tabla):
        global iam_role
        try:
            query = f"""
                UNLOAD ('SELECT * FROM public.{tabla}')
                TO 's3://{bucket}/{ruta}/{tabla}'
                IAM_ROLE '{iam_role}'
                FORMAT AS AVRO
            """
            print("Ejecutando BACKUP...")
            return ejecutar_query(query)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error al ejecutar BACKUP: {e}")
    else:
        print(f"No se realizo backup de tabla: {tabla}")

#se realiza esta funcion ya que la anterior solo es compatible con parquet para redshift serverless
def backup_tabla_redshift_serverless(bucket, ruta, tabla):
    global conn
    if existe_tabla(tabla):
        #query para obtener registros de tabla a realizar backup
        query = f""" 
        select * from public.{tabla}
        """
        #ejecucion de la query
        df = pd.read_sql_query(query, conn)

        #correccion para formato fechas
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or 'fecha' in col.lower() or 'date' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

        registros = df.to_dict(orient='records')

        #variable que tendra el esquema
        esquema = {
            'doc': f"Backup de tabla: {tabla}",
            'name': f'{tabla}',
            'namespace': 'backup.redshift',
            'type': 'record',
            'fields': []
        }

        #se realiza un recorrido para ver los tipos de columnas
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                avro_type = 'int'
            elif pd.api.types.is_float_dtype(dtype):
                avro_type = 'float'
            elif pd.api.types.is_bool_dtype(dtype):
                avro_type = 'boolean'
            elif pd.api.types.is_datetime64_any_dtype(dtype) or 'datetime' in col_name.lower() or 'date' in col_name.lower() or 'fecha' in col_name.lower():
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                avro_type = 'string'
            else:
                avro_type = 'string'
            
            esquema['fields'].append({'name': col_name, 'type': [avro_type, 'null']})

    
        #objeto tipo esquema para avro
        parsed_esquema = parse_schema(esquema)
    
        #se carga los registros en memoria para pasar a s3 posteriormente
        buffer = io.BytesIO()
        writer(buffer, parsed_esquema, registros)
        buffer.seek(0)

        s3 = boto3.client('s3')
        #carga de datos en s3
        s3.put_object(Bucket=bucket, Key=ruta+tabla+".avro", Body=buffer.read())
        print(f"Backup completado para tabla: {tabla}")
        
#funcion que se encarga de verificar si la tabla existe
def existe_tabla(tabla):
    try:
        query = f"""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = '{tabla}'
        """
        cursor.execute(query)
        return cursor.fetchone() is not None
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error al verificar existencia de la tabla {tabla}: {e}")

#obtiene conexion con redshift
def get_conn():
    global conn
    if conn is None or conn.closed != 0:
        user = 'admin_edu'
        password = 'Consu190892'
        dbname = 'dev'
        host = 'redshift-wk-edu.261315157165.us-east-2.redshift-serverless.amazonaws.com'
        port = '5439'
        conn_string = f"dbname='{dbname}' port='{port}' user='{user}' password='{password}' host='{host}'"
        #print(conn_string)
        conn = psycopg2.connect(conn_string)
    return conn

#funcion que ejecuta una query devuelve verdadero en caso de ejecutar correctamente    
def ejecutar_query(query):
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        raise Exception('error al ejecutar query {}'.format(e))
    return True

#funcion para verificar que el bucket existe
def verifica_bucket(nombre):
    if nombre != None and len(nombre) > 1:
        s3 = boto3.client("s3")
        try:
            s3.head_bucket(Bucket=nombre)
            return True
        except botocore.exceptions.ClientError as e:
            print("Bucket no existe o no tiene acceso")
            return False
    else:
        return False

#funcion para restaurar tabla
def restaurar_tabla(bucket, ruta, tabla):
    #primero verificar si el archivo de backup existe
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=ruta)
    except botocore.exceptions.ClientError as e:
        print(f"El archivo de backup {ruta} no existe o no se tiene acceso.")
        return False

    #si existe el backup entonces ahora verificar si la tabla existe para eliminar antes de crearla.
    if existe_tabla(tabla):
        #eliminar y volver a crear
        try:
            print(f"Eliminando tabla {tabla} existente")
            ejecutar_query(f"DROP TABLE public.{tabla}")
        except Exception as e:
            print(f"Error al eliminar la tabla {tabla}: {e}")
            return False
    #ahora proceder a crear la tabla
    if create_table(tabla):
        if carga_insert_con_copy(bucket, ruta, tabla, 'AVRO'):
            print("Datos cargados con exito")

try:
    args = getResolvedOptions(sys.argv, ['ACCION', 'ARCHIVOS', 'BUCKET_INGESTA', 'BUCKET_BACKUP', 'ROLE_REDSHIFT'])
    #role iam que ejecutara acciones por lo general esto lo leo desde secrets manager pero para efectos de prueba está en duro
    iam_role = args['ROLE_REDSHIFT']
    ##############
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
    except Exception as e:
        raise Exception('error conexion redshift {}'.format(e))
    ##################################################
    if args['ACCION'] == 'LOAD':
        s3_cliente = boto3.client('s3')
        if verifica_bucket(args['BUCKET_INGESTA']) == True:
            print("######Comienza carga JOBS######")
            carga_datos(args['BUCKET_INGESTA'], "entel-csv/jobs.csv", "jobs")
            print("######Comienza carga DEPARTMENTS######")
            carga_datos(args['BUCKET_INGESTA'], "entel-csv/departments.csv", "departments")
            print("######Comienza carga HIRED EMPLOYEES######")
            carga_datos(args['BUCKET_INGESTA'], "entel-csv/hired_employees.csv", "hired_employees")

    elif args['ACCION'] == 'BACKUP':
        print("######Comienza proceso de backup Tabla Jobs######")
        backup_tabla_redshift_serverless(args['BUCKET_BACKUP'], "jobs/", "jobs")
        print("######Comienza proceso de backup Tabla Departments######")
        backup_tabla_redshift_serverless(args['BUCKET_BACKUP'], "departments/", "departments")
        print("######Comienza proceso de backup Tabla Hired Employess######")
        backup_tabla_redshift_serverless(args['BUCKET_BACKUP'], "hired_employees/", "hired_employees")
        
    elif args['ACCION'] == 'RESTORE':
        arc_restores = ast.literal_eval(args['ARCHIVOS'])
        if(len(arc_restores)>0):
            for restore in arc_restores:
                print(f"#####Comienzo Restore Tabla: {restore}#####")
                restaurar_tabla(args['BUCKET_BACKUP'], restore+"/"+restore+".avro", restore)
        else:
            print("No ingreso ningun archivo para realizar RESTORE")
    else:
        print("Opcion desconocida")
    
except Exception as e:
    traceback.print_exc()