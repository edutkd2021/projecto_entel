# Lambda: Consulta de Empleados Contratados por Trimestre

Esta función AWS Lambda realiza una consulta a una base de datos **Amazon Redshift** para obtener un resumen del número de empleados contratados por **departamento** y **cargo (job)**, desglosado por **trimestre (Q1 a Q4)** del año 2021. La función está expuesta a través de un **API Gateway (GET)**.

## URL del Endpoint

```
GET https://cj9uf8nax2.execute-api.us-east-2.amazonaws.com/dev/consulta
```

## 🛠️ Tecnologías Utilizadas

- AWS Lambda (Python)
- Amazon Redshift
- API Gateway
- psycopg2 (PostgreSQL client para Python)
- JSON (para respuesta HTTP)

## Parámetros de Entrada

Este endpoint no requiere parámetros de entrada. Solo debes hacer una solicitud `GET` a la URL del endpoint.

## Ejemplo de Respuesta (HTTP 200)

```json
[
  {
    "departament": "Marketing",
    "job": "Designer",
    "Q1": 3,
    "Q2": 2,
    "Q3": 1,
    "Q4": 4
  },
  {
    "departament": "Engineering",
    "job": "Backend Developer",
    "Q1": 5,
    "Q2": 3,
    "Q3": 2,
    "Q4": 6
  }
]
```

##  Posibles Errores

- `500 Internal Server Error`: Error al conectar o ejecutar la consulta en Redshift. La respuesta incluirá el mensaje de error.

Ejemplo:

```json
{
  "error": "FATAL: password authentication failed for user \"admin_edu\""
}
```

##  Estructura de la Función Lambda

```python
def lambda_handler(event, context):
    # 1. Conexión a Redshift
    # 2. Consulta SQL para contar contrataciones por trimestre en 2021
    # 3. Serializa los resultados a JSON
    # 4. Devuelve respuesta HTTP con código 200 o 500
```

##  Seguridad

- El endpoint permite llamadas públicas (`Access-Control-Allow-Origin: *`), por lo tanto, se recomienda usar medidas de autenticación si se va a exponer a producción.
- Las credenciales de la base de datos están embebidas en el código (lo cual **no se recomienda** en ambientes reales). Para producción, considera usar **AWS Secrets Manager** o **variables de entorno cifradas**.

##  Pruebas

Puedes probar el endpoint directamente con herramientas como:

- Postman
- curl desde terminal:

```bash
curl https://cj9uf8nax2.execute-api.us-east-2.amazonaws.com/dev/consulta
```

##  Filtrado por Año

Actualmente la función **solo consulta datos del año 2021**. Si necesitas soporte para años dinámicos, será necesario modificar la consulta SQL.

---

##  Archivos relacionados

- `lambda_function.py`: Contiene el código fuente de la Lambda
- `requirements.txt`: Debe incluir `psycopg2-binary` para empaquetado local si se implementa fuera del entorno Lambda en consola

---

##  Autor

- Desarrollado por: [Tu nombre o equipo]
- Fecha: Julio 2025
