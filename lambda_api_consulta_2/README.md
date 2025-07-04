# Lambda: Departamentos con Contrataciones por Encima del Promedio en 2021

Esta función AWS Lambda consulta una base de datos **Amazon Redshift** para devolver los departamentos que contrataron más empleados que el promedio durante el año 2021. Está expuesta mediante un **API Gateway (GET)** y devuelve una lista ordenada de los departamentos con más contrataciones.

##  URL del Endpoint

```
GET https://86j4eor398.execute-api.us-east-2.amazonaws.com/dev/consulta
```

##  Tecnologías Utilizadas

- AWS Lambda (Python)
- Amazon Redshift
- psycopg2 (cliente PostgreSQL para Python)
- API Gateway (opcional)
- JSON (para formateo de respuestas HTTP)

##  Parámetros de Entrada

Esta función no requiere ningún parámetro de entrada.

##  Ejemplo de Respuesta (HTTP 200)

```json
[
  {
    "id": 1,
    "department": "Engineering",
    "hired": 25
  },
  {
    "id": 3,
    "department": "Marketing",
    "hired": 18
  }
]
```

##  Lógica de Negocio

- La consulta se limita al año 2021.
- Se cuentan los empleados contratados por cada departamento.
- Se seleccionan **solo los departamentos** cuyo total de contrataciones esté **por sobre el promedio** general.
- El resultado se ordena en forma descendente por cantidad de contrataciones.

##  Posibles Errores

- `500 Internal Server Error`: Problemas al conectarse a Redshift o ejecutar la consulta. El cuerpo de la respuesta contendrá el mensaje de error.

```json
{
  "error": "FATAL: password authentication failed for user \"admin_edu\""
}
```

##  Estructura de la Función Lambda

```python
def lambda_handler(event, context):
    # 1. Conexión a Redshift
    # 2. Ejecuta la consulta con subquery para calcular el promedio
    # 3. Serializa los resultados a JSON
    # 4. Devuelve respuesta HTTP con código 200 o 500
```

##  Seguridad

- El acceso es público (`Access-Control-Allow-Origin: *`), lo que puede implicar riesgos si no se controla.
- Las credenciales de Redshift están embebidas en el código fuente (**no recomendado** para ambientes productivos).
  -  Recomendación: usa **AWS Secrets Manager** o **variables de entorno cifradas** para proteger las credenciales.

##  Pruebas

Puedes probar la función mediante:

- Consola AWS Lambda (con eventos vacíos)
- curl o Postman (si está expuesta vía API Gateway)

```bash
curl https://<tu-api>.execute-api.us-east-2.amazonaws.com/dev/departamentos-top
```

##  Archivos relacionados

- `lambda_function.py`: Código principal de la función Lambda.
- `requirements.txt`: Lista de dependencias necesarias para empaquetar la Lambda localmente.

---

##  Autor

- Desarrollado por: Eduardo Acevedo
- Fecha: Julio 2025
