import psycopg2
import json

def lambda_handler(event, context):
    try:
        conn = psycopg2.connect(
            host="redshift-wk-edu.261315157165.us-east-2.redshift-serverless.amazonaws.com",
            dbname="dev",
            user="admin_edu",
            password="Consu190892",
            port=5439
        )
        cur = conn.cursor()

        query = """
        SELECT     
            d.id AS dept_id,
            d.departament AS dept_name,
            COUNT(he.id) AS empleados_contratados
        FROM 
            departments d
        JOIN 
            hired_employees he ON d.id = he.departament_id
        WHERE 
            EXTRACT(YEAR FROM he.fecha) = 2021
        GROUP BY 
            d.id, d.departament
        HAVING 
            COUNT(he.id) > (
                SELECT 
                    AVG(sub.total)
                FROM (
                    SELECT 
                        COUNT(*) AS total
                    FROM 
                        hired_employees
                    WHERE 
                        EXTRACT(YEAR FROM fecha) = 2021
                    GROUP BY 
                        departament_id
                    ) sub
                )
        ORDER BY 
            empleados_contratados DESC;"""

        cur.execute(query)
        results = cur.fetchall()

        response = []
        for row in results:
            response.append({
                "id": row[0],
                "department": row[1],
                "hired": row[2]
            })

        cur.close()
        conn.close()

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
