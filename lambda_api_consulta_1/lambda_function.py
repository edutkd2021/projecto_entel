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
          d.departament,
          j.job,
          COUNT(CASE WHEN EXTRACT(QUARTER FROM fecha) = 1 THEN 1 END) AS Q1,
          COUNT(CASE WHEN EXTRACT(QUARTER FROM fecha) = 2 THEN 1 END) AS Q2,
          COUNT(CASE WHEN EXTRACT(QUARTER FROM fecha) = 3 THEN 1 END) AS Q3,
          COUNT(CASE WHEN EXTRACT(QUARTER FROM fecha) = 4 THEN 1 END) AS Q4
        FROM hired_employees he
        INNER JOIN departments d ON he.departament_id = d.id
        INNER JOIN jobs j ON he.job_id = j.id
        WHERE EXTRACT(YEAR FROM fecha) = 2021
        GROUP BY d.departament, j.job
        ORDER BY d.departament ASC, j.job ASC;
        """

        cur.execute(query)
        results = cur.fetchall()

        response = []
        for row in results:
            response.append({
                "departament": row[0],
                "job": row[1],
                "Q1": row[2],
                "Q2": row[3],
                "Q3": row[4],
                "Q4": row[5]
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
