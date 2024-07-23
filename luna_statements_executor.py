import psycopg2
import boto3
import json
import logging
import time
from botocore.config import Config
from boto3 import client as boto3_client



# Configurar logging
logging.basicConfig(filename='errores.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de la base de datos y AWS Lambda
DB_CONFIG = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}

NOTIFICATION_CENTER_SNS_ARN = ""

LAMBDA_CONFIG = {
    'function_name': ''
}

SQL_QUERY = """
    SELECT distinct pld.personal_loans_document_id, pp.phone_number, pm.user_id, plc.start_date, plc.end_date,
            pm.name || ' ' || pm.first_lastname ||' ' || coalesce(pm.second_lastname, '') as name
        FROM statement.personal_loans_document pld
        JOIN product.core_contract cc on cc.contract_id = pld.contract_id
        JOIN statement.personal_loans_cycle plc ON plc.personal_loans_cycle_id = pld.personal_loans_cycle_id
        JOIN person.main pm on pm.person_id  = cc.person_id
        JOIN person.phone pp on cc.person_id = pp.person_id
        WHERE plc.end_date = '2024-08-31'
"""

BOTO3_CONFIG = Config(retries={"max_attempts": 5, "mode": "standard"})

SNS_CLIENT = boto3_client("sns", config=BOTO3_CONFIG)
LAMBDA_CLIENT = boto3.client('lambda')

def query_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        logging.error(f"Error en la consulta a la base de datos: {e}")
        return None

def invoke_lambda(results):
     
    
    for result in results:

        time.sleep(7)

        payload = {
            "personal_loans_document_id": result[0],
            "phone_number": result[1]
        }
        user_id = result[2]
        start_date = result[3].strftime('%d-%m-%Y')
        end_date = result[4].strftime('%d-%m-%Y')
        periodo = f'"{start_date}" al "{end_date}"'
        name = result[5] + " " + result[6]

        print("Payload to send --> ", payload)
        
        try:
            response = LAMBDA_CLIENT.invoke(
                FunctionName=LAMBDA_CONFIG['function_name'],
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            response_payload = json.loads(response['Payload'].read())
            if response_payload['status_code'] != 200:
                print("response payload en error es: ", response_payload)
                log_error_result(result[0], response_payload)
            else:    
                log_result(result[0], response_payload['download_url'], user_id, name, result[1])    
                
                notification_request = {
                    "type": "email",
                    "user_ids": [user_id],
                    "subject": "Estado de cuenta",
                    "sender": "prestamos@info.storicard.com",
                    "template_name": "email_statement",
                    "team": "luna",
                    "body": {
                        "NAME": name,
                        "PERIODO": periodo,
                    }, 
                    "attachments": [
                        {
                            "file_name": "estado_de_cuenta",
                            "url": response_payload['download_url']
                        }
                    ]
                }
                
                response = SNS_CLIENT.publish(
                    TopicArn=NOTIFICATION_CENTER_SNS_ARN,
                    Message=json.dumps(notification_request, default=json_converter),
                    MessageAttributes={
                        "action": {
                            "DataType": "String",
                            "StringValue": "send",
                        }
                    },
                )
                
                print("Email event sent with response:", response)
                print("-----------------------------------------")

        except Exception as e:
            logging.error(f"Error al invocar la función Lambda para document ID {result[0]}: {e}")
            
    print("-----------------------------------------")
    print("------------ Process finished -----------")        



def log_error_result(document_id, error_response):
    try:
        with open('error_results.txt', 'a') as file:
            log_entry = {
                "personal_loans_document_id": document_id,
                "error": error_response
            }
            file.write(json.dumps(log_entry) + ',\n')
    except Exception as e:
        logging.error(f"Error al escribir en el archivo error_results.txt: {e}")

def log_result(document_id, download_url, user_id, name, phone):
    try:
        with open('results.txt', 'a') as file:
            log_entry = {
                "personal_loans_document_id": document_id,
                "user_id":user_id,
                "name":name,
                "phone":phone,
                "download_url": download_url
            }
            file.write(json.dumps(log_entry) + ',\n')
    except Exception as e:
        logging.error(f"Error al escribir en el archivo results.txt: {e}")        

def json_converter(o):
    """
    Decimals are improperly casted from pyyqml into a Decimal object when in fact they should return a float. Here
    objects are checked to see if they are a Decimal and if so they are casted to a float. Date time objects are also
    converted.

    :param o: an object
    :return: A casted object
    Decoders: https://github.com/PyMySQL/PyMySQL/blob/master/pymysql/converters.py
    """
    # Ensures that when converting a dictionary to a JSON string that the datetime object is properly converted
    if isinstance(o, datetime) or isinstance(o, date):
        try:
            return o.__str__()
        except Exception as e:
            logger.error("Error converting datetime value to string: %s", e)
            return None
    elif isinstance(o, Decimal):
        return float(o)

def main():
    results = query_database()
    if results:
        invoke_lambda(results)

if __name__ == "__main__":
    main()
