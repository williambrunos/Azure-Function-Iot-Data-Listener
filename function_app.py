import azure.functions as func
import logging
import json
import base64
import re

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", 
                  path="messages/{name}",
                  connection="BlobStorageConncectionString") 
def azr_iot_data_listener(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob\n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    try:
        # Read the blob content
        data = myblob.read().decode('utf-8')

        # Use regular expressions to find all JSON objects in the blob content
        json_objects = re.findall(r'\{.*?\}(?=\s*\{|\s*$)', data, re.DOTALL)
        
        if not json_objects:
            logging.info('No JSON objects found in the blob')
            return

        for json_object in json_objects:
            try:
                # Parse JSON
                json_data = json.loads(json_object)
                # Decode the base64 encoded Body field
                body_decoded_str = base64.b64decode(json_data["Body"]).decode('utf-8')
                body_decoded = json.loads(body_decoded_str)
                
                temperature = body_decoded.get("temperature", 0)
                humidity = body_decoded.get("humidity", 0)
                motion_detected = body_decoded.get("motion_detected", 0)
                motion_measurements = body_decoded.get("motion_measurements", 0)
                
                logging.info(f"[temperature-data: {temperature}]")
                logging.info(f"[humidity-data: {humidity}]")
                logging.info(f"[motion-detected-data: {motion_detected}]")
                logging.info(f"[motion-measurements-data: {motion_measurements}]")
                
            except json.JSONDecodeError as e:
                logging.error(f'Error decoding JSON object: {e}')
                logging.error(f'JSON content: {json_object}')
            except Exception as e:
                logging.error(f'Error processing blob {myblob.name}: {e}')
    except Exception as e:
        logging.error(f'Error reading blob {myblob.name}: {e}')