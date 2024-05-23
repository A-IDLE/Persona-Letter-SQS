import datetime
import json
import time
import urllib.request
import urllib.parse
import random
import os
import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import websocket #NOTE: websocket-client (https://github.com/websocket-client/websocket-client)
import uuid
import io
import MySQLdb

# Load environment variables
load_dotenv()
server_address = "127.0.0.1:8188"

# Configure logging
logging.basicConfig(
    filename='app.log', 
    filemode='a', 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)


def update_task_status(task_id, status=1):
    conn = None
    try:
        # Retrieve database credentials from environment variables
        host = os.getenv("host")
        db_name = os.getenv("db_name")
        user = os.getenv("user")
        password = os.getenv("password")
        
        if not host or not db_name or not user or not password:
            raise ValueError("Database credentials are not fully set in the environment variables")
        
        conn = MySQLdb.connect(
            host=host,
            db=db_name,
            user=user,
            passwd=password
        )
        
        cursor = conn.cursor()
        sql = '''UPDATE tbl_letter SET letter_image_status = %s WHERE letter_id = %s'''
        cursor.execute(sql, (status, task_id))
        conn.commit()
        
        logging.info("DB updated successfully.")
    except (MySQLdb.Error, ValueError) as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def upload_s3(image_name, image):
    BUCKET_NAME = os.getenv("S3_BUCKET")

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("CREDENTIALS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("CREDENTIALS_SECRET_KEY"),
    )

    object_name = f"letters/{image_name}"

    try:
        result = s3.upload_fileobj(image, BUCKET_NAME, object_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False
    except Exception as e:
        logging.error(e)
        return False
    
# Function to find and print the entries with the specified title
def find_entries_with_title(data, title):
    for key, value in data.items():
        if "_meta" in value and value["_meta"].get("title") == title:
            return key
    return None

def queue_prompt(prompt, client_id):
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request("http://{}/prompt".format(server_address), data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_images(ws, prompt, client_id):
    prompt_id = queue_prompt(prompt, client_id)['prompt_id']
    output_images = {}
    current_node = ""
    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message['type'] == 'executing':
                data = message['data']
                if data['prompt_id'] == prompt_id:
                    if data['node'] is None:
                        break
                    else:
                        current_node = data['node']
        else:
            if current_node == 'save_image_websocket_node':
                images_output = output_images.get(current_node, [])
                images_output.append(out[8:])
                output_images[current_node] = images_output

    return output_images

def make_images(message, client_id):
    letter_id = message["letter_id"]
    keywords = message["keywords"]
    prompt_text = message["prompt_text"]
    # json_file_path = 'workflow_api.json'

    # with open(json_file_path, 'r') as file:
    #     prompt_text = json.load(file)
    positive_prompt_id = find_entries_with_title(prompt_text, "Positive")
    # negative_prompt_id = find_entries_with_title(prompt_text, "Negative")
    sampler_id = find_entries_with_title(prompt_text, "Sampler")
    sampler_id2 = find_entries_with_title(prompt_text, "Sampler2")
    origin_text = prompt_text[positive_prompt_id]["inputs"]["text"]
    prompt_text[positive_prompt_id]["inputs"]["text"] = keywords+origin_text
    # prompt_text[negative_prompt_id]["inputs"]["text"] = "nsfw"
    prompt_text[sampler_id]["inputs"]["seed"] = random.randint(0, 1000000)
    if(sampler_id2 != None):
        prompt_text[sampler_id2]["inputs"]["seed"] = random.randint(0, 1000000)

    ws = websocket.WebSocket()
    ws.connect("ws://{}/ws?clientId={}".format(server_address, client_id))
    images = get_images(ws, prompt_text, client_id)

    for node_id in images:
        for idx, image_data in enumerate(images[node_id]):
            image_name = f"{letter_id}_{idx}.jpg"
            result = upload_s3(image_name, io.BytesIO(image_data))
            if not result:
                logging.error(f"Failed to upload image {image_name}")
                return False
        
    update_task_status(letter_id)

sqs = boto3.client(
    'sqs',
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("CREDENTIALS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("CREDENTIALS_SECRET_KEY"),
)

def receive_messages(max_number_of_messages: int = 1) -> None:
    try:
        QUEUE_URL = os.getenv("AWS_QUEUE_URL")
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=max_number_of_messages
        )
        messages = response.get('Messages', [])
        if not messages:
            print(f"No messages received {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return

        for message in messages:
            messageJson = json.loads(message['Body'])
            logging.info(f"Received message: {messageJson}")
            client_id = str(uuid.uuid4())
            make_images(messageJson, client_id=client_id)
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            logging.info(f"Deleted message with receipt handle: {receipt_handle}")
    except (BotoCoreError, ClientError) as error:
        logging.error(f"Failed to receive messages: {error}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    while True:
        try:
            time.sleep(5)
            receive_messages(max_number_of_messages=5)
        except Exception as e:
            logging.error(f"An error occurred in the main loop: {e}")
