import json
from urllib import request, parse
import random
import os
import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import websocket #NOTE: websocket-client (https://github.com/websocket-client/websocket-client)
import uuid
import json
import urllib.request
import urllib.parse
import io
import mysql.connector
from mysql.connector import Error

load_dotenv()

def update_task_status(task_id, status=1):
    try:
        # Establish a database connection (replace with your database credentials)
        conn = mysql.connector.connect(
            host=os.getenv("host"),
            database=os.getenv("db_name"),
            user=os.getenv("user"),
            password=os.getenv("password")
        )
        
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Define your UPDATE statement
            sql = '''UPDATE tbl_letter SET letter_image_status = %s WHERE id = %s'''
            
            # Execute the UPDATE statement with provided parameters
            cursor.execute(sql, (status, task_id))
            
            # Commit the changes
            conn.commit()
            
            print("Task updated successfully.")
    except Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        if conn.is_connected():
            cursor.close()
            conn.close()

def upload_s3(image_name, image):
    BUCKET_NAME = os.getenv("S3_BUCKET")
    S3_URL = os.getenv("S3_URL")

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("CREDENTIALS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("CREDENTIALS_SECRET_KEY"),
    )

    object_name = image_name

    # 파일로 업로드
    try:
        result = s3.upload_fileobj(image, BUCKET_NAME, object_name)
        # s3.upload_file(object_name, BUCKET_NAME, object_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False
    except Exception as e:
        logging.error(e)
        return False
        
    

def queue_prompt(prompt):
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req =  urllib.request.Request("http://{}/prompt".format(server_address), data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_images(ws, prompt):
    prompt_id = queue_prompt(prompt)['prompt_id']
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
                        break #Execution is done
                    else:
                        current_node = data['node']
        else:
            if current_node == 'save_image_websocket_node':
                images_output = output_images.get(current_node, [])
                images_output.append(out[8:])
                output_images[current_node] = images_output

    return output_images

def make_images(message):
    # JSON 파일 경로
    json_file_path = 'workflow_api.json'

    # JSON 파일 열기 및 읽기
    with open(json_file_path, 'r') as file:
        prompt_text = json.load(file)

    server_address = "127.0.0.1:8188"
    client_id = str(uuid.uuid4())

    #set the text prompt for our positive CLIPTextEncode
    prompt_text["6"]["inputs"]["text"] = "masterpiece best quality man" + message.keywords

    #set the seed for our KSampler node
    prompt_text["3"]["inputs"]["seed"] = random.randint(0, 1000000)

    ws = websocket.WebSocket()
    ws.connect("ws://{}/ws?clientId={}".format(server_address, client_id))
    images = get_images(ws, prompt_text)

    print(images.keys())

    #Commented out code to display the output images:
    # for node_id in images:
    #     for image_data, idx in images[node_id]:
        # from PIL import Image
        # image = Image.open(io.BytesIO(image_data))
        # image.show()
    for image_data, idx in images[0]:
        image_name = f"{message.letter_id}_{idx}.jpg"
        result = upload_s3(image_name, io.BytesIO(image_data))
        if not result:
            print(f"Failed to upload image {image_name}")
            return False
        
    update_task_status(message.letter_id)


# AWS SQS 클라이언트 설정
sqs = boto3.client(
    'sqs',
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("CREDENTIALS_ACCESS_KEY"),
	aws_secret_access_key=os.getenv("CREDENTIALS_SECRET_KEY"),
)

# SQS 대기열 URL 설정

def receive_messages(max_number_of_messages: int = 1) -> None:
    try:
        QUEUE_URL = os.getenv("AWS_QUEUE_URL")
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=max_number_of_messages
        )
        messages = response.get('Messages', [])
        if not messages:
            # print("No messages received")
            return

        for message in messages:
            print(f"Received message: {message['Body']}")
            message = json.loads(message['Body'])
            make_images(message)
            # 메시지 삭제 (선택 사항)
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            print(f"Deleted message with receipt handle: {receipt_handle}")
    except (BotoCoreError, ClientError) as error:
        print(f"Failed to receive messages: {error}")

if __name__ == "__main__":
    # 메시지 보내기 테스트
    # send_message("Hello, this is a test message2")

    # 메시지 수신 테스트
    while True:
        receive_messages(max_number_of_messages=5)
