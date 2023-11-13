from google.cloud import pubsub_v1 
import datetime 
from omegaconf import OmegaConf

config = OmegaConf.load("./config.yaml")

credential_json_path= config.gcp_config.credential_json_path

proj_id = config.gcp_config.project_id
topic_name = config.gcp_config.pubsub_topic_name
topic_path = f"projects/{proj_id}/topics/{topic_name}"

### subscription_id = 'data_from_file_sub

### subscriber_client instance 
subscriber_client = pubsub_v1.SubscriberClient.from_service_account_json(credential_json_path)

# subscription_id = "food_topic_sub"
subscription_id = config.config_subscription.subscription_id
subscription_path = subscriber_client.subscription_path(proj_id, subscription_id)

### 메세지를 전달받을 Callback 함수 정의 
def callback(message):
    receive_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(receive_time)
    print(F"Received MEssage: {message.data.decode('utf-8-sig')}")
    ## 메세지 처리후 확인 메세지 보내기
    message.ack()
    
### 구독을 생성하고 Callback 함수 연결합니다 
streaming_pull_future = subscriber_client.subscribe(subscription_path,callback=callback)
timeout = 100

with subscriber_client:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel() # Trigeer the Shutdown 
        streaming_pull_future.result() # block until the shutdown