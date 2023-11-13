from google.cloud import pubsub_v1 
import time 
import numpy as np
from omegaconf import OmegaConf

config = OmegaConf.load("./config.yaml")

credential_json_path = config.gcp_config.credential_json_path

proj_id = config.gcp_config.project_id
topic_name = config.gcp_config.pubsub_topic_name

### pubsub_client instance 
publisher_client = pubsub_v1.PublisherClient.from_service_account_json(credential_json_path)

## set topic_path
topic_path = publisher_client.topic_path(proj_id, topic_name)

## create topic using topic_path 
## 토픽 생성 
# topic = publisher_client.create_topic(
#     request = {"name":topic_path}
# )
data_csv_path = config.data.csv_data_path
with open(data_csv_path) as csv_file:
    for row in csv_file:
        print(row)
        data = row.encode('utf-8-sig')
        ### topic에 data -> publish
        future = publisher_client.publish(topic_path, data=data)
        print(future.result())
        time.sleep(1)
