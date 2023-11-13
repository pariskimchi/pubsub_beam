from importlib.resources import path
from venv import create
import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions,GoogleCloudOptions
import argparse 
from google.cloud import bigquery 
from omegaconf import OmegaConf

config = OmegaConf.load("./config.yaml")


credential_json_path = config.gcp_config.credential_json_path

proj_id = config.gcp_config.project_id
topic_name = config.gcp_config.pubsub_topic
topic_path = f"projects/{proj_id}/topics/{topic_name}"

dataset_name = config.bq_config.bq_dataset_name

#project-id:dataset_id.table_id
delivered_table_spec = f'{proj_id}:{dataset_name}.delivered_orders'
other_table_spec = f'{proj_id}:{dataset_name}.other_status_orders'


### argument으로 들어가야 될것 
## format: projects/<project-id>/topcis/<topic>
parser = argparse.ArgumentParser()

parser.add_argument('--input',
                    dest='input',
                    required=True, 
                    help='input topic through pubsub, beam_pipeline to BigQuery')

path_args, pipeline_args = parser.parse_known_args()
inputs_pattern = path_args.input

options = PipelineOptions(pipeline_args)
## 실시간 처리이므로 streaming option 설정 
options.view_as(StandardOptions).streaming = True

## beam pipeline 생성 
p = beam.Pipeline(options=options)

### 전처리 function들 설정 
def remove_last_colon(row):		# OXJY167254JK,11-09-2020,8:11:21,854A854,Chow M?ein:,65,Cash,Sadabahar,Delivered,5,Awesome experience
    cols = row.decode('utf-8-sig').split(',')		# [(OXJY167254JK) (11-11-2020) (8:11:21) (854A854) (Chow M?ein:) (65) (Cash) ....]
    item = str(cols[4])			# item = Chow M?ein:

    if item.endswith(':'):
        cols[4] = item[:-1]		# cols[4] = Chow M?ein

    return ','.join(cols)		# OXJY167254JK,11-11-2020,8:11:21,854A854,Chow M?ein,65,Cash,Sadabahar,Delivered,5,Awesome experience

def remove_special_characters(row):    # oxjy167254jk,11-11-2020,8:11:21,854a854,chow m?ein,65,cash,sadabahar,delivered,5,awesome experience
    import re
    cols = row.split(',')			# [(oxjy167254jk) (11-11-2020) (8:11:21) (854a854) (chow m?ein) (65) (cash) ....]
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','', col)
        ret = ret + clean_col + ','			# oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience,
    ret = ret[:-1]						# oxjy167254jk,11-11-2020,8:11:21,854A854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience
    return ret

def print_row(row):
    print(row)

#############

## pubsub topic으로부터 데이터 읽어서 
## 전처리 적용 하는 파이프라인 : cleaned_data pipeline
## 전처리 function은 beam.Map()을 통해 적용
cleaned_data = (
    p
    | beam.io.ReadFromPubSub(topic=inputs_pattern)
    | beam.Map(remove_last_colon)
    | beam.Map(lambda row: row.lower())
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row:row+ ',1')
)

### delivered, others 두가지 분류로 필터링 해서 
## 두가지 분류의 필터링 파이프라인 두 개 생성 
## beam.Filter(lambda row:) 적용

## delivered_orders pipeline
delivered_orders = (
    cleaned_data
    | 'delivered filter' >> beam.Filter(
        lambda row: row.split(',')[8].lower() == 'delivered'
    )
)

## other_orders pipeline 
other_orders = (
    cleaned_data
    | 'Undelivered Filter' >> beam.Filter(
        lambda row: row.split(',')[8].lower() != 'delivered'
    )
)

##### bigQuery 설정 

## Bigquery client 생성 
bq_client = bigquery.Client.from_service_account_json(credential_json_path)

dataset_id = f"{bq_client.project}.{dataset_name}"

### bigQuery 내의 dataset 가져오기 
## 안되면 생성
try:
    bq_client.get_dataset(dataset_id)
except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'US'
    dataset.description = 'dataset for pubsub beam streaming '
    
    dataset_ref = bq_client.create_dataset(dataset, timeout=30)


def to_json(csv_str):
    fields = csv_str.split(',')
    
    json_str = {"customer_id":fields[0],
                 "date": fields[1],
                 "timestamp": fields[2],
                 "order_id": fields[3],
                 "items": fields[4],
                 "amount": fields[5],
                 "mode": fields[6],
                 "restaurant": fields[7],
                 "status": fields[8],
                 "ratings": fields[9],
                 "feedback": fields[10],
                 "new_col": fields[11]
                 }
    return json_str

### table에 사용될 schema 정의
table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING,new_col:STRING'


### delivered order pipeline을 json으로 parsing한 후에 
### BigQuery 내에 delivered_orders 테이블에 데이터 적재 (write)
#  delivered_table_spec = f'{proj_id}:{dataset_name}.delivered_orders'

# print(delivered_table_spec)
(delivered_orders
    | 'delivered to json' >> beam.Map(to_json)
    | 'write delievered' >> beam.io.WriteToBigQuery(
        delivered_table_spec, # BigQuery내에 적재될 테이블 
        schema = table_schema, # table schema 설정
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning': {'type':'DAY'}}
    )
)

### undelivered pipeline도 적용 
(other_orders
	| 'others to json' >> beam.Map(to_json)
	| 'write other_orders' >> beam.io.WriteToBigQuery(
	other_table_spec,
	schema=table_schema,
	create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
	write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
	additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
	)
)

### 결과 확인용 view 생성 
def create_view():
    print("CREATING VIEW THREAD........")

    view_name = "view_daily_food_orders"
    dataset_ref = bq_client.dataset('dataset_daily_food_orders')
    view_ref = dataset_ref.table(view_name)
    view_to_create = bigquery.Table(view_ref)
    
    view_to_create.view_query = F"""
        SELECT * FROM f`{proj_id}.{dataset_name}.delivered_orders`
        WHERE _PARTITIONDATE = DATE(current_date())
    """
    view_to_create.view_use_legacy_sql = False
    
    try:
        bq_client.create_table(view_to_create)
    except:
        print("View Already exists")
        
    
from threading import Timer 
t = Timer(25.0, create_view)
t.start()

from apache_beam.runners.runner import PipelineState
ret = p.run()
ret.wait_until_finish()

if ret.state == PipelineState.DONE:
    print("SUCCESSS")
else:
    print("ERROR Running Beam pipeline")