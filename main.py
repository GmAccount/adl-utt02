from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
import findspark
from tasks import filter_items, celery_app
from fastapi.responses import JSONResponse
from celery.result import AsyncResult





import os,time, traceback, datetime


import findspark
findspark.init('/home/hadoop/spark/')
from pyspark.sql import SparkSession


os.environ['SPARK_HOME'] = "/home/hadoop/spark/"
os.environ['HADOOP_CONF_DIR'] = '/home/hadoop/hadoop/etc/hadoop'
#os.environ['YARN_CONF_DIR'] = ''

app = FastAPI()

class Item(BaseModel):
    id_offer_gt: Optional[int] = None 
    acc_id: Optional[int] = None  
    sp_offerid: Optional[str] = None 
    id_mailer: Optional[int] = None 
    mid: Optional[int] = None 
    ip_id: Optional[str] = None
    ip_user: Optional[str] = None
    userid: Optional[int] = None 
    offer_name: Optional[str] = None
    revenue: Optional[float] = None 
    date: Optional[str] = None
    dt_action: Optional[str] = None
    list_id: Optional[int] = None
    id_grp: Optional[int] = None
    isp_id: Optional[int] = None
    isp_name: Optional[str] = None
    vertical_name: Optional[str] = None
    domain: Optional[str] = None
    gender: Optional[str] = None
    age: Optional[int] = None
    poviderData: Optional[str] = None
    grp_name: Optional[str] = None
    cc: Optional[str] = None
    city: Optional[str] = None
    zip: Optional[str] = None
    stat: Optional[str] = None
    isp: Optional[str] = None
    asn: Optional[int] = None
    entered_user: Optional[str] = None
    optin: Optional[int] = None
    f_name: Optional[str] = None
    l_name: Optional[str] = None
    listName: Optional[str] = None
    os_family: Optional[str] = None
    os_version_string: Optional[str] = None
    device_family: Optional[str] = None
    device_brand: Optional[str] = None
    device_model: Optional[str] = None
    is_mobile: Optional[int] = None
    is_tablet: Optional[int] = None
    is_touch_capable: Optional[int] = None
    is_pc: Optional[int] = None
    isp_click: Optional[int] = None
    webbr_cc: Optional[str] = None
    asn_usage_type: Optional[str] = None
    weekday: Optional[int] = None
    msg_date: Optional[str] = None
    price_device: Optional[float] = None
    Timestamp: Optional[str] = None
    year: Optional[int] = None
    quarter: Optional[int] = None
    month: Optional[int] = None




@app.post("/items/", status_code=200)
async def get_items(item: Item):
    uri_hdfs = []
    output = {}
    #uri_hdfs_base = "/root/parquet_files/"
    uri_hdfs_base = "/flume/data/parquetfiles/"

    item_dict = item.dict()
    partitions = ["id_grp","year","quarter","month"]
    for param in partitions:
        param_val = getattr(item, param, None)
        if type(param_val) == str or type(param_val) == int:
            if param_val:
                uri_hdfs_base += param+'='+str(param_val)+'/'

    result = filter_items.apply_async(kwargs={'uri_hdfs_base': uri_hdfs_base, 'partitions':partitions,'item_dict':item_dict})
    if result:
        output['jobID'] = result.id
        output['stats'] = result.info
        output['status'] = result.state
    return output



@app.get("/tasks/{task_id}")
def get_status(task_id):
    task_result = celery_app.AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return result



