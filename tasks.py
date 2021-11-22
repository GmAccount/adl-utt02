import os,time, traceback, datetime
###from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from celery import Celery, current_task
from celery.exceptions import  Reject
import os
from pyspark import SparkConf, SparkContext


import findspark
findspark.init('/home/hadoop/spark/')
from pyspark.sql import SparkSession


os.environ['SPARK_HOME'] = "/home/hadoop/spark/"
os.environ['HADOOP_CONF_DIR'] = '/home/hadoop/hadoop/etc/hadoop'




celery_app = Celery('tasks', broker='redis://localhost:6379/1', backend='redis://localhost:6379/1')
celery_app.autodiscover_tasks()


@celery_app.task(name="filter_items")
def filter_items(uri_hdfs_base, partitions, item_dict):
    result = {}
    error = ''
    task_full_state = {"worker_name": filter_items.request.hostname, 
                       "date_begin": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                       "task_name": "filter_items",
                       "items": {}
                       }
    try:

        """"
        conf = SparkConf().setAppName("adl-utt-pyspark") \
                          #.set("spark.shuffle.service.enabled", "true") \
                          #.set("spark.dynamicAllocation.enabled", "true") \
                          .set('spark.executor.cores', '3') \
                          .set("spark.executor.memory", "6000m") \
                          .set('spark.num.executors','3') \
                          .set("spark.driver.memory", "2g") \
                          .setMaster("yarn")

                          #.setMaster("spark://host01.adl-utt.net:7077")
        """




        spark = SparkSession.builder.appName('adl-utt-project01').master("yarn").config("spark.sql.parquet.enableVectorizedReader","false").config('spark.executor.cores', '8').config('spark.num.executors','4').config("spark.driver.memory", "2g").getOrCreate()
       
        print(uri_hdfs_base)
        print(item_dict)

        filter_items.update_state(state='PROGRESS', meta=task_full_state)
        parDF=spark.read.parquet(uri_hdfs_base)

        item_dict_filter = {k: v for k, v in item_dict.items() if v is not None}

        #print(item_dict_filter)


        var_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        outfile = '/tmp/outfile_'+var_str+'.csv'
   

        if parDF:
            parDF.createOrReplaceTempView("filterTable")
            query_options= ''

            #print(partitions)
            #print(item_dict)

        ######

            list_param_where = []
            for param in item_dict:
                if param not in partitions:

                    if item_dict[param] is not None:
                        param_val = item_dict[param]

                        if type(param_val) == str: param_val = "'"+param_val+"'"
                        if type(param_val) == int: param_val = ""+str(param_val)+""

                        list_param_where.append(param+'='+str(param_val))

            if len(list_param_where)>0:
                query_options=" where "+" and ".join(list_param_where)


            query = "select revenue,date,dt_action,isp_id,isp_name,vertical_name,gender,age,cc,city,zip,stat,os_family,os_version_string,device_family,device_brand,device_model,is_mobile,is_tablet,is_touch_capable,is_pc,price_device,weekday from filterTable  "+query_options

            print(query)
            parkSQL = spark.sql(query)

            print(parkSQL.show(10))

            
            df = parkSQL.toPandas()
            df = df.fillna('')


            #print(df.show(10))
            df.to_csv(outfile)
            count_res = len(df)
        
            result = df.to_dict()
            #print(result)
            
            task_full_state.update({"items_count":count_res})
            
        filter_items.update_state(state='SUCCESS', meta=task_full_state)
     
            
        
    except Exception as e:
        error = traceback.format_exc() if type(e).__name__ != 'AnalysisException' else 'No partition matching provided params!'
        print(error)
        task_full_state['exc_type'] =  type(e).__name__
        task_full_state['exc_message'] = error
        filter_items.update_state(state='FAILURE', meta=task_full_state)
        raise Reject(reason=error)
    return task_full_state

