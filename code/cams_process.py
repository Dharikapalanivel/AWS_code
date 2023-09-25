from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3
from sqlalchemy import create_engine
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import date

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 30),
    'email': ['dharika.palanivel@hp.com','jiyad.basheer@hp.com','aditi.verma@hp.com'],
    'email_on_failure': True,
    #'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    #'execution_timeout': timedelta(minutes=60),
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),

}


EMEA="BH,GQ,AM,HR,IR,IS,AO,BI,GE,NC,NL,UZ,AX,BA,FR,GL,GN,JO,MT,AT,CF,CY,LS,TM,UA,BE,DZ,GW,MG,SK,SM,HU,MK,TG,TZ,BL,FO,GP,MW,BW,CZ,LI,MC,OM,SZ,BJ,IE,NO,SL,IL,KE,LV,DK,MA,TN,UG,AE,DE,GI,LR,LT,ME,SY,BG,DJ,GF,GR,NG,ST,AL,EE,ES,FI,TR,AD,CI,EMEAKZ,LY,MR,SI,TJ,VA,YE,CD,EG,BF,BY,MU,SJ,SO,ZW,CH,GH,GM,IT,LB,ML,TD,AZ,CV,ET,IQ,KW,LU,CM,ER,GA,MZ,NE,ZA,ZM,CG,KM,MQ,SN,GB,KG,MD,NA,PL,PT,QA,SD,RO,RU,PF,SC,SE,PS,RS,SA,RE,RW"
AMS="CO,DM,TC,AG,AN,GT,BM,MS,KN,PA,SV,AW,DO,NI,BZ,CR,GD,JM,AI,BO,CL,VC,VI,KY,BB,TT,AR,MX,GY,SR,VG,EC,HN,US,VE,BS,CA,UY,BR,GU,HT,LC,PM,PY,PE,PR"
APJ="IN,MM,JP,VN,MY,TW,NP,KR,KH,WF,LA,MV,HK,MO,NZ,AF,BN,FJ,TH,LK,MH,ID,MN,AU,BD,BT,CN,WS,PG,SG,PK,PH"

EMEA_LIST= EMEA.split(',')
AMS_LIST= AMS.split(',')
APJ_LIST= APJ.split(',')

EMEA_country=','.join("'{0}'".format(x) for x in EMEA_LIST)
AMS_country=','.join("'{0}'".format(x) for x in AMS_LIST)
APJ_country=','.join("'{0}'".format(x) for x in APJ_LIST)
all_country=EMEA_country+','+AMS_country+','+APJ_country




def get_date(**context):
    
    sql=""" SELECT date_trunc('second', last_job_run_date) as  last_job_run_date FROM cams.cams_job_run_info """
    ph_hook=PostgresHook(postgres_conn_id="cams_pg")
    conn = ph_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    result=cursor.fetchone()[0]
    context['ti'].xcom_push(key="last_job_run_date",  value=str(result))
    print(result)
    

with DAG(dag_id='cams_process',
         max_active_runs=1,
         concurrency=30,
         catchup=False,
         dagrun_timeout=timedelta(minutes=120),
         default_args=default_args,
         schedule_interval='7 0 * * *') as dag:

    Cams_Start = DummyOperator(task_id='Cams_Start')

    Cams_Done = DummyOperator(task_id='Cams_Done')
	
    date_task=PythonOperator(task_id="last_job_run_date",python_callable=get_date,provide_context=True)
    
    script_bucket_prefix = "c360-airflow-dags-dev/care_pack"
    script_file_name = "code.zip"
       
    ssetl = AWSBatchOperator(
        task_id="ssetl",
        job_name="cams_etl",
        job_definition="c360_batch_job",
        job_queue="c360_jobs_queue",
	provide_context=True,
        parameters={'job_run_date':"{{ ti.xcom_pull(task_ids='last_job_run_date', key='last_job_run_date') }}" ,'country':all_country,'delta_hours':'2','start_date':'None','end_date':'None'},

        overrides={
            'vcpus': 1,
            'memory': 10000,
            'command': ['cams_ssetl.py',"Ref::job_run_date","Ref::country","Ref::delta_hours","Ref::start_date","Ref::end_date"], 
            'environment': [
            {
                'name': 'BATCH_FILE_S3_URL',
                'value': "s3://" + script_bucket_prefix + "/" + script_file_name
            },
            {'name': 'BATCH_FILE_TYPE','value': 'py_zip'}#,{'name':'country','value' :all_country}
			
			]
        
        }
    )
    email = AWSBatchOperator(
        task_id="email",
        job_name="cams_etl",
        job_definition="c360_batch_job",
        job_queue="c360_jobs_queue",
        provide_context=True,
        parameters={},
        overrides={
            'vcpus': 1,
            'memory': 1024,
            'command': ['cams_email.py'], 
            'environment': [
            {
                'name': 'BATCH_FILE_S3_URL',
                'value': "s3://c360-airflow-dags-dev/care_pack/cams_email.py"
            },
            {'name': 'BATCH_FILE_TYPE','value': 'py'}
            ]
            }
        )

    update_cams_job_run_info = PostgresOperator(
            task_id = 'cams_job_run_info',
            sql = """ update cams.cams_job_run_info
            set last_job_run_date =(select greatest(max(last_maint_ts_pki),max(last_maint_ts_prod),max(last_maint_ts_pro)) from cams.cams_stage) 
            where job_name ='Cams_process'; """,
            postgres_conn_id = 'cams_pg',
            autocommit = True)
  
    
    call_master_sp = PostgresOperator(
            task_id = 'call_master_sp',
            sql = 'call cams.load_cams_pg()',
            postgres_conn_id = 'cams_pg',
            autocommit = True)
    audit_script_file_name='cams_audit.zip'       
    audit_summary = AWSBatchOperator(
        task_id="audit_summary",
        job_name="cams_audit",
        job_definition="c360_batch_job",
        job_queue="c360_jobs_queue",
        provide_context=True,
        parameters={},       
        overrides={
            'vcpus': 1,
            'memory': 1026,
            'command': ['cams_audit.py'], 
            'environment': [
            {
                'name': 'BATCH_FILE_S3_URL',
                'value': "s3://" + script_bucket_prefix + "/" + audit_script_file_name
            },
            {'name': 'BATCH_FILE_TYPE','value': 'py_zip'}#,{'name':'country','value' :all_country}
			
			]
        
        }
    )
            
    """
    audit_summary =BashOperator(task_id='audit_summary' ,bash_command="python /usr/local/airflow/dags/scripts/cams_audit.py "  )
        
        
    
    call_history_sp = PostgresOperator(
            task_id = 'call_history_sp',
            sql = 'call cams.load_history_wk()',
            postgres_conn_id = 'cams_pg',
            autocommit = True)
    """
    
    
    
    

    

#Cams_Start >>  Cams_stage >> ssetl>>call_master_sp >> call_history_sp >> Cams_Done

Cams_Start >>  date_task >>  ssetl  >> call_master_sp >>  update_cams_job_run_info >> audit_summary >> email >> Cams_Done