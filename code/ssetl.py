import boto3
import pandas as pd
from sqlalchemy import insert,text
from sqlalchemy import select, Table, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import configparser
import datetime
import logging
import sys
import json
import io
import atexit
#import pyodbc
import pymssql

from logging import handlers
# Set Logging Information
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s  %(levelname)-7s - %(name)-13s - %(funcName)-18s - %(lineno)-3d - %(message)s')
log_stringio = io.StringIO()
streamhandler = logging.StreamHandler(log_stringio)
streamhandler.setLevel(logging.INFO)
logger.addHandler(streamhandler)
start = datetime.datetime.now()
print("start",start)



######################################################################################


def get_engine(driver,host,port,username,pwd,database,query=None,**engine_kwargs):

    url=URL.create(drivername=driver,username=username,password=pwd,host=host,port=port,database=database,query=query if query else {})
    engine = create_engine(url, **engine_kwargs)
    end = datetime.datetime.now()
    logger.info(f" time taken : {end-start}")
    return engine

def get_secret(secret_name,region_name):
    """Gets account password from AWS Secrets Manager"""
    import boto3
    import json
   
    secret_name =secret_name 
    region_name = region_name
    sts_client = boto3.client('sts')
    assumed_role_object=sts_client.assume_role(RoleArn="arn:aws:iam::409599951855:role/TIGER_ANALYTICS_DEVELOPER",RoleSessionName="AssumeRoleSession1")
    credentials=assumed_role_object['Credentials']
    secrets_client = boto3.client("secretsmanager",region_name=region_name,
								aws_access_key_id=credentials['AccessKeyId'],
								aws_secret_access_key=credentials['SecretAccessKey'],
								aws_session_token=credentials['SessionToken'],)
    secret = secrets_client.get_secret_value(**{'SecretId': secret_name})['SecretString']
    res=json.loads(secret)
        
    return res 

def get_secret2(secret_name,region_name):
    """Gets account password from AWS Secrets Manager"""
    import boto3
    import json
   
    secret_name =secret_name 
    region_name = region_name
    secrets_client = boto3.client("secretsmanager","us-west-2")
    secret = secrets_client.get_secret_value(**{'SecretId': secret_name})['SecretString']
    res=json.loads(secret)
        
    return res      
#CAST('2007-05-08 12:35:29.123' AS datetime)
def get_query(d1,d2,all_country):

    sql_q= ''' select  upper(addr.SBMT_ISO_2_CHAR_CTRY_CD) as country,lower(replace(pers.PRIM_EMAIL_ADDR_ID, '|','')) as customer_email,  pki.FCPK_SRL_NR as cp_serial_number,   pki.FCPK_PROD_ID as cp_product_id,
    CAST(CONVERT(varchar, pki.REGIS_TS, 23) as date) as cp_registration_date, CAST(CONVERT(varchar, prod.SRVC_STRT_DT, 23) as date ) as cp_start_date,CAST(CONVERT(varchar, prod.SRVC_END_DT, 23) as date ) as cp_end_date,
    pki.REGIS_FCPK_MGMT_PRCS_ID as cp_registration_prcs,CASE WHEN prod.SRVC_INACT_FG = 0  THEN 1 ELSE 0 END as flag_latest_hw,CAST(CONVERT(varchar,pro.WUE_DT, 23) as date ) as exchange_date,
	isnull(pro.PROD_OOS_SRL_NR,'NA') as hw_serial_number,pro.PROD_OOS_PROD_ID as hw_product_id, ref.DRVD_SLDT_CUST_ID as drv_cust,CAST(CONVERT(varchar, pki.PUBL_TS, 23) as date  ) as publ_ts,CAST( pki.LAST_MAINT_TS as datetime) as last_maint_ts_pki,CAST( prod.LAST_MAINT_TS as datetime ) as last_maint_ts_prod, CAST( pro.LAST_MAINT_TS as datetime )as last_maint_ts_pro
    FROM
    [Cams_Reporting].[dbo].[FCPKI_PROD_OOS_S] prod  WITH (NOLOCK)  
    join [Cams_Reporting].[dbo].[PROD_OOS_S] pro  WITH (NOLOCK) on pro.PROD_OOS_KY = prod.PROD_OOS_KY
    join [Cams_Reporting].[dbo].[FCPKI_S] pki  WITH (NOLOCK) on pki.FCPK_SRL_NR = prod.FCPK_SRL_NR
    LEFT JOIN [Cams_Reporting].[dbo].[FCPK_ORD_REF_S] ref WITH (NOLOCK) ON  ref.FCPK_ORD_REF_KY = pki.FCPK_ORD_REF_KY
    left join [Cams_Reporting].[dbo].[FCPKI_OTHR_PTY_ROLE] prole  WITH (NOLOCK) on prole.FCPK_SRL_NR = pki.FCPK_SRL_NR
    LEFT JOIN [Cams_Reporting].[dbo].[FCPK_PERS_CNTCT] pers  WITH (NOLOCK) ON prole.PERS_CNTCT_INT_KY = pers.PERS_CNTCT_INT_KY
    left JOIN [Cams_Reporting].[dbo].[FCPK_SBMT_ADDR] addr  WITH (NOLOCK) on prole.FCPK_SBMT_ADDR_KY = addr.FCPK_SBMT_ADDR_KY
    '''

    if d2 == 'None':
    
        where_clause='''WHERE
        (  ( pki.LAST_MAINT_TS > '{d1}'  )
            or ( prod.LAST_MAINT_TS > '{d1}' )
            or (pro.LAST_MAINT_TS > '{d1}'  )
            )
            and 
        addr.SBMT_ISO_2_CHAR_CTRY_CD IN ({all_country})  and len(pki.FCPK_SRL_NR) > 5   and prole.OTHR_PTY_ROLE_CD = 'ENDCUST'    and FCPKI_ORD_STAT_CD <> 'CANCELLED' 
        and ( CAST(CONVERT(varchar, prod.SRVC_STRT_DT, 23) as date ) > '2015-11-01'  
            or CAST(CONVERT(varchar, prod.SRVC_END_DT, 23) as date ) > '2017-11-01')
        '''.format(d1=d1,all_country=all_country)
  

    else:
            where_clause='''WHERE
        ( (CAST(CONVERT(varchar, pki.LAST_MAINT_TS, 23) as date  )  between '{d1}' and '{d2}' )
            or (CAST(CONVERT(varchar, prod.LAST_MAINT_TS, 23) as date  )  between '{d1}' and '{d2}' )
            or (CAST(CONVERT(varchar, pro.LAST_MAINT_TS, 23) as date  )  between '{d1}' and '{d2}' )
            )
            and 
        addr.SBMT_ISO_2_CHAR_CTRY_CD IN ({all_country})  and len(pki.FCPK_SRL_NR) > 5   and prole.OTHR_PTY_ROLE_CD = 'ENDCUST'    and FCPKI_ORD_STAT_CD <> 'CANCELLED' 
        and ( CAST(CONVERT(varchar, prod.SRVC_STRT_DT, 23) as date ) > '2015-11-01'  
            or CAST(CONVERT(varchar, prod.SRVC_END_DT, 23) as date ) > '2017-11-01')
        '''.format(d1=d1,d2=d2,all_country=all_country)

    sql_query=sql_q + where_clause	
    print(where_clause)

    end = datetime.datetime.now()
    logger.info(f" time taken : {end-start}")
    return sql_query
	
	
def extract(d1,d2,all_country):
    try:
        cams_details=get_secret("cams_password","us-west-2")
        cams_password=cams_details['cams_password']

        source_driver='mssql+pymssql'
        source_host='202207-04p-ag1.corp.hpicloud.net'
        source_port=2048
        source_username='CAMS_C360DE_208925'
        source_db='CAMS_Reporting'
	
        #sql_engine=get_engine(source_driver, source_host, source_port, source_username,cams_password, source_db,query=({'driver':"ODBC Driver 17 for SQL Server",'Trusted_Connection':"no"}))
        
        sql_engine=get_engine(source_driver, source_host, source_port, source_username,cams_password, source_db)
        with sql_engine.begin() as connection:

            sql_query=get_query(d1,d2,all_country)
            select_exp=text(sql_query) 

            df=pd.read_sql_query(select_exp,connection)

            return df

    except Exception as err:
        logger.error(err)
        raise Exception
    finally :
        sql_engine.dispose()
        end = datetime.datetime.now()
        logger.info(f" time taken for extraction : {end-start}")
	
	
def transform(df):
    try:
        from c360.tokenization.tokens import Tokenizer

        #t = Tokenizer(profile_name="default")
        t = Tokenizer()
        logger.info("Dropping duplicates...")
        df=df.sort_values(['last_maint_ts_pki','last_maint_ts_prod','last_maint_ts_pro'],ascending=True).drop_duplicates(subset=['cp_serial_number', 'hw_serial_number','hw_product_id','cp_product_id'], keep='last')
        row_count=len(df.index)
        logger.info(f'No.of records after removing dups : {row_count}')
        end = datetime.datetime.now()
        logger.info(f" time taken : {end-start}")

        logger.info("Tokenezing...")
        df['cp_serial_number'] = t.tokens(df['cp_serial_number'], data_class="serial_number")
        df['hw_serial_number'] = t.tokens(df['hw_serial_number'], data_class="serial_number")
        df['customer_email'] = t.tokens(df['customer_email'], data_class="email")
        df['drv_cust'] = t.tokens(df['drv_cust'], data_class="id")

        end = datetime.datetime.now()
        logger.info(f" time taken for tokenizing: {end-start}")

        return df

    except Exception as err:
        logger.error(err)
        raise Exception
		
def call_stored_proc(d1,d2,pg_engine):
    
    try:
        logger.info("calling sp()...")
        with pg_engine.begin() as conn4:
            stored_proc_command=text(" call cams.cams_master_history_refresh('{d1}','{d2}',array[{all_country}]);""" .format(d1=d1,d2=d2,all_country=all_country))
            print(stored_proc_command)
            #conn4.execute(stored_proc_command) #call storeproc to del all data from master and insert from stage
            end = datetime.datetime.now()
            logger.info(f" time taken for stored proc {d1}-{d2} : {end-start}")
    except Exception as err:
        #trans.rollback()
        logger.error(err)
        raise Exception
	
def load(df):
    try:
        pg_secret_name="rds/delta/ww360/cams"
        pg_region="us-west-2"
        pg_details=get_secret(pg_secret_name,pg_region)
        target_username=pg_details['username']
        target_password=pg_details['password']
        target_db=pg_details['dbname']
        target_port=pg_details['port']
        target_host=pg_details['host']
        target_driver='postgresql+psycopg2'

        logger.info("conv to dict")
        query_result=df.to_dict('records')
        end = datetime.datetime.now()
        logger.info(f" dict conv end : {end-start}")

        pg_engine=get_engine(target_driver, target_host, target_port, target_username,target_password, target_db)
        metadata = MetaData()
        stg_table = Table('cams_stage', metadata,autoload_with=pg_engine,schema ='cams')
        
        with pg_engine.begin() as conn3:
            truncate_query=text("truncate cams.cams_stage")
            conn3.execute(truncate_query)
            end = datetime.datetime.now()
            logger.info(f" time taken for truncating : {end-start}")
            conn3.execute(insert(stg_table),query_result)
            end = datetime.datetime.now()
            logger.info(f" time taken for inserting : {end-start}")
        
	
    except Exception as err:
        logger.error(err)
        raise Exception
    finally:

        pg_engine.dispose()
        end = datetime.datetime.now()
        logger.info(f" time taken for loading : {end-start}")
		

		


if __name__ == '__main__':

    try:
        last_job_run_date=sys.argv[1]  #get from aws batch parameter   
        all_country=sys.argv[2]                  #get from aws batch parameter 
        deltatime=int(sys.argv[3])    #get from aws batch parameter
        start_date=sys.argv[4]         #get from aws batch parameter  
        end_date=sys.argv[5]           #get from aws batch parameter  
		         
        logger.info(f" last_job_run_date {last_job_run_date}")   
        logger.info(f"countrt {all_country}")
		
        if start_date != 'None' and end_date != 'None':   #provide start and end date in airflow if we need to refresh data for any particular time period
            d1=datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            d2=datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        else:  
            d2='None'#datetime.date.today()
            d1=datetime.datetime.strptime(last_job_run_date, "%Y-%m-%d %H:%M:%S")-datetime.timedelta(hours=deltatime)
        logger.info(f"d1:{d1}")
        logger.info(f"d2:{d2}")
        df=extract(d1,d2,all_country)
        row_count=len(df.index)
        logger.info(f'No.of records extracted : {row_count}')
        df=transform(df)
        load(df)

    except Exception as err:
        logger.error(err)
        raise Exception
    finally:
        end = datetime.datetime.now()
        logger.info(f"  executingg finally block: {end-start}")
        logger.info("DONE and exiting")
			
