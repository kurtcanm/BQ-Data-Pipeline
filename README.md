# Data Pipeline Between Oracle DWH and GCP BigQuery DWH

### Local Development Environment
```console
$ lsb_release -a | grep Description
Description:	Ubuntu 18.04.3 LTS
```
### Installing Cloud SDK for Ubuntu and Initialization of Project

Add package source of Cloud SDK distribution URI  
```console
$ echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
```
Install apt-transport-https and import the Google Cloud public key
```console
$ sudo apt-get install apt-transport-https ca-certificates gnupg
$ curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
```
Update and install the Cloud SDK
```console
$ sudo apt-get update && sudo apt-get install google-cloud-sdk
```

Install Python SDK component and initialize project

```console
$ sudo apt install google-cloud-sdk-app-engine-python
$ gcloud init
```

Query the project info

```console
$ gcloud projects list --format="table(name, project_id, createTime.date(tz=LOCAL))"
    
	NAME               PROJECT_ID            CREATE_TIME
n11-test-platform  n11-test-platform     2020-06-05T22:35:46
```
Enable Cloud Functions, BigQuery,  App Engine Admin, Cloud Scheduler, Cloud Pub/Sub API's from GCP Console

### Create Python environment for test the code
```console
$ cd your-project
$ python3.7 -m virtualenv venv
$ source venv/bin/activate
(env)$ pip install google-cloud-bigquery
```
### Installing cx_Oracle and necessary packages
```console
$ sudo apt-get install build-essential unzip python-dev libaio-dev
```

Download linux instantclient from [here](http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html) 

In order to point $ORACLE_HOME path, unzip the files in the somewhere in local and folder was instantclient_12_2 for the case.

```console
$ export ORACLE_HOME=$(pwd)/instantclient_12_2
```

Create a symlink to  SO file. Version number 12.1 for the case.
```console
$ cd $ORACLE_HOME
$ ln -s libclntsh.so.12.1   libclntsh.so  
```

Update ~/.bashrc to avoid manuel EXPORT commands
```console
$ vim ~/.bashrc

export ORACLE_HOME=/location/of/instantclient_12_2
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
```

 Add instantclient path to the conf file via vim, then update the ldpath
```console
$ vim /etc/ld.so.conf.d/oracle.conf
$ sudo ldconfig
```

Install cx_oracle module for the created Python environment:
```console
(env)$ pip install cx_oracle
```
### Configurations and Methods

GCP BigQuery  and Oracle information for Python application
```ini
[BQ]
CRED: GOOGLE_APPLICATION_CREDENTIALS
CRED_PATH: credentials.json
DATASET: n11_VA_tablolari
TABLE: siparis_satis_kalemleri
DB: my_database
MODE: REQUIRED

[ORACLE]
USERNAME: admin
PASSWORD: p@ssw0rd
DSN: oracle-test-database-1.*****.eu-central-1.rds.amazonaws.com:1521/orcl
ENCODINGS: UTF-8
SQL: select * from dwh.siparis_satis_kalemleri
FIELDS: [
        ('SIPARIS_KALEM_ID','INTEGER'),
        ('ADET','INTEGER'),
        ('IPTAL_ADET','INTEGER'),
        ('IADE_ADET','INTEGER'),
        ('FIYAT','NUMERIC'),
        ('SATICI_INDIRIM','NUMERIC'),
        ('SEPET_KAMPANYA_FIYATI','NUMERIC'),
        ('BASKET_CAMPAIGN_PRICE','NUMERIC'),
        ('F_GMV','NUMERIC'),
        ('F_SUBVANSIYON','NUMERIC'),
        ('ETL_TARIHI','DATE'),
        ('MUSTERI_ID','INTEGER'),
        ('SATICI_ID','INTEGER'),
        ('URUN_ID','INTEGER'),
        ('KATEGORI_ID','INTEGER'),
        ('ODENEN_TUTAR','NUMERIC'),
        ('KUPON_ID','INTEGER'),
        ('SIPARIS_STATU','STRING'),
        ('SIPARIS_KALEMI_STATU','STRING'),
        ('SATINALMA_TARIHI','TIMESTAMP'),
        ('SATINALMA_TARIHI_DT','DATE'),
        ('SIPARIS_ID','INTEGER'),
        ('SIPARIS_NUMARASI','STRING')
        ]
```
Import Python libraries
```python
import os, ast
import cx_Oracle
import configparser
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
```

Oracle connection example
```python
try:
    # Connect to the Oracle Database
    connection = cx_Oracle.Connection(
        username,
        password,
        dsn,
        encoding=encoding
    )    
    return connection

except Exception as e:
        raise
```

Prepare SQL query for t-1 migration 
```python
date = datetime.today() - timedelta(days=1)
sql += '''where ETL_TARIHI = to_date('{}', 'YYYY-MM-DD')'''.format(date.strftime('%Y-%m-%d'))
```
SQL execution according to prepared statement
```python
try:
    with connection.cursor() as cursor:        
        # Execute the SQL statement
        cursor.execute(sql)
        
        # Fetch all rows
        rows = cursor.fetchall()

        return rows

except Exception as e:
    raise
    
finally:
    # Release the connection
    if connection:
        connection.close()
```

Create or get table
```python
try:
    # API call
    table = bigquery_client.get_table(table_ref)
    
except:
    # Create table schema
    schema = [ bigquery.SchemaField(field[0], field[1], mode) for field in oracle_fields]
    
    # Create table components
    table = bigquery.Table(table_ref, schema = schema)
    
    # Create table API call
    table = bigquery_client.create_table(table)
    
finally:        
    return table

```

Stream data to BigQuery table
```python
try:
    bigquery_client.insert_rows(table, rows_to_insert)  
            
except Exception as e:
    raise
```

Run the code!
```python
# Parse config file
config = configparser.ConfigParser()
config.read('config.ini')

# Create log file
logging.basicConfig(filename = 'app.log', filemode = 'a')

# Set OS environment set for bq credentials
os.environ[credentials] = credentials_file_path

try:
    
    # Create Oracle connection
    oracle_connection = connect_oracle(username, password, dsn, encoding)
    
    # Fetch from Oracle table
    select_oracle_table(connection, sql)
    
    # Prepare a reference to the dataset
    dataset_ref = bigquery_client.dataset( config.get('BQ','DATASET') )

    # Prepare a reference to the table
    table_ref = dataset_ref.table( config.get('BQ','TABLE') )

    # Prepare bq schema
    oracle_schema = ast.literal_eval( config.get('ORACLE','FIELDS') )

    # Get bq table
    bq_table = bq_get_or_create_table(bigquery_client, dataset_id, table_id, oracle_fields)
    
    # Stream fetched data into bq table
    export_items_to_bq(bq_table, rows_to_insert)
    
    
except Exception as e:
    # Catch and log if error raises
    logging.error(e)
```

### Code Deploy and Schedule the job

Deploy Python file
```console
$ gcloud functions deploy [FUNCTION_NAME] --entry-point main --runtime python37 --trigger-resource [TOPIC_NAME] --trigger-event google.pubsub.topic.publish --timeout 600s
```

```console
$ gcloud functions deploy oracle-to-bq --entry-point main --runtime python37 --trigger-resource bq-test-topic --trigger-event google.pubsub.topic.publish --timeout 600s

Deploying function (may take a while - up to 2 minutes)...done.                                                                                                                                                                              
availableMemoryMb: 256
entryPoint: main
eventTrigger:
  eventType: google.pubsub.topic.publish
  failurePolicy: {}
  resource: projects/n11-test-platform/topics/bq-test-topic
  service: pubsub.googleapis.com
ingressSettings: ALLOW_ALL
labels:
  deployment-tool: cli-gcloud
name: projects/n11-test-platform/locations/us-central1/functions/oracle-to-bq
runtime: python37
serviceAccountEmail: ****
sourceUploadUrl: *****
status: ACTIVE
timeout: 600s
updateTime: '2020-06-08T21:23:11.207Z'
versionId: '3'
```

Schedule the job 
```console
$ gcloud scheduler jobs create pubsub [JOB_NAME] --schedule [SCHEDULE] --topic [TOPIC_NAME] --message-body [MESSAGE_BODY]
```

```console
$ gcloud scheduler jobs create pubsub daily_job --schedule  "15 3 * * *" --topic bq-test-topic --message-body "This job runs once per day at 03:15AM"

Creating App Engine application in project [n11-test-platform] and region [asia-east2]....done.                                                                                                                                              
name: projects/n11-test-platform/locations/asia-east2/jobs/daily_job
pubsubTarget:
  data: ****
  topicName: projects/n11-test-platform/topics/bq-test-topic
retryConfig:
  maxBackoffDuration: 3600s
  maxDoublings: 16
  maxRetryDuration: 0s
  minBackoffDuration: 5s
schedule: 15 3 * * *
state: ENABLED
timeZone: Etc/UTC
userUpdateTime: '2020-06-08T21:37:50Z'

```
