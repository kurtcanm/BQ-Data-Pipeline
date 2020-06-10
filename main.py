import os, ast
import cx_Oracle
import configparser
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery


def connect_oracle(username, password, dsn, encoding):
    
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


def select_oracle_table(connection, sql):
    
    # Prepare daily sql query
    date = datetime.today() - timedelta(days=1)
    sql += '''where ETL_TARIHI = to_date('{}', 'YYYY-MM-DD')'''.format(date.strftime('%Y-%m-%d'))
    
    
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
            

def bq_get_or_create_table(bigquery_client, dataset_id, table_id, oracle_fields):

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


def export_items_to_bq(table, rows_to_insert):
    
    try:
        bigquery_client.insert_rows(table, rows_to_insert)  
                
    except Exception as e:
        raise
        


def main():
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
        
if __name__ == '__main__':
    main()