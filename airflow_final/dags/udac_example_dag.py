from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'ramzi_alaruri',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry':False,
    'retry_delay':300,
    'retries': 3
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_business_to_redshift = StageToRedshiftOperator(
    task_id='Stage_business',
    dag=dag,
    s3_bucket='ramzi-final-project',
    s3_prefix='business',
    table = "staging_business",
    copy_options="FORMAT AS JSON 'auto'"

)


stage_checkin_to_redshift = StageToRedshiftOperator(
    task_id='Stage_checkin',
    dag=dag,
    s3_bucket='ramzi-final-project',
    s3_prefix='checkin',
    table = "staging_checkin",
    copy_options="FORMAT AS JSON 'auto'"

)

stage_tip_to_redshift = StageToRedshiftOperator(
    task_id='Stage_tips',
    dag=dag,
    s3_bucket='ramzi-final-project',
    s3_prefix='tip',
    table = "staging_tip",
    copy_options="FORMAT AS JSON 'auto'"

)

stage_users_to_redshift = StageToRedshiftOperator(
    task_id='Stage_users',
    dag=dag,
    s3_bucket='ramzi-final-project',
    s3_prefix='user',
    table = "staging_users",
    copy_options="FORMAT AS JSON 'auto'"

)

stage_ufos_to_redshift = StageToRedshiftOperator(
    task_id='Stage_ufos',
    dag=dag,
    s3_bucket='ramzi-final-project',
    s3_prefix='ufos',
    table = "staging_ufos",
    copy_options="FORMAT AS CSV",
    include_header = "include_header"


)


load_business_dimension_table = LoadDimensionOperator(
    task_id='Load_business_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.business_table_insert,
    table = "business"
)

load_checkin_dimension_table = LoadDimensionOperator(
    task_id='Load_checkin_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.checkin_table_insert,
    table = "checkin"
)

load_tip_dimension_table = LoadDimensionOperator(
    task_id='Load_tip_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "tip",
    sql_query = SqlQueries.tip_table_insert
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql_query = SqlQueries.users_table_insert
)

load_ufos_dimension_table = LoadDimensionOperator(
    task_id='Load_ufos_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "ufos",
    sql_query = SqlQueries.ufos_table_insert
)

load_business_fact_table = LoadDimensionOperator(
    task_id='Load_business_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "business_facts",
    sql_query = SqlQueries.business_facts_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_quality_data_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = ["business_facts", "business", "checkin", "tip", "users", "ufos"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task

create_tables_task>>stage_business_to_redshift
create_tables_task>>stage_checkin_to_redshift
create_tables_task>>stage_tip_to_redshift
create_tables_task>>stage_users_to_redshift
create_tables_task>>stage_ufos_to_redshift


stage_business_to_redshift>>load_business_dimension_table
stage_checkin_to_redshift>>load_checkin_dimension_table
stage_tip_to_redshift>>load_tip_dimension_table
stage_users_to_redshift>>load_users_dimension_table
stage_ufos_to_redshift>>load_ufos_dimension_table


load_business_dimension_table>>load_business_fact_table
load_checkin_dimension_table>>load_business_fact_table
load_tip_dimension_table>>load_business_fact_table
load_users_dimension_table>>load_business_fact_table
load_ufos_dimension_table>>load_business_fact_table


load_business_dimension_table>>run_quality_checks
load_checkin_dimension_table>>run_quality_checks
load_tip_dimension_table>>run_quality_checks
load_users_dimension_table>>run_quality_checks
load_ufos_dimension_table>>run_quality_checks


run_quality_checks>>end_operator
load_business_fact_table>>end_operator


"""
Control Flow we want to
 1. Begin_execution
 2. Run in parellel stage_events + stage_songs
 3. have created the fact table
 4. load the dimesional tables (4 of them) in parellel
 5. run the data run_quality_checks
 6. end the execution
"""
