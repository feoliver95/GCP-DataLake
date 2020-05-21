from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
DataProcPySparkOperator, DataprocClusterDeleteOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators import gcs_to_bq
from airflow.operators import BashOperator, PythonOperator, bash_operator
from airflow.utils.trigger_rule import TriggerRule
from airflow import models
from datetime import datetime
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


#PROJETO_ID
project_id="dots-project-18052020"

#NOME DO BUCKET C
bucketname="dotz-project"

#NOME DO DATASET STAGE
nome_dataset="dotz_lake"

#NOME DO DATASET DE VISUALIZACAO
nome_dataset_visualizacao="visualizacao"








now =datetime.now()

dia=now.strftime("_%d%m%Y")

pasta_carga="carga"+dia+"/"
#gs://dots-project/dots-stage-parquet/carga_19052020/
source_file="origem-stage-parquet/{}".format(pasta_carga)

caminho_jobs="gs://"+bucketname+"/jobs_dataproc/"


region="southamerica-east1"
zone="southamerica-east1-a"







default_args = {
            'owner': 'Felipe Oliveira',
            'depends_on_past': False,
            'start_date':airflow.utils.dates.days_ago(1),
            'retries': 1,
            'retry_delay': timedelta(minutes=2),
            'dataflow_default_options': {
                'project': project_id,
                'region': 'us-central1',
                'zone': "us-central1"
            }
        }

dag = DAG(
    'workflow-dotz',
    default_args=default_args,
    description='workflow-dotz',
    schedule_interval="0 4 * * *"
    )



create_cluster = DataprocClusterCreateOperator(
        task_id='cria-cluster',
        project_id=project_id,
        master_machine_type="n1-standard-1",
        worker_machine_type="n1-standard-1",
        cluster_name='parquet-converter',
        num_workers=2,
        region=region,
        zone=zone,
        master_disk_size=15,
        dag=dag

)


job_price_quote= DataProcPySparkOperator(
        task_id="job_price_quote",
        main=caminho_jobs+"CsvToParquet-price_quote_composer.py",
        cluster_name='parquet-converter',
        region=region,
        dag=dag
    )

job_comp_boss= DataProcPySparkOperator(
        task_id="job_comp_boss",
        main=caminho_jobs+"CsvToParquet-comp_boss_composer.py",
        cluster_name='parquet-converter',
        region=region,
        dag=dag
    )

job_bills_of_materials= DataProcPySparkOperator(
        task_id="job_bills_of_materials",
        main=caminho_jobs+"CsvToParquet-bill_of_materials_composer.py",
        cluster_name='parquet-converter',
        region=region,
        dag=dag
    )

load_price_quote= GoogleCloudStorageToBigQueryOperator(
        task_id="load_price_quote",
        bucket=bucketname,
        source_objects=[source_file+'price_quote/*.parquet'],
        source_format='PARQUET',
        create_disposition = 'CREATE_IF_NEEDED',
        destination_project_dataset_table= project_id+":"+nome_dataset+".price_quote",
        write_disposition='WRITE_APPEND',
        time_partitioning={"type":"DAY"},
        autodetect=True,
        ignore_unknown_values = True,
        #cluster_fields= True,
        dag=dag)


load_comp_boss= GoogleCloudStorageToBigQueryOperator(
        task_id="load_comp_boss",
        bucket=bucketname,
        source_objects=[source_file+'comp_boss/*.parquet'],
        source_format='PARQUET',
        create_disposition = 'CREATE_IF_NEEDED',
        destination_project_dataset_table= project_id+":"+nome_dataset+".comp_boss",
        write_disposition='WRITE_APPEND',
        time_partitioning={"type":"DAY"},
        autodetect=True,
        ignore_unknown_values = True,
        #cluster_fields= True,
        dag=dag)


load_bills_of_materials= GoogleCloudStorageToBigQueryOperator(
        task_id="load_bills_of_materials",
        bucket=bucketname,
        source_objects=[source_file+'bill_of_materials/*.parquet'],
        source_format='PARQUET',
        create_disposition = 'CREATE_IF_NEEDED',
        destination_project_dataset_table= project_id+":"+nome_dataset+".bills_of_materials",
        write_disposition='WRITE_APPEND',
        time_partitioning={"type":"DAY"},
        autodetect=True,
        ignore_unknown_values = True,
        #cluster_fields= True,
        dag=dag)

bill_of_materials_unpivot = BigQueryOperator(
    task_id="bill_of_materials_unpivot",
    bql="""

with bills as (
SELECT tube_assembly_id , 
 [
  
  STRUCT(component_id_1 as componente, quantity_1 as quantidade),
    STRUCT(component_id_2  as componente, quantity_2 as quantidade),
    STRUCT(component_id_3  as componente, quantity_3 as quantidade) ,
  STRUCT(component_id_4  as componente, quantity_4 as quantidade),
   STRUCT(component_id_5  as componente, quantity_5 as quantidade),
    STRUCT(component_id_6  as componente, quantity_6 as quantidade),
    STRUCT(component_id_7  as componente, quantity_7 as quantidade),
   STRUCT(component_id_8  as componente, quantity_8 as quantidade)
  
  ] as componentes
  from `dotz_views.bills_of_materials_view`  
  )
  
SELECT
  tube_assembly_id ,
  metricas

FROM
  bills
CROSS JOIN
  UNNEST(bills.componentes) AS metricas

  


  """,
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
    destination_dataset_table=project_id+":"+nome_dataset_visualizacao+".bill_of_materials_unpivot",
    allow_large_results=True,
    dag=dag
        )

flat_grafico = BigQueryOperator(
    task_id="flat_grafico",
    bql="""

  SELECT
  pq.tube_assembly_id price_id,
  supplier,
  quote_date,
  annual_usage,
  min_order_quantity,
  bracket_pricing,
  quantity,
  cost,
  nm.*
FROM
  `dotz_views.price_quote_view` AS pq
LEFT JOIN
  `visualizacao.nested_materials` AS nm
ON
  (pq.tube_assembly_id = nm.tube_assembly_id )
ORDER BY
  nm.tube_assembly_id DESC
  """,
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
    destination_dataset_table=project_id+":"+nome_dataset_visualizacao+".flat_grafico",
    allow_large_results=True,
    dag=dag
        )

nested_materials = BigQueryOperator(
    task_id="nested_materials",
    bql="""

  with nested_materials as (
select * from `visualizacao.bill_of_materials_unpivot`  as materials_unpivot
left join dotz_views.comp_boss_view as cb on ( cb.component_id  =  materials_unpivot.metricas.componente )
where component_id is not null
order by  component_id desc

)

SELECT 
    tube_assembly_id, 
   
    ARRAY_AGG(STRUCT( metricas.componente, metricas.quantidade, component_id, component_type_id, type, connection_type_id,
    outside_shape, base_type , height_over_tube, bolt_pattern_long, bolt_pattern_wide, groove, base_diameter, shoulder_diameter, unique_feature, orientation, weight)) AS attributes
from nested_materials
group by tube_assembly_id
order by tube_assembly_id
  """,
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
    destination_dataset_table=project_id+":"+nome_dataset_visualizacao+".nested_materials",
    allow_large_results=True,
    dag=dag
        )


delete_cluster = DataprocClusterDeleteOperator(
        task_id='deleta-cluster',
        project_id=project_id,
        cluster_name='parquet-converter',
        region=region,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )


create_cluster >>  job_bills_of_materials >> job_comp_boss >> job_price_quote >> [delete_cluster,load_bills_of_materials,load_comp_boss,load_price_quote] \
>> bill_of_materials_unpivot >> nested_materials >> flat_grafico