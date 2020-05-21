# Teste Dotz - DataLake

## Arquitetura
Solução de DataLake usando serviços de nuvem - Google Cloud Platform, com melhores praticas de desenvolvimento.


Utilizaremos as soluções:

* Composer (Apache Airflow ) - Para orquestração dos nossos jobs de transformação e carregamento.
* Storage : Armazenamento dos arquivos de origem.

* DataProc (Pyspark) - Para execução de Job Pyspark para transformação de arquivo CSV para Parquet, para os seguintes beneficios:
	* Detecção de esquema. Os dados em ‘Parquet’ já vem mapeados. Em um CSV nunca sabemos que tipo de valores estão contidos em uma coluna, se são texto, números, fatores, datas etc, no Parquet os dados já vem mapeados o que facilita demais
	* Suporte a Estruturas de colunas encadeadas (Caso exista)
	* Performace de escrita no BigQuery
	

	
* Big Query - Banco de Dados - DataLake. Utilizaremos 3 camadas
* 1 - Camada Stage - onde os dados serão armazenados em tabelas particionadas por data de ingestao(timestamp)
* 2 - Camada de View Deduplicadora - Onde os dados serão deduplicados, pegando somente dados da partição da ultima data de ingestão.
* 3- Camada Visualização - Tabelas materializada com seus respectivos Joins , para consumo dos Dashboard envolvido ( Sendo assim os joins serão realizados uma unica vez e o dashboard não precisara realizar os joins(que não é bom no bigquery) para gerar sua visão de negocio.

### Pré requisitos

* Conta Google Cloud Platform
* IDLE de sua preferencia para editar codigo Python

### Configurando Ambiente GCP

* Dentro do ambiente GCP em Storage , criar um Bucket com o nome desejado e anotar esse nome porque vamos utilizar posteriormente em variáveis em nossos jobs Pyspark e Composer. Dentro do Storage Criar as seguintes pastas:
	* origem-stage
	* origem-stage-parquet
	* jobs_dataproc

* Ativar API do composer
* Ativar API Dataproc no GCP
* Ativar API BigQuery , criaremos 3 tabelas posteriormente:, tab
	* dotz_lake
	* dotz_views
	* visualizacao


### Criando Datasets no BigQuery

* Copie cada bloco e execute no cloud shell para criar seus respectivos datasets
Dataset dotz_lake

	    bq mk \
	    --dataset \
	    dotz_lake 
	    
	 Dataset dotz_views
	 
	    bq mk \
	    --dataset \
	    dotz_views 
	Dataset visualizacao

	    bq mk \
	    --dataset \
	    visualizacao
	        
### Criando Tabelas no BigQuery

* Copie cada bloco e execute no cloud shell para criar suas respectivas tabelas
Tabela bills_of_materials

		bq mk \
		--table \
		--time_partitioning_type=DAY \
		dotz_lake.bills_of_materials 
		
	Tabela comp_boss

		bq mk \
		--table \
		--time_partitioning_type=DAY \
		dotz_lake.comp_boss 
	Tabela price_quote

		bq mk \
		--table \
		--time_partitioning_type=DAY \
		dotz_lake.price_quote 

 
    


### Alterando parametros jobs Pyspark e fazendo as importações dos arquivos de origem e de jobs.
* Na pasta **files** temos nossos arquivos de origem, fazer a importação desses arquivos para o Storage na pasta **origem-stage'**

* Na  pasta '**jobs_dataproc'** temos arquivos com jobs Pyspark. Arquivos: **CsvToParquet-bill_of_materials.py,** **CsvToParquet-comp_boss.py**, **CsvToParquet-price_quote.p**y. Para cada um deles:
	* Abrir o Arquivo e alterar a variável '**bucketname'** para o nome do bucket que foi criado anteriormente
	* Após realizar as alteraçoes, no storage na pasta criada "**jobs_dataproc"** fazer a importação desses arquivos.

## Executando Job de ingestão (sem o Composer)

### Execução do Job Pyspark de conversão de arquivos CSV para parquet.
**Obs**: Caso queira pular essa tapa de Dataproc, na pasta **files_parquet** tem a pasta '**carga**', nela tem os arquivos já transformado. pode realizar o upload manualmente no Storage na pasta **origem-stage-parquet**/e lá e fazer o upload.
	
1 - Criar maquina do Dataproc. Execute os comandos abaixo:
obs: altere o parametro do bucket para o nome do bucket que foi criado.

    gcloud dataproc clusters create parquet-converter \
        --master-machine-type n1-standard-2 \
        --worker-machine-type n1-standard-2 \
    	--region=us-central1 --zone=us-central1-a \
    	--bucket=dots-project


2 - Para enviar os jobs Pyspark utilize os comandos
* job bill_of_materials
    		
 

        gcloud dataproc jobs submit pyspark --cluster=parquet-converter \
              gs://dots-project/jobs_dataproc/CsvToParquet-bill_of_materials.py --region=us-central1
 * Job price_quote

	

		  gcloud dataproc jobs submit pyspark --cluster=parquet-converter \
    		 gs://dots-project/jobs_dataproc/CsvToParquet-price_quote.py --region=us-central1
          
* Job comp_boss


   

      gcloud dataproc jobs submit pyspark --cluster=parquet-converter \
          gs://dots-project/jobs_dataproc/CsvToParquet-comp_boss.py --region=us-central1

* Deletando Cluster

		 gcloud dataproc clusters delete parquet-converter --region=us-central1

## Ingestão de dados no BigQuery.

Usando Cloud Shell vamos realizar a ingestão dos dados a partir dos arquivos parquet gerados no job pyspark anteriormente.

Ingestão bill of materials

    bq load \
        --source_format=PARQUET \
    	--time_partitioning_type=DAY \
        dotz_lake.bills_of_materials \
        gs://dots-project/origem-stage-parquet/carga/bill_of_materials/*.parquet

Ingestão price_quote

    bq load \
        --source_format=PARQUET \
    	--time_partitioning_type=DAY \
        dotz_lake.price_quote \
        gs://dots-project/origem-stage-parquet/carga/price_quote/*.parquet

Ingestão comp_boss

    bq load \
        --source_format=PARQUET \
    	--time_partitioning_type=DAY \
        dotz_lake.comp_boss \
        gs://dots-project/origem-stage-parquet/carga/comp_boss/*.parquet
	
 ### Criando Views deduplicadoras no BigQuery

* Copie cada bloco e execute no cloud shell para criar suas respectivas views

	comp_boss view

    	bq mk \
    	--use_legacy_sql=false \
    	--description "description" \
    	--view 'SELECT *
    	FROM `dotz_lake.comp_boss`    
    	WHERE DATE(_PARTITIONTIME) = DATE((SELECT max(_PARTITIONTIME) as maximo from `dotz_lake.comp_boss` )) ' \
    	dotz_views.comp_boss_view


    price_quote view
    
    	bq mk \
    	--use_legacy_sql=false \
    	--description "description" \
    	--view 'SELECT *
    	FROM `dotz_lake.price_quote`  
    	WHERE DATE(_PARTITIONTIME) = DATE((SELECT max(_PARTITIONTIME) as maximo from `dotz_lake.price_quote` )) ' \
    	dotz_views.price_quote_view
    	




	bill_of_materials view


		bq mk \
	        	--use_legacy_sql=false \
	        	--description "description" \
	        	--view 'SELECT *
	        	FROM `dotz_lake.bills_of_materials` 
	        	WHERE DATE(_PARTITIONTIME) = DATE((SELECT max(_PARTITIONTIME) as maximo from `dotz_lake.bills_of_materials`)) ' \
	        	dotz_views.bills_of_materials_view

### Criando camada de Visualização, tabelas materializadas com joins.

**PROBLEMA ENCONTRADO:** Identificado que a tabela bills_of_materials tem em colunas 8 component_id e quantity diferente, para cada coluna de component era uma ligação/informação diferente que se relaciona com a tabela comp_boss que contem informações do componente do cubo.

**SOLUÇÂO:**


Vamos realizar unpivot dos campos component_id e quantity da tabela bills_of_materials, para conseguirmos fazer os joins com seus respectivos id na tabela comp_boss.
Tabela: bill_of_materials_unpivot

    bq query \
    --destination_table visualizacao.bill_of_materials_unpivot \
    --use_legacy_sql=false \
    --replace=True \
    'with bills as (
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
      UNNEST(bills.componentes) AS metricas'
	    
    
 Quando foi feito o unpivot da tabela bills_of_materials, a tabela passou a ter mais linha, sendo que cada "cubo" tem mais de um componente, cada linha do tubo material tem 8 colunas de components_id, com o unpivot a quantidade de linhas dessa tabela passou ser 8x maior.
 Solução: 1 - com a tabela 'unpivot' vamos realizar o join com a tabela comp_boss para obtermos as informações de cada componente
 2 - Com as informações dos componente, vamos estruturar os valores de array, usando como chave o id do cubo_material, onde que no resultados teremos informações para cada cubo um array com informações dos materiais utilizadados.
 Vamos guardar o resultado em uma tabela chamada: nested_materials onde contem a coluna chave do cubo mais o array com informações dos componentes.
 

    bq query \
    --destination_table visualizacao.nested_materials \
    --use_legacy_sql=false \
    --replace=True \
    'with nested_materials as (
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
    order by tube_assembly_id'

Agora sim, podemos realizar o join da tabela price_quote com a tabela nasted_materials. Onde teremos as informações das cotas do tubo e um ARRAY com informações do componente daquele respectivo cubo gerando assim a tabela final para ser consumida pelo relatório

    bq query \
    --destination_table visualizacao.flat_grafico \
    --use_legacy_sql=false \
    --replace=True \
    '  SELECT
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
      nm.tube_assembly_id DESC'

### Automatizando e orquestrando todo processo usando o composer(Apache Airflow)

1- Na pasta '**jobs_dataproc**' onde fizemos as alterações anteriormente dos arquivos pyspark, agora vamos fazer tambem as mesmas alterações
Arquivos: **CsvToParquet-bill_of_materials_composer.py,** **CsvToParquet-comp_boss_composer.py**, **CsvToParquet-**price_quote_composer.p**y.** Para cada um deles:
	* Abrir o Arquivo e alterar a variável '**bucketname'** para o nome do bucket que foi criado anteriormente
	* Após realizar as alteraçoes, no storage na pasta criada "**jobs_dataproc"** fazer a importação desses arquivos.

Obs: A diferença desses arquivos para os outros que alteramos anteriormente é que nesse o job guarda o resultado da transformação no storage particionando pela data da carga.
Ex: carga_20052020.

Após a alteração e o upload dos arquivos alterados para a pasta job_dataproc no storage, vamos alterar os padrados da nossa Dag que sera utilizada no composer. Na pasta composer temos o arquivo 'dotz-composer.py',abrir e alterar as variaveis:
* project_id - alterando para o id do seu projeto GCP
* bucketname - com o nome do bucket criado anteriormente.

Após realizar as alterações, fazer a importação desse arquivo para o nosso ambiente composer  GCP em "Dags"

