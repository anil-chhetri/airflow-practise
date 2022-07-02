create file format my_parquet_format
  type = parquet; 

create or replace if not exists stage azure_data_ingestion_stage
  url = 'azure://dphidatastorage.blob.core.windows.net/datastores'
  credentials=(azure_sas_token='sp=racwdl&st=2022-07-02T03:58:09Z&se=2022-07-02T11:58:09Z&spr=https&sv=2021-06-08&sr=c&sig=wtUXRFBLMo0t2E9Cl6d65WxerpT9zV4b%2BpA1iQPlWqw%3D')
  file_format = 'my_parquet_format';


--list the files in azure blob. 
LIST @azure_data_ingestion_stage;

--creating file format.
create file format my_parquet_format
  type = parquet;


  ---quering table structure of parquet file.

  select *
  from table(
    infer_schema(
      location=>'@azure_data_ingestion_stage'
      , file_format=>'my_parquet_format'
      )
    );


-- create table based on parquet file
create table test
  using template (
    select array_agg(object_construct(*))
      from table(
        infer_schema(
          location=>'@azure_data_ingestion_stage',
          file_format=>'my_parquet_format'
        )
      ));




create table test1( data variant);

insert into test1
select $1
from @azure_data_ingestion_stage
(FILE_FORMAT => 'my_parquet_format', PATTERN => 
'yellow_tripdata_2019-01.parquet')
limit 10;


--getting json file based on parquet
select * from test1,
lateral flatten(data);