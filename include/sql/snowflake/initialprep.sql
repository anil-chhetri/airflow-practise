

create file format if not exists my_parquet_format
  type = parquet; 

create or replace  stage azure_data_ingestion_stage
  url = 'azure:url'
  credentials=(azure_sas_token='secret')
  file_format = 'my_parquet_format';


