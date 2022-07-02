

create file format if not exists my_parquet_format
  type = parquet; 

create or replace  stage azure_data_ingestion_stage
  url = 'azure://dphidatastorage.blob.core.windows.net/datastores/raw'
  credentials=(azure_sas_token='sp=racwlme&st=2022-07-02T13:04:15Z&se=2022-07-02T21:04:15Z&spr=https&sv=2021-06-08&sr=c&sig=9vuqbwFvjPumpjPeCALQCP9kfQuscvz8pgydVy5bqTw%3D')
  file_format = 'my_parquet_format';


