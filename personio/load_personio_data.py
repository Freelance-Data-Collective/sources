import base64
from dlt.pipeline import Pipeline
from dlt.pipeline.typing import GCPPipelineCredentials

from source.personio_source import PersonioSource as P
from source.personio_source_dummy import PersonioSourceDummy as D


#get credentials at this url - replace"test-1" with your org name
#https://test-1.personio.de/configuration/api/credentials/management

# To test, replace P with D (dummy source with sample data)
p = D(client_id='',
      client_secret='')

# let's create bigquery credentials
gcp_credentials_json = {
    "type": "service_account",
    "project_id": "zinc-mantra-353207",
    "private_key": "",
    "client_email": "data-load-tool-public-demo@zinc-mantra-353207.iam.gserviceaccount.com",
}


schema_prefix = 'raw'
schema_name = 'personio'


#helpers:

def save_schema(p, fn):
    schema_yaml = p.get_default_schema().as_yaml(remove_defaults=True)
    f = open(f'{fn}.yml', "w")
    f.write(schema_yaml)
    f.close()

def load(data=[],
            table_name='mytbl',
            schema_name='mypipe',
            credentials= {},
            schema_infile=None,
            schema_outfile=None,
            dry_run=False):
    cred = GCPPipelineCredentials.from_services_dict(credentials, dataset_prefix=schema_prefix)
    p = Pipeline(schema_name)
    if schema_infile:
        schema_infile = Pipeline.load_schema_from_file(f"{schema_infile}.yml")
    p.create_pipeline(cred, schema=schema_infile)
    p.extract(data, table_name=table_name)
    p.unpack()
    if schema_outfile:
        save_schema(p, schema_outfile)
    if not dry_run:
        p.load()



# get the tables we can load. Each "table" row is a dict {'table_name':'', 'data':row_generator_function}
tables = p.tasks()

for table in tables:
    load(data=table['data'],
                table_name=table['table_name'],
                schema_name=schema_name,
                credentials=gcp_credentials_json,
                #if you want to reset the schema, output it here
                #schema_outfile='personio_schema_out',
                schema_infile='personio_schema',
                dry_run=False)

    print(f"loaded {table['table_name']}")

