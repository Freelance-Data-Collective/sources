from dlt_personio_source import load_personio_tables

creds = {
  "type": "service_account",
  "project_id": "zinc-mantra-353207",
  "private_key_id": "ffff",
  "private_key": "ffff",
  "client_email": "data-load-tool@zinc-mantra-353207.iam.gserviceaccount.com",
  "client_id": "100909481823688180493"
}

# put in your credentials
# and remove/invalidate the dummy data flag.
load_personio_tables(client_id='',
                     client_secret='',
                     target_credentials=creds,
                     tables = ['employees', 'absences', 'absence_types', 'attendances'],
                     schema_name='personio_raw',
                     dummy_data = True)

