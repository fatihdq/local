from sqlalchemy import create_engine
import pandas as pd
import glob
import os
import datetime

from function.functions_erajaya_store_tokopedia import FunctionsErajayaStoreTokopedia
from google.cloud import secretmanager

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './commercesense-prod-511d8f71bcea1.json'

client = secretmanager.SecretManagerServiceClient()
name = f"projects/674162212838/secrets/db_honeybee_warehouse/versions/latest"
response = client.access_secret_version(name=name)
secret_value = eval(response.payload.data.decode("UTF-8"))

DATABASE_URI = 'postgresql+psycopg2://{user}:{password}@{public_ip}/{db}'.format(**secret_value)
ENGINE = create_engine(DATABASE_URI,connect_args={"application_name":"Local Transformation"})
CONN = ENGINE.connect()

START_TIME = datetime.datetime.now()
PLATFORM = 'Tokopedia'


DATA_PATH = '/Applications/Folder/Work/new_data_scrapt'
PATH = '{}/electronic/tokopedia/2023-01-11'.format(DATA_PATH)

ALL_FILES = sorted(glob.glob(os.path.join(PATH, "*.csv")))

if __name__ == '__main__':
    for files in ALL_FILES:
        print(files)

        transform =  FunctionsErajayaStoreTokopedia(files,PLATFORM)
        df = pd.read_csv(files)
        ## Transform
        df_transformed = transform.basicTransform(df)

        transform.insertSnapshots(CONN,df_transformed)

        
    CONN.close()
    print("======== Duration: {} ========".format(round((datetime.datetime.now()-START_TIME).total_seconds(),2)))


