
# In[1]:


import pandas as pd

import numpy as np

import os
import re
from unidecode import unidecode
#import GbqModule as gbx

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def run_query(query,pdt_dict,gb_cred_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gb_cred_path
    client = bigquery.Client(project=pdt_dict['project_id'])
    query_job = client.query(query)
    results = query_job.result()
    return results

def create_dataset(pdt_dict,gb_cred_path,del_prev=0):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gb_cred_path
    client = bigquery.Client(project=pdt_dict['project_id'])
    dataset_ref = client.dataset(pdt_dict['dataset_id'])
    dataset_exist = does_database_exist(pdt_dict,gb_cred_path)
    if dataset_exist:
        if del_prev:
            client.get_dataset(dataset_ref) 
            client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True) 
            print(f"Existing Dataset {pdt_dict['dataset_id']} deleted successfully.")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset = client.create_dataset(dataset)
            print(f"Created dataset {pdt_dict['dataset_id']}.")
        else:
            print(f"Dataset {pdt_dict['dataset_id']} already exists.")
    else:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        print(f"Created dataset {pdt_dict['dataset_id']}.")
    

def create_table(pdt_dict,df,gb_cred_path,del_prev=0):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gb_cred_path
    client = bigquery.Client(project=pdt_dict['project_id'])
    dataset_ref = client.dataset(pdt_dict['dataset_id'])
    dataset_exist = does_database_exist(pdt_dict,gb_cred_path)
    if dataset_exist:
        table_exist = does_table_exist(pdt_dict,gb_cred_path)
        if table_exist:
            if del_prev:
                delete_table(pdt_dict,gb_cred_path)
                print(f"Existing Table {pdt_dict['table_id']} deleted successfully.")
                table_ref = dataset_ref.table(pdt_dict['table_id'])
                table = bigquery.Table(table_ref)
                table = client.create_table(table)
                print(f"Created table {pdt_dict['table_id']}. REPLACED")
                job = client.load_table_from_dataframe(df, table_ref)
                job.result()
                print(f"Loaded {job.output_rows} rows into {pdt_dict['dataset_id']}:{pdt_dict['table_id']}.")
            else:
                print(f"Table {pdt_dict['table_id']} already exists. NOT REPLACING")
        else:
            table_ref = dataset_ref.table(pdt_dict['table_id'])
            table = bigquery.Table(table_ref)
            table = client.create_table(table)
            print(f"Created table {pdt_dict['table_id']}.")
            job = client.load_table_from_dataframe(df, table_ref)
            job.result()
            print(f"Loaded {job.output_rows} rows into {pdt_dict['dataset_id']}:{pdt_dict['table_id']}.")
    else:
        print(f"Dataset {pdt_dict['dataset_id']} does NOT exists.")
        

def append_table(pdt_dict,df,gb_cred_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gb_cred_path
    client = bigquery.Client(project=pdt_dict['project_id'])
    dataset_ref = client.dataset(pdt_dict['dataset_id'])
    table_exist = does_table_exist(pdt_dict,gb_cred_path)
    if table_exist:
        table_ref = dataset_ref.table(pdt_dict['table_id'])
        table = client.get_table(table_ref, retry=None)
        schema = table.schema
        df_with_schema = pd.DataFrame(columns=[field.name for field in schema])
        df_with_schema = pd.concat([df_with_schema, df], ignore_index=True)
        job = client.load_table_from_dataframe(df_with_schema, table_ref)
        job.result()
        print(f"Appended {len(df)} rows to table {pdt_dict['table_id']}.")


def delete_table(pdt_dict,gb_cred_path):
    table_exist = does_table_exist(pdt_dict,gb_cred_path)
    if table_exist:
        query = f"Drop table {pdt_dict['project_id']}.{pdt_dict['dataset_id']}.{pdt_dict['table_id']}"
        run_query(query,pdt_dict,gb_cred_path)
        print(f"Deleted Table {pdt_dict['project_id']}.{pdt_dict['dataset_id']}.{pdt_dict['table_id']}")


def does_database_exist(pdt_dict,gb_cred_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gb_cred_path
    client = bigquery.Client(project=pdt_dict['project_id'])
    dataset_ref = client.dataset(pdt_dict['dataset_id'])
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {pdt_dict['dataset_id']} already exists.")
        return True
    except Exception as e:
        print(e)
        print(f"Dataset {pdt_dict['dataset_id']} does NOT exists.")
    return False

def does_table_exist(pdt_dict,gb_cred_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gb_cred_path
    client = bigquery.Client(project=pdt_dict['project_id'])
    dataset_ref = client.dataset(pdt_dict['dataset_id'])
    dataset_exist = does_database_exist(pdt_dict,gb_cred_path)
    if dataset_exist:
        try:
            table_ref = dataset_ref.table(pdt_dict['table_id'])
            table = client.get_table(table_ref, retry=None)
            schema = table.schema
            print(f"Table {pdt_dict['table_id']} already exists.")
            return True
        except Exception as e:
            print(e)
            print(f"Table {pdt_dict['table_id']} does NOT exists.")
            return False
    else:
        print(f"Dataset {pdt_dict['dataset_id']} does NOT exists.")
    return False
    

# In[2]:

def get_extra_data(data_path,my_comb_data):
    date_list=set()
    for folder in os.listdir(data_path):
        adate = folder.split(" ")[0].split('_')[1]
        atime = folder.split(" ")[1].split('_')
        adatetime = pd.to_datetime(f"{adate} {atime[0]}:{atime[1]}:{atime[2]}",format='%d-%m-%y %H:%M:%S')
        date_list.add(adatetime.date())
        # print(adatetime.date())
    my_data = my_comb_data.copy()
    my_data = pd.to_datetime(my_data['date']).dt.date
    new_dates = date_list - set(my_data)
    print(new_dates)
    
    
    
    combined_data_4times = {}
    for folder in os.listdir(data_path):
        folder_path = os.path.join(data_path, folder)
        adate = folder.split(" ")[0].split('_')[1]
        atime = folder.split(" ")[1].split('_')
        adatetime = pd.to_datetime(f"{adate} {atime[0]}:{atime[1]}:{atime[2]}",format='%d-%m-%y %H:%M:%S')
        if adatetime.date() not in new_dates:
            continue
        xlsx_files = [file for file in os.listdir(folder_path) if file.lower().endswith('.xlsx')]
        for file in xlsx_files:
            file_name, file_ext = os.path.splitext(file)
            if file_name not in combined_data_4times:
                combined_data_4times[file_name] = []
            file_path = os.path.join(folder_path, file)
            df = pd.read_excel(file_path,sheet_name="Sheet1")
            df['datetime'] = adatetime
            df['date'] = df['datetime'].dt.date
            cat = file_name.split('_')[1]
            df['search_keyword']=cat
            combined_data_4times[file_name].append(df)
    total_4times_df = pd.DataFrame()
    for file_name, dfs in combined_data_4times.items():
        if len(dfs) >=1:
            combined_df = pd.concat(dfs, ignore_index=True,axis=0)
            total_4times_df = pd.concat([total_4times_df,combined_df],axis=0,ignore_index=True)
    
    return total_4times_df


def get_extra_data_new(data_path):
    date_list=set()
    for folder in os.listdir(data_path):
        adate = folder.split(" ")[0].split('_')[1]
        atime = folder.split(" ")[1].split('_')
        adatetime = pd.to_datetime(f"{adate} {atime[0]}:{atime[1]}:{atime[2]}",format='%d-%m-%y %H:%M:%S')
        date_list.add(adatetime.date())
        # print(adatetime.date())    
    
    combined_data_4times = {}
    for folder in os.listdir(data_path):
        folder_path = os.path.join(data_path, folder)
        adate = folder.split(" ")[0].split('_')[1]
        atime = folder.split(" ")[1].split('_')
        adatetime = pd.to_datetime(f"{adate} {atime[0]}:{atime[1]}:{atime[2]}",format='%d-%m-%y %H:%M:%S')
        xlsx_files = [file for file in os.listdir(folder_path) if file.lower().endswith('.xlsx')]
        for file in xlsx_files:
            file_name, file_ext = os.path.splitext(file)
            if file_name not in combined_data_4times:
                combined_data_4times[file_name] = []
            file_path = os.path.join(folder_path, file)
            df = pd.read_excel(file_path,sheet_name="Sheet1")
            df['datetime'] = adatetime
            df['date'] = df['datetime'].dt.date
            cat = file_name.split('_')[1]
            df['search_keyword']=cat
            combined_data_4times[file_name].append(df)
    total_4times_df = pd.DataFrame()
    for file_name, dfs in combined_data_4times.items():
        if len(dfs) >=1:
            combined_df = pd.concat(dfs, ignore_index=True,axis=0)
            total_4times_df = pd.concat([total_4times_df,combined_df],axis=0,ignore_index=True)
    return total_4times_df



home = r"C:\Users\Admin\Downloads\myntra K"
data_path = os.path.join(home,'Myntra Data')





# In[6]:


tables = {
    'data_table' : 'myntra_data',
}


# In[7]:
project_id = 'e-commerce-443412'
dataset_id = 'myntra'
key_path = r"C:\Users\Admin\Downloads\myntra K\e-commerce-k-project.json"


old_dates = run_query(f'select distinct date from {dataset_id}.{tables["data_table"]}',{'project_id':project_id,
            'dataset_id':dataset_id,
            'table_id':tables["data_table"]},key_path)

columns = [field.name for field in old_dates.schema]
df_raw = [] #pd.DataFrame()
for row in old_dates:
    df_raw.append(list(row))
old_dates= pd.DataFrame(df_raw, columns = columns)

# the dates that are present in database



# In[8]:
#raw_data = get_extra_data_new(data_path)

raw_data = get_extra_data(data_path,old_dates)

# only take that date from my local data that is not present in database

#print(len(raw_data['date'].unique()))


# In[9]:


unkownPH = '!-<-Unknown->-!'

def get_clean_price(x):
    if type(x) != str:
        return unkownPH
    try:    
        x = x.replace(',','').strip()
        x = float(re.findall("\d+[.]?\d*",x)[0])
    except:
        x=unkownPH
    return x

def get_clean_discount_4t(x):                               ## tackle 
    if (type(x) == str) and ("OFF" in x):
        if 'Rs' in x:
            result = unkownPH
        else:
            result = float(re.findall("\d+[.]?\d*",x)[0])
    else:
        result = unkownPH
    return result

def get_clean_customer_rated(x):
    if type(x)!=str:
        return unkownPH
    if 'k' in x:
        x = float(x.replace('k',''))
        x = x*1000
    else:
        x = float(x)
    return x

def get_missing_prices(x):
    a,b,c=x['sp']!=unkownPH,x['mrp']!=unkownPH,x['discount']!=unkownPH
    if not c:
        if b and not a:
            x['sp'] = x['mrp']
            x['discount']=0
            return x
        elif a and not b:
            x['mrp'] = x['sp']
            x['discount']=0
            return x
    try : 
        if a and b and not c:
            x['discount'] = ((x['mrp']-x['sp'])/x['mrp'])*100
        elif b and c and not a:
            x['sp'] = x['mrp']*((100-x['discount'])/100)
        elif a and c and not b:
            x['mrp'] = x['sp']/((100-x['discount'])/100)
    except:
        print(x)
    return x

def b_clean(x):
    x = x.replace('&','AND')
    x = x.replace('+','PLUS')
    x = unidecode(x)
    x = re.findall("[a-zA-Z0-9]+",x)
    x = ''.join(x)
    return x.upper()

def clean_4times_data(data):    
    data=data.copy()
    data.rename(columns = {'Uid':'pid'},inplace=True)
    rename_dict = dict()
    for i in raw_data.columns:
        rename_dict[i]='_'.join(i.lower().split())
    data.rename(columns=rename_dict,inplace=True)
    

    data.dropna(subset=['link'],inplace=True)
    data['brand'] = data['brand'].apply(lambda x: b_clean(x))
    data['sp'] = data['sp'].apply(lambda x: get_clean_price(x))
    data['mrp'] = data['mrp'].apply(lambda x: get_clean_price(x))
    data['discount'] = data['discount'].apply(lambda x: get_clean_discount_4t(x))
    data['customer_rated'] = data['customer_rated'].apply(lambda x:get_clean_customer_rated(x))
    data = data.apply(lambda x:get_missing_prices(x),axis=1)
    data['category'] = data['link'].apply(lambda x : x.split('/')[0])
    data['category'] = data['category'].apply(lambda x : (" ".join(x.split('-'))).title())
    data['ad_flag']=0
    data['location'] = 'Delhi'
    data['pid'] = data['pid'].astype(str)
    data.replace(unkownPH,np.nan,inplace=True)
    return data


# In[10]:


clean_data = clean_4times_data(raw_data) 


# In[12]:

clean_data.info()
clean_data.head()
clean_data['date'].unique() 

create_table({'project_id':project_id,
            'dataset_id':'myntra',
            'table_id':'myntra_data'},clean_data, key_path)

# this program will append data in the gbq

