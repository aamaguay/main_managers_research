#!/usr/bin/env python
# coding: utf-8

# # Imports

# In[1]:


import pandas as pd
import numpy as np
import dask.dataframe as dd
import json
from datetime import date
# import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from tqdm import tqdm

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,IntegerType, StructType,StringType
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import isnan, when, count, col , split


# # Functions

# In[2]:


def detect_year(begin_value, end_value, dict_years, ci_id):
    ls_year = list(range(begin_value.year, end_value.year+1))
    filter_year = pd.Series(ls_year)[ pd.Series(ls_year).isin(list(range(2014,2019))) ]
    for yr in filter_year:
        if ci_id not in dict_years[yr]:
            dict_years[yr].append(ci_id) 
    return dict_years

def remove_empty_ls(dct):
    if ((pd.Series(dct.values()).str.len()==0).sum()>=1):
        bol = pd.Series(dct.values()).str.len() >=1
        dct_filter = dict( list(zip(pd.Series(dct.keys())[bol], pd.Series(dct.values())[bol])) )
        return dct_filter
    else:
        return dct

def get_dct_by_year(dct,yr_value):
    dct_by_yr = dict()
    for comp_id, dct_years in dct.items():
        try:
            dct_by_yr[comp_id] = dct_years[yr_value]
        except Exception as e:
            pass
    return dct_by_yr

def identify_sector(firm_id, dc_by_sector):
    for sector_label, list_id_firms in dc_by_sector.items():
        if firm_id in list_id_firms:
            label_firm = sector_label
            break
    return label_firm
    
def dict_of_firms_with_similar_managers(dc_by_year):
    dc_result = dict()
    ls_exclude = []
    for origin_id, origin_list in dc_by_year.items():
        for end_id, end_list in dc_by_year.items():
            if (end_id not in ls_exclude):
                if (origin_id != end_id):
                    common_ci = set(origin_list) & set(end_list)
                    if len(common_ci) > 0:
                        if (origin_id not in dc_result.keys()) & (end_id not in dc_result.keys()):
                            dc_result[origin_id] = {end_id:len(common_ci)}
                            dc_result[end_id] = {origin_id:len(common_ci)}
                        elif (origin_id not in dc_result.keys()) & (end_id in dc_result.keys()):
                            dc_result[origin_id] = {end_id:len(common_ci)}
                            dc_result[end_id][origin_id] = len(common_ci)
                        elif (origin_id in dc_result.keys()) & (end_id not in dc_result.keys()):
                            dc_result[end_id] = {origin_id:len(common_ci)}
                            dc_result[origin_id][end_id] = len(common_ci)
                        elif (origin_id in dc_result.keys()) & (end_id in dc_result.keys()):
                            dc_result[origin_id][end_id] = len(common_ci)
                            dc_result[end_id][origin_id] = len(common_ci)
                        print(origin_id, end_id)
        ls_exclude.append(origin_id)
    return dc_result

def evaluate_append_multiprocessing( origin_id,origin_list, dict_batch, dc_result):
    for end_id, end_list in dict_batch.items():
        common_ci = set(origin_list) & set(end_list)
        if len(common_ci) > 0:
            try:
                dc_result[origin_id][end_id] = len(common_ci)
            except Exception as e:
                dc_result[origin_id] = {end_id:len(common_ci)}
            try:
                dc_result[end_id][origin_id] = len(common_ci)
            except Exception as e:
                dc_result[end_id] = {origin_id:len(common_ci)}

def runParallelNotebooks(origin_id,origin_list,workloads,dc_result):
    pool = ThreadPool(len(workloads))
    pool.map(lambda workload: evaluate_append_multiprocessing(origin_id,origin_list,workload,dc_result), workloads)

def runParallelResults(workloads_dict):
    pool = ThreadPool(len(workloads_dict))
    return pool.map(lambda workload: dict_intersection_between_firms(workload), workloads_dict )

def dict_intersection_between_firms(dc_by_year):
    dc_result = dict()
    len_all_dict = len(list(dc_by_year.keys()))
    print(f"total companies: {len_all_dict }\n")
    i = 1
    for origin_id, origin_list in tqdm(dc_by_year.items()):
        if (i <= (len_all_dict-1)):
            new_dict_reduce = dict( list(dc_by_year.items())[i:] )
            len_reduce_dict = len(new_dict_reduce.keys())
            #create batchs
            ls_of_batch = []
            batch_size = 100
            num_batches = 1+ int(len_reduce_dict/batch_size)
            ls_reduce_dict =  list( new_dict_reduce.items() )
            for n in range(num_batches):
                batch = ls_reduce_dict[n*batch_size: (n+1)*batch_size]
                total_batch = len(batch)
                ls_of_batch.append( dict(batch) )
            # print(f"nr. of batches:{len(ls_of_batch)}--- comp: {i}")
            runParallelNotebooks(origin_id, origin_list, ls_of_batch, dc_result) 
            #print(f"connections saved from firm:{origin_id}.-- Nr.{i}",)
            i+=1
    print('finish process')
    return dc_result


# # Load Dataset

# In[3]:


df_manager = pd.read_csv('/home/user/Desktop/files_desktop/DATA_managers/adm2018.csv')


# In[4]:


df_manager.head(3)


# In[5]:


df_manager.isna().sum()


# # Fix columns format

# In[6]:


df_manager["adm_ftermino"].fillna("2019-06-18", inplace=True)
df_manager['date_begin'] = pd.to_datetime((df_manager.adm_fnombramiento.str.split().str[0]), infer_datetime_format=True)
df_manager['date_end'] = pd.to_datetime((df_manager.adm_ftermino.str.split().str[0]), infer_datetime_format=True)
df_manager['expediente'] = df_manager.expediente.astype(str)
df_manager['adm_cedula' ]= df_manager.adm_cedula.str.strip()


# # Filter dataset
# 
# - Part1: Records between the years 2014 -2018
# - Part2: Records of those who started working outside the range of years (2014-2018) and finished their work activities within the range of years analyzed.
# - Part3: Records of those who started working between the range of years (2014-2018) and until now they are working (2019).
# - Part4: Records of those who started working outside the range of years (2014-2018) and until now they are working (2019).

# In[7]:



part1 = df_manager[  ((df_manager.date_end < '2019-01-01') & (df_manager.date_end >= '2014-01-01')) & 
((df_manager.date_begin < '2019-01-01') & (df_manager.date_begin >= '2014-01-01'))]
part2 = df_manager[  ((df_manager.date_end < '2019-01-01') & (df_manager.date_end >= '2014-01-01')) & 
( (df_manager.date_begin < '2014-01-01') )]

part3 = df_manager[  ( (df_manager.date_end == '2019-06-18') ) & 
((df_manager.date_begin < '2019-01-01') & (df_manager.date_begin >= '2014-01-01'))]
part4 = df_manager[  ( (df_manager.date_end == '2019-06-18') ) & 
( (df_manager.date_begin < '2014-01-01') )]

df_manager_final = pd.concat([part1, part2, part3, part4 ], ignore_index=True)


# # Load Data of companies

# In[8]:


# dk_firms = dd.read_csv('/home/user/Desktop/files_desktop/DATA_managers/BASE.csv')
# dk_firms = dk_firms.rename(columns={'Año':'anio'})
# dk_firms.columns = pd.Series(dk_firms.columns).str.lower()

# # filter columns
# only_firm_by_sector_dk = ((dk_firms[['expediente','sector']]).drop_duplicates(subset=['expediente']))


# In[9]:


spark = SparkSession.builder     .master('local[*]')     .config("spark.driver.memory", "20g")     .appName('MyFirstCSVLoad')     .getOrCreate()
dk_firms = spark.read.format("csv").load("/home/user/Desktop/files_desktop/DATA_managers/BASE.csv", header = 'true')


# In[10]:


only_firm_by_sector_spk = (dk_firms.select(col('expediente'),col('rama_actividad')))    .dropDuplicates(['expediente'])


# In[11]:


only_firm_by_sector_df = only_firm_by_sector_spk.toPandas()


# # Update datasets with the same *expediente ids* for both dataset (records of administrators and finance data)

# In[12]:


df_manager_update = df_manager_final[df_manager_final.expediente.isin(only_firm_by_sector_df.expediente)]

df_rama_exped_id = only_firm_by_sector_df[only_firm_by_sector_df.expediente.isin(df_manager_update.expediente)]    .groupby(['rama_actividad'])        .apply( lambda x: list(set(x['expediente'])) )            .to_frame('expediente_id')                .reset_index()


# # Form dictionary with *rama_actividad* as keys and a list of firms.
# ## E.g
# ```
# {'A': ['159855',
#   '9064',
#   '301751',
#   '170521',
#   '709746'],
# ...,
#  'T': ['148299', '6693'],
#  'U': ['175145'],
#  'Z': ['166147', '166095', '159545', '700555']
# }
# ```

# In[13]:


dict_rama_exped_id = dict(list(zip(list(df_rama_exped_id.rama_actividad), list(df_rama_exped_id.expediente_id))))
dict_rama_exped_id = {sector_lab: [ company_id+'-'+sector_lab for company_id in list_firms_ids] for sector_lab, list_firms_ids in dict_rama_exped_id.items() }
# save dict as json file
# with open('/home/user/Desktop/files_desktop/DATA_managers/dict_rama_exped_id.json', 'w',encoding='utf-8') as f:
#     json.dump(dict_rama_exped_id, f, ensure_ascii=False, indent=4)

# df_final.expediente.drop_duplicates().isin(only_firm_by_sector_df.expediente).sum() # 60580, 60538
# Opening JSON file
# f = open('/home/user/Desktop/files_desktop/DATA_managers/dict_rama_exped_id.json')
# returns JSON object as a dictionary
# data = json.load(f)


# In[14]:


df_manager_update = df_manager_update.merge(only_firm_by_sector_df, how='left', on='expediente')
df_manager_update['new_expediente_rama'] = df_manager_update.expediente +'-'+ df_manager_update.rama_actividad
dict_firm_manager_by_year = dict()

for i,row in df_manager_update.iterrows():
    if row['new_expediente_rama'] not in dict_firm_manager_by_year.keys():
        anio_dict = { 2014:[],2015:[],2016:[],2017:[],2018:[]}
        dict_results_yr = detect_year(row['date_begin'], row['date_end'], anio_dict, row['adm_cedula']  )
        dict_firm_manager_by_year[row['new_expediente_rama']] = dict_results_yr
    else:
        anio_dict = dict_firm_manager_by_year[row['new_expediente_rama']]
        dict_results_yr = detect_year(row['date_begin'], row['date_end'], anio_dict, row['adm_cedula'] )
        dict_firm_manager_by_year[row['new_expediente_rama']] = dict_results_yr


# # Remove empty list by year

# In[15]:


dict_firm_manager_by_year_update = {firm_id: remove_empty_ls(dct_yr) for firm_id, dct_yr in dict_firm_manager_by_year.items() }


# # Form dict of firm by year to estimate common managers

# In[16]:


manager_by_firms_2014,manager_by_firms_2015,manager_by_firms_2016,manager_by_firms_2017,manager_by_firms_2018 =  [ get_dct_by_year(dict_firm_manager_by_year_update, i) for i in range(2014,2019)]


# # Estimate companies with administrators

# In[18]:


ls_dict_managers_by_year_part1 = [manager_by_firms_2014,manager_by_firms_2015]
ls_result_intersection_part1 = runParallelResults(ls_dict_managers_by_year_part1)


# In[19]:


# save dict as json file
# for v in [0,1]:
#     with open(f"/home/user/Desktop/files_desktop/DATA_managers/dict_intersection_{v}.json", 'w',encoding='utf-8') as f:
#         json.dump( ls_result_intersection_part1[v] , f, ensure_ascii=False, indent=4)


# In[20]:


ls_dict_managers_by_year_part2 = [manager_by_firms_2016,manager_by_firms_2017, manager_by_firms_2018]
ls_result_intersection_part2 = runParallelResults(ls_dict_managers_by_year_part2)


# In[21]:


# for v in [0,1,2]:
#     with open(f"/home/user/Desktop/files_desktop/DATA_managers/dict_intersection2_{v}.json", 'w',encoding='utf-8') as f:
#         json.dump( ls_result_intersection_part2[v] , f, ensure_ascii=False, indent=4)


# # Upload json files

# In[27]:


d_inters_14, d_inters_15, d_inters_16, d_inters_17, d_inters_18 =  [ json.load(open(f"/home/user/Desktop/files_desktop/DATA_managers/dict_intersection_{v_year}.json") ) for v_year in range(2014,2019)]


# In[31]:


d_inters_18

