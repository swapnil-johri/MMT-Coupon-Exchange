#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd

import os
import requests
import time
 
import urllib.parse
def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)
 
    if job['status'] == 3:
        return job['query_result_id']
    
    return None
 
def get_fresh_query_result(redash_url, query_id, api_key):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})
 
    response = s.post('{}/api/queries/{}/refresh'.format(redash_url, query_id))
 
    if response.status_code != 200:
        raise Exception('Refresh failed.')
 
    result_id = poll_job(s, redash_url, response.json()['job'])
 
    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')
 
    return response.json()['query_result']['data']['rows']
 
 
    
api_key = '8IrjI06J618heUl05YG1uKlhCDy0MFTuXC5Y3Y73'        
df = pd.DataFrame.from_dict(get_fresh_query_result('http://redash.mmt.com/', 4389, api_key))
df = df[['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE']]

df1 = df['ATTRIBUTE_VALUE'].str.split(',', expand = True)
df1 = df1.fillna('N/A')
df['GR_Bucket'] = ' '
df['IHM_Bucket'] = ' '
df['DND_Bucket'] = ' '
df['APPONLY_Bucket'] = ' '
df['INDIAIP_Bucket'] = ' '
df['SDND_Bucket'] = ' '
df['New_GR'] = ' '
a = []
b = []
dfgr = pd.read_excel('C:/Users/int802/Desktop/GR% wise Buckets.xlsx', sheet_name = 'GR')
dfihm = pd.read_excel('C:/Users/int802/Desktop/GR% wise Buckets.xlsx', sheet_name = 'IHM')
dfao = pd.read_excel('C:/Users/int802/Desktop/GR% wise Buckets.xlsx', sheet_name = 'APPONLY')
dfii = pd.read_excel('C:/Users/int802/Desktop/GR% wise Buckets.xlsx', sheet_name = 'INDIAIP')
dfs = pd.read_excel('C:/Users/int802/Desktop/GR% wise Buckets.xlsx', sheet_name = 'SDND')

dfgr = dfgr.fillna(100)
dfihm = dfihm.fillna(100)
dfao = dfao.fillna(100)
dfii = dfii.fillna(100)
dfs = dfs.fillna(100)









#TO CREATE A SEPARATE COLUMN FOR GR, IHM, DND, APPONLY, INDIAIP, SDND


def GR(): 
    lgr = []
    lgr1 = []
    for index, row in df1.iterrows():
        for i in df1.columns:
            if('INTL_GR' in row[i] or 'INTL_Gr' in row[i]):
                lgr1.append(row[i])
        lgr.append(lgr1)
        lgr1 = []
    df['GR_Bucket'] = lgr
    

def IHM(): 
    lihm = []
    lihm1 = []
    for index, row in df1.iterrows():
        for i in df1.columns:
            if('IHM' in row[i]):
                lihm1.append(row[i])
        lihm.append(lihm1)
        lihm1 = []
    df['IHM_Bucket'] = lihm


def DND(): 
    ldnd = []
    ldnd1 = []
    for index, row in df1.iterrows():
        for i in df1.columns:
            if('INTL_DND' in row[i]):
                ldnd1.append(row[i])
        ldnd.append(ldnd1)
        ldnd1 = []
    df['DND_Bucket'] = ldnd
    

def APPONLY(): 
    lao = []
    lao1 = []
    for index, row in df1.iterrows():
        for i in df1.columns:
            if('INTL_APPONLY' in row[i]):
                lao1.append(row[i])
        lao.append(lao1)
        lao1 = []
    df['APPONLY_Bucket'] = lao
    

def INDIAIP(): 
    liip = []
    liip1 = []
    for index, row in df1.iterrows():
        for i in df1.columns:
            if('INTL_INDIAIP' in row[i]):
                liip1.append(row[i])
        liip.append(liip1)
        liip1 = []
    df['INDIAIP_Bucket'] = liip

    
def SDND(): 
    lsdnd = []
    lsdnd1 = []
    for index, row in df1.iterrows():
        for i in df1.columns:
            if('INTL_SDND' in row[i]):
                lsdnd1.append(row[i])
        lsdnd.append(lsdnd1)
        lsdnd1 = []
    df['SDND_Bucket'] = lsdnd
    
    

    
GR()
IHM()
DND()
APPONLY()
INDIAIP()
SDND()










# #TO CREATE A NEW COLUMN FOR EACH(MENTIONED ABOVE) NEW BUCKET



for index, row in df.iterrows():
    for index2, row2 in dfgr.iterrows():
        if(row2.Greater_Than_ET <= row.GRASP < row2.Lesser_Than):
            row.New_GR = row.New_GR.replace(' ', row2.Coupon)
        elif(row.GRASP < 0):
            row.New_GR = row.New_GR.replace(' ', 'INTL_GR_4') 
    a = list(row)
    b.append(a)

df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR'])
df = df2
del df2
a = []
b = []
df['New_IHM'] = ' '





for index, row in df.iterrows():
    for index2, row2 in dfihm.iterrows():
        if(row2.Greater_Than_ET <= row.GRASP < row2.Lesser_Than):
            row.New_IHM = row.New_IHM.replace(' ', row2.Coupon)
        elif(row.GRASP < 0):
            row.New_IHM = row.New_IHM.replace(' ', 'IHM0') 
    a = list(row)
    b.append(a)
       
df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM'])
df = df2
del df2
a = []
b = []
df['New_APPONLY'] = ' '





for index, row in df.iterrows():
    if(len(row.DND_Bucket) != 0):
        for index2, row2 in dfao.iterrows():
            if(row2.Greater_Than_ET <= row.GRASP < row2.Lesser_Than):
                row.New_APPONLY = row.New_APPONLY.replace(' ', row2.Coupon)
            elif(row.GRASP < 0):
                row.New_APPONLY = row.New_APPONLY.replace(' ', 'INTL_APPONLY0') 
    elif(len(row.DND_Bucket) == 0   and   len(row.APPONLY_Bucket) !=0):
        row.New_APPONLY = row.New_APPONLY.replace(' ', row.APPONLY_Bucket[0]) 
        
    a = list(row)
    b.append(a)
df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY'])
df = df2
del df2
a = []
b = []
df['New_INDIAIP'] = ' '


            

for index, row in df.iterrows():
    if(len(row.DND_Bucket) != 0):
        for index2, row2 in dfii.iterrows():
            if(row2.Greater_Than_ET <= row.GRASP < row2.Lesser_Than):
                row.New_INDIAIP = row.New_INDIAIP.replace(' ', row2.Coupon)
            elif(row.GRASP < 0):
                row.New_INDIAIP = row.New_INDIAIP.replace(' ', 'INTL_INDIAIP0') 
    elif(len(row.DND_Bucket) == 0   and   len(row.INDIAIP_Bucket) !=0):
        row.New_INDIAIP = row.New_INDIAIP.replace(' ', row.INDIAIP_Bucket[0])  
    a = list(row)
    b.append(a)
df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP'])
df = df2
del df2
a = []
b = []
df['New_SDND'] = ' '




for index, row in df.iterrows():
    if(len(row.DND_Bucket) != 0):
        for index2, row2 in dfs.iterrows():
            if(row2.Greater_Than_ET <= row.GRASP < row2.Lesser_Than):
                row.New_SDND = row.New_SDND.replace(' ', row2.Coupon)
            elif(row.GRASP < 0):
                row.New_SDND = row.New_SDND.replace(' ', 'INTL_SDND0') 
    elif(len(row.DND_Bucket) == 0   and   len(row.SDND_Bucket) !=0):
        row.New_SDND = row.New_SDND.replace(' ', row.SDND_Bucket[0])  
    a = list(row)
    b.append(a)
df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP', 'New_SDND'])
df = df2
del df2
a = []
b = []













#TO REPLACE THE EXISTING BUCKET IN 'ATTRIBUTE_VALUE' WITH NEW BUCKET




for index, row in df.iterrows():
    if(len(row.GR_Bucket) != 0):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.GR_Bucket[0], row.New_GR)
        if(len(row.GR_Bucket) == 2):
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.GR_Bucket[1], '', 1)
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(',,', ',')
    
    elif(row.New_GR != ' '):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE + ',' + row.New_GR
                                           
    a = list(row)
    b.append(a)
        
        

df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP', 'New_SDND'])
df = df2
del df2
a = []
b = []

    

    
    

    
for index, row in df.iterrows():
    if(len(row.IHM_Bucket) != 0):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.IHM_Bucket[0], row.New_IHM)
        if(len(row.IHM_Bucket) == 2):
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.IHM_Bucket[1], '', 1)
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(',,', ',')
    
    elif(row.New_IHM != ' '):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE + ',' + row.New_IHM
                                           
    a = list(row)
    b.append(a)
        
        

df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP', 'New_SDND'])
df = df2
del df2
a = []
b = []






for index, row in df.iterrows():
    if(len(row.APPONLY_Bucket) != 0):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.APPONLY_Bucket[0], row.New_APPONLY)
        if(len(row.APPONLY_Bucket) == 2):
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.APPONLY_Bucket[1], '', 1)
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(',,', ',')
    
    elif(row.New_APPONLY != ' '):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE + ',' + row.New_APPONLY
                                           
    a = list(row)
    b.append(a)
        
        

df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP', 'New_SDND'])
df = df2
del df2
a = []
b = []






for index, row in df.iterrows():
    if(len(row.INDIAIP_Bucket) != 0):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.INDIAIP_Bucket[0], row.New_INDIAIP)
        if(len(row.INDIAIP_Bucket) == 2):
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.INDIAIP_Bucket[1], '', 1)
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(',,', ',')
    
    elif(row.New_INDIAIP != ' ') :
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE + ',' + row.New_INDIAIP
                                           
    a = list(row)
    b.append(a)
        
        

df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP', 'New_SDND'])
df = df2
del df2
a = []
b = []






for index, row in df.iterrows():
    if(len(row.SDND_Bucket) != 0):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.SDND_Bucket[0], row.New_SDND)
        if(len(row.SDND_Bucket) == 2):
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(row.SDND_Bucket[1], '', 1)
            row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(',,', ',')
    
    elif(row.New_SDND != ' ') :
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE + ',' + row.New_SDND
                                           
    a = list(row)
    b.append(a)
        
        

df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP', 'New_SDND'])
df = df2
del df2
a = []
b = []











#To remove extra ,'s from data frame

for index, row in df.iterrows():
    if(',,,' in row.ATTRIBUTE_VALUE):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(',,,', ',')
    if(',,' in row.ATTRIBUTE_VALUE):
        row.ATTRIBUTE_VALUE = row.ATTRIBUTE_VALUE.replace(',,', ',')
    a = list(row)
    b.append(a)
        
        

df2 = pd.DataFrame(b, columns = ['BKG_HTLSEQ', 'BKG_CITYCD', 'GMV', 'GR', 'GRASP', 'ATTRIBUTE_VALUE', 'GR_Bucket' ,'IHM_Bucket', 'DND_Bucket', 'APPONLY_Bucket', 'INDIAIP_Bucket', 'SDND_Bucket', 'New_GR', 'New_IHM', 'New_APPONLY', 'New_INDIAIP', 'New_SDND'])
df = df2
del df2
a = []
b = []
        
        

        
df_final = df[['BKG_HTLSEQ', 'BKG_CITYCD', 'GRASP', 'ATTRIBUTE_VALUE']]

df_final[['BKG_HTLSEQ', 'BKG_CITYCD']] = df_final[['BKG_HTLSEQ', 'BKG_CITYCD']].astype(str)
df[['BKG_HTLSEQ', 'BKG_CITYCD']] = df[['BKG_HTLSEQ', 'BKG_CITYCD']].astype(str)
df.to_csv('C:/Users/int802/Desktop/Final Python df.csv', index = False)
df_final.to_csv('C:/Users/int802/Desktop/Final Python.csv', index = False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




