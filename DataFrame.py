import os
import json
import pandas as pd
from pandas.io.json import json_normalize

file_path = 'C:/Users/...'
os.listdir(file_path)

data = pd.read_csv('CSV file path and name')
#document that contains the data to be transformed


left_side_df = data.drop(columns = ['Environment', 'Timezone', 'Build Trigger', 'Downstream Job'])
left_side_df.reset_index(inplace = True) 

df_au = data.loc[data['Environment'] == 'STG-AU']
df_au.reset_index(inplace = True) 

df_id = data.loc[data['Environment'] == 'STG-ID']
df_id.reset_index(inplace = True) 

df_my = data.loc[data['Environment'] == 'STG-MY']
df_my.reset_index(inplace = True) 

df_ph = data.loc[data['Environment'] == 'STG-PH']
df_ph.reset_index(inplace = True) 

df_sg = data.loc[data['Environment'] == 'STG-SG']
df_sg.reset_index(inplace = True) 

df_th = data.loc[data['Environment'] == 'STG-TH']
df_th.reset_index(inplace = True) 

df_id #show the data frame created

#data cleansing/ data frame sorting based on country

#df_au.set_index((df_au['Environment']), inplace=True)
df_au_r = df_au.drop(columns = ['Job_Name', 'Job_Description', 'Environment', 'Enabled'])
df_au_r.rename(columns={'Timezone': 'Timezone-AU', 'Build Trigger': 'Build Trigger-AU', 'Downstream Job': 'Downstream Job-AU'}, inplace=True)
result = pd.concat([left_side_df, df_au_r], axis=1)

#df_id.set_index((df_id['Environment']), inplace=True)
df_id_r = df_id.drop(columns = ['Job_Name', 'Job_Description', 'Environment', 'Enabled'])
df_id_r.rename(columns={'Timezone': 'Timezone-ID', 'Build Trigger': 'Build Trigger-ID', 'Downstream Job': 'Downstream Job-ID'}, inplace=True)
result = pd.concat([result, df_id_r], axis=1)

#df_my.set_index((df_my['Environment']), inplace=True)
df_my_r = df_my.drop(columns = ['Job_Name', 'Job_Description', 'Environment', 'Enabled'])
df_my_r.rename(columns={'Timezone': 'Timezone-MY', 'Build Trigger': 'Build Trigger-MY', 'Downstream Job': 'Downstream Job-MY'}, inplace=True)
result = pd.concat([result, df_my_r], axis=1)

#df_ph.set_index((df_ph['Environment']), inplace=True)
df_ph_r = df_ph.drop(columns = ['Job_Name', 'Job_Description', 'Environment', 'Enabled'])
df_ph_r.rename(columns={'Timezone': 'Timezone-PH', 'Build Trigger': 'Build Trigger-PH', 'Downstream Job': 'Downstream Job-PH'}, inplace=True)
result = pd.concat([result, df_ph_r], axis=1)

#df_sg.set_index((df_sg['Environment']), inplace=True)
df_sg_r = df_sg.drop(columns = ['Job_Name', 'Job_Description', 'Environment', 'Enabled'])
df_sg_r.rename(columns={'Timezone': 'Timezone-SG', 'Build Trigger': 'Build Trigger-SG', 'Downstream Job': 'Downstream Job-SG'}, inplace=True)
result = pd.concat([result, df_sg_r], axis=1)

#df_th.set_index((df_th['Environment']), inplace=True)
df_th_r = df_th.drop(columns = ['Job_Name', 'Job_Description', 'Environment', 'Enabled'])
df_th_r.rename(columns={'Timezone': 'Timezone-TH', 'Build Trigger': 'Build Trigger-TH', 'Downstream Job': 'Downstream Job-TH'}, inplace=True)
result = pd.concat([result, df_th_r], axis=1)

#show data frame
df_th_r

#show result
result

#show concatenation of ph and th (sample output)
zz = pd.concat([df_ph_r, df_th_r], axis=1)
zz

#code for CSV
TO_CSV_QUOTING = 1
TO_CSV_FLOAT_FORMAT = '%.2f'
TO_CSV_HEADER = True
TO_CSV_INDEX = False
TO_CSV_ENCODING = 'utf-8'
COLUMNS = ['Job_Name', 'Job_Description', 'Enabled', 'URL',
           'Timezone-AU', 'Build Trigger-AU', 'Downstream Job-AU',
           'Timezone-ID','Build Trigger-ID', 'Downstream Job-ID',
           'Timezone-MY','Build Trigger-MY', 'Downstream Job-MY'
           'Timezone-PH','Build Trigger-PH', 'Downstream Job-PH'
           'Timezone-SG','Build Trigger-SG', 'Downstream Job-SG'
           'Timezone-TH','Build Trigger-TH', 'Downstream Job-TH']
           
#extract df to csv
with open('C:/Users/...file path of data', 'w', encoding = 'utf-8') as file_out:
    file_in = result.to_csv(quoting=TO_CSV_QUOTING, float_format=TO_CSV_FLOAT_FORMAT,
                                        header=TO_CSV_HEADER, index=TO_CSV_INDEX,
                                        encoding=TO_CSV_ENCODING, columns=COLUMNS)
    for line in file_in:
        file_out.write(line)
