import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x:  '%.5f'%x)
np.set_printoptions(suppress=True)
np.set_printoptions(precision=3)

dx_ext = pd.read_parquet('../data/processed/office_claims.parquet')
hx_ext = pd.read_parquet('../data/processed/institutional_claims.parquet')


# flatten office claims
dx_ext.dtypes
dx_diag_fact = pd.melt(
  dx_ext,
  id_vars=['CHANNEL', 'PATIENT_ID', 'CLAIM_ID', 'SVC_DT'],
  value_vars=[f'ICD9_DGNS_CD_{x}' for x in range(1, 9)],
  value_name='DIAG_CD')\
    .drop(columns=['variable'])\
    .query('DIAG_CD==DIAG_CD')\
    .drop_duplicates()

dx_diag_fact.shape
# (10110051, 5)
dx_diag_fact.head()
dx_diag_fact['DIAG_CD'].isnull().sum()


dx_npi_fact = pd.melt(
  dx_ext,
  id_vars=['CHANNEL', 'PATIENT_ID', 'CLAIM_ID', 'SVC_DT'],
  value_vars=[f'NPI_{x}' for x in range(1, 14)],
  value_name='NPI')\
    .drop(columns=['variable'])\
    .query('NPI==NPI')\
    .drop_duplicates()

dx_npi_fact.shape
# (4742114, 5)
dx_npi_fact.head()
dx_npi_fact['NPI'].isnull().sum()







# flatten institutional claims
hx_ext.dtypes
hx_diag_fact = pd.melt(
  hx_ext,
  id_vars=['CHANNEL', 'PATIENT_ID', 'CLAIM_ID', 'SVC_DT'],
  value_vars=[f'ICD9_DGNS_CD_{x}' for x in range(1, 11)],
  value_name='DIAG_CD')\
    .drop(columns=['variable'])\
    .query('DIAG_CD==DIAG_CD')\
    .drop_duplicates()

hx_diag_fact.shape
# (2609057, 5)
hx_diag_fact.head()
hx_diag_fact['DIAG_CD'].isnull().sum()

hx_npi_fact = hx_ext.filter(
  items=['CHANNEL', 'PATIENT_ID', 'CLAIM_ID', 'SVC_DT', 'NPI'])\
    .query('NPI==NPI')\
    .drop_duplicates()

hx_npi_fact.shape
# (839100, 5)
hx_npi_fact.head()
hx_npi_fact['NPI'].isnull().sum()




# create final fact tables
pat_npi_fact = dx_npi_fact.append(hx_npi_fact).reset_index(drop = True)
pat_npi_fact.shape
# (5581214, 5)
pat_npi_fact.drop_duplicates().shape
# (5581214, 5)

pat_diag_fact = dx_diag_fact.append(hx_diag_fact).reset_index(drop = True)
pat_diag_fact.shape
# (12719108, 5)
pat_diag_fact.drop_duplicates().shape
# (12719108, 5)

disease_def = pd.read_parquet('../data/input/disease_def.parquet')

pat_diag_fact = pat_diag_fact\
  .merge(disease_def[['DIAG_CD']], on = ['DIAG_CD'], how = 'inner')\
  .reset_index(drop = True)

pat_diag_fact.shape
# (1424215, 5)

target_diag = pat_diag_fact[['PATIENT_ID', 'CLAIM_ID']]\
  .drop_duplicates()\
  .reset_index(drop=True)

pat_npi_fact = pat_npi_fact\
  .merge(target_diag, on = ['PATIENT_ID', 'CLAIM_ID'], how='inner')\
  .reset_index(drop = True)
pat_npi_fact.shape
# (1140897, 5)

pat_npi_fact.to_parquet('../data/fact/pat_npi_fact.parquet', index=False)
pat_diag_fact.to_parquet('../data/fact/pat_diag_fact.parquet', index=False)



#processing doctor demographics file
prov = pd.read_excel(
  '../data/raw/Medicare-Physician-and-Other-Supplier-NPI-Aggregate-CY2012.xlsx',
  engine='openpyxl',
  sheet_name='DATA',
  dtype={'NPI':str}
  )

prov = prov.filter(
  items=[
    'NPI',
    'NPPES Provider Last Name / Organization Name',
    'NPPES Provider First Name',
    'NPPES Credentials',
    'NPPES Provider Gender',
    'NPPES Entity Code',
    'NPPES Provider Street Address 1',
    'NPPES Provider Street Address 2',
    'NPPES Provider City',
    'NPPES Provider Zip Code',
    'NPPES Provider State',
    'NPPES Provider Country',
    'Provider Type',
    'Number of Unique Beneficiaries',
    'Number of Beneficiaries Age Less 65',
    'Number of Beneficiaries Age 65 to 74',
    'Number of Beneficiaries Age 75 to 84',
    'Number of Beneficiaries Age Greater 84',
    'Total Medicare Payment Amount',
    'Average Age of Beneficiaries',
    'Number of Female Beneficiaries',
    'Number of Male Beneficiaries',
    'Number of Non-Hispanic White Beneficiaries',
    'Number of Black or African American Beneficiaries',
    'Number of Asian Pacific Islander Beneficiaries',
    'Number of Hispanic Beneficiaries',
    'Number of American Indian/Alaska Native Beneficiaries',
    'Percent (%) of Beneficiaries Identified With Alzheimer’s Disease or Dementia',
    'Percent (%) of Beneficiaries Identified With Asthma',
    'Percent (%) of Beneficiaries Identified With Atrial Fibrillation',
    'Percent (%) of Beneficiaries Identified With Cancer',
    'Percent (%) of Beneficiaries Identified With Chronic Kidney Disease',
    'Percent (%) of Beneficiaries Identified With Chronic Obstructive Pulmonary Disease',
    'Percent (%) of Beneficiaries Identified With Depression',
    'Percent (%) of Beneficiaries Identified With Diabetes',
    'Percent (%) of Beneficiaries Identified With Heart Failure',
    'Percent (%) of Beneficiaries Identified With Hyperlipidemia',
    'Percent (%) of Beneficiaries Identified With Hypertension',
    'Percent (%) of Beneficiaries Identified With Ischemic Heart Disease',
    'Percent (%) of Beneficiaries Identified With Osteoporosis',
    'Percent (%) of Beneficiaries Identified With Rheumatoid Arthritis / Osteoarthritis',
    'Percent (%) of Beneficiaries Identified With Schizophrenia / Other Psychotic Disorders',
    'Percent (%) of Beneficiaries Identified With Stroke',
    'Average HCC Risk Score of Beneficiaries'
]
)

prov.shape
# (925328, 42)
prov.dtypes
prov.head()

prov.rename(columns={
'NPI':'NPI',
'NPPES Provider Last Name / Organization Name':'Last_Name',
'NPPES Provider First Name':'First_Name',
'NPPES Credentials':'Credentials',
'NPPES Provider Gender':'Gender',
'NPPES Entity Code':'Provider_Type',
'NPPES Provider Street Address 1':'Address_1',
'NPPES Provider Street Address 2':'Address_2',
'NPPES Provider City':'City',
'NPPES Provider Zip Code':'Zip',
'NPPES Provider State':'State',
'NPPES Provider Country':'Country',
'Provider Type':'Specialty_Code',
'Number of Unique Beneficiaries':'Number_of_Pats',
'Number of Beneficiaries Age Less 65':'Number_of_Pats_Less_65',
'Number of Beneficiaries Age 65 to 74':'Number_of_Pats_Age_65_74',
'Number of Beneficiaries Age 75 to 84':'Number_of_Pats_75_84',
'Number of Beneficiaries Age Greater 84':'Number_of_Pats_Greater_84',
'Total Medicare Payment Amount':'Total_Medicare_Payment_Amount',
'Average Age of Beneficiaries':'Average_Pat_Age',
'Number of Female Beneficiaries':'Number_of_Female_Pats',
'Number of Male Beneficiaries':'Number_of_Male_Pats',
'Number of Non-Hispanic White Beneficiaries':'Number_of_White_Pats',
'Number of Black or African American Beneficiaries':'Number_of_African_American_Pats',
'Number of Asian Pacific Islander Beneficiaries':'Number_of_Asian_Pats',
'Number of Hispanic Beneficiaries':'Number_of_Hispanic_Pats',
'Number of American Indian/Alaska Native Beneficiaries':'Number_of_American_Native_Pats',
'Percent (%) of Beneficiaries Identified With Alzheimer’s Disease or Dementia':'Percent_Pats_With_Alzheimer_Dementia',
'Percent (%) of Beneficiaries Identified With Asthma':'Percent_Pats_With_Asthma',
'Percent (%) of Beneficiaries Identified With Atrial Fibrillation':'Percent_Pats_With_Atrial_Fibrillation',
'Percent (%) of Beneficiaries Identified With Cancer':'Percent_Pats_With_Cancer',
'Percent (%) of Beneficiaries Identified With Chronic Kidney Disease':'Percent_Pats_With_Chronic_Kidney_Disease',
'Percent (%) of Beneficiaries Identified With Chronic Obstructive Pulmonary Disease':'Percent_Pats_With_COPD',
'Percent (%) of Beneficiaries Identified With Depression':'Percent_Pats_With_Depression',
'Percent (%) of Beneficiaries Identified With Diabetes':'Percent_Pats_With_Diabetes',
'Percent (%) of Beneficiaries Identified With Heart Failure':'Percent_Pats_With_Heart_Failure',
'Percent (%) of Beneficiaries Identified With Hyperlipidemia':'Percent_Pats_With_Hyperlipidemia',
'Percent (%) of Beneficiaries Identified With Hypertension':'Percent_Pats_With_Hypertension',
'Percent (%) of Beneficiaries Identified With Ischemic Heart Disease':'Percent_Pats_With_Ischemic_Heart_Disease',
'Percent (%) of Beneficiaries Identified With Osteoporosis':'Percent_Pats_With_Osteoporosis',
'Percent (%) of Beneficiaries Identified With Rheumatoid Arthritis / Osteoarthritis':'Percent_Pats_With_Rheumatoid_Arthritis',
'Percent (%) of Beneficiaries Identified With Schizophrenia / Other Psychotic Disorders':'Percent_Pats_With_Schizophrenia',
'Percent (%) of Beneficiaries Identified With Stroke':'Percent_Pats_With_Stroke',
'Average HCC Risk Score of Beneficiaries':'Average_Pat_HCC_Risk_Score'},
            inplace=True)
prov.head()

prov['Provider_Type'].value_counts()
prov.query("Provider_Type == 'I'").to_parquet('../data/fact/provider.parquet', index=False)

