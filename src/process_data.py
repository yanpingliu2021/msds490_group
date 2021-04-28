#import libraries
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x:  '%.5f'%x)
np.set_printoptions(suppress=True)
np.set_printoptions(precision=3)

# processing enrollment files
enrollment_2008 = pd.read_csv('../data/raw/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv',
                              parse_dates=['BENE_BIRTH_DT', 'BENE_DEATH_DT'],
                              dtype={'BENE_RACE_CD':str, 'BENE_SEX_IDENT_CD':str,
                                     'SP_STATE_CODE': str, 'BENE_COUNTY_CD':str})
enrollment_2009 = pd.read_csv('../data/raw/DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv',
                              parse_dates=['BENE_BIRTH_DT', 'BENE_DEATH_DT'],
                              dtype={'BENE_RACE_CD':str, 'BENE_SEX_IDENT_CD':str,
                                     'SP_STATE_CODE': str, 'BENE_COUNTY_CD':str})
enrollment_2010 = pd.read_csv('../data/raw/DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv',
                              parse_dates=['BENE_BIRTH_DT', 'BENE_DEATH_DT'],
                              dtype={'BENE_RACE_CD':str, 'BENE_SEX_IDENT_CD':str,
                                     'SP_STATE_CODE': str, 'BENE_COUNTY_CD':str})
enrollment_2008.insert(0, 'YEAR',  2008)
enrollment_2009.insert(0, 'YEAR',  2009)
enrollment_2010.insert(0, 'YEAR',  2010)
enrollment = enrollment_2008.append(enrollment_2009).append(enrollment_2010).reset_index(drop = True)
print(enrollment_2008.shape)
print(enrollment_2009.shape)
print(enrollment_2010.shape)
print(enrollment.shape)
enrollment.head()
enrollment.dtypes
enrollment.rename(columns={
  'DESYNPUF_ID':'PATIENT_ID',
  'BENE_BIRTH_DT':'BIRTH_DT',
  'BENE_DEATH_DT':'DEATH_DT',
  'BENE_SEX_IDENT_CD':'SEX',
  'BENE_RACE_CD':'RACE',
  'BENE_ESRD_IND':'ESRD_IND',
  'SP_STATE_CODE':'ST_CD',
  'BENE_COUNTY_CD':'COUNTY_CD',
  'PLAN_CVRG_MOS_NUM': 'COVERAGE'
}, inplace=True)
enrollment.drop(columns=['BENE_HI_CVRAGE_TOT_MONS',
                         'BENE_SMI_CVRAGE_TOT_MONS',
                         'BENE_HMO_CVRAGE_TOT_MONS'], inplace=True)
enrollment.to_parquet('../data/processed/enrollment.parquet', index=False)
enrollment.groupby(['YEAR', 'COVERAGE']).size()


# process carrier claims
carriercoltypes = {
  'CLM_ID':str,
  'ICD9_DGNS_CD_1':str,
  'ICD9_DGNS_CD_2':str,
  'ICD9_DGNS_CD_3':str,
  'ICD9_DGNS_CD_4':str,
  'ICD9_DGNS_CD_5':str,
  'ICD9_DGNS_CD_6':str,
  'ICD9_DGNS_CD_7':str,
  'ICD9_DGNS_CD_8':str,
  'PRF_PHYSN_NPI_1':str,
  'PRF_PHYSN_NPI_2':str,
  'PRF_PHYSN_NPI_3':str,
  'PRF_PHYSN_NPI_4':str,
  'PRF_PHYSN_NPI_5':str,
  'PRF_PHYSN_NPI_6':str,
  'PRF_PHYSN_NPI_7':str,
  'PRF_PHYSN_NPI_8':str,
  'PRF_PHYSN_NPI_9':str,
  'PRF_PHYSN_NPI_10':str,
  'PRF_PHYSN_NPI_11':str,
  'PRF_PHYSN_NPI_12':str,
  'PRF_PHYSN_NPI_13':str,
  'HCPCS_CD_1':str,
  'HCPCS_CD_2':str,
  'HCPCS_CD_3':str,
  'HCPCS_CD_4':str,
  'HCPCS_CD_5':str,
  'HCPCS_CD_6':str,
  'HCPCS_CD_7':str,
  'HCPCS_CD_8':str,
  'HCPCS_CD_9':str,
  'HCPCS_CD_10':str,
  'HCPCS_CD_11':str,
  'HCPCS_CD_12':str,
  'HCPCS_CD_13':str}
carrier1 = pd.read_csv(
  '../data/raw/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv',
  dtype=carriercoltypes,
  parse_dates=['CLM_FROM_DT', 'CLM_THRU_DT'])

carrierselcols = ['DESYNPUF_ID', 'CLM_ID', 'CLM_FROM_DT'] + \
  [x for x in carrier1.columns.tolist() if x.startswith('HCPCS_CD')
   or x.startswith('PRF_PHYSN_NPI') or x.startswith('ICD9_DGNS_CD')]
carrier1 = carrier1.filter(items=carrierselcols)

carrier2 = pd.read_csv(
  '../data/raw/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv',
  dtype=carriercoltypes,
  parse_dates=['CLM_FROM_DT', 'CLM_THRU_DT'])
carrier2 = carrier2.filter(items=carrierselcols)
carrier = carrier1.append(carrier2).reset_index(drop=True)
carrier.head()
carrier.dtypes
carrier.rename(columns={
  'DESYNPUF_ID':'PATIENT_ID',
  'CLM_ID':'CLAIM_ID',
  'CLM_FROM_DT':'SVC_DT',
  'PRF_PHYSN_NPI_1':'NPI_1',
  'PRF_PHYSN_NPI_2':'NPI_2',
  'PRF_PHYSN_NPI_3':'NPI_3',
  'PRF_PHYSN_NPI_4':'NPI_4',
  'PRF_PHYSN_NPI_5':'NPI_5',
  'PRF_PHYSN_NPI_6':'NPI_6',
  'PRF_PHYSN_NPI_7':'NPI_7',
  'PRF_PHYSN_NPI_8':'NPI_8',
  'PRF_PHYSN_NPI_9':'NPI_9',
  'PRF_PHYSN_NPI_10':'NPI_10',
  'PRF_PHYSN_NPI_11':'NPI_11',
  'PRF_PHYSN_NPI_12':'NPI_12',
  'PRF_PHYSN_NPI_13':'NPI_13'}, inplace=True)

carrier.insert(0, 'CHANNEL', 'DX')
carrier.to_parquet('../data/processed/office_claims.parquet', index=False)


#process inpatient and outpatient claims
ipcoltypes = {
  'CLM_ID':str,
  'AT_PHYSN_NPI':str,
  'OP_PHYSN_NPI':str,
  'OT_PHYSN_NPI':str,
  'ADMTNG_ICD9_DGNS_CD':str,
  'CLM_DRG_CD':str,
  'ICD9_DGNS_CD_1':str,
  'ICD9_DGNS_CD_2':str,
  'ICD9_DGNS_CD_3':str,
  'ICD9_DGNS_CD_4':str,
  'ICD9_DGNS_CD_5':str,
  'ICD9_DGNS_CD_6':str,
  'ICD9_DGNS_CD_7':str,
  'ICD9_DGNS_CD_8':str,
  'ICD9_DGNS_CD_9':str,
  'ICD9_DGNS_CD_10':str,
  'ICD9_PRCDR_CD_1':str,
  'ICD9_PRCDR_CD_2':str,
  'ICD9_PRCDR_CD_3':str,
  'ICD9_PRCDR_CD_4':str,
  'ICD9_PRCDR_CD_5':str,
  'ICD9_PRCDR_CD_6':str,
  'HCPCS_CD_1':str,
  'HCPCS_CD_2':str,
  'HCPCS_CD_3':str,
  'HCPCS_CD_4':str,
  'HCPCS_CD_5':str,
  'HCPCS_CD_6':str,
  'HCPCS_CD_7':str,
  'HCPCS_CD_8':str,
  'HCPCS_CD_9':str,
  'HCPCS_CD_10':str,
  'HCPCS_CD_11':str,
  'HCPCS_CD_12':str,
  'HCPCS_CD_13':str,
  'HCPCS_CD_14':str,
  'HCPCS_CD_15':str,
  'HCPCS_CD_16':str,
  'HCPCS_CD_17':str,
  'HCPCS_CD_18':str,
  'HCPCS_CD_19':str,
  'HCPCS_CD_20':str,
  'HCPCS_CD_21':str,
  'HCPCS_CD_22':str,
  'HCPCS_CD_23':str,
  'HCPCS_CD_24':str,
  'HCPCS_CD_25':str,
  'HCPCS_CD_26':str,
  'HCPCS_CD_27':str,
  'HCPCS_CD_28':str,
  'HCPCS_CD_29':str,
  'HCPCS_CD_30':str,
  'HCPCS_CD_31':str,
  'HCPCS_CD_32':str,
  'HCPCS_CD_33':str,
  'HCPCS_CD_34':str,
  'HCPCS_CD_35':str,
  'HCPCS_CD_36':str,
  'HCPCS_CD_37':str,
  'HCPCS_CD_38':str,
  'HCPCS_CD_39':str,
  'HCPCS_CD_40':str,
  'HCPCS_CD_41':str,
  'HCPCS_CD_42':str,
  'HCPCS_CD_43':str,
  'HCPCS_CD_44':str,
  'HCPCS_CD_45':str
}
inpatient = pd.read_csv('../data/raw/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv',
                        parse_dates=['CLM_FROM_DT', 'CLM_THRU_DT', 'CLM_ADMSN_DT', 'NCH_BENE_DSCHRG_DT'],
                        dtype=ipcoltypes)
inpatient.head()
inpatient.dtypes
ipselcols = ['DESYNPUF_ID', 'CLM_ID', 'CLM_FROM_DT', 'AT_PHYSN_NPI']+\
  [x for x in inpatient.columns if x.startswith('HCPCS_CD_') or x.startswith('ICD9_')]
inpatient = inpatient.filter(items=ipselcols)
inpatient.rename(columns={
  'DESYNPUF_ID':'PATIENT_ID',
  'CLM_ID':'CLAIM_ID',
  'CLM_FROM_DT':'SVC_DT',
  'AT_PHYSN_NPI':'NPI'
  }, inplace=True)

outpatient = pd.read_csv('../data/raw/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv',
                        parse_dates=['CLM_FROM_DT', 'CLM_THRU_DT'],
                        dtype=ipcoltypes)
outpatient.head()
outpatient.dtypes
outpatient = outpatient.filter(items=ipselcols)
outpatient.rename(columns={
  'DESYNPUF_ID':'PATIENT_ID',
  'CLM_ID':'CLAIM_ID',
  'CLM_FROM_DT':'SVC_DT',
  'AT_PHYSN_NPI':'NPI'
  }, inplace=True)


inpatient.insert(0, 'CHANNEL', 'IP')
outpatient.insert(0, 'CHANNEL', 'OP')
hx = inpatient.append(outpatient).reset_index(drop = True)
hx.to_parquet('../data/processed/institutional_claims.parquet', index=False)
hx.head()
hx.count()


# hx.iloc[:1000, :].to_excel('../data/institutional_claims.xlsx', index=False)
# carrier.iloc[:1000, :].to_excel('../data/office_claims.xlsx', index=False)
# enrollment.iloc[:1000, :].to_excel('../data/enrollment.xlsx', index=False)