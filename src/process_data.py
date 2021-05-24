#import libraries
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x:  '%.5f'%x)
np.set_printoptions(suppress=True)
np.set_printoptions(precision=3)


prov = pd.read_csv("../data/raw/DAC_NationalDownloadableFile.csv",
                   dtype={
                    'NPI':str,
                    ' Ind_PAC_ID':str,
                    ' Ind_enrl_ID':str,
                    ' lst_nm':str,
                    ' frst_nm':str,
                    ' mid_nm':str,
                    ' suff':str,
                    ' gndr':str,
                    ' Cred':str,
                    ' Med_sch':str,
                    ' Grd_yr':float,
                    ' pri_spec':str,
                    ' sec_spec_1':str,
                    ' sec_spec_2':str,
                    ' sec_spec_3':str,
                    ' sec_spec_4':str,
                    ' sec_spec_all':str,
                    ' org_nm':str,
                    ' org_pac_id':str,
                    ' num_org_mem':float,
                    ' adr_ln_1':str,
                    ' adr_ln_2':str,
                    ' ln_2_sprs':str,
                    ' cty':str,
                    ' st':str,
                    ' zip':str,
                    ' phn_numbr':str,
                    ' hosp_afl_1':str,
                    ' hosp_afl_lbn_1':str,
                    ' hosp_afl_2':str,
                    ' hosp_afl_lbn_2':str,
                    ' hosp_afl_3':str,
                    ' hosp_afl_lbn_3':str,
                    ' hosp_afl_4':str,
                    ' hosp_afl_lbn_4':str,
                    ' hosp_afl_5':str,
                    ' hosp_afl_lbn_5':str,
                    ' ind_assgn':str,
                    ' grp_assgn':str,
                    ' adrs_id':str
                  })
prov.head()
prov.shape
# (2265285, 40)
prov.columns.tolist()
prov.columns = [x.strip().lower() for x in prov.columns.tolist()]

prov[['npi']].nunique()
# 1163422
prov[['npi', 'lst_nm', 'frst_nm']].drop_duplicates().shape
# (1163422, 3)
prov[['npi', 'lst_nm', 'frst_nm', 'gndr', 'cred',
      'med_sch', 'grd_yr', 'pri_spec']].drop_duplicates().shape
# (1167726, 8)


prov = prov.loc[~prov['lst_nm'].isna()]
prov = prov.loc[~prov['frst_nm'].isna()]
prov.shape
# (2265221, 40)
prov = prov.loc[~prov['org_nm'].isna()].reset_index(drop=True)
prov.shape
#(2097917, 40)


zip_lat_lon = pd.read_excel('../data/raw/us-zip-code-latitude-and-longitude.xlsx',
                            engine='openpyxl', dtype = {'Zip':str})
zip_lat_lon.columns = [x.lower() for x in zip_lat_lon.columns]
zip_lat_lon.dtypes
zip_lat_lon.shape
zip_lat_lon.nunique()
prov['zip'] = prov['zip'].apply(lambda x: x[:5].zfill(5))
prov = prov\
  .drop_duplicates()\
  .reset_index(drop = True)\
  .merge(zip_lat_lon, on = ['zip'], how = 'inner')
prov.shape
#(2078794, 42)

prov_affl = prov[['npi', 'org_nm', 'org_pac_id']]\
  .drop_duplicates()\
  .reset_index(drop = True)
prov_affl.shape
#(1179477, 3)

prov_affl.to_parquet('../data/processed/provider_affiliation.parquet', index=False)
prov_affl.head()
prov_affl.nunique()


org_demo = prov[['org_pac_id', 'org_nm', 'num_org_mem',
                  'adr_ln_1', 'adr_ln_2', 'cty', 'st',	'zip',
                  'lat', 'long']]\
  .drop_duplicates()\
  .reset_index(drop = True)

org_demo_random = org_demo\
  .groupby('org_pac_id', as_index=False)\
  .apply(lambda x: x.sample(1, random_state=42))

org_demo.shape
#(231160, 10)
org_demo_random.shape
# (75514, 10)
org_demo_random.to_parquet('../data/processed/organization_demographics.parquet', index=False)



#process measurement raw data
ec_score = pd.read_csv('../data/raw/ec_score_file.csv', dtype={'npi':str, ' org_pac_id':str})
ec_score.columns = [x.strip() for x in ec_score.columns]
ec_score.shape
ec_score = ec_score\
  .query('final_mips_score == final_mips_score')\
  .fillna(-1)\
  .groupby(['npi'])\
  .agg(
     quality_score	= ('quality_category_score', 'max'),
     promoting_interoperability_score = ('pi_category_score', 'max'),
     cost_score = ('cost_category_score', 'max'),
     improvement_activities_score = ('ia_category_score', 'max'),
     overall_mips_score = ('final_mips_score', 'max')
  )\
  .reset_index()
ec_score.shape
# (666264, 6)
ec_score.dtypes




prov_demo = prov[
  ['npi', 'lst_nm', 'frst_nm', 'gndr', 'cred', 'med_sch', 'grd_yr', 'pri_spec', 'org_pac_id', 'org_nm']
  ].drop_duplicates(subset = ['npi', 'org_pac_id']).reset_index(drop = True)
prov_demo.shape
# (1179477, 10)

clinician_measurement = prov_demo\
  .merge(ec_score, on = ['npi'], how = 'inner')

clinician_measurement.shape
# (594225, 15)
clinician_measurement.isna().sum()
clinician_measurement.replace({-1: np.nan}, inplace=True)
clinician_measurement.isna().sum()

"""
npi                                      0
lst_nm                                   0
frst_nm                                  0
gndr                                     0
cred                                403518
med_sch                                  2
grd_yr                                 377
pri_spec                                 0
org_pac_id                               0
org_nm                                   0
quality_score                         3892
promoting_interoperability_score    101960
cost_score                          280255
improvement_activities_score            13
overall_mips_score                       0
"""
clinician_measurement.to_parquet('../data/processed/clinician_measurement.parquet', index=False)
