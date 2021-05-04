#import libraries
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x:  '%.5f'%x)
np.set_printoptions(suppress=True)
np.set_printoptions(precision=3)


clinical_util = pd.read_csv("../data/raw/ClinicianUtilization_2017.csv")
clinical_util.head()
clinical_util.shape
# (1354610, 10)

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
prov = prov.loc[~prov['lst_nm'].isna()]
prov = prov.loc[~prov['frst_nm'].isna()]
prov.shape
# (2265221, 40)

prov['rank'] = prov\
  .groupby("npi")["num_org_mem"]\
  .rank(method="dense", ascending=False, na_option='keep')\
  .fillna(1)

prov_affl = pd.melt(
  prov,
  id_vars=['npi'],
  value_vars=['org_nm', 'hosp_afl_lbn_1', 'hosp_afl_lbn_2', 'hosp_afl_lbn_3', 'hosp_afl_lbn_4', 'hosp_afl_lbn_5'],
  value_name='afl_org_nm')\
    .drop(columns=['variable'])\
    .query('afl_org_nm==afl_org_nm')\
    .drop_duplicates()\
    .reset_index(drop=True)


prov_affl.to_parquet('../data/processed/provider_affiliation.parquet', index=False)
prov_affl.head()
prov_affl.count()
