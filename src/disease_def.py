import pandas as pd
import re
import numpy as np

diag_ref = pd.read_excel('../data/raw/CMS32_DESC_LONG_SHORT_DX.xlsx')
diag_ref.dtypes
diag_ref.head()
diag_ref = diag_ref.iloc[:, :-1]
diag_ref.rename(
    columns=
    {
        'DIAGNOSIS CODE':'DIAG_CD',
        'LONG DESCRIPTION':'DIAG_LONG_DESC',
        'SHORT DESCRIPTION':'DIAG_SHORT_DESC',
    },
    inplace=True
)

# build disease definition used to calculate Charlson comorbidity score
# https://cran.r-project.org/web/packages/comorbidity/vignettes/comorbidityscores.html
# Myocardial infarction: 410.x, 412.x
mi_def = diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(410.*)|^(412.*)')].copy()
mi_def.insert(3, 'disease', 'myocardial infarction')
# Congestive heart failure: 398.91, 402.01, 402.11, 402.91, 404.01, 404.03, 404.11, 404.13, 404.91, 404.93, 425.4 - 425.9, 428.x
chf_def = diag_ref.loc[diag_ref['DIAG_CD'].isin(
    ['39891','40201','40211','40291','40401','40403','40411','40413','40491','40493', '4254',
     '4255', '4257', '4258', '4259'])].copy()
chf_def = chf_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(428.*)')].copy())
chf_def.insert(3, 'disease', 'congestive heart failure')

# Peripheral vascular disease: 093.0, 437.3, 440.x, 441.x, 443.1 - 443.9, 447.1, 557.1, 557.9, V43.4
pvd_def = diag_ref.loc[diag_ref['DIAG_CD'].isin(
    ['0930', '4373',  '43300','43301','43310','43311','43320','43321','43330',
     '43331','43380','43381','43390','43391', '4471', '5571', '5579', 'V434'])].copy()
pvd_def = pvd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(441.*)')].copy())
pvd_def = pvd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(440.*)')].copy())
pvd_def.insert(3, 'disease', 'peripheral vascular disease')

# Cerebrovascular disease: 362.34, 430.x - 438.x
cd_def = diag_ref.loc[diag_ref['DIAG_CD'].isin(['36234'])].copy()
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(430.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(431.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(432.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(433.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(434.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(435.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(436.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(437.*)')].copy())
cd_def = cd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(438.*)')].copy())
cd_def.insert(3, 'disease', 'cerebrovascular disease')

# Dementia: 290.x, 294.1, 331.2
de_def = diag_ref.loc[diag_ref['DIAG_CD'].isin(['29410', '29411', '29420', '29421', '3312'])].copy()
de_def = de_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(290.*)')].copy())
de_def.insert(3, 'disease', 'dementia')

# Chronic pulmonary disease: 416.8, 416.9, 490.x - 505.x, 506.4, 508.1, 508.8
cpd_def = diag_ref.loc[diag_ref['DIAG_CD'].isin(['4168', '4169', '5064', '5081', '5088'])].copy()
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(490.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(491.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(492.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(493.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(494.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(495.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(496.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(497.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(498.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(499.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(500.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(501.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(502.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(503.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(504.*)')].copy())
cpd_def = cpd_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(505.*)')].copy())
cpd_def.insert(3, 'disease', 'chronic pulmonary disease')

# Rheumatic disease: 446.5, 710.0 - 710.4, 714.0 - 714.2, 714.8, 725.x
ra_def = diag_ref.loc[diag_ref['DIAG_CD']\
    .isin(
        ['4465', '7100', '7101', '7102', '7103', '7104', '7140', '7141', '7142', '7148']
        )].copy()
ra_def = ra_def.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(725.*)')].copy())
ra_def.insert(3, 'disease', 'rheumatic disease')

# Peptic ulcer disease: 531.x - 534.x
pud_ref = diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(531.*)')].copy()
pud_ref = pud_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(532.*)')].copy())
pud_ref = pud_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(533.*)')].copy())
pud_ref = pud_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(534.*)')].copy())
pud_ref.insert(3, 'disease', 'peptic ulcer disease')


# Mild liver disease: 070.22, 070.23, 070.32, 070.33, 070.44, 070.54, 070.6, 070.9, 570.x, 571.x, 573.3, 573.4, 573.8, 573.9, V42.7
mld_ref = diag_ref.loc[diag_ref['DIAG_CD']\
    .isin(
        ['07022', '07023', '07032', '07033', '07044', '07054', '0706','0709','5733','5734','5738','5739','V427']
        )].copy()
mld_ref = mld_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(570.*)')].copy())
mld_ref = mld_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(571.*)')].copy())
mld_ref.insert(3, 'disease', 'mild liver disease')

# Diabetes without chronic complication: 250.0 - 250.3, 250.8, 250.9
# Diabetes with chronic complication: 250.4 - 250.7
diab_ref = diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2500.*)')].copy()
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2501.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2502.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2503.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2504.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2505.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2506.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2507.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2508.*)')].copy())
diab_ref = diab_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(2509.*)')].copy())
diab_ref.insert(3, 'disease', 'diabetes')
diab_ref['disease'] = np.where(diab_ref['DIAG_LONG_DESC'].str.contains('type II'), 'type 2 diabetes', 'type 1 diabetes')

# Hemiplegia or paraplegia: 334.1, 342.x, 343.x, 344.0 - 344.6, 344.9
hem_ref = diag_ref.loc[diag_ref['DIAG_CD']\
    .isin(
        ['3341', '3340', '3449']
        )].copy()
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(342.*)')].copy())
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(343.*)')].copy())
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(3440.*)')].copy())
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(3442.*)')].copy())
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(3443.*)')].copy())
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(3444.*)')].copy())
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(3445.*)')].copy())
hem_ref = hem_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(3446.*)')].copy())
hem_ref.insert(3, 'disease', 'hemiplegia or paraplegia')

# Renal disease: 403.01, 403.11, 403.91, 404.02, 404.03, 404.12, 404.13, 404.92, 404.93, 582.x, 583.0 - 583.7, 585.x, 586.x, 588.0, V42.0, V45.1, V56.x
renal_ref = diag_ref.loc[diag_ref['DIAG_CD']\
    .isin(
        ['40301', '40311', '40391', '40402', '40403', '40412', '40413', '40492',
         '40493', '5880', 'V420', 'V451']
        )].copy()
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(582.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(585.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(586.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(V56.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5830.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5831.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5832.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5833.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5834.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5835.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5836.*)')].copy())
renal_ref = renal_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5837.*)')].copy())
renal_ref.insert(3, 'disease', 'renal disease')

# Any malignancy, including lymphoma and leukaemia, except malignant neoplasm of skin: 140.x - 172.x, 174.x - 195.8, 200.x - 208.x, 238.6
# too broad, exclude

# Moderate or severe liver disease: 456.0 - 456.2, 572.2- 572.8
msliver_ref = diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(4560.*)')].copy()
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(4561.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(4562.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5722.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5723.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5724.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5725.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5726.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5727.*)')].copy())
msliver_ref = msliver_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(5728.*)')].copy())
msliver_ref.insert(3, 'disease', 'moderate or severe liver disease')
# Metastatic solid tumour: 196.x - 199.x
tumor_ref = diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(196.*)')].copy()
tumor_ref = tumor_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(197.*)')].copy())
tumor_ref = tumor_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(198.*)')].copy())
tumor_ref = tumor_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(199.*)')].copy())
tumor_ref.insert(3, 'disease', 'metastatic solid tumor')

# AIDS/HIV: 042.x - 044.x
hiv_ref = diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(042.*)')].copy()
hiv_ref = hiv_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(043.*)')].copy())
hiv_ref = hiv_ref.append(diag_ref.loc[diag_ref['DIAG_CD'].str.contains(r'^(044.*)')].copy())
hiv_ref.insert(3, 'disease', 'aids/hiv')



disease_def = mi_def\
    .append(chf_def)\
    .append(pvd_def)\
    .append(cd_def)\
    .append(de_def)\
    .append(cpd_def)\
    .append(ra_def)\
    .append(pud_ref)\
    .append(mld_ref)\
    .append(diab_ref)\
    .append(hem_ref)\
    .append(renal_ref)\
    .append(msliver_ref)\
    .append(tumor_ref)\
    .append(hiv_ref)

disease_def.to_parquet('../data/input/disease_def.parquet', index=False)
disease_def['disease'].value_counts()