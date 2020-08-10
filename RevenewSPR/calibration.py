import os
import pickle
import logging
import time
import xgboost as xgb
import pandas as pd
import datatable as dt
import numpy as np
from sklearn.model_selection import PredefinedSplit
from timeit import default_timer as timer
from src.preprocessing.lists.sum_list import sum_list
from src.preprocessing.lists.min_list import min_list
from src.preprocessing.lists.max_list import max_list
from src.preprocessing.lists.std_list import std_list
from src.preprocessing.lists.mean_list import mean_list
from src.preprocessing.lists.log_list import log_list

# Global options
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
np.seterr(all='ignore')  # Suppresses warning messages about invalid log transforms
pd.set_option('display.float_format', '{:.4f}'.format)
pd.set_option('use_inf_as_na', True)
start = timer()

# Set up logging
log_file = '../log.txt'
logging.basicConfig(filename=log_file, level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
logging.info('\n<============================================================================>')
logging.info(f'\nCurrent working directory: {os.getcwd()}')
logging.info(f'\nApplication started ... ({time.ctime()})\n')

# Load calibration data
# y = (pd.read_csv('src/flatfiles/Final Preprocessed For MichaelSampled Latest.txt', sep='\t')
#      .loc[:, ['ProjectID', 'Report_Group_Flag', 'Y_Has_Claim', 'Partition']]
# )
# y.to_parquet('src/flatfiles/Y_Calibration.parquet')
y = pd.read_parquet('src/flatfiles/Y_Calibration.parquet')

# count_profiles = (dt.fread('datasets/Count_Profiles_Calibration.csv', show_progress=True)
count_profiles = (pd.read_parquet('src/flatfiles/Count_Profiles_Calibration.parquet')
                  # .to_pandas()
                  .merge(y, on=['ProjectID', 'Report_Group_Flag'])
                  .drop(columns=['Y_Has_Claim', 'Partition'])
                  )
# count_profiles.to_parquet('src/flatfiles/Count_Profiles_Calibration.parquet')

# duplicate_reports = (dt.fread('datasets/Duplicate_Reports_Calibration.csv', show_progress=True)
duplicate_reports = (pd.read_parquet('src/flatfiles/Duplicate_Reports_Calibration.parquet')
                     # .to_pandas()
                     .merge(y, on=['ProjectID', 'Report_Group_Flag'])
                     .drop(columns=['Y_Has_Claim', 'Partition'])
                     # ).sample(100000)
                     )
# duplicate_reports.to_parquet('src/flatfiles/Duplicate_Reports_Calibration.parquet')

# vendor_profiles = (dt.fread('datasets/Vendor_Profiles_Calibration.csv', show_progress=True)
#                    .to_pandas()
#                    )
# vendor_profiles.to_parquet('src/flatfiles/Vendor_Profiles_Calibration.parquet')
vendor_profiles = pd.read_parquet('src/flatfiles/Vendor_Profiles_Calibration.parquet')

# Average across vendors
logging.info('\n>> Averaging across vendors ... ({})'.format(time.ctime()))
vendor_means = (vendor_profiles
                .groupby(['ProjectID', 'Vendor_Number'])
                .mean()
                .reset_index())

logging.info('\n>> Matching duplicate reports to vendor-level aggregates ... ({})'.format(time.ctime()))
df = duplicate_reports.merge(vendor_means, on=['ProjectID', 'Vendor_Number'])

# Free up memory
del duplicate_reports
del vendor_profiles
del vendor_means

# Get vendor discrepancies
logging.info('\n>> Calculating vendor invoice discrepancies ... ({})'.format(time.ctime()))
df['Invoice_Number_Length_Discr'] = df['Invoice_Number_Length'] - df['Vendor_Invoice_Number_Length']
df['Invoice_Number_Numeric_Discr'] = df['Invoice_Number_Numeric'] - df['Vendor_Invoice_Number_Numeric']
df['Invoice_Number_AlphasPct_Discr'] = df['Invoice_Number_AlphasPct'] - df['Vendor_Invoice_Number_AlphasPct']
df['Invoice_Number_NumsPct_Discr'] = df['Invoice_Number_NumsPct'] - df['Vendor_Invoice_Number_NumsPct']
df['Invoice_Number_LeadingZeroes_Discr'] = df['Invoice_Number_LeadingZeroes'] - df[
    'Vendor_Invoice_Number_LeadingZeroes']
df['Daily_Invoice_Count_Discr'] = df['Daily_Invoice_Count'] - df['Vendor_Daily_Invoice_Count']
df['Days_Invoice_to_Check_Date_Discr'] = df['Days_Invoice_to_Check_Date'] - df['Vendor_Days_Invoice_to_Check_Date']
df['Days_Invoice_to_Clearing_Date_Discr'] = df['Days_Invoice_to_Clearing_Date'] - df[
    'Vendor_Days_Invoice_to_Clearing_Date']
df['Days_Invoice_to_Posting_Date_Discr'] = df['Days_Invoice_to_Posting_Date'] - df[
    'Vendor_Days_Invoice_to_Posting_Date']
df['Days_Invoice_to_Void_Date_Discr'] = df['Days_Invoice_to_Void_Date'] - df['Vendor_Days_Invoice_to_Void_Date']
df['Gross_Invoice_Amount_Discr'] = df['Gross_Invoice_Amount'] - df['Vendor_Gross_Invoice_Amount']
df['UniqueDigits_Gross_Invoice_Amount_Discr'] = df['UniqueDigits_Gross_Invoice_Amount'] - df[
    'Vendor_UniqueDigits_Gross_Invoice_Amount']
df['HasFrac_Gross_Invoice_Amount_Discr'] = df['HasFrac_Gross_Invoice_Amount'] - df[
    'Vendor_HasFrac_Gross_Invoice_Amount']
df['NotZeroEnding_Gross_Invoice_Amount_Discr'] = df['NotZeroEnding_Gross_Invoice_Amount'] - df[
    'Vendor_NotZeroEnding_Gross_Invoice_Amount']
df['Gross_Invoice_Amount_Local_Discr'] = df['Gross_Invoice_Amount_Local'] - df['Vendor_Gross_Invoice_Amount_Local']
df['HasFrac_Gross_Invoice_Amount_Local_Discr'] = df['HasFrac_Gross_Invoice_Amount_Local'] - df[
    'Vendor_HasFrac_Gross_Invoice_Amount_Local']
df['NotZeroEnding_Gross_Invoice_Amount_Local_Discr'] = df['NotZeroEnding_Gross_Invoice_Amount_Local'] - df[
    'Vendor_NotZeroEnding_Gross_Invoice_Amount_Local']
df['Check_Amount_Discr'] = df['Check_Amount'] - df['Vendor_Check_Amount']
df['HasFrac_Check_Amount_Discr'] = df['HasFrac_Check_Amount'] - df['Vendor_HasFrac_Check_Amount']
df['NotZeroEnding_Check_Amount_Discr'] = df['NotZeroEnding_Check_Amount'] - df['Vendor_NotZeroEnding_Check_Amount']
df['Absolute_Amount_Discr'] = df['Absolute_Amount'] - df['Vendor_Absolute_Amount']
df['HasFrac_Absolute_Amount_Discr'] = df['HasFrac_Absolute_Amount'] - df['Vendor_HasFrac_Absolute_Amount']
df['NotZeroEnding_Absolute_Amount_Discr'] = df['NotZeroEnding_Absolute_Amount'] - df[
    'Vendor_NotZeroEnding_Absolute_Amount']
df['Doc_Num_Spread_Discr'] = df['Doc_Num_Spread'] - df['Vendor_Doc_Num_Spread']
df['PaymentDoc_Num_Spread_Discr'] = df['PaymentDoc_Num_Spread'] - df['Vendor_PaymentDoc_Num_Spread']
df['Voucher_Num_Spread_Discr'] = df['Voucher_Num_Spread'] - df['Vendor_Voucher_Num_Spread']

# Output data dimensions
table_size = df.shape
logging.info(f'\n>> Data in memory has {table_size[0]} rows and {table_size[1]} columns ... ({time.ctime()})')


# Rollup to ReportID level
rollup1 = ['ProjectID', 'Report_Group_Flag', 'ReportID']
logging.info('\n>> Rolling up to ReportID level ... ({})'.format(time.ctime()))

# Get aggregates
means = df[mean_list].groupby(rollup1).mean()
means.columns = means.columns + '_Mean'
maxes = df[max_list].groupby(rollup1).max()
maxes.columns = maxes.columns + '_Max'
mins = df[min_list].groupby(rollup1).min()
mins.columns = mins.columns + '_Min'
sums = df[sum_list].groupby(rollup1).sum()
sums.columns = sums.columns + '_Sum'
stds = df[std_list].groupby(rollup1).std()
stds.columns = stds.columns + '_SDev'

# Merge with count profiles
logging.info('\n>> Merging with ReportGroup level count profiles ... ({})'.format(time.ctime()))
agg1 = (means
        .merge(maxes, right_index=True, left_index=True)
        .merge(mins, right_index=True, left_index=True)
        .merge(sums, right_index=True, left_index=True)
        .merge(stds, right_index=True, left_index=True))
agg1.reset_index(inplace=True)
agg1 = agg1.merge(count_profiles, on=rollup1, how='left')

# Free up memory
del mins
del maxes
del means
del sums
del stds

# Output data dimensions
table_size = agg1.shape
logging.info(f'\n>> Data in memory has {table_size[0]} rows and {table_size[1]} columns ... ({time.ctime()})')

agg1 = agg1.drop(columns=['ReportGroup', 'ReportID']).fillna(0)

# Rollup to Report_Group_Flag level
logging.info(f'\n>> Rolling up to Report_Group_Flag level ... ({time.ctime()})')
rollup2 = ['ProjectID', 'Report_Group_Flag']
agg2 = agg1.groupby(rollup2).max()

# Output data dimensions
table_size = agg2.shape
logging.info(f'\n>> Data in memory has {table_size[0]} rows and {table_size[1]} columns ... ({time.ctime()})')
#
# # Coerce variables to numeric type for XGBoost
# numeric_list = ['ProjectID',
#                 'ReportID',
#                 'Vendor_Number',
#                 'Document_Type',
#                 'Payment_Document_Number',
#                 'Document_Number',
#                 'Check_Number',
#                 'Invoice_Number',
#                 'Check_Date',
#                 'Clearing_Date',
#                 'Posting_Date',
#                 'Void_Date',
#                 ]
#
# for feature in numeric_list:
#     try:
#         agg2[feature] = pd.to_numeric(agg2[feature], errors='coerce')
#     except Exception as e:
#         print(e)

# Do log transform on selected variables
logging.info(f'\n>> Doing log transform on selected variables ... ({time.ctime()})')
for feature in log_list:
    try:
        agg2['Log_' + feature] = np.log10(agg2[feature])
    except Exception as e:
        pass

# Merge features and target
final = agg2.merge(y, on=['ProjectID', 'Report_Group_Flag'])
final.to_csv('src/flatfiles/final_data.csv', header=True, index=False)

# Create feature matrix
X = final.drop(columns=['Y_Has_Claim', 'Partition', 'ProjectID', 'Report_Group_Flag'])
y = final['Y_Has_Claim']

# Get partition that was used in SPSS version
ps = PredefinedSplit(final['Partition'].replace({'1_Training': 0, '2_Testing': 1}))

# Partition data into training and testing sets
for train_index, test_index in ps.split():
    X_train, X_test = X.iloc[train_index], X.iloc[test_index]
    y_train, y_test = y.iloc[train_index], y.iloc[test_index]

# Output status to consolde
logging.info(f'\n>> Calibrating XGBoost model ... ({time.ctime()})')

# Set classifier parameters
clf = xgb.XGBClassifier(max_depth=6, learning_rate=.3, n_estimators=10, silent=True, objective='binary:logistic',
                        booster='gbtree', n_jobs=10, gamma=0, min_child_weight=1, max_delta_step=0, subsample=1,
                        colsample_bytree=1, colsample_bylevel=1, reg_alpha=0, reg_lambda=1, scale_pos_weight=1,
                        base_score=.5, random_state=3048570)

# Fit classifier to data
clf.fit(X_train, y_train, eval_metric='auc', eval_set=[(X_test, y_test)], verbose=True)

# Record calibration date
calibration_date = time.strftime('%Y%m%d')

# Tag model with creation date
model_name = 'XGBoostModel_' + calibration_date + '.dat'

# Output serialized model to use for scoring
pickle.dump(clf, open('src/savedmodels/' + model_name, 'wb'))

# Stop timer
end = timer()
elapsed = end - start
logging.info(
    'Application finished in {:.2f} seconds ... ({})\n'
    '\n<============================================================================>\n\n'.format(
        elapsed, time.ctime()))

if __name__ == '__main__':
    ''
