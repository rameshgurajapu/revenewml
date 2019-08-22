#!/usr/bin/env python
import click


@click.command()
@click.option('--database', prompt='\nPlease enter a database for SPR scoring')
@click.option('--dsn', prompt='\nPlease enter ODBC data source name (DSN)')
def main(database, dsn):
    # Import packages
    import os
    import sys
    import shap
    import time
    import pickle
    import logging
    import configparser
    import numpy as np
    import pandas as pd
    from tqdm import tqdm
    from sqlalchemy import create_engine
    from timeit import default_timer as timer
    from src.preprocessing.lists.sum_list import sum_list
    from src.preprocessing.lists.min_list import min_list
    from src.preprocessing.lists.max_list import max_list
    from src.preprocessing.lists.std_list import std_list
    from src.preprocessing.lists.mean_list import mean_list
    from src.preprocessing.lists.log_list import log_list
    from src.preprocessing.top_features import get_top_features

    # Set global options
    os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'  # Necessary on Mac
    np.seterr(all='ignore')  # Suppresses warning messages about invalid log transforms
    sys.path.append('../')

    # Get application path
    application_path = os.getcwd()

    # Read config file
    config = configparser.ConfigParser()
    config_file = application_path + '/config.ini'
    config.read(config_file)
    model = config['Calibration']['model']

    dsn_db = f'{dsn}\{db}'

    # Create connection strings
    cnxn_str = f'mssql+pyodbc://@{dsn_db}'

    # Make database connection engine
    engine = create_engine(
        cnxn_str,
        fast_executemany=True,
        echo=True,
        # echo_pool=False,
        # implicit_returning=False,
        # isolation_level="AUTOCOMMIT",
    )

    # Set up logging
    start = timer()
    log_file = application_path + '/log.txt'
    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    handler = logging.StreamHandler()
    logger = logging.getLogger()
    logger.addHandler(handler)
    logging.info('\n<============================================================================>')
    logging.info(f'\nApplication path: {application_path}')
    logging.info(f'\nCurrent working directory: {os.getcwd()}')
    logging.info(f'\nApplication started ... ({time.ctime()})\n')
    logging.info(f'Step 1 of 6: Loading raw data into memory ... ({time.ctime()})')

    # Load duplicate reports
    logging.info(f'\n>> Querying duplicate reports table ... ({time.ctime()})')
    duplicates_table = '[Duplicate Reports]'
    duplicate_reports_query = open(application_path + '/src/preprocessing/sql/duplicate_reports.sql').read().format(
        database, duplicates_table, database, duplicates_table)  # Set database and table names for query here
    duplicate_reports = pd.read_sql(sql=duplicate_reports_query, con=engine)
    duplicate_reports['ProjectID'] = database
    table_size = duplicate_reports.shape
    logging.info(
        f'\n>> Duplicate reports data in memory has {table_size[0]} rows and {table_size[1]} columns ... ({time.ctime()})'
    )
    duplicate_reports = duplicate_reports.fillna(0)

    # Load vendor reports
    logging.info(f'\n>> Querying invoices table ... ({time.ctime()})')
    invoices_table = 'invoice'
    vendor_profiles_query = open(application_path + '/src/preprocessing/sql/vendor_profiles.sql').read().format(
        database, invoices_table, database, invoices_table)  # Set database and table names for query here
    vendor_profiles = pd.read_sql(sql=vendor_profiles_query, con=engine)
    vendor_profiles['ProjectID'] = database
    table_size = vendor_profiles.shape
    logging.info(
        f'\n>> Vendor invoices data in memory has {table_size[0]} rows and {table_size[1]} columns ... ({time.ctime()})'
    )
    vendor_profiles = vendor_profiles.fillna(0)

    # Load groups count profiles
    logging.info(f'\n>> Loading report count profiles ... ({time.ctime()})')
    count_profiles_query = open(application_path + '/src/preprocessing/sql/count_profiles.sql').read().format(
        database, duplicates_table, database, duplicates_table)  # Set database and table names for query here
    count_profiles = pd.read_sql(sql=count_profiles_query, con=engine)
    count_profiles['ProjectID'] = database
    table_size = count_profiles.shape
    logging.info(
        f'\n>> Count profiles data in memory has {table_size[0]} rows and {table_size[1]} columns ... ({time.ctime()})'
    )
    count_profiles = count_profiles.fillna(0)
    logging.info(f'\nStep 2 of 6: Assembling datasets for scoring ... ({time.ctime()})')

    # Average across vendors
    logging.info(f'\n>> Averaging across vendors ... ({time.ctime()})')
    vendor_means = (vendor_profiles
                    .groupby(['ProjectID', 'Vendor_Number'])
                    .mean()
                    .reset_index()
                    )
    logging.info(f'\n>> Matching duplicate reports to vendor-level aggregates ... ({time.ctime()})')
    df = duplicate_reports.merge(vendor_means, on=['ProjectID', 'Vendor_Number'])

    # Free up memory
    del duplicate_reports
    del vendor_profiles
    del vendor_means

    # Get vendor discrepancies at invoice level
    logging.info(f'\n>> Calculating vendor invoice discrepancies ... ({time.ctime()})')
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

    # Rollup to ReportID level
    rollup1 = ['ProjectID', 'Report_Group_Flag', 'ReportID']
    logging.info(f'\n>> Rolling up to ReportID level ... ({time.ctime()})')

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
    logging.info(f'\n>> Merging with ReportGroup level count profiles ... ({time.ctime()})')
    agg1 = (means
            .merge(maxes, right_index=True, left_index=True)
            .merge(mins, right_index=True, left_index=True)
            .merge(sums, right_index=True, left_index=True)
            .merge(stds, right_index=True, left_index=True))
    agg1.reset_index(inplace=True)
    agg1 = agg1.merge(count_profiles, on=rollup1, how='left').drop(columns=['ReportID', 'ReportGroup'])

    # Free up memory
    del mins
    del maxes
    del means
    del sums
    del stds

    # Rollup to Report_Group_Flag level
    logging.info(f'\n>> Rolling up to Report_Group_Flag level ... ({time.ctime()})\n')
    rollup2 = ['ProjectID', 'Report_Group_Flag']
    agg2 = agg1.groupby(rollup2).max()

    # Do log transform on selected variables
    for feature in log_list:
        try:
            log_feature = np.log10(agg2[feature])
            agg2['Log_' + feature] = log_feature
        except Exception as e:
            print(e)

    # Output final data
    table_size = agg2.shape
    scoring_data = agg2.copy()
    logging.info(
        f'\n>> Data in memory has {table_size[0]} rows and {table_size[1]} columns ... ({time.ctime()})')

    # Calculate correlations among predictive features
    correlations = scoring_data.corr()

    # Load calibrated model
    saved_model = application_path + '/src/savedmodels/' + model
    logging.info(
        f'\nStep 3 of 6: Scoring data with pre-calibrated XGBoost model "{saved_model}"... ({time.ctime()})\n')
    clf = pickle.load(open(saved_model, 'rb'))

    # Predicted probabilities
    y_prob = pd.merge(scoring_data[['AmountNet__NETONE_Mean', 'AmountNet__NETZERO_Mean']].reset_index(),
                      pd.Series(clf.predict_proba(scoring_data)[:, 1], name='Prob_Claim'),
                      left_index=True, right_index=True)

    # Set score equal to zero if claim already exists
    y_prob.loc[y_prob.AmountNet__NETONE_Mean > 0, 'Prob_Claim'] = 0.0
    y_prob.loc[y_prob.AmountNet__NETZERO_Mean > 0, 'Prob_Claim'] = 0.0

    # Predicted classes
    y_pred = pd.cut(y_prob.Prob_Claim, bins=[0.0, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0], right=True,
                    labels=['Below 0.40', '0.40-0.49', '0.50-0.59', '0.60-0.69', '0.70-0.79', '0.80-1.00'])

    # Configure the model-explaining algorithm
    explainer = shap.TreeExplainer(
        model=clf, model_output='margin',  # 'probability', 'log_loss',
        feature_dependence='tree_path_dependent')  # 'independent')

    # Explain the model's predictions using SHAP values
    logging.info(f'Step 4 of 6: Interpreting individual feature effects ... ({time.ctime()})\n')
    shap_values = explainer.shap_values(scoring_data)

    # Create score data frame
    df_scores = pd.DataFrame({
        'Score': y_prob.Prob_Claim,
        'ScoreTierA': y_pred.map({
            'Below 0.40': 6,
            '0.40-0.49': 5,
            '0.50-0.59': 4,
            '0.60-0.69': 3,
            '0.70-0.79': 2,
            '0.80-1.00': 1
        }), 'ScoreTierADesc': y_pred})

    # Top Features
    df_features = pd.DataFrame()
    logging.info(f'Step 5 of 6: Extracting top features based on Shap value ... ({time.ctime()})\n')
    df_features = df_features.append([
        get_top_features(
            ix=ix,
            X=scoring_data,
            correlations=correlations,
            shap_values=shap_values)
        for ix in tqdm(range(len(scoring_data)))
    ])

    # Output status to console
    logging.info(f'\nStep 6 of 6: Writing results to database ... ({time.ctime()})\n')

    # Write to SQL database
    df_output = (scoring_data
                 .reset_index()
                 .loc[:, ['ProjectID', 'Report_Group_Flag']]
                 .merge(df_scores, left_index=True, right_index=True)
                 .merge(df_features, left_index=True, right_index=True)
                 )

    # Set database table
    table_name = 'ModelScores'
    df_output.to_sql(name=table_name, con=engine, if_exists='append', method='multi')

    # Stop timer
    end = timer()
    elapsed = end - start
    logging.info(
        'Application finished in {:.2f} seconds ... ({})\n'
        '\n<============================================================================>\n\n'.format(
            elapsed, time.ctime()))


if __name__ == '__main__':
    main()
