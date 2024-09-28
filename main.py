import pandas as pd
import ast
from datetime import datetime, timedelta

df = pd.read_csv('data.csv')

# Convert column from string to list
df['contracts'] = df['contracts'].fillna('[]').apply(lambda x: ast.literal_eval(x))

# Ensure that single dictionaries are wrapped in a list
df['contracts'] = df['contracts'].apply(lambda x: [x] if isinstance(x, dict) else x)

# Explode the 'contracts' list into separate rows
df_exploded = df.explode('contracts', ignore_index=True)

# Extract data from 'contracts' into new columns
df_exploded.loc[df_exploded['contracts'].notna(), 'contract_id'] = df_exploded[df_exploded['contracts'].notna()]['contracts'].apply(lambda x: x['contract_id'] if 'contract_id' in x.keys() else None)
df_exploded.loc[df_exploded['contracts'].notna(), 'bank'] = df_exploded[df_exploded['contracts'].notna()]['contracts'].apply(lambda x: x['bank'] if 'bank' in x.keys() else None)
df_exploded.loc[df_exploded['contracts'].notna(), 'summa'] = df_exploded[df_exploded['contracts'].notna()]['contracts'].apply(lambda x: x['summa'] if 'summa' in x.keys() else None)
df_exploded.loc[df_exploded['contracts'].notna(), 'loan_summa'] = df_exploded[df_exploded['contracts'].notna()]['contracts'].apply(lambda x: x['loan_summa'] if 'loan_summa' in x.keys() else None)
df_exploded.loc[df_exploded['contracts'].notna(), 'claim_date'] = df_exploded[df_exploded['contracts'].notna()]['contracts'].apply(lambda x: x['claim_date'] if 'claim_date' in x.keys() else None)
df_exploded.loc[df_exploded['contracts'].notna(), 'claim_id'] = df_exploded[df_exploded['contracts'].notna()]['contracts'].apply(lambda x: x['claim_id'] if 'claim_id' in x.keys() else None)
df_exploded.loc[df_exploded['contracts'].notna(), 'contract_date'] = df_exploded[df_exploded['contracts'].notna()]['contracts'].apply(lambda x: x['contract_date'] if 'contract_date' in x.keys() else None)

################
# tot_claim_cn #
################
df_exploded['claim_date'] = pd.to_datetime(df_exploded['claim_date'], format='%d.%m.%Y')

current_date = datetime.now()
cutoff_date = current_date - timedelta(days=180)

df_exploded['within_cutoff_date'] = df_exploded['claim_date'].apply(lambda x: 1 if x >= cutoff_date else 0)

df_exploded['tot_claim_cn'] = df_exploded.groupby('id')['within_cutoff_date'].transform('sum')

# In case no claims, then put -3 as a value of this feature.
df_exploded.loc[df_exploded['tot_claim_cn'] == 0, 'tot_claim_cn'] = -3

#########################
# disb_bank_loan_wo_tbc #
#########################


df_exploded.loc[
                (~df_exploded['bank'].isin(['LIZ', 'LOM', 'MKO', 'SUG'])) & (df_exploded['bank'].notna()) &
                (df_exploded['contract_date'].notna()),
                'disb_bank_loan_wo_tbc'] = df_exploded.loc[
                                            (~df_exploded['bank'].isin(['LIZ', 'LOM', 'MKO', 'SUG'])) & (df_exploded['bank'].notna()) &
                                            (df_exploded['contract_date'].notna()),
                                            'loan_summa']

# If no loans at all, then put -1 as a value of this feature.
df_exploded.loc[((df_exploded['loan_summa'].isna()) | (df_exploded['loan_summa'] == '') | (df_exploded['bank'].isin(['LIZ', 'LOM', 'MKO', 'SUG'])) | (df_exploded['bank'].isna())), 'disb_bank_loan_wo_tbc'] = -1

# In case no claims, then put -3 as a value of this feature.
df_exploded.loc[(df_exploded['claim_date'].isna()) & (df_exploded['bank'].isin(['LIZ', 'LOM', 'MKO', 'SUG'])) | (df_exploded['bank'].isna()), 'disb_bank_loan_wo_tbc'] = -3

###################
# day_sinlastloan #
###################
loan_dates = df_exploded.loc[(df_exploded['summa'].notna()) & (df_exploded['summa'] != ''), ['id', 'application_date', 'contract_date']]
loan_dates['contract_date'] = pd.to_datetime(loan_dates['contract_date'], format='%d.%m.%Y')

loan_dates['max_date'] = loan_dates.groupby('id')['contract_date'].transform('max')
loan_dates = loan_dates[['id', 'application_date', 'max_date']].drop_duplicates()

loan_dates['application_date'] = pd.to_datetime(loan_dates['application_date']).dt.tz_localize(None)

# Calculate the difference in days between 'application_date' and 'max_date'
loan_dates['day_sinlastloan'] = (loan_dates['application_date'] - loan_dates['max_date']).dt.days


# Merge everything back to the original format

df_exploded_extract = df_exploded.merge(loan_dates[['id', 'day_sinlastloan']], on='id', how='left')

# In case no loans at all, then put -1 as a value of this feature.
df_exploded_extract.loc[df_exploded_extract['day_sinlastloan'].isna() & ((df_exploded_extract['loan_summa'].isna()) | (df_exploded_extract['loan_summa'] == '')), 'day_sinlastloan'] = -1

# In case no claims at all, then put -3 as a value of this feature.
df_exploded_extract.loc[((df_exploded_extract['day_sinlastloan'].isna()) | (df_exploded_extract['day_sinlastloan'] == -1)) & (df_exploded['claim_date'].isna()), 'day_sinlastloan'] = -3


df_exploded_extract = df_exploded_extract[['id', 'tot_claim_cn', 'disb_bank_loan_wo_tbc', 'day_sinlastloan']].drop_duplicates()
df_exploded_extract = df_exploded_extract.groupby(['id', 'tot_claim_cn', 'day_sinlastloan'])['disb_bank_loan_wo_tbc'].agg(list).reset_index()


def process_list(lst):
    # Check if there are any values >= 0
    positives = [x for x in lst if x >= 0]

    if positives:
        # If there are positive values sum them up
        return sum(positives)
    else:
        # If there are no positive values, keep only -1 or -3
        return [-3 if -3 in lst else -1][0]


# Apply the custom function to each list in the 'col' column
df_exploded_extract['disb_bank_loan_wo_tbc'] = df_exploded_extract['disb_bank_loan_wo_tbc'].apply(process_list)

# Final result
df_final = df.merge(df_exploded_extract, on='id', how='left')

