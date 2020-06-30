""" /*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */ """

import os
import sys

import awswrangler as wr
import pandas as pd
import boto3
import argparse
import re

orgs_client = boto3.client('organizations')

# Fetches all accounts from the organizations API
def get_accounts ():
    accounts={ 'Accounts':[] }
    paginator = orgs_client.get_paginator('list_accounts')
    page_iterator = paginator.paginate()
    for page in page_iterator:
        accounts['Accounts'] = accounts['Accounts'] + page['Accounts']    
    return accounts    

# Fetches all tags for an AWS Account ID
def get_tags_for_account (account):
    tags={ 'Tags':[] }
    paginator = orgs_client.get_paginator('list_tags_for_resource')
    page_iterator = paginator.paginate(
        ResourceId=account
    )
    for page in page_iterator:
        tags['Tags'] = tags['Tags'] + page['Tags']    
    return tags

# Returns value of cell 0 at a given index name (idx) from a Dataframe, if it exists. Otherwise, returns None. 
# This is used to handle situations where the Dataframe doesn't contain the index (i.e. a particular tag isn't used in the account)
def get_kv_pair_value(df, idx):
    try:
        return df.loc[idx][0]
    except KeyError:
        return None


# OPTIONAL ARGUMENT PARSER (getResolvedOptions doesn't support optional arguments at the moment)
arg_parser = argparse.ArgumentParser(
    description="Enriches CUR data stored in S3 and enriches S3 by adding organizational account tags as columns to line items.", 
    epilog="NOTES: The script expects the source CUR data to be stored in parquet. The Cost and Usage Report configured to generate this data should be set to aoutput for ATHENA. Organizational account tags are merged based on line_item_usage_account_id. Because this queries the Organizations API, it must be run from the Master Account."
)
arg_parser.add_argument('--s3_source_bucket', required=True, help="The source bucket where the CUR data is located")
arg_parser.add_argument('--s3_source_prefix', required=True, help="The source prefix (path) where the CUR data is located. This should be the prefix immediately preceding the partition prefixes (i.e. path before /year=2020)")
arg_parser.add_argument('--s3_target_bucket', required=True, help="The destination bucket to put enriched CUR data.")
arg_parser.add_argument('--s3_target_prefix', required=True, help="The destination prefix (path) where enriched CUR data will be placed. Put this in a prefix that is OUTSIDE of the original CUR data prefix.")
arg_parser.add_argument('--incremental_mode_months', type=int, required=False, default=0, help="When set, last x months of CUR data is processed. When not set or set to 0, all data is processed. For incremental updates, recommend setting this to 2.")
arg_parser.add_argument('--create_table', action='store', required=False, default=False, help="When set, a table will be created in Glue automatically and made available through Athena.")
arg_parser.add_argument('--overwrite_existing_table', type=bool, action='store', required=False, default=False, help="When set, the existing table will be overwritten. This should be set when performing updates.")
arg_parser.add_argument('--partition_by_account', type=bool, action='store', required=False, default=False, help="When set, an additional partition for line_item_usage_account_id will be created. Example: /year=2020/month=1/line_item_usage_account_id=123456789012")
arg_parser.add_argument('--database_name', type=str, required=False, default=None, help="The name of the Glue database to create the table in. Must be set when --create_table is set")
arg_parser.add_argument('--table_name', type=str, required=False, default=None, help="The name of the Glue table to create or overwrite. Must be set when --create_table is set")
arg_parser.add_argument('--extra-py-files', type=str, required=False, default=None)
arg_parser.add_argument('--scriptLocation', type=str, required=False, default=None)
arg_parser.add_argument('--job-bookmark-option', type=str, required=False, default=None)
arg_parser.add_argument('--job-language', type=str, required=False, default=None)
print(vars(arg_parser.parse_args() ))
args = vars(arg_parser.parse_args())


S3_SOURCE_BUCKET = args["s3_source_bucket"]
S3_SOURCE_PREFIX = args["s3_source_prefix"]
S3_TARGET_BUCKET = args["s3_target_bucket"]
S3_TARGET_PREFIX = args["s3_target_prefix"]
INCREMENTAL_MODE_MONTHS = args["incremental_mode_months"]
PARTITION_BY_ACCOUNT = args['partition_by_account']
CREATE_TABLE = args["create_table"]
DATABASE_NAME = args["database_name"]
TABLE_NAME = args["table_name"]
OVERWRITE_EXISTING_TABLE = args["overwrite_existing_table"]

if TABLE_NAME == None or DATABASE_NAME == None: raise Exception('Must specify Glue Database and Table when "--create_table" is set')
if wr.catalog.does_table_exist(DATABASE_NAME,TABLE_NAME) and not OVERWRITE_EXISTING_TABLE: raise Exception('The table {}.{} already exists but OVERWRITE_EXISTING_TABLE isn''t set.'.format(DATABASE_NAME, TABLE_NAME))


# First we retrieve the accounts through the organizations API
print("Fetching accounts")
accounts = pd.json_normalize(get_accounts(), record_path='Accounts')  
accounts.info()

print("Fetching account tags")
# Next we retrieve the tags for each account via the organizations API and load them into a new 'Tags' column
accounts['Tags'] = accounts.apply(lambda x: get_tags_for_account(x['Id'])['Tags'], axis=1)

# We need to convert the tag values in each row into columns in our DataFrame. 
# Each result in the 'Tags' column is list of dict with this format: [{'Key': 'CostCenter', 'Value': 'MyTestCostCenter'}, {'Key': 'BudgetCode', 'Value': 'MyTestBudgetCode'}]
# 'Key' is the name of the tag, and 'Value' is its value
# Every account can have different tags, or none at all. So how do we take these tag keys, make them columns, and then fill those columns with the value?
# The steps below are:
#   1. Get a list of all unique 'Key' values (tag names).
uniqueTagKeys=pd.concat([pd.DataFrame(pd.json_normalize(x)) for x in accounts['Tags']], ignore_index=True)['Key'].drop_duplicates().values.tolist()

print("Creating account tags DataFrame")
account_tags = pd.DataFrame()
account_tags["account_tag__account_id"] = accounts['Id']
account_tags=account_tags.convert_dtypes()

#   2. Iterate through each one and use it to create a new column, 'tag_TAGNAME' in a DataFrame
for tagKey in uniqueTagKeys:
#   3. Use .apply with a lambda function on the 'Tags' column to retrieve nested 'Value' in each item in the column, if it exists
#   4. Set the value of the new column to the series that was returned by .apply
    account_tags['tag_' + tagKey] = accounts['Tags'].apply(lambda x: get_kv_pair_value(pd.DataFrame(x, columns=['Key', 'Value']).set_index('Key'),tagKey))
# Note: A lambda (get_kv_pair_value) is used so that it can handle exceptions when the tag does not exist for a particular account or the account had no tags at all

account_tags.set_index("account_tag__account_id")
print("Account tags DataFrame")
account_tags.info()

# Get a list of objects
print("Listing objects in path: {}".format("s3://"+S3_SOURCE_BUCKET+"/"+S3_SOURCE_PREFIX))

s3_objects = wr.s3.list_objects("s3://"+S3_SOURCE_BUCKET+"/"+S3_SOURCE_PREFIX)
# Sort objects by month, adding leading zeros to the months to sort correctly
s3_objects.sort(key=lambda x: re.sub(r'(month=)(\d+)', lambda m : m.group(1)+m.group(2).zfill(2),x))
print("Objects found: {}".format(s3_objects))

# If INCREMENTAL_MODE_MONTHS is greater than zero, only use the last INCREMENTAL_MODE_MONTHS items in the list. This assumes everything is partitioned by '/year=xxxx/month=xx/'
if INCREMENTAL_MODE_MONTHS > 0:
  s3_objects = s3_objects[-INCREMENTAL_MODE_MONTHS:]
  print("Incremental mode set. Filtered objects: {}".format(s3_objects))

for s3_obj in s3_objects:
  print("=============")
  print('READING: {}'.format(s3_obj))
  df = wr.s3.read_parquet(path=s3_obj)
  df.set_index('identity_line_item_id')
  print('Merging account tags')
  df = df.merge(account_tags, left_on='line_item_usage_account_id', right_on='account_tag__account_id')
  df.drop(columns=['account_tag__account_id'], inplace=True)

  print("=== BEGIN DATAFRAME INFO ===")
  df.info()
  with pd.option_context('display.max_rows', None, 'display.max_columns', None): print(df.columns)
  print("=== END DATAFRAME INFO ===")
  print ("WRITING TO PATH: {}".format("s3://"+S3_TARGET_BUCKET+"/"+S3_TARGET_PREFIX+"/"+ wr._utils.parse_path(s3_obj)[1].rsplit("/",1)[0]))
  s3_written_objs = wr.s3.to_parquet(
    df = df,
    path = "s3://"+S3_TARGET_BUCKET+"/"+S3_TARGET_PREFIX+"/"+ wr._utils.parse_path(s3_obj)[1].rsplit("/",1)[0],
    partition_cols=  ["line_item_usage_account_id"] if PARTITION_BY_ACCOUNT else [],
    mode = "overwrite",
    compression="snappy",
    index=False,
    dataset = True

  )
  print("Wrote: {}".format(s3_written_objs))
  print("=============")

if CREATE_TABLE:
  if OVERWRITE_EXISTING_TABLE:
    print("Deleting {}.{} if it exists".format(DATABASE_NAME, TABLE_NAME))
    wr.catalog.delete_table_if_exists(
      database=DATABASE_NAME,
      table=TABLE_NAME
    )
  

  print ("Creating Table {}.{} with path {}".format(DATABASE_NAME, TABLE_NAME, "s3://"+S3_TARGET_BUCKET+"/"+S3_TARGET_PREFIX+"/"+ wr._utils.parse_path(s3_obj)[1].rsplit("/",3)[0]+"/"))
  wr.catalog.create_parquet_table(
    path = "s3://"+S3_TARGET_BUCKET+"/"+S3_TARGET_PREFIX+"/"+wr._utils.parse_path(s3_obj)[1].rsplit("/",3)[0]+"/",
    database = DATABASE_NAME,
    table = TABLE_NAME,
    columns_types= wr._data_types.athena_types_from_pandas(df=df.drop(columns='line_item_usage_account_id') if PARTITION_BY_ACCOUNT else df, index=False),
    partitions_types={'year': 'string','month':'string','line_item_usage_account_id':'string'} if PARTITION_BY_ACCOUNT else {'year': 'string','month':'string'},
  )


  print ("Updated Table Partitions {}.{}".format(DATABASE_NAME, TABLE_NAME))
  wr.athena.repair_table(
    database=DATABASE_NAME,
    table=TABLE_NAME
  ) 

print ("Finished")