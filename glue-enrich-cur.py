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


# OPTIONAL ARGUMENT PARSER (getResolvedOptions doesn't support optional arguments at the moment)
arg_parser = argparse.ArgumentParser(
    description="Enriches CUR data stored in S3 and enriches S3 by adding organizational account tags as columns to line items.", 
    epilog="NOTES: The script expects the source CUR data to be stored in parquet. The Cost and Usage Report configured to generate this data should be set to aoutput for ATHENA. Organizational account tags are merged based on line_item_usage_account_id. Because this queries the Organizations API, it must be run from the Master Account."
)
arg_parser.add_argument('--s3_source_bucket', required=True, help="The source bucket where the CUR data is located")
arg_parser.add_argument('--s3_source_prefix', required=True, help="The source prefix (path) where the CUR data is located. This should be the prefix immediately preceding the partition prefixes (i.e. path before /year=2020)")
arg_parser.add_argument('--s3_target_bucket', required=True, help="The destination bucket to put enriched CUR data.")
arg_parser.add_argument('--s3_target_prefix', required=False, default="", help="The destination prefix (path) where enriched CUR data will be placed. Put this in a prefix that is OUTSIDE of the original CUR data prefix.")
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
print("Fetching accounts and organizations account tags")
accounts = get_accounts()
account_tags_list = []
for account in accounts.get('Accounts', {}):
    account_dict = {}
    account_dict["account_id"] = account["Id"]
    account_dict["name"] = account["Name"]
    account_dict["email"] = account["Email"]
    account_dict["arn"] = account["Arn"]
    tags = get_tags_for_account(account["Id"])
    for tag in tags['Tags']:
        account_dict['account_tag_'+tag["Key"]] = tag["Value"]
    account_tags_list.append(account_dict)

print("Creating account tags DataFrame")
# Create DataFrame with accounts and associated tags
account_tags_df = pd.DataFrame(account_tags_list).convert_dtypes()
account_tags_df.info()

# Write account_tags_def to S3 in Parquet format
s3_written_objs = wr.s3.to_parquet(
    df = account_tags_df,
    path="s3://"+S3_TARGET_BUCKET+"/"+(S3_TARGET_PREFIX+"/" if S3_TARGET_PREFIX != "" else "")+"account_tags",
    mode = "overwrite",
    compression="snappy",
    index=False,
    dataset = True
  )

# Create a table in Glue data catalog for account tags
wr.s3.store_parquet_metadata(
    path="s3://"+S3_TARGET_BUCKET+"/"+(S3_TARGET_PREFIX+"/" if S3_TARGET_PREFIX != "" else "")+"account_tags",
    database=DATABASE_NAME,
    table="account_tags",
    dataset=True,
    mode="overwrite"
)

# Fetch catalog column names, drop account_id
account_tags_columns = list(wr.catalog.get_table_types(
    database=DATABASE_NAME, 
    table='account_tags'
).keys())
account_tags_columns.remove('account_id')

# Get a list of objects
print("Listing objects in path: {}".format("s3://"+S3_SOURCE_BUCKET+"/"+S3_SOURCE_PREFIX))

s3_objects = wr.s3.list_objects("s3://"+S3_SOURCE_BUCKET+"/"+S3_SOURCE_PREFIX)
print("Objects found: {}".format(s3_objects))

s3_prefixes = set()
for object in s3_objects:
    s3_prefixes.add(object[:object.rfind('/')])
s3_prefixes = list(s3_prefixes)

# Sort objects by month, adding leading zeros to the months to sort correctly
s3_objects.sort(key=lambda x: re.sub(r'(month=)(\d+)', lambda m : m.group(1)+m.group(2).zfill(2),x))
s3_prefixes.sort(key=lambda x: re.sub(r'(month=)(\d+)', lambda m : m.group(1)+m.group(2).zfill(2),x))
print("Prefixes found: {}".format(s3_prefixes))

# If INCREMENTAL_MODE_MONTHS is greater than zero, only use the last INCREMENTAL_MODE_MONTHS items in the list. This assumes everything is partitioned by '/year=xxxx/month=xx/'
if INCREMENTAL_MODE_MONTHS > 0:
  s3_prefixes = s3_prefixes[-INCREMENTAL_MODE_MONTHS:]
  print("Incremental mode set. Filtered objects: {}".format(s3_prefixes))

# Rather than trying to do a join on all the data at once, we're breaking it up by partition (prefix). his also allows us to perform incremental updates, leaving prior months 
# CUR data unaltered for historical purposes if desired. It can also easier to troubleshoot with subsets of data.
for s3_prefix in s3_prefixes:
  # Make sure garbage collection happens
  #del df
  #gc.collect()  
  print("=============")
  print('READING METADATA: {}'.format(s3_prefix+"/"))
  cur_column_types, partitions = wr.s3.read_parquet_metadata(
    path=s3_prefix+"/",
    dataset=True
  )
  print('CLEANING UP TEMP TABLES IF THEY EXIST')
  wr.catalog.delete_table_if_exists(
    database=DATABASE_NAME,
    table='temp_cur_enriched'
  )
  wr.catalog.delete_table_if_exists(
    database=DATABASE_NAME,
    table='temp_cur_original'
  )

  # Create a temporary table in the data catalog from the source CUR data. 
  print('CREATING TEMP TABLE FOR: {}'.format(s3_prefix+"/"))
  wr.catalog.create_parquet_table(
    path = s3_prefix+"/",
    database = DATABASE_NAME,
    table = 'temp_cur_original',
    columns_types= cur_column_types,
    #partitions_types=partitions,
    mode = 'overwrite'
  ) 
  
  # Fetch catalog column names
  temp_cur_original_columns = wr.catalog.get_table_types(
    database=DATABASE_NAME, 
    table='temp_cur_original'
  )
  print('CLEANING UP {}'.format("s3://"+S3_TARGET_BUCKET+"/"+(S3_TARGET_PREFIX+"/" if S3_TARGET_PREFIX != "" else "")+ s3_prefix.split('/', 3)[3])+"/")
  wr.s3.delete_objects(
    path="s3://"+S3_TARGET_BUCKET+"/"+(S3_TARGET_PREFIX+"/" if S3_TARGET_PREFIX != "" else "")+ s3_prefix.split('/', 3)[3]+"/*"
  )

  print('Merging account tags and writing output to {}'.format("s3://"+S3_TARGET_BUCKET+"/"+(S3_TARGET_PREFIX+"/" if S3_TARGET_PREFIX != "" else "")+ s3_prefix.split('/', 3)[3]))
  
  # Note: Using a SELECT * statement won't work if we want to partition by line_item_usage_account_id because it has to be the last column in the list. 
  # The lines below build a list of column names to use in the CREATE TABLE AS SELECT (CTAS) statement. The 'account_id' column from account_tags is
  # also dropped since it's redundant.
  temp_cur_original_columns = list(temp_cur_original_columns.keys())
  if PARTITION_BY_ACCOUNT: 
    temp_cur_original_columns.remove('line_item_usage_account_id')
    column_string = ",".join(temp_cur_original_columns)+","+",".join(account_tags_columns)+',line_item_usage_account_id'
  else:
    column_string = column_string = ",".join(temp_cur_original_columns)+","+",".join(account_tags_columns)

  # Rather than loading the CUR data into a dataframe and merging, which can be memory and CPU intensive, the command below uses an Athena CTAS statement to 
  # create a new table with the joined data. The data is written to S3 in Parquet format. 
  # Note: Another reason for using Athena is that CUR Parquet files use the Parquet V2 format and occasionally uses DELTA encoded columns. These columns
  # are not currently readable with Pandas - and more specifically the pyarrow package. 
  # https://github.com/awslabs/aws-data-wrangler/issues/442
  # 
  wr.athena.read_sql_query(
    database=DATABASE_NAME,
    ctas_approach=False,
    sql="""
    CREATE TABLE temp_cur_enriched WITH 
     (
       format='PARQUET',
       parquet_compression='SNAPPY',
       external_location = ':s3_target;'
       :partition_clause;
     )
    AS 
     SELECT :columns; FROM temp_cur_original LEFT JOIN account_tags ON temp_cur_original.line_item_usage_account_id = account_tags.account_id
     """,
    params={
      "s3_target": "s3://"+S3_TARGET_BUCKET+"/"+(S3_TARGET_PREFIX+"/" if S3_TARGET_PREFIX != "" else "")+ s3_prefix.split('/', 3)[3],
      "partition_clause" : ",partitioned_by=array['line_item_usage_account_id']" if PARTITION_BY_ACCOUNT else "",
      "columns": column_string
    }
  )

  # Once the enriched CUR data is written to S3 by Athena, we can remove the table definition the CTAS statement created in the catalog. 
  print('CLEANING UP TEMP TABLES IF THEY EXIST')
  wr.catalog.delete_table_if_exists(
    database=DATABASE_NAME,
    table='temp_cur_enriched'
  )
  # Also remote the table containing the original CUR data
  wr.catalog.delete_table_if_exists(
    database=DATABASE_NAME,
    table='temp_cur_original'
  )
  print("=============")


if CREATE_TABLE:
  if OVERWRITE_EXISTING_TABLE:
    print("Deleting {}.{} if it exists".format(DATABASE_NAME, TABLE_NAME))
    wr.catalog.delete_table_if_exists(
      database=DATABASE_NAME,
      table=TABLE_NAME
    )
  

  print ("Extracting parquet metadata from {}".format( "s3://"+S3_TARGET_BUCKET+"/"+s3_prefix.split('/', 3)[3]+"/"))
  cur_column_types, partitions = wr.s3.read_parquet_metadata(
    path="s3://"+S3_TARGET_BUCKET+"/"+s3_prefix.split('/', 3)[3]+"/",
    dataset=True,
    sampling=1
  ) 

  print ("Creating Table {}.{} with path {}".format(DATABASE_NAME, TABLE_NAME, "s3://"+S3_TARGET_BUCKET+"/"+(S3_SOURCE_PREFIX+"/" if S3_SOURCE_PREFIX != "" else "")))
  wr.catalog.create_parquet_table(
    path = "s3://"+S3_TARGET_BUCKET+"/"+(S3_SOURCE_PREFIX+"/" if S3_SOURCE_PREFIX != "" else ""),
    database = DATABASE_NAME,
    table = TABLE_NAME,
    columns_types= cur_column_types,
    partitions_types=partitions
  )

  
  print ("Updated Table Partitions {}.{}".format(DATABASE_NAME, TABLE_NAME))
  wr.athena.repair_table(
    database=DATABASE_NAME,
    table=TABLE_NAME
  ) 


print ("Finished")