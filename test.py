account_tags_columns = ["account_tag_billing-contact", "account_tag_cost-center", "name"]
temp_cur_original_columns = ["test1", "test2"]

INCLUDE_FIELDS = [item.lower().strip() for item in [" Test1", "test2"]]
INCLUDE_ACCOUNT_TAGS = [item.lower().strip() for item in ["billing-contact ", "cost-center", "name"]]
EXCLUDE_ACCOUNT_TAGS =[item.lower().strip() for item  in ["billing-contact "]]
EXCLUDE_FIELDS = [item.lower().strip() for item in [" Test1"]]
PARTITION_BY_ACCOUNT = False

temp_cur_original_columns = [item for item in temp_cur_original_columns if item.lower().strip() not in EXCLUDE_FIELDS]
account_tags_columns = [item for item in account_tags_columns if item.lower().strip().replace('account_tag_', '') not in EXCLUDE_ACCOUNT_TAGS]

if len(INCLUDE_FIELDS) > 0: temp_cur_original_columns = [item for item in temp_cur_original_columns if item.lower().strip() in INCLUDE_FIELDS]
if len(INCLUDE_ACCOUNT_TAGS) > 0: account_tags_columns = [item for item in account_tags_columns if item.lower().strip().replace('account_tag_', '') in INCLUDE_ACCOUNT_TAGS]

if PARTITION_BY_ACCOUNT: 
    temp_cur_original_columns = [item for item in temp_cur_original_columns if item not in ['line_item_usage_account_id']]
    column_string = "\""+"\",\"".join(temp_cur_original_columns)+"\","+"\""+"\",\"".join(filter(None,account_tags_columns))+"\""+',"line_item_usage_account_id"'
else:
    column_string = column_string = "\""+"\",\"".join(temp_cur_original_columns)+"\","+"\""+"\",\"".join(account_tags_columns)+"\""

column_string = column_string.replace(",\"\"", "").replace("\"\",", "")
print("Final list of columns: {}".format(column_string))

