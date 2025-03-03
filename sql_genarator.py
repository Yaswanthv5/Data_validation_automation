string_list = ['VARCHAR', 'CHAR', 'STRING', 'CLOB', 'TEXT']
num_list = ['INT', 'INTEGER', 'SMALLINT', 'BIGINT', 'FLOAT', 'LONG']
decimal_list = ['DECIMAL']
time_list = ['TIME', 'TIMESTAMP', 'DATE']
list_schema = [['CHAR', 'VARCHAR', 'CLOB', 'STRING', 'DATE INTERVAL TO HOUR', 'TIME', 'VOID'], ['DECIMAL', 'NUMBER', 'FLOAT'], ['SHORT', 'LONG', 'BIGINT', 'SMALLINT', 'INTEGER', 'INT', 'VOID', 'IDENTITY'], ['TIMESTAMP', 'TIME WITH TIMEZONE']]

def sql_generate(c_dict, table_name, condition=''):
    if len(condition) > 0:
        td_queries, ib_queries = ({'Count': f"(select count(*) as Total_count from {table_name} {condition}) a"}, {'Count': f"select count(*) as Total_count from {table_name} {condition}"})
    else:
        td_queries, ib_queries = ({'Count': f"(select count(*) as Total_count from {table_name}) a"}, {'Count': f"select count(*) as Total_count from {table_name}"})

    sum_list_td = []
    sum_list_ib = []
    max_time, min_time, null_lt = [], [], []
    l_list, lt, s_list, st, sr_list = [], [], [], [], []

    for col, info in c_dict.items():
        null_lt.append(f"sum(case WHEN {col} is null then 1 else 0 end) as Number_of_Null_Values_{col}")
        if info['data_type'] in num_list:
            sum_list_td.append(f"sum(cast({col} as Bigint)) as sum_{col}")
            sum_list_ib.append(f"sum(cast({col} as Bigint)) as sum_{col}")
        elif info['data_type'] in decimal_list:
            sum_list_td.append(f"sum(cast({col} as decimal(38,12))) as sum_{col}")
            sum_list_ib.append(f"sum(cast({col} as decimal(38,12))) as sum_{col}")
        elif info['data_type'] in time_list:
            max_time.append(f"max({col}) as MAX_{col}")
            min_time.append(f"min({col}) as MIN_{col}")
        elif info['data_type'] in string_list:
            sr_list.append(col)
            l_list.append(f"max(length({col})) as max_len_{col}")
            s_list.append(f"min(length({col})) as min_len_{col}")
            if info['data_type'] == 'CHAR':
                lt.append(f"max(length(rtrim({col}))) as max_len_{col}")
                st.append(f"min(length(rtrim({col}))) as min_len_{col}")
            else:
                lt.append(f"max(length({col})) as max_len_{col}")
                st.append(f"min(length({col})) as min_len_{col}")

    if len(condition) > 0:
        td_queries['Null'] = f"(select {','.join(null_lt)} from {table_name} {condition}) a"
        ib_queries['Null'] = f"select {','.join(null_lt)} from {table_name} {condition}"
        td_queries['Sum'] = f"(select {','.join(sum_list_td)} from {table_name} {condition}) a"
        ib_queries['Sum'] = f"select {','.join(sum_list_ib)} from {table_name} {condition}"
        if len(max_time) > 0:
            td_queries['Max'] = f"(select {','.join(max_time)},{','.join(min_time)} from {table_name} {condition}) a"
            ib_queries['Max'] = f"select {','.join(max_time)},{','.join(min_time)} from {table_name} {condition}"
        if len(s_list) > 0:
            td_queries['Length'] = f"(select {','.join(l_list)},{','.join(s_list)} from {table_name} {condition}) a"
            td_queries['RLength'] = f"(select {','.join(lt)},{','.join(st)} from {table_name} {condition}) a"
            ib_queries['Length'] = f"select {','.join(l_list)},{','.join(s_list)} from {table_name} {condition}"
    else:
        td_queries['Null'] = f"(select {','.join(null_lt)} from {table_name}) a"
        ib_queries['Null'] = f"select {','.join(null_lt)} from {table_name}"
        td_queries['Sum'] = f"(select {','.join(sum_list_td)} from {table_name}) a"
        ib_queries['Sum'] = f"select {','.join(sum_list_ib)} from {table_name}"
        if len(max_time) > 0:
            td_queries['Max'] = f"(select {','.join(max_time)},{','.join(min_time)} from {table_name}) a"
            ib_queries['Max'] = f"select {','.join(max_time)},{','.join(min_time)} from {table_name}"
        if len(l_list) > 0:
            td_queries['Length'] = f"(select {','.join(l_list)},{','.join(s_list)} from {table_name}) a"
            td_queries['RLength'] = f"(select {','.join(lt)},{','.join(st)} from {table_name}) a"
            ib_queries['Length'] = f"select {','.join(l_list)},{','.join(s_list)} from {table_name}"

    return {'Td_queries': td_queries, 'Iceberg_queries': ib_queries}

def generate_diff_schema(source, target):
    diff_dict = {}
    for column, base_info in source.items():
        if column.upper() in target:
            target_info = target[column.upper()]
            if base_info['data_type'] in list_schema[0]:
                if target_info['data_type'] == 'VARCHAR':
                    if ('Length' in base_info and 'Length' in target_info and base_info['Length'] != target_info['Length']):
                        diff_dict[column] = {'data_type': (base_info['data_type'], target_info['data_type']), 'length': (base_info['Length'], target_info['Length'])}
                elif target_info['data_type'] not in ['STRING', 'TIMESTAMP', 'CHAR']:
                    diff_dict[column] = {'data_type': (base_info['data_type'], target_info['data_type'])}
            elif base_info['data_type'] in list_schema[1]:
                if target_info['data_type'] == 'DECIMAL':
                    if ('Length' in base_info and 'Length' in target_info and base_info['Length'] != target_info['Length']):
                        diff_dict[column] = {'data_type': (base_info['data_type'], target_info['data_type']), 'length': (base_info['Length'], target_info['Length'])}
                elif target_info['data_type'] not in list_schema[1]:
                    diff_dict[column] = {'data_type': (base_info['data_type'], target_info['data_type'])}
            elif base_info['data_type'] in list_schema[2]:
                if target_info['data_type'] not in list_schema[2]:
                    diff_dict[column] = {'data_type': (base_info['data_type'], target_info['data_type'])}
            elif base_info['data_type'] in list_schema[3]:
                if target_info['data_type'] not in list_schema[3]:
                    diff_dict[column] = {'data_type': (base_info['data_type'], target_info['data_type'])}
        else:
            if base_info['data_type'] != '':
                diff_dict[column] = {'data_type': f"Not exist in Source ({base_info['data_type']})"}
    for column, target_info in target.items():
        if column.upper() not in source and target_info['data_type'] != '':
            diff_dict[column] = {'data_type': f"Not exist in Target ({target_info['data_type']})"}
    return diff_dict