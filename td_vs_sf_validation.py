from td_ddl_extract import *
from aws_athena import *
from sql_genarator import *
import pandas as pd # type: ignore
from datetime import datetime
import os
import time
from PD_encryptor import decrypt_pd
import json
import threading
from queue import Queue
from hash_dataframe import *

date_str = datetime.now().strftime('%m%d%Y%H%M%S')

if not os.path.exists('Script_testing/Teradata'):
    os.makedirs('Script_testing/Teradata')

if not os.path.exists('Script_testing/Iceberg'):
    os.makedirs('Script_testing/Iceberg')

if not os.path.exists('Script_testing/Metadata'):
    os.makedirs('Script_testing/Metadata')

def teradata_ddl(table_name, url, userid, pwd, td_condition, queue):
    if 'v.' in table_name.lower():
        table_name_ch = table_name.upper().replace('_V.', '_T.').replace('_V', '')
        try:
            td_dl = td_ddl_query(table_name_ch, url, userid, pwd)
            if str(td_condition).lower() == 'nan':
                td_condition = ""
            td_q = sql_generate(td_dl, table_name, td_condition)['Td_queries']
            print("Teradata job completed")
            queue.put(td_dl)
            queue.put(td_q)
        except Exception as error:
            print(error)
            queue.put('')
    else:
        try:
            td_dl = td_ddl_query(table_name, url, userid, pwd)
            if str(td_condition).lower() == 'nan':
                td_condition = ""
            td_q = sql_generate(td_dl, table_name, td_condition)['Td_queries']
            print("Teradata job completed")
            queue.put(td_dl)
            queue.put(td_q)
        except Exception as error:
            print(error)
            queue.put('')

def Target_ddl(table_name, database_name, ib_condition, queue):
    try:
        target_dl = get_table_ddl(table_name, database_name)
        if str(ib_condition).lower() == 'nan':
            ib_condition = ""
        iceberg_q = sql_generate(target_dl, table_name, ib_condition)['Iceberg_queries']
        print("Iceberg Job completed")
        queue.put(target_dl)
        queue.put(iceberg_q)
    except Exception as error:
        print(error)
        queue.put('')
        queue.put('')

def Teradata_query(query, url, userid, pwd, queue, path, key='Length'):
    if key in ('Length', 'RLength'):
        try:
            td_data = query_executor_length(query, url, userid, pwd)
            print("\n---Td query Results---\n")
            print(td_data.head(1000).to_string())
            if len(td_data) <= 10000:
                td_data.to_excel(path, sheet_name='Teradata')
            queue.put(td_data)
        except Exception as error:
            print(error)
            queue.put('')
    else:
        try:
            td_data = query_executor(query, url, userid, pwd)
            print("\n---Td query Results---\n")
            print(td_data.head(1000).to_string())
            if len(td_data) <= 10000:
                td_data.to_excel(path, sheet_name='Teradata')
            queue.put(td_data)
        except Exception as error:
            print(error)
            queue.put('')

def Iceberg_query(query, queue, path, database="data_validation_db"):
    iceberg_data = get_table_data(query, database)
    print("\n------Printing Iceberg Results----\n")
    if len(iceberg_data) > 0:
        print(iceberg_data.head(1000).to_string())
        if len(iceberg_data) <= 10000:
            iceberg_data.to_excel(path, sheet_name='Iceberg')
    else:
        iceberg_data = ""
    queue.put(iceberg_data)

def comparison_job(source_df, target_df, queue):
    mismatched_columns = []
    mismatched_dataframe = pd.DataFrame()
    source_df = source_df.rename(columns=str.lower).sort_values(by=list(i.lower() for i in source_df.columns), axis=0).reset_index(drop=True)
    target_df = target_df.rename(columns=str.lower).sort_values(by=list(i.lower() for i in target_df.columns), axis=0).reset_index(drop=True)
    s_dict = source_df.to_dict('list')
    t_dict = target_df.to_dict('list')

    if source_df.shape == target_df.shape:
        if source_df.equals(target_df):
            queue.put(mismatched_columns)
            queue.put(mismatched_dataframe)
        else:
            for col in source_df.columns:
                if not source_df[col].equals(target_df[col]):
                    if s_dict[col] != t_dict[col]:
                        mismatched_columns.append(col)
            queue.put(mismatched_columns)
            if len(source_df) == len(target_df):
                for index, row in source_df.iterrows():
                    for column in source_df.columns:
                        if str(source_df.loc[index, column]) != str(target_df.loc[index, column]):
                            mismatched_dataframe.loc[index, f'{column}_diff'] = f'{source_df.loc[index, column]} vs {target_df.loc[index, column]}'
                print(mismatched_dataframe.to_string())
                queue.put(mismatched_dataframe)
    else:
        for col in source_df.columns:
            if col in target_df.columns:
                if not source_df[col].equals(target_df[col]) and s_dict[col] != t_dict[col]:
                    mismatched_columns.append(col)
            else:
                mismatched_columns.append(f"Column {col} not in Target")
        queue.put(mismatched_columns)
        if len(source_df) == len(target_df) and source_df.shape == target_df.shape:
            for index, row in source_df.iterrows():
                for column in source_df.columns:
                    if source_df.loc[index, column] != target_df.loc[index, column]:
                        mismatched_dataframe.loc[index, f'{column}_diff'] = f'{source_df.loc[index, column]} vs {target_df.loc[index, column]}'
            print(mismatched_dataframe.to_string())
            queue.put(mismatched_dataframe)

def prompt_user(timeout):
    print("\n\nDo you want to continue? (Y/N)")
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = input().strip().lower()
        if response in {'y', 'yes'}:
            print("\n---Starting Comparison\n")
            return True
        elif response in {'n', 'no'}:
            print("\nTerminating the job-------------")
            return False
        else:
            print("Invalid Response, Please Enter---(Y/N)")
    print("\nReached the Time limit. Terminating the program------\n")
    return False

def automate(path):
    teradata_queue_lt = Queue()
    iceberg_queue_lt = Queue()
    compare_queue = Queue()
    import_details = pd.read_csv(path, sep=',', header=0)
    conf_file = open('Data_Extraction_Config.json', 'r')
    app_config = json.load(conf_file)
    connection_name = list(import_details['Connection_Name'].unique())
    conn_details = app_config.get(connection_name[0])
    url = conn_details["url"]
    userid = conn_details["userid"]
    pwd = decrypt_pd(conn_details["password"])

    for index, row in import_details.iterrows():
        table_name = row["Table_Name"]
        iceberg_table_name = row["Iceberg_Table"]
        iceberg_database = row["Iceberg_database"]
        query_td = str(row["query_TD"])
        query_ib = str(row["query_Iceberg"])
        td_condition = row['condition_td']
        ib_condition = row['condition_ib']
        print(connection_name, table_name, iceberg_table_name, iceberg_database, query_td, query_ib)

        td_thread = threading.Thread(target=teradata_ddl, args=(table_name, url, userid, pwd, td_condition, teradata_queue_lt,))
        td_thread.start()
        iceberg_thread = threading.Thread(target=Target_ddl, args=(iceberg_table_name, iceberg_database, ib_condition, iceberg_queue_lt,))
        iceberg_thread.start()

        td_thread.join()
        iceberg_thread.join()

        td_dl = teradata_queue_lt.get()
        if len(td_dl) <= 0:
            print("Teradata Failed---")
            exit()
        td_q = teradata_queue_lt.get()
        print(td_q)
        for c, info in td_q.items():
            query_path = os.path.join("Script_testing/Teradata", f"{table_name}_{date_str}.txt")
            with open(query_path, 'a+') as query_file:
                query_file.write(c + ':' + info + '\n\n')

        iceberg_dl = iceberg_queue_lt.get()
        iceberg_q = iceberg_queue_lt.get()
        if iceberg_dl == "" and iceberg_q == "":
            import_details.loc[index, 'Status'] = "Failed with Error"
            continue

        diff_meta = generate_diff_schema(td_dl, iceberg_dl)
        meta_path = os.path.join("Script_testing/Metadata", f"{table_name}_metadata.txt")
        with open(meta_path, 'a+') as t_info:
            t_info.write(f"\n\n{date_str}\n\nTeradata Metadata------\n\n")
            for c, info in td_dl.items():
                t_info.write(c + ':' + str(info) + '\n')
            t_info.write("\n\n-Iceberg Metadata---------\n\n")
            for c, info in iceberg_dl.items():
                t_info.write(c + ':' + str(info) + '\n')
            t_info.write("\n\nDifference Metadata--------------\n\n")
            for c, info in diff_meta.items():
                t_info.write(c + ':' + str(info) + '\n')

        print(iceberg_q)
        for i1, info in iceberg_q.items():
            query_path = os.path.join("Script_testing/Iceberg", f"{table_name}_{date_str}.txt")
            with open(query_path, 'a+') as query_file:
                query_file.write(i1 + ':' + info + '\n\n')

        response = prompt_user(60)
        if response:
            s = {}
            status = []
            remarks = []
            if not os.path.exists(f'Script_testing/Teradata/{table_name}'):
                os.makedirs(f'Script_testing/Teradata/{table_name}')
            if not os.path.exists(f'Script_testing/Iceberg/{iceberg_table_name}'):
                os.makedirs(f'Script_testing/Iceberg/{iceberg_table_name}')
            if not os.path.exists(f'Script_testing/Summary/QA'):
                os.makedirs(f'Script_testing/Summary/QA')

            path_sm_td = f'Script_testing/Summary/QA/{table_name}.xlsx'
            path_sm_ib = f'Script_testing/Summary/{iceberg_table_name}.xlsx'
            writer_td = pd.ExcelWriter(path_sm_td, engine='xlsxwriter')
            writer_iceberg = pd.ExcelWriter(path_sm_ib, engine='xlsxwriter')

            c_kl = [i for i in td_q.keys() if i in iceberg_q.keys()]
            c_kl.append('RLength')
            tc = c_kl

            for k in c_kl:
                path_td = f'Script_testing/Teradata/{table_name}/{k}_{date_str}.xlsx'
                path_ib = f'Script_testing/Iceberg/{iceberg_table_name}/{k}_{date_str}.xlsx'
                td_query_thread = threading.Thread(target=Teradata_query, args=(td_q[k], url, userid, pwd, teradata_queue_lt, path_td, k))
                td_query_thread.start()
                if k == 'RLength':
                    iceberg_query_thread = threading.Thread(target=Iceberg_query, args=(iceberg_q['Length'], iceberg_queue_lt, path_ib, iceberg_database))
                else:
                    iceberg_query_thread = threading.Thread(target=Iceberg_query, args=(iceberg_q[k], iceberg_queue_lt, path_ib, iceberg_database))
                iceberg_query_thread.start()

                td_query_thread.join()
                iceberg_query_thread.join()

                td_query_data = teradata_queue_lt.get()
                if len(td_query_data) <= 0:
                    status.append("Failed")
                    remarks.append("-")
                    continue

                iceberg_query_data = iceberg_queue_lt.get()
                if len(iceberg_query_data) <= 0:
                    status.append("Failed")
                    remarks.append("-")
                    continue

                td_query_data.to_excel(writer_td, sheet_name=k + '_td', index=False)
                iceberg_query_data.to_excel(writer_td, sheet_name=k + '_ib', index=False)

                comparison_thread = threading.Thread(target=comparison_job, args=(td_query_data, iceberg_query_data, compare_queue))
                comparison_thread.start()
                comparison_thread.join()

                comparison_data = compare_queue.get()
                comparison_df = compare_queue.get()
                if not comparison_df.empty:
                    comparison_df.to_excel(writer_td, sheet_name=k + '_diff', index=False)
                    s[k] = comparison_data
                    if len(s[k]) > 0:
                        status.append("Not match")
                        remarks.append(s[k])
                    else:
                        status.append("Match")
                        remarks.append("-")

            summary_dict = {'TestCases': tc, 'Status': status, 'Remarks': remarks}
            summary_df = pd.DataFrame.from_dict(summary_dict)
            summary_df.to_excel(writer_td, sheet_name="Summary", index=False)
            summary_df.to_excel(f'Script_testing/Teradata/{table_name}/summary_{date_str}.xlsx')
            summary_df.to_excel(f'Script_testing/Iceberg/{iceberg_table_name}/summary_{date_str}.xlsx')
            writer_td.save()
            writer_iceberg.save()

            s = [True if s_.lower() == 'match' else False for s_ in status]
            if all(s):
                import_details.loc[index, 'Status'] = "Pass"
                import_details.loc[index, 'Remarks'] = ".."
            else:
                import_details.loc[index, 'Status'] = "Fail"
                import_details.loc[index, 'Remarks'] = summary_df.to_string()
        else:
            exit()

    if len(import_details) > 2:
        import_details.to_excel(f'Script_testing/job_{date_str}.xlsx')
        print("Check the status")

def manual_func(connection_type):
    teradata_queue_lt = Queue()
    iceberg_queue_lt = Queue()
    compare_queue = Queue()
    summary = {}
    remarks = []

    conf_file = open('Data_Extraction_Config.json', 'r')
    app_config = json.load(conf_file)
    connection_name = str(connection_type)
    conn_details = app_config.get(connection_name)
    url = conn_details["url"]
    userid = conn_details["userid"]
    pwd = decrypt_pd(conn_details["password"])

    print("\nEnter the Teradata query")
    td_query = input()
    if "select * from" in td_query.lower() and str(connection_type) == "pod_connection":
        print("Please check the query Data Queries are Restricted---")
        exit()
    else:
        if "length(" in td_query.lower():
            key = "Length"
        else:
            key = "Data"
        tdf_query = f"{td_query}"
        table_name = re.findall(r'\s+\bfrom\b\s+(\w+.\w+)', tdf_query.lower())
        path_td = f'Script_testing/{table_name[0]}.xlsx'
        path_ib = f'Script_testing/{table_name[0]}_{date_str}.xlsx'
        writer_td = pd.ExcelWriter(path_td, engine='xlsxwriter')

        td_query_thread = threading.Thread(target=Teradata_query, args=(tdf_query, url, userid, pwd, teradata_queue_lt, path_ib, key))
        print("\nEnter the Iceberg Query")
        ib_query = input()
        if "select * from" in ib_query and str(connection_type) == "pod_connection":
            print("Please check the query Data Queries are Restricted---")
            exit()
        iceberg_query_thread = threading.Thread(target=Iceberg_query, args=(ib_query, iceberg_queue_lt, path_ib))

        method_input = int(input("Enter the type of Method(Hashed Average/Data-to-Data):\n1.Hashed Average\n2.Data-to-Data\n"))
        tries = 0
        if not 0 < method_input < 2:
            print("Enter a Valid response-----")
            tries += 1
            if tries == 3:
                print("Too many entries..., Terminating the tool")
                exit()

        if method_input == 1:
            print("Starting the Hashed Average comparison------")
            td_query_thread.start()
            iceberg_query_thread.start()
            td_query_thread.join()
            iceberg_query_thread.join()
            td_query_data = teradata_queue_lt.get()
            iceberg_query_data = iceberg_queue_lt.get()
            if len(iceberg_query_data) > 0:
                iceberg_query_data.to_csv(index=False)
            spark_source_df = pandas_to_spark(td_query_data)
            spark_target_df = pandas_to_spark(iceberg_query_data)
            source_pf = profile_dataset(spark_source_df)
            source_pf.show()
            target_pf = profile_dataset(spark_target_df)
            target_pf.show()
            td_query_data_ = source_pf.toPandas()
            iceberg_query_data_ = target_pf.toPandas()
            td_query_data_.to_excel(writer_td, sheet_name='Td', index=False)
            iceberg_query_data_.to_excel(writer_td, sheet_name='ICEBERG', index=False)
            if len(td_query_data) == len(iceberg_query_data):
                summary['Count'] = 'Count Match'
                remarks.append(str(len(td_query_data)) + " vs " + str(len(iceberg_query_data)))
            else:
                summary['Count'] = 'Count Mismatch'
                remarks.append(str(len(td_query_data)) + " vs " + str(len(iceberg_query_data)))
            comparison_thread = threading.Thread(target=comparison_job, args=(td_query_data_, iceberg_query_data_, compare_queue))
            comparison_thread.start()
            comparison_thread.join()
            comparison_data = compare_queue.get()
            comparison_df = compare_queue.get()
            if len(comparison_data) > 0:
                summary['Data'] = 'Data Mismatch'
                remarks.append(comparison_data)
                comparison_df.to_excel(writer_td, sheet_name='Difference', index=False)
            if len(comparison_data) == 0 and comparison_df.empty:
                summary['Data'] = 'Data Match'
                remarks.append('-')
            summary_dict = {'key': summary.keys(), 'Status': summary.values(), 'Remarks': remarks}
            summary_df = pd.DataFrame.from_dict(summary_dict)
            summary_df.to_excel(writer_td, sheet_name='Summary')
            print(summary_df.to_string())
            print("----Job completed------")
        elif method_input == 2:
            print("Starting the Data to Data comparison------")
            td_query_thread.start()
            iceberg_query_thread.start()
            td_query_thread.join()
            iceberg_query_thread.join()
            td_query_data = teradata_queue_lt.get()
            iceberg_query_data = iceberg_queue_lt.get()
            td_query_data.to_excel(writer_td, sheet_name='TD', index=False)
            iceberg_query_data.to_excel(writer_td, sheet_name='Iceberg', index=False)
            if len(td_query_data) == len(iceberg_query_data):
                summary['Count'] = 'Count Match'
                remarks.append(str(len(td_query_data)) + " vs " + str(len(iceberg_query_data)))
            else:
                summary['Count'] = 'Count Mismatch'
                remarks.append(str(len(td_query_data)) + " vs " + str(len(iceberg_query_data)))
            comparison_thread = threading.Thread(target=comparison_job, args=(td_query_data, iceberg_query_data, compare_queue))
            comparison_thread.start()
            comparison_thread.join()
            comparison_data = compare_queue.get()
            comparison_df = compare_queue.get()
            if len(comparison_data) > 0:
                summary['Data'] = 'Data Mismatch'
                remarks.append(comparison_data)
                comparison_df.to_excel(writer_td, sheet_name='Difference', index=False)
            if len(comparison_data) == 0 and comparison_df.empty:
                summary['Data'] = 'Data Match'
                remarks.append('-')
            summary_dict = {'key': summary.keys(), 'Status': summary.values(), 'Remarks': remarks}
            summary_df = pd.DataFrame.from_dict(summary_dict)
            summary_df.to_excel(writer_td, sheet_name='Summary')
            print(summary_df.to_string())
            print("\n----Job completed------")
        writer_td.save()

def main():
    print("Enter Type of Method?(Auto/Manual)")
    method = input().lower().strip()
    if method in {'m', 'manual'}:
        print("Enter the Environment type\n1.dev_connection.\n2.pod_connection\n3.uat_connection")
        env = int(input())
        if 3 < env < 0:
            print("Please enter valid environment option")
        env_l = ['dev_connection', 'pod_connection', 'uat_connection']
        manual_func(env_l[env - 1])
    elif method in ('a', 'auto'):
        path = 'Data_Extraction.csv'
        automate(path)
    else:
        print("Enter the valid Method to proceed")

if __name__ == '__main__':
    main()