import sys
import os
import pandas as pd
import testloadfn as tl
import logtableinsert as lt
import pyodbc as py

def load_file(file_name):
    excel_file = file_name + ".xlsx"
    csv_file = file_name + ".csv"
    json_file = file_name + ".json"

    dir_path = "C:\\Users\\ravin\\OneDrive\\Desktop\\Landing_folder"

    for path in os.listdir(dir_path):
        ext = path.split(".")
        if len(ext) != 2:
            continue  # Skip files without a valid extension
        ext = ext[1]
        if path == excel_file or path == csv_file or path == json_file:
            print("File found:", path)
            tab_names = path.split(".")[0].replace(' ', '_')
            file_path = os.path.join(dir_path, path)
            if ext == 'csv':
                #df = pd.read_csv(file_path);
                df = pd.read_csv(file_path, encoding='utf-8')
                count_row = len(df)
                tl.dataload(df, file_path, ext, tab_names, count_row)
            elif ext == 'xlsx':
                df = pd.read_excel(file_path, sheet_name=None)
                for sheetname, df_sheet in df.items():
                    count_row = len(df_sheet)
                    res = len(df[sheetname]) - 1
                    if res > 1:
                        table_name = f"{tab_names}_{sheetname}"
                        tl.dataload(df_sheet, file_path, ext, table_name, count_row)
                    else:
                        tl.dataload(df_sheet, file_path, ext, tab_names, count_row)
            elif ext == 'json' or ext == 'JSON':
                df = pd.read_json(file_path).fillna("None")
                count_row = len(df)
                tl.dataload(df, file_path, ext, tab_names, count_row)
            break
    else:
        print("No such file found:", file_name)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py file_name")
        sys.exit(1)
    file_name = sys.argv[1]
    load_file(file_name)

