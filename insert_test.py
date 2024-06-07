import os
import pandas as pd
import testloadfn as tl
import logtableinsert as lt

dir_path="C:\\Users\\ravin\\OneDrive\\Desktop\\Landing_folder";

for path in os.listdir(dir_path):
    ext = path.split(".")[1];
    tab_names = path.split(".")[0];
    print(ext);
    file_path = os.path.join(dir_path, path);
    print(file_path);
    tab_names=tab_names.replace(' ','_');
    print(tab_names);
    
    if ext == 'csv':
        df=pd.read_csv(file_path,encoding='latin1'); #encoding='utf-8'
        df=df.fillna("None");
        count_row = len(df);
        print(count_row);
        tl.dataload(df,file_path, ext,tab_names, count_row);
        #lt.insert_log_table(file_path, ext,tab_names, count_row);
        
    elif ext=='xlsx':
        df = pd.read_excel(file_path, sheet_name=None);
        #df.fillna("NULL" ,inplace=True);
        for sheetname, df_sheet in df.items():
            count_row = len(df_sheet);
            print(count_row);
            res = (len(df[sheetname])-1);
            print(res)
            if res>1:
                table_name = f"{tab_names}_{sheetname}"
                #table_name = f"{table_name}_{sheetname}"
                print(table_name);
                #loading mutliple sheets of excel to Database
                tl.dataload(df_sheet,file_path, ext,table_name, count_row);
                print(table_name);                
                
            else:
                table_name = f"{tab_names}"
                #table_name = f"{table_names} "
                print(table_name);
                df=pd.read_excel(file_path);
                df=df.fillna("None");
                #df.fillna("NULL" ,inplace=True);               
                tl.dataload(df,file_path, ext,tab_names, count_row);
               
    elif ext=='json' or ext=='JSON':
        df=pd.read_json(file_path);
        df=df.fillna("None");
        count_row=len(df);
        print(count_row);
        tl.dataload(df,file_path, ext,tab_names, count_row);
        #lt.insert_log_table(file_path, ext,tab_names, count_row);
                
    
    
    

                            
    
