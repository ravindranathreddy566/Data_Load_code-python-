def dataload(df, file_path, ext, tab_names, count_row):
    import datetime
    import pyodbc as py
    import logtableinsert as lt
    import db_connection as db
    import pandas as pd

    conn, cursor = db.establish_connection()

    current_date = datetime.date.today()
    current_date = str(current_date).replace('-', '_')
    print(current_date)

    df = df.replace(' ', 'Null')
    columns = df.columns.tolist()
    columns = [col.replace(" ", "") for col in columns]
    columns = [col.replace("-", "_") for col in columns]
    dtypes = df.dtypes.tolist()
    table_name = f"{tab_names}"
    print(table_name)

    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    create_tab_qry = f'CREATE TABLE {table_name} ('
    tab_columns = ""
    columnslist = ""
    tags = ""

    primary_key_column = None

    for col, dtype in zip(columns, dtypes):
        if dtype == 'int64' and df[col].duplicated().sum() == 0 and df[col].isnull().sum() == 0:
            primary_key_column = col
            dtype = 'int'
        elif dtype == 'int64':
            dtype = 'int'
        elif dtype == 'float64':
            dtype = 'float'
        elif dtype == 'datetime64[ns]':
            dtype = 'date'
        else:
            dtype = 'varchar(1000)'

        tab_columns += f"[{col}] {dtype}, \n"
        columnslist += f"[{col}],"
        tags += "?,"

    if primary_key_column:
        tab_columns += f"PRIMARY KEY ([{primary_key_column}]), \n"

    tab_columns = tab_columns.rstrip(", \n")
    columnslist = columnslist.rstrip(",")
    tags = tags.rstrip(",")
    create_tab_qry += tab_columns + ")"

    try:
        print(create_tab_qry)
        cursor.execute(create_tab_qry)
        cursor.commit()
        conn.commit()
        print("Table Created Successfully...")
    except py.ProgrammingError as e:
        print("Error While creating Table: " + str(e))

    try:
        insert_query = f"INSERT INTO {table_name} ({columnslist}) VALUES ({tags})"
        values = df.values.tolist()
        cursor.executemany(insert_query, values)
        print("Data loaded into the table...")
        cursor.commit()
        conn.commit()
        lt.insert_log_table(file_path, ext, table_name, count_row)
    except py.ProgrammingError as e:
        print("Error at Insert: " + str(e))

    cursor.close()
    conn.close()
