import datetime
import db_connection

def insert_log_table(file_path, ext, table_name, count_row):
    conn, cursor = db_connection.establish_connection()
    cursor=conn.cursor()
   
    current_date = datetime.date.today()
    current_date = str(current_date).replace('-', '_')
    #table_name = f"{table_name}_{current_date}"

    # SQL query to insert into the log table
    sql_query = "INSERT INTO load_log_info (SRC_FILE_NAME, SRC_FILE_TYPE, TABLE_NAME, RECORDS_COUNT) VALUES (?, ?, ?, ?)"
    cursor.execute(sql_query, (file_path, ext, table_name, count_row))

    print("Data Loaded into Log table successfully")
    conn.commit()
