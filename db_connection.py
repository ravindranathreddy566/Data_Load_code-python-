import pyodbc

def establish_connection():
    server = 'LAPTOP-K9225UC9\\SQLEXPRESS'
    database = 'source_db'
    username = 'Ravi'
    password = 'Rrr@20031996'
    
    # Create connection string
    conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    return conn, cursor

