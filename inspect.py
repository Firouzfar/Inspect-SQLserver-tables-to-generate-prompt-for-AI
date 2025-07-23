import pyodbc
import sys


def connect_dB(admin_conn_str):
    if input('Is Windows Authentication enabled on database? (y/n) ') == 'y':
        try:
            with pyodbc.connect(admin_conn_str, autocommit=True) as conn:
                cursor = conn.cursor()
                return cursor, True
        except pyodbc.Error as e:
            print(f"Error: {str(e)}")
            return None, False
    else:
        return None, False


def detect_db(cursor):
    cursor.execute("SELECT name FROM sys.databases")
    databases = [row.name for row in cursor.fetchall()]
    print('There are the following databases on this engine.')
    print(databases)
    db_name = input('Which one of them are you interested in? Write its name without quotation:\n')
    while db_name not in databases:
        db_name = input('Enter the correct name of the database:\n')
    return db_name



def add_user(cursor, flag):
    if flag:
        new_user = "newuser_for_test"
        new_password = "StrongPassword123!"

    else:
        new_user = input('Enter user name: ')
        new_password = input('Enter password for the user: ')

    try:
        cursor.execute(f"""
                            CREATE LOGIN [{new_user}] 
                            WITH PASSWORD = '{new_password}'
                                    """)
        db_name = detect_db(cursor)
        cursor.execute(f"""
                            USE [{db_name}];
                            CREATE USER [{new_user}] FOR LOGIN [{new_user}];
                            ALTER ROLE [db_owner] ADD MEMBER [{new_user}];
                        """)
        # print(f"Granted db_owner permissions to '{new_user}'")
        return db_name, new_user
    except pyodbc.Error as e0:
        if 'already exists' not in str(e0):
            print(e0)



def delete_sql_user(admin_conn_str, user_to_delete):
    try:
        # Connect to server
        with pyodbc.connect(admin_conn_str, autocommit=True) as conn:
            cursor = conn.cursor()

            #print(f"\nDeleting user '{user_to_delete}'...")

            # Step 1: Remove user from all databases
            cursor.execute("SELECT name FROM sys.databases")
            databases = [row.name for row in cursor.fetchall()]

            for db in databases:
                try:
                    # Switch to database
                    cursor.execute(f"USE [{db}];")

                    # Check if user exists
                    cursor.execute("""
                        SELECT name 
                        FROM sys.database_principals 
                        WHERE name = ? AND type_desc IN ('SQL_USER', 'WINDOWS_USER')
                    """, (user_to_delete,))

                    if cursor.fetchone():
                        # Drop user
                        cursor.execute(f"DROP USER [{user_to_delete}];")
                        # print(f"  - Removed user from database '{db}'")
                except pyodbc.Error as e:
                    print(f"  - Error in '{db}': {str(e)}")

            # Step 2: Drop server login
            try:
                cursor.execute("USE [master];")
                cursor.execute(f"DROP LOGIN [{user_to_delete}];")
                # print(f"\nSuccessfully deleted login '{user_to_delete}'")
            except pyodbc.Error as e:
                print(f"\nError deleting login: {str(e)}")
                print("Make sure the login exists and you have proper permissions")

    except pyodbc.Error as e:
        print(f"\nConnection error: {str(e)}")
        print("Possible causes:")
        print("- Incorrect admin credentials")
        print("- SQL Server service not running")
        print("- Firewall blocking connection")
        print("- Server name/instance incorrect")
def write_comments_to_text(output_file, db_name, cursor):
    with open(output_file, 'w') as f:
        f.write(f"The database name is {db_name}.")

        # Get all tables
        cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = cursor.fetchall()
        f.write(' It contains ' + str(len(tables)) + ' tables:\n\n' )
        i = 0
        for schema, table in tables:
            i += 1
            full_table_name = f"{schema}.{table}" if schema != 'dbo' else table
            f.write(f"{i}-Table: {full_table_name}\n")

            # Get columns with data types
            f.write("  Columns:\n")
            cursor.execute("""
                            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                            ORDER BY ORDINAL_POSITION
                        """, (schema, table))
            for col in cursor.fetchall():
                null_info = "NULL" if col.IS_NULLABLE == 'YES' else "NOT NULL"
                length_info = f"({col.CHARACTER_MAXIMUM_LENGTH})" if col.CHARACTER_MAXIMUM_LENGTH else ""
                f.write(f"    - {col.COLUMN_NAME}: {col.DATA_TYPE}{length_info} ({null_info})\n")

            # Get primary keys
            f.write("  Primary Keys:\n")
            cursor.execute("""
                            SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
                            AND CONSTRAINT_NAME LIKE 'PK_%'
                        """, (schema, table))

            pks = [row.COLUMN_NAME for row in cursor.fetchall()]
            for pk in pks:
                f.write(f"    - {pk}\n")

            # Get foreign keys
            f.write("  Foreign Keys:\n")
            cursor.execute("""
                            SELECT 
                                fk.name AS constraint_name,
                                c1.name AS column_name,
                                OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
                                OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
                                c2.name AS referenced_column
                            FROM sys.foreign_keys fk
                            INNER JOIN sys.foreign_key_columns fkc 
                                ON fk.object_id = fkc.constraint_object_id
                            INNER JOIN sys.columns c1 
                                ON fkc.parent_object_id = c1.object_id 
                                AND fkc.parent_column_id = c1.column_id
                            INNER JOIN sys.columns c2 
                                ON fkc.referenced_object_id = c2.object_id 
                                AND fkc.referenced_column_id = c2.column_id
                            WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = ?
                                AND OBJECT_NAME(fk.parent_object_id) = ?
                        """, (schema, table))

            fks = cursor.fetchall()
            if not fks:
                f.write("    - None\n")
            for fk in fks:
                ref_table = f"{fk.referenced_schema}.{fk.referenced_table}" if fk.referenced_schema != 'dbo' else fk.referenced_table
                f.write(f"    - {fk.column_name} -> {ref_table}.{fk.referenced_column}\n")

            f.write("\n")

        print(f"\nSchema successfully written to '{output_file}'")
        input('Press any key...')


def main():
    print('This program inspects SQLserver database and generates a text file to be used as input for AI.')
    admin_server = input("Enter your SQL Server name (press Enter for default: DESKTOP-NSER04S\\SQLEXPRESS): \n") or "DESKTOP-NSER04S\\SQLEXPRESS"
    admin_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={admin_server};Trusted_Connection=yes;"
    output_file = 'ai_prompt.txt'
    cursor, flag = connect_dB(admin_conn_str)
    db_name, new_user = add_user(cursor, flag)
    write_comments_to_text(output_file, db_name, cursor)
    if flag:
        delete_sql_user(admin_conn_str, new_user)


if __name__ == "__main__":
    main()
