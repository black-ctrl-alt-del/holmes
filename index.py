import json
import psycopg2
from tabulate import tabulate
import os

configBlob = open('config.json')
config = json.load(configBlob)


def connectPostgreSQL(database):
    try:
        con = psycopg2.connect(
            database=database['database'],
            user=database['user'],
            password=database['password'],
            host=database['host'],
            port=database['port']
            )
        print(f"[HOLMES] DATABASE: {database['database']}-{database['schema']}@{database['host']}")
        cursor = con.cursor()
        # GET POOL OF CONNECTIONS
        cursor.execute("SELECT COUNT(usesysid) FROM pg_stat_activity;")
        fetch_users_in_activity = cursor.fetchall()
        # GET DATABASE SIZE
        cursor.execute(f"SELECT pg_size_pretty(pg_database_size('{database['database']}'));")
        fetch_database_size = cursor.fetchall()

        print("\n* General Informations")
        print(f"- Connections Pool: {fetch_users_in_activity[0][0]}")
        print(f"- Size total: {str(fetch_database_size[0][0])}")

        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{database['schema']}'")
        tables = cursor.fetchall()
        table_result_header = ["Table Name", "N. Registers", "Size"]
        table_config = [table_result_header]
        for table in tables:
            table_name = table[0]
            fetch_number_registers = cursor.execute(f"SELECT COUNT(*) FROM {database['schema']}.{table_name};")
            number_registers = cursor.fetchall()
            fetch_size_table = cursor.execute(f"SELECT pg_size_pretty( pg_total_relation_size('{table_name}') )")
            size_table = cursor.fetchall()
            table_config.append([table_name, number_registers[0][0], size_table[0][0]])
        print("\n* Tables Info")
        print(tabulate(table_config))
    except e:
        print(f"[HOLMES] Erro ao conectar ao banco de dados {database['database']}@{database['host']}")
        print(e)



def main():
    os.system("cls")
    print(""" 
        ██╗  ██╗ ██████╗ ██╗     ███╗   ███╗███████╗███████╗
        ██║  ██║██╔═══██╗██║     ████╗ ████║██╔════╝██╔════╝
        ███████║██║   ██║██║     ██╔████╔██║█████╗  ███████╗
        ██╔══██║██║   ██║██║     ██║╚██╔╝██║██╔══╝  ╚════██║
        ██║  ██║╚██████╔╝███████╗██║ ╚═╝ ██║███████╗███████║
        ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝     ╚═╝╚══════╝╚══════╝ DATABASE INSPECTOR
        """)
    for database in config:
        if database['type'] == "postgresql":
            connectPostgreSQL(database)
       
main()