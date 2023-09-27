import json
import psycopg2
from tabulate import tabulate
import os
import time

'''
Developer: Koenomatachi San
Description: Monitoring PostgreSQL/Mysql databases.
'''
class text_colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def connectPostgreSQL(database):
    try:
        con = psycopg2.connect(
            database=database['database'],
            user=database['user'],
            password=database['password'],
            host=database['host'],
            port=database['port']
            )
        print(f"{text_colors.HEADER}[HOLMES]{text_colors.ENDC} DATABASE: {text_colors.OKGREEN}{database['database']}-{database['schema']}@{database['host']}{text_colors.ENDC}")
        cursor = con.cursor()
        # GET POOL OF CONNECTIONS
        cursor.execute("SELECT COUNT(usesysid) FROM pg_stat_activity;")
        fetch_users_in_activity = cursor.fetchall()
        # GET DATABASE SIZE
        cursor.execute(f"select sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint from pg_tables where schemaname = '{database['schema']}';")
        fetch_database_size = cursor.fetchall()

        print(f"\n* {text_colors.OKBLUE}General Informations{text_colors.ENDC}")
        print(f"- {text_colors.WARNING}Connections Pool:{text_colors.ENDC} {fetch_users_in_activity[0][0]}")
        print(f"- {text_colors.WARNING}Size total:{text_colors.ENDC} {str(fetch_database_size[0][0]/ 1024 / 1024 / 1024)} Gb")

        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{database['schema']}'")
        tables = cursor.fetchall()
        table_result_header = ["Table Name", "N. Registers", "Size"]
        table_config = [table_result_header]
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {database['schema']}.{table_name};")
            number_registers = cursor.fetchall()
            cursor.execute(f"SELECT pg_size_pretty( pg_total_relation_size('{table_name}') )")
            size_table = cursor.fetchall()
            table_config.append([table_name, number_registers[0][0], size_table[0][0]])
        print(f"\n{text_colors.OKBLUE}* Tables Info{text_colors.ENDC}{text_colors.WARNING}")
        print(tabulate(table_config))
        print(f"{text_colors.ENDC}")
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
    configBlob = open('config.json')
    config = json.load(configBlob)
    for database in config:
        if database['type'] == "postgresql":
            connectPostgreSQL(database)
       
def verifyIfConfigFileExists():
    verify = os.path.exists("config.json")
    if verify == False:
        raise ValueError('[HOLMES] ARQUIVO DE CONFIGURACAO NAO ENCONTRADO')

while True:
    verifyIfConfigFileExists()
    main();
    time.sleep(60)