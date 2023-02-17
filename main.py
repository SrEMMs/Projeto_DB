#Bibliotecas necessárias para execução do Código
import psycopg2
import pandas as pd
from datetime import date
from sqlalchemy import create_engine

data = date.today()

#Conexão com o banco postgres 
def conecta_db():
    conn = psycopg2.connect(
        host = 'localhost',
        database = 'db_pedidos',
        user = 'postgres',
        password = 'postgres',
        port = '5433'
    )
    return conn

def criar_db(sql, conn):
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def fechar_db(conn):
    conn.close()

#Importação dos dados do aquivo oirder_datails.cvs para o arquivo order_details_'data do dia'.csv
def extrair_csv():
    texto = pd.read_csv('data/order_details.csv')
    texto.to_csv("order_details_" + str(data) + ".csv", index = False, sep=';')

#Extração das tabelas do banco northwind
def extrair_tabelas():
    base = psycopg2.connect(
        database = 'northwind',
        user = 'northwind_user',
        password = 'thewindisblowing',
        host = 'localhost',
        port = '5432'
    )

    conector = base.cursor()

    conector.execute("select tablename from pg_catalog.pg_tables where schemaname = 'public'")
    tabelas = conector.fetchall()
    for i in tabelas:
        texto = ''.join(i)
        query = f"SELECT * FROM {texto}"
        df = pd.read_sql_query(query, base)
        df.to_csv(f"{texto}_" + str(data) + ".csv", index = False, sep=';')

#Criação da tabela pedidos
def exportacao_banco():

    conn = conecta_db()
    sql = 'DROP TABLE IF EXISTS public.pedidos'

    criar_db(sql, conn)

    sql = '''CREATE TABLE public.pedidos
        (
            order_id smallint NOT NULL,
            customer_id bpchar,
            employee_id smallint,
            order_date date,
            required_date date,
            shipped_date date,
            ship_via smallint,
            freight real,
            ship_name character varying(40),
            ship_address character varying(60),
            ship_city character varying(15),
            ship_region character varying(15),
            ship_postal_code character varying(10),
            ship_country character varying(15),
            product_id smallint,
            unit_price real,
            quantity smallint,
            discount real
        )'''
    criar_db(sql, conn)

    dataf1 = pd.read_csv(f'orders_' + str(data) + '.csv', sep=';')
    dataf2 = pd.read_csv(f'order_details_' + str(data) + '.csv', sep=';')

    dataf = pd.merge(dataf1 , dataf2, how = 'inner')

    engine = create_engine('postgresql://postgres:postgres@localhost:5433/db_pedidos')

    dataf.to_sql('orders', con=engine, if_exists='replace')

#Criação do relatório final, com a junção da tabela orders e order_details
def relatorio():
    conn = conecta_db()

    df = pd.read_sql_query('select * from orders',conn)
    df.to_csv(f'relatorio_' + str(data) + '.csv', index = False, sep = ';')

def main():
    op = 0

    while op != 5:
        print(''' 
            Indicium Challenge
            [1] Dados order_details 
            [2] Extração tabelas do banco de dados
            [3] Exportação para a tabela Pedidos
            [4] Relatório do dia
            [5] Sair do programa
            ''')
        op = int(input('Escolha uma Opção: '))

        if op == 1:
            extrair_csv()
        elif op == 2:
            extrair_tabelas()
        elif op == 3:
            extrair_csv()
            extrair_tabelas()
            exportacao_banco()
        elif op == 4:
            extrair_csv()
            extrair_tabelas()
            exportacao_banco()
            relatorio()
        elif op == 5:
            print('Finazilando o Programa!')
        else:
            print('Opção Inválida! Por favor escolha novamente')


if __name__ == '__main__':
    main()

