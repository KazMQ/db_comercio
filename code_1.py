import pandas as pd
from sqlalchemy import create_engine, text

#Configurando o banco
host = 'localhost'
user = 'root'
password = ''
database = 'bd_comercio'

def busca(tabela):
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        with engine.connect() as conexao:
            query = f'SELECT * FROM {tabela}'
            df = pd.read_sql(text(query), conexao)
            return df
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return pd.DataFrame()

try:
    df_base = busca('basedp')
    df_roubo_comercio = busca('basedp_roubo_comercio')

    #Limpando os dados
    df_base.columns = [col.strip().replace('\ufeff','') for col in df_base.columns]
    df_roubo_comercio.columns = [col.strip().replace('\ufeff','') for col in df_roubo_comercio.columns]

    #Juntando os bancos de dados
    df_novo = pd.merge(df_base, df_roubo_comercio)

    #print(df_novo.head())
    
except Exception as e:
    print(f"Erro ao obter dados: {e}")