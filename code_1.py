import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

# (Opcional) Use tabulate para formatação de tabelas no console
try:
    from tabulate import tabulate

    def exibir_tabela(dados, headers=None, titulo=None):
        if titulo:
            print(f"\n{titulo}")
            print("-" * len(titulo))
        print(tabulate(dados, headers=headers, tablefmt="grid"))
except ImportError:
    def exibir_tabela(dados, headers=None, titulo=None):
        if titulo:
            print(f"\n{titulo}")
            print("-" * len(titulo))
        if isinstance(dados, pd.DataFrame):
            print(dados.to_string(index=False))
        else:
            for linha in dados:
                print(" | ".join(str(x) for x in linha))

# Configuração do banco
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

# Obter e preparar os dados
try:
    df_base = busca('basedp')
    df_roubo_comercio = busca('basedp_roubo_comercio')

    # Limpeza de colunas
    df_base.columns = [col.strip().replace('\ufeff', '') for col in df_base.columns]
    df_roubo_comercio.columns = [col.strip().replace('\ufeff', '') for col in df_roubo_comercio.columns]
    
    # Junção dos DataFrames
    df_novo = pd.merge(df_base, df_roubo_comercio)
    
    #Filtrando os anos
    df_novo = df_novo[(df_novo['ano'] >= 2022) & (df_novo['ano'] <= 2023)]


    
    


except Exception as e:
    print(f"Erro ao obter dados: {e}")
    exit()

# Cálculos estatísticos
try:
    print("\nCalculando estatísticas...")

    valores = df_novo['roubo_comercio'].to_numpy()

    media = np.mean(valores)
    mediana = np.median(valores)
    distancia = abs((media - mediana) / mediana)

    q1, q2, q3 = np.quantile(valores, [0.25, 0.50, 0.75])
    iqr = q3 - q1
    limite_inf = q1 - 1.5 * iqr
    limite_sup = q3 + 1.5 * iqr
    minimo = np.min(valores)
    maximo = np.max(valores)

    outliers_inf = df_novo[df_novo['roubo_comercio'] < limite_inf]
    outliers_sup = df_novo[df_novo['roubo_comercio'] > limite_sup]

    # Tabelas
    exibir_tabela([
        ["Média", media],
        ["Mediana", mediana],
        ["Distância relativa (média-mediana)", distancia]
    ], headers=["Métrica", "Valor"], titulo="Medidas de tendência central")

    exibir_tabela([
        ["Q1", q1],
        ["Q2 (Mediana)", q2],
        ["Q3", q3],
        ["IQR (Q3 - Q1)", iqr]
    ], headers=["Quartil", "Valor"], titulo="Quartis e IQR")

    exibir_tabela([
        ["Limite Inferior", limite_inf],
        ["Valor Mínimo", minimo],
        ["Valor Máximo", maximo],
        ["Limite Superior", limite_sup]
    ], headers=["Extremos", "Valor"], titulo="Valores Extremos e Limites de Outliers")

    # Ranqueamento
    df_desc = df_novo.sort_values(by='roubo_comercio', ascending=False).reset_index(drop=True)
    exibir_tabela(df_desc, headers='keys', titulo="Ranqueamento dos Municípios - Ordem Decrescente")

except Exception as e:
    print(f"Erro ao obter informações estatísticas: {e}")
    exit()

# Gráficos de outliers
try:
    fig, ax = plt.subplots(1, 2, figsize=(14, 6))

    # Outliers inferiores
    if not outliers_inf.empty:
        dados = outliers_inf.sort_values(by='roubo_comercio')
        ax[0].barh(dados['munic'], dados['roubo_comercio'], color='tomato')
        ax[0].set_title('Outliers Inferiores')
    else:
        ax[0].text(0.5, 0.5, "Sem Outliers", ha='center', va='center', fontsize=12)
        ax[0].set_title('Outliers Inferiores')

    ax[0].set_xlabel('roubo_comercio')

    # Outliers superiores
    if not outliers_sup.empty:
        dados = outliers_sup.sort_values(by='roubo_comercio')
        ax[1].barh(dados['munic'], dados['roubo_comercio'], color='seagreen')
        ax[1].set_title('Outliers Superiores')
    else:
        ax[1].text(0.5, 0.5, "Sem Outliers", ha='center', va='center', fontsize=12)
        ax[1].set_title('Outliers Superiores')

    ax[1].set_xlabel('roubo_comercio')

    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"Erro ao exibir gráfico: {e}")