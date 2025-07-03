import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
import pymysql
#from scipy.stats import kurtosis, skew
import re
from tabulate import tabulate

# (Opcional) Formatação de tabelas
try:
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

# Conexão com banco
host = 'localhost'
user = 'root'
password = '7812136'
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

def limpar_colunas(df):
    df.columns = [re.sub(r'[^\x00-\x7F]+', '', col).strip() for col in df.columns]
    return df

# Obter e preparar os dados
try:
    df_base = busca('basedp')
    df_roubo = busca('basedp_roubo_comercio')

    df_base = limpar_colunas(df_base)
    df_roubo = limpar_colunas(df_roubo)

    print("Colunas em df_base:", df_base.columns.tolist())
    print("Colunas em df_roubo:", df_roubo.columns.tolist())

    # Identificar chave comum
    chaves_comuns = set(df_base.columns).intersection(df_roubo.columns)
    if not chaves_comuns:
        raise ValueError("Nenhuma chave comum encontrada entre os dois DataFrames para junção.")

    chave = list(chaves_comuns)[0]  # Usar a primeira chave comum
    df_novo = pd.merge(df_base, df_roubo, on=chave)

    # Filtro por ano
    df_novo = df_novo[(df_novo['ano'] >= 2022) & (df_novo['ano'] <= 2023)]

    if 'roubo_comercio' not in df_novo.columns:
        raise ValueError("Coluna 'roubo_comercio' não encontrada no DataFrame.")

except Exception as e:
    print(f"Erro ao obter dados: {e}")
    exit()

# Cálculos estatísticos
try:
    print("\nCalculando estatísticas...")

    valores = df_novo['roubo_comercio'].to_numpy()

    media = np.mean(valores)
    mediana = np.median(valores)
    desvio_padrao = np.std(valores, ddof=1)
    variancia = np.var(valores, ddof=1)
    coef_var = desvio_padrao / media if media != 0 else np.nan

    assimetria = valores.skew()
    curtose_val = valores.kurtosis()

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
        ["Desvio Padrão", desvio_padrao],
        ["Coef. Variação", coef_var],
        ["Distância relativa (média-mediana)", distancia],
        ["Assimetria (skewness)", assimetria],
        ["Curtose (kurtosis)", curtose_val]
    ], headers=["Métrica", "Valor"], titulo="Medidas Estatísticas")

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

    df_desc = df_novo.sort_values(by='roubo_comercio', ascending=False).reset_index(drop=True)
    exibir_tabela(df_desc[['munic', 'roubo_comercio']], headers='keys', titulo="Ranqueamento dos Municípios - Ordem Decrescente")

except Exception as e:
    print(f"Erro ao obter informações estatísticas: {e}")
    exit()

# Gráficos
try:
    fig, ax = plt.subplots(1, 2, figsize=(14, 6))

    if not outliers_inf.empty:
        dados = outliers_inf.sort_values(by='roubo_comercio')
        ax[0].barh(dados['munic'], dados['roubo_comercio'], color='tomato')
        ax[0].set_title('Outliers Inferiores')
    else:
        ax[0].text(0.5, 0.5, "Sem Outliers", ha='center', va='center', fontsize=12)
        ax[0].set_title('Outliers Inferiores')
    ax[0].set_xlabel('roubo_comercio')

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

# Gráficos de Assimetria e Curtose
try:
    plt.figure(figsize=(12, 5))

    # Histograma com curvas para assimetria
    plt.subplot(1, 2, 1)
    plt.hist(valores, bins=20, color='skyblue', edgecolor='black', density=True)
    plt.axvline(media, color='red', linestyle='--', label=f'Média: {media:.2f}')
    plt.axvline(mediana, color='green', linestyle='--', label=f'Mediana: {mediana:.2f}')
    plt.title(f'Distribuição dos Dados\nAssimetria: {assimetria:.2f}')
    plt.xlabel('roubo_comercio')
    plt.ylabel('Densidade')
    plt.legend()

    # Boxplot para curtose
    plt.subplot(1, 2, 2)
    plt.boxplot(valores, vert=False, patch_artist=True,
                boxprops=dict(facecolor='lightyellow', color='orange'),
                medianprops=dict(color='red'))
    plt.title(f'Boxplot da Distribuição\nCurtose: {curtose_val:.2f}')
    plt.xlabel('roubo_comercio')

    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"Erro ao exibir gráfico de assimetria e curtose: {e}")