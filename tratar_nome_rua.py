
import pandas as pd

def filter_distinct_lat_lon(df, lat_col='latitude', lon_col='longitude'):
    """
    Filtra linhas distintas baseadas nas colunas de latitude e longitude.

    :param df: DataFrame do Pandas contendo os dados.
    :param lat_col: Nome da coluna de latitude.
    :param lon_col: Nome da coluna de longitude.
    :return: Novo DataFrame com linhas distintas.
    """
    distinct_df = df.drop_duplicates(subset=[lat_col, lon_col])
    return distinct_df

# Exemplo de DataFrame
data = {
    'ruas': ['Nome antigo 1', 'Nome antigo 2', 'Nome antigo 3', 'Nome antigo 2', 'Nome antigo 1'],
    'latitude': [-23.550520, -23.551620, -23.552720, -23.551620, -23.550520],
    'longitude': [-46.633308, -46.634308, -46.635308, -46.634308, -46.633308]
}
df = pd.DataFrame(data)

# Aplicando o filtro
distinct_df = filter_distinct_lat_lon(df)

print(distinct_df)
