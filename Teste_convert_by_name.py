import pandas as pd
from fuzzywuzzy import process, fuzz

# Carregando os dados do DataFrame
df = pd.read_excel('C:/Users/pichau/Documents/GitHub/tratar_base_service/SPDadosCriminais_2024.xlsx')

# Passo 1: Remover espaços e padronizar capitalização
# Primeiro, garantir que todos os valores sejam strings
df['LOGRADOURO'] = df['LOGRADOURO'].astype(str).str.strip().str.title()

print(f'Total da base: {df.shape[0]}')

# Filtrando linhas distintas por LOGRADOURO
unique_streets_df = df.drop_duplicates(subset=['LOGRADOURO'])
print(f'Linhas distintas: {unique_streets_df.shape[0]}')

# Passo 2: Utilizar fuzzywuzzy para normalização
def normalize_street_names(df, column):
    unique_streets = df[column].unique()
    threshold = 80  # Define a porcentagem mínima de similaridade para considerar como correspondência
    normalized = {}
    contador = 0
    for street in unique_streets:
        if isinstance(street, str):
            matches = process.extract(street, unique_streets, scorer=fuzz.ratio)
            best_match = max(matches, key=lambda x: x[1])
            normalized[street] = best_match[0] if best_match[1] >= threshold else street
        else:
            normalized[street] = street  # Manter o valor original se não for string
        contador += 1
        print(contador)

    df[column + '_normalizado'] = df[column].map(normalized)
    return df

# Normalizando os nomes das ruas
df = normalize_street_names(df, 'LOGRADOURO')

# Resultado final
print(df)
