import pandas as pd
from rapidfuzz import process, fuzz
from concurrent.futures import ThreadPoolExecutor, as_completed

# Carregando os dados do DataFrame
df = pd.read_excel('C:/Users/pichau/Documents/GitHub/tratar_base_service/SPDadosCriminais_2024.xlsx')

# Passo 1: Remover espaços e padronizar capitalização
df['LOGRADOURO'] = df['LOGRADOURO'].astype(str).str.strip().str.title()

print(f'Total da base: {df.shape[0]}')

# Filtrando linhas distintas por LOGRADOURO
unique_streets_df = df.drop_duplicates(subset=['LOGRADOURO'])
print(f'Linhas distintas: {unique_streets_df.shape[0]}')


# Passo 2: Utilizar rapidfuzz para normalização
def normalize_street_names(df, column):
    unique_streets = df[column].unique()
    threshold = 80  # Define a porcentagem mínima de similaridade para considerar como correspondência
    normalized = {}

    # Função para processar um lote de ruas
    def process_batch(batch, batch_index):
        local_normalized = {}
        for street in batch:
            matches = process.extract(street, unique_streets, scorer=fuzz.ratio, limit=5)  # Limite para otimizar
            best_match = max(matches, key=lambda x: x[1])
            local_normalized[street] = best_match[0] if best_match[1] >= threshold else street
        print(f'Batch {batch_index} processado')
        return local_normalized

    # Dividir a lista em lotes para processamento paralelo
    batch_size = 1000
    batches = [unique_streets[i:i + batch_size] for i in range(0, len(unique_streets), batch_size)]

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_batch, batch, index): (batch, index) for index, batch in enumerate(batches)}
        for future in as_completed(futures):
            batch, batch_index = futures[future]
            try:
                batch_result = future.result()
                normalized.update(batch_result)
                print(f'Lote {batch_index + 1} de {len(batches)} concluído')
            except Exception as exc:
                print(f'Erro ao processar lote {batch_index}: {exc}')

    df[column + '_normalizado'] = df[column].map(normalized)
    return df


# Normalizando os nomes das ruas
df = normalize_street_names(df, 'LOGRADOURO')

for column in df.columns:
    df[column] = df[column].astype(str)

df['LOGRADOURO_normalizado'] = df['LOGRADOURO_normalizado'].str.upper()

# Resultado final
print(df.head(50))  # Exibir as primeiras 50 linhas para verificar

# df = df.drop(columns=['HORA_OCORRENCIA_BO'])
# Salvar o DataFrame resultante
df.to_parquet('df.parquet.gzip', compression='gzip')
