# import pandas as pd
# # import numpy as np
# # import matplotlib.pyplot as plt
# # import seaborn as sns
# # import plotly.express as px
# # from sklearn.neighbors import KNeighborsClassifier
# # from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
#
# from geopy.geocoders import Nominatim
# from geopy.exc import GeopyError
# import time
#
#
# def correct_street_name(lat, lon, geolocator, retry=3):
#     for _ in range(retry):
#         try:
#             location = geolocator.reverse((lat, lon), language='pt', timeout=10)
#             if location and 'road' in location.raw['address']:
#                 return location.raw['address']['road']
#             else:
#                 return None
#         except GeopyError as e:
#             print(f"Erro: {e}. Tentando novamente...")
#             time.sleep(1)
#     return None
#
#
#
# df = pd.read_excel('C:/Users/pichau/Documents/GitHub/tratar_base_service/SPDadosCriminais_2024.xlsx')
#
# string_col = df.select_dtypes(include="object").columns
# df[string_col]=df[string_col].astype("string")
#
#
#
# df = df.dropna(subset=['LONGITUDE'])
# df = df.dropna(subset=['LATITUDE'])
#
# print(f'total da base: {df.count()}')
#
# # unique_streets_df = df[['LOGRADOURO', 'LATITUDE', 'LONGITUDE']].drop_duplicates()
#
#
# unique_streets_df = df.drop_duplicates(subset=['LATITUDE', 'LONGITUDE'])
# print(unique_streets_df.shape[0]) # df.shape[0]
#
# # Inicialização do geolocator Nominatim
# geolocator = Nominatim(user_agent="testrequestdataframepcsspsp")
#
# # Lista para armazenar os nomes corrigidos
# ruas_corrigidas_dict = {}
#
# # Aplicação da função de correção aos nomes únicos das ruas
# for index, row in unique_streets_df.iterrows():
#     rua_corrigida = correct_street_name(row['LATITUDE'], row['LONGITUDE'], geolocator) or row['LOGRADOURO']
#     ruas_corrigidas_dict[row['LOGRADOURO']] = rua_corrigida
#     print(rua_corrigida)
#     time.sleep(2)  # Pausa de 2 segundos entre as requisições para respeitar os limites de taxa
#
# # Mapeamento dos nomes corrigidos de volta ao DataFrame original
# df['ruas_corrigidas'] = df['ruas'].map(ruas_corrigidas_dict)
#
#
# df.to_parquet('df.parquet.gzip',
#               compression='gzip')
#


import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
# import plotly.express as px
# from sklearn.neighbors import KNeighborsClassifier
# from sklearn.metrics import accuracy_score, confusion_matrix, classification_report

from geopy.geocoders import Nominatim
from geopy.exc import GeopyError
import time


def correct_street_name(lat, lon, geolocator, retry=3):
    for _ in range(retry):
        try:
            location = geolocator.reverse((lat, lon), language='pt', timeout=10)
            if location and 'road' in location.raw['address']:
                return location.raw['address']['road']
            else:
                return None
        except GeopyError as e:
            print(f"Erro: {e}. Tentando novamente...")
            time.sleep(1)
    return None



df = pd.read_excel('C:/Users/pichau/Documents/GitHub/tratar_base_service/SPDadosCriminais_2024.xlsx')

string_col = df.select_dtypes(include="object").columns
df[string_col]=df[string_col].astype("string")



df = df.dropna(subset=['LONGITUDE'])
df = df.dropna(subset=['LATITUDE'])

print(f'total da base: {df.count()}')

# unique_streets_df = df[['LOGRADOURO', 'LATITUDE', 'LONGITUDE']].drop_duplicates()


unique_streets_df = df.drop_duplicates(subset=['LATITUDE', 'LONGITUDE'])
print(unique_streets_df.shape[0]) # df.shape[0]

# Inicialização do geolocator Nominatim
geolocator = Nominatim(user_agent="testrequestdataframepcsspsp2")

# Lista para armazenar os nomes corrigidos
ruas_corrigidas_dict = {}

# Aplicação da função de correção aos nomes únicos das ruas
for index, row in unique_streets_df.iterrows():
    rua_corrigida = correct_street_name(row['LATITUDE'], row['LONGITUDE'], geolocator) or row['LOGRADOURO']
    ruas_corrigidas_dict[row['LOGRADOURO']] = rua_corrigida
    print(rua_corrigida)
    time.sleep(2)  # Pausa de 2 segundos entre as requisições para respeitar os limites de taxa

# Mapeamento dos nomes corrigidos de volta ao DataFrame original
df['ruas_corrigidas'] = df['ruas'].map(ruas_corrigidas_dict)


df.to_parquet('df.parquet.gzip',
              compression='gzip')

