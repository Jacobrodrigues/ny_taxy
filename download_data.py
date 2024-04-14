import os
import glob
import pandas as pd
from urllib import request

def transform_csv_parquet(caminho_final_csv):
    print(f"Transformando arquivo CSV para Parquet para o arquivo {caminho_final_csv}.")
    dados_csv = pd.read_csv(caminho_final_csv, compression='gzip')

    caminho_final_parquet = caminho_final_csv.replace('.csv.gz', '.parquet')
    dados_csv.to_parquet(caminho_final_parquet)
    print(f"Arquivo Parquet salvo com sucesso: {caminho_final_parquet}")

def download_and_transform(url, tipo, caminho):
    print(f"Baixando e transformando arquivo tipo '{tipo}'")
    for ano in range(2019, 2023):
        os.mkdir(f"{caminho}{tipo}{ano}")
        for mes in range(1, 13):
            sufix = f"{tipo}_tripdata_{ano}-{mes:02d}.csv.gz"
            url_final = f"{url}/{sufix}"
            caminho_final_csv = f"{caminho}{tipo}{ano}/{sufix}"

            if os.path.exists(caminho_final_csv.replace('.csv.gz', '.parquet')):
                print(f"O arquivo para o mês {mes} já existe. Pulando a transformação e download.")
                continue

            if not os.path.exists(caminho_final_csv):
                try:
                    download_file(url_final, caminho_final_csv)
                    transform_csv_parquet(caminho_final_csv)
                except Exception as e:
                    print(f"Não foi possível baixar e transformar o arquivo: {str(e)}")
                    continue

def download_file(url, caminho_final):
    print(f"Baixando arquivo: {url}")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    req = request.Request(url, headers=headers)
    with request.urlopen(req) as response, open(caminho_final, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
    print(f"Arquivo baixado com sucesso: {caminho_final}")

def delete_files(diretorio):
    arquivos = glob.glob(os.path.join(diretorio, '*.csv.gz'))

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio, arquivo)
        if os.path.isfile(caminho_arquivo):
            os.remove(caminho_arquivo)

url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
tipos = ['yellow', 'green', 'fhv', 'fhvhv', 'misc']
caminho = '/Users/raimundo.rodrigues/Desktop/pyairbyte/elt/datasets/'

for tipo in tipos:
    download_and_transform(f"{url}/{tipo}", tipo, caminho)

#delete_files(caminho)

print("Download e transformação para Parquet completos!")