import os
from pathlib import Path
import re
import csv
from datetime import datetime, date
import pandas

def sort_snirh(line, date_index):
    if line[0] ==  "EstacaoCodigo" or line[date_index] == "":
        return datetime(1, 1, 1, 0, 0)
    return datetime.strptime(line[date_index], "%d/%m/%Y")

def sort_sala_de_situacao(data):
    return datetime.strptime(data, "%d/%m/%Y %H:%M:%S")

def processar(pasta_estacoes: Path = Path("DADOS_ESTACOES/"), pasta_processados: Path | None = None):
    print("Processando dados baixados...   ", end = "\n", flush = True)

    if pasta_processados is None:
        pasta_processados = Path("PROCESSADOS")
        pasta_processados.mkdir(exist_ok = True)

#SNIRH
        nomes_arquivos = set()
        print("\tProcessando arquivos SNIRH...   ", end="", flush = True)
        #Descobrir todos os tipos de arquivo que o snirh tem (Formato: {numero da estação}_{tipo do arquivo}.csv)
        for estacao in pasta_estacoes.iterdir():
            dir_snirh = estacao / "snirh/"
            if dir_snirh.exists():
                for arquivo in (estacao / "snirh/").iterdir():
                    nome_arquivo = re.sub("[0-9_]", "", arquivo.name)
                    nomes_arquivos.add(nome_arquivo)


        #Juntar todos os arquivos de mesmo tipo de todas as estaçãos em um só arquivo
        for nome in nomes_arquivos:
            arquivo_juncao = pasta_processados / f"{nome}"
            linhas = []
            for estacao in pasta_estacoes.iterdir():
                arquivo = estacao / "snirh/" / f"{estacao.name}_{nome}"

                if arquivo.exists():
                    with arquivo.open(mode="r", newline="", encoding="latin_1") as arquivo:
                        reader = csv.reader(arquivo, delimiter=";")
                        for line in reader:
                            if len(line) > 0:
                                if line[0] == estacao.name or (len(linhas) == 0 and line[0] == "EstacaoCodigo"):
                                    linhas.append(line)
            linhas = sorted(linhas, key=lambda linha: sort_snirh(linha, 2))
            with arquivo_juncao.open(mode="w", newline="") as csvfile:
                writer = csv.writer(csvfile, delimiter=";")
                writer.writerows(linhas)
        print("Pronto!")

#SALA DE SITUAÇÃO
    print("\tProcessando arquivos da sala de situações...   ")
    dfs = []
    counter = 1
    total = len(list(pasta_estacoes.iterdir()))
    for estacao in pasta_estacoes.iterdir():
        print(f"\tPROCESSANDO: {counter}/{total}", end = "\r")
        arquivo = estacao / "sala_de_situacao.xlsx"
        if arquivo.exists():
            arquivo = pandas.read_excel(arquivo, skiprows=[0,1, 2, 3, 5, 6, 7, 8])
            if arquivo.shape[0] < 8:
                continue
            arquivo.insert(0, 'CodigoEstacao', estacao.name)
            dfs.append(arquivo)

        counter += 1
    final = pandas.concat(dfs, ignore_index=True)
    final.sort_values(by="Data", key=lambda data: data.map(sort_sala_de_situacao), inplace=True)
    final.to_csv(pasta_processados / "sala_de_situacao.csv", sep=";", index=False)

    
    print(f"\t\033[AProcessando arquivos da sala de situações...   Pronto!")
    print(" "*100)
if __name__ == "__main__":
    processar()


