import os
from pathlib import Path
import re
import csv
from datetime import datetime, date
import pandas
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def sort_snirh(line, date_index):
    if line[0] ==  "EstacaoCodigo" or line[date_index] == "":
        return datetime(1, 1, 1, 0, 0)
    return datetime.strptime(line[date_index], "%d/%m/%Y")

def abrir_sala_de_situacoes(arquivo: (int, Path, int)) -> pandas.DataFrame | None:
    idx, arquivo, total = arquivo
    print(f"\tPROCESSANDO: {idx}/{total}    ", end = "\r")
    if arquivo.exists():
        codigo_estacao = arquivo.parent.name
        arquivo = pandas.read_excel(arquivo, skiprows=[0,1, 2, 3, 5, 6, 7, 8])
        if arquivo.shape[0] < 8:
            return None
        arquivo.set_index(arquivo["Data"].map(lambda data: pandas.Timestamp(datetime.strptime(data, "%d/%m/%Y %H:%M:%S"))), inplace=True)
        arquivo.drop("Data", axis="columns", inplace = True)
        arquivo.insert(0, 'CodigoEstacao', codigo_estacao)
        return arquivo

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
    pasta_sala_de_situacao = pasta_processados / "Sala de situação"
    pasta_sala_de_situacao.mkdir(exist_ok=True)

    print("\tCarregando arquivos da sala de situações...   ")
    counter = 1
    total = len(list(pasta_estacoes.iterdir()))
    arquivos = ((tup[0],tup[1] / "sala_de_situacao.xlsx", total) for tup in enumerate(pasta_estacoes.iterdir()))
    with ProcessPoolExecutor() as executor:
        arquivos = filter(lambda i: i is not None, executor.map(abrir_sala_de_situacoes, arquivos))
    print("\033[A\tCarregando arquivos da sala de situações...   Pronto!")
    print("\tJuntando arquivos por data...    ", end = "", flush=True)
    final = pandas.concat(arquivos)
    grouping = final.groupby(by=lambda data: data.year)
    print("Pronto!")
    print("\tCriando novos arquivos...   ")
    for (ano, df) in grouping:
        print(f"\tCriando arquivo do ano {ano}   ", end = "\r")
        df.sort_index(inplace=True)
        df.to_csv(pasta_sala_de_situacao / f"{ano}.csv", sep=";")    
    print("\033[A\tCriando novos arquivos...   Pronto!")
    
    print(" "*100)




if __name__ == "__main__":
    processar()



