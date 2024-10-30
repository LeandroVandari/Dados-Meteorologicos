import modelo
import eel
import tkinter as tk
from tkinter import filedialog
import logging
from io import StringIO
import sys
import polars as pl
import baixar_dados
from sklearn.preprocessing import StandardScaler
import zipfile
from pathlib import Path
import tensorflow
import os
import pickle

root = tk.Tk()
root.withdraw()

log_stream = StringIO()

log = logging.getLogger("tensorflow")
logging.basicConfig(stream=log_stream)

configuracoes = {"modelo": None, "pasta_dados": None}


def main():
    eel.init("web")
    eel.start("main.html", mode="default")



@eel.expose
def carregar_modelo(caminho):
    global configuracoes
    configuracoes["modelo"] = tensorflow.keras.models.load_model(caminho)


@eel.expose
def requisitar_pasta():
    file_path = filedialog.askdirectory()
    configuracoes["pasta_dados"] = file_path
    print(f"Pasta selecionada: {file_path}")
    return file_path


@eel.expose
def treinar(dias_a_frente):
    print(dias_a_frente)
    (model, erro) = modelo.treinar(
        caminho_pasta=configuracoes["pasta_dados"],
        dias_a_frente=dias_a_frente,
        arquivo_cota=configuracoes["arquivo_cota"],
    )
    configuracoes["modelo"] = model
    return erro


@eel.expose
def salvar():
    configuracoes["modelo"].save("MODELO.keras")


@eel.expose
def arquivo_cota(nome):
    configuracoes["arquivo_cota"] = nome

@eel.expose
def requisitar_hoje(email):
    baixar_dados.requisitar_hoje(email)

@eel.expose
def arquivo_hoje(nome):
    configuracoes["dados_hoje"] = nome

estacoes_usadas = []

@eel.expose
def executar():
    zip_hoje = zipfile.ZipFile(configuracoes["dados_hoje"])
    pasta_hoje = baixar_dados.criar_pasta(Path.cwd(), "DadosHoje")
    zip_hoje.extractall(pasta_hoje)
    subdir = os.listdir(pasta_hoje)[0]

    (dados_hoje, _) = modelo.abrir_pasta(str(pasta_hoje / subdir))
    with open("scaler.bin", "rb") as f:

        scaler = pickle.load(f)
    dados_hoje = dados_hoje.sort("Data", descending=True).drop("Data")[0]
    with open("scale.txt", "r") as f:
        lines = f.readlines()
        y_max = int(lines[0].strip())
        y_min = int(lines[1].strip())
    print(dados_hoje)
    dados_scaled = scaler.transform(dados_hoje)
    previsao = configuracoes["modelo"].predict(dados_scaled)[0,0]
    converter = lambda prev: prev * (y_max - y_min) + y_min
    print(converter(previsao))
    return converter(previsao)



class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ""

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


""" sys.stdout=StreamToLogger(log, logging.INFO) """


if __name__ == "__main__":
    main()
