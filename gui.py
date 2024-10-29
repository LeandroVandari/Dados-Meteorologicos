import tensorflow
import modelo
import eel
import tkinter as tk
from tkinter import filedialog
import logging
from io import StringIO
import sys

root = tk.Tk()
root.withdraw()

log_stream = StringIO()

log = logging.getLogger('tensorflow')
logging.basicConfig(stream=log_stream)

configuracoes =  {
    "modelo": None,
    "pasta_dados": None
}

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
def treinar():
    model = modelo.treinar(caminho_pasta=configuracoes["pasta_dados"])
    configuracoes["modelo"] = model

@eel.expose
def salvar():
    configuracoes["modelo"].save("MODELO.keras")


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, level):
       self.logger = logger
       self.level = level
       self.linebuf = ''

    def write(self, buf):
       for line in buf.rstrip().splitlines():
          self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass

""" sys.stdout=StreamToLogger(log, logging.INFO) """


if __name__ == "__main__":
    main()