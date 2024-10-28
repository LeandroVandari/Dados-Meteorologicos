import tensorflow
import modelo
import eel

modelo = None

def main():
    eel.init("web")
    eel.start("main.html", mode="default")

@eel.expose
def carregar_modelo(caminho):
    global modelo
    modelo = tensorflow.keras.models.load_model(caminho)


if __name__ == "__main__":
    main()