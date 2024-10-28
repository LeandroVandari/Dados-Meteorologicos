import tkinter
import tkinter.filedialog
from tkinter import ttk
import tensorflow
import modelo

def main():
    modelo = None
    window = tkinter.Tk()
    window.configure(bg="#382b36")
    window.title("Previsor de nível fluviométrico")
    window.geometry("500x80")
    style = ttk.Style()

    style.configure("TButton", foreground="#f9c5f1", background="#60515e", width=25)

    botao_carregar = ttk.Button(window, text="Carregar um modelo", command=carregar_modelo)
    botao_carregar.place(rely= 0.5, relx = 0.25, anchor=tkinter.CENTER)


    botao_criar = ttk.Button(window, text="Treinar um modelo", command=lambda: treinar_modelo(window))
    botao_criar.place(rely=0.5, relx=0.75, anchor=tkinter.CENTER)

    window.mainloop()



def carregar_modelo():
    global modelo
    window.destroy()
    arquivo = tkinter.filedialog.askopenfilename()
    modelo = tensorflow.keras.models.load_model(arquivo)


def treinar_modelo(window):
    global modelo
    caminho_pasta = None
    window.destroy()

    window = tkinter.Tk()

    window.configure(bg="#382b36")
    window.title("Previsor de nível fluviométrico")
    window.geometry("500x500")

    style = ttk.Style()
    style.configure("TButton", foreground="#f9c5f1", background="#60515e")
    style.configure("TLabel", foreground="#f9c5f1", background="#382b36" )
    style.configure("TEntry", foreground="#f9c5f1",  background="#60515e", width=25)

    pasta_dados = ttk.Label(window, text="Pasta com dados: ")
    pasta_dados.place(relx= 0.15, rely= 0.05, anchor=tkinter.CENTER)

    caminho_text = ttk.Entry(window)
    caminho_text.place(relx=0.15, rely = 0.1, anchor=tkinter.CENTER)

    selecionar = ttk.Button(window, text="Selecionar", command=pegar_pasta_dados)
    selecionar.place(relx = 0.3, rely = 0.1, anchor=tkinter.CENTER)





    window.mainloop()

def pegar_pasta_dados():
    global caminho_pasta
    caminho_pasta = tkinter.filedialog.askdirectory()


if __name__ == "__main__":
    main()