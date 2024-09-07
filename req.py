import requests
import zipfile
import io
import os

from snirh import headers

def main():
    end_dir = criar_pasta(os.getcwd(), "DADOS_ESTACOES/")


    with open("lista_estacoes.txt", "r", encoding="utf-8") as f:
        estacoes = f.readlines()
        tamanho = len(estacoes)
        for i, estacao in enumerate(estacoes):

            codigo = estacao.split()[0]
            print(f"Baixando {codigo} -- ({i+1}/{tamanho})\r", end="")

            arquivo_snirh = requests.get(
                "http://www.snirh.gov.br/hidroweb/rest/api/documento/download/files",
                stream=True,
                params={
                    "codigoestacao": codigo,
                    "tipodocumento": "csv",
                    "forcenewfiles": "Y",
                },
                headers=headers,
            )
            arquivo_zip = zipfile.ZipFile(io.BytesIO(arquivo_snirh.content))

            dir_estacao = criar_pasta(end_dir, str(codigo))
            dir_snirh = criar_pasta(dir_estacao, "snirh")

            arquivo_zip.extractall(dir_snirh)

            arquivo_sala_de_situação = requests.get(
                f"https://saladesituacao.rs.gov.br/api/station/ana/sheet/{codigo}",
            )
            with open(os.path.join(dir_estacao, "sala_de_situacao.xlsx"), "wb") as f:
                f.write(arquivo_sala_de_situação.content)

def criar_pasta(pai, nome) -> str:
    pasta = os.path.join(pai, nome)
    os.makedirs(pasta, exist_ok=True)

    return pasta

if __name__ == "__main__":
    main()



