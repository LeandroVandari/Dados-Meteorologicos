import requests
import zipfile
import io
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from snirh import headers

def baixar(enumeracao):
    idx, codigo = enumeracao
    print(f"Baixando {codigo} -- ({idx})", end="\r")
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
    if arquivo_sala_de_situação.content != b'{"message":"Erro: Esta\\u00e7\\u00e3o sem dados"}\n':
        with open(os.path.join(dir_estacao, "sala_de_situacao.xlsx"), "wb") as f:
            f.write(arquivo_sala_de_situação.content)
    elif len(os.listdir(dir_snirh)):
        os.rmdir(estacao)


def main():
    with open("lista_estacoes.txt", "r", encoding="utf-8") as f:
        estacoes = f.readlines()
        print(f"Total: {len(estacoes)} estações")

        codigos = enumerate([estacao.split()[0] for estacao in estacoes])
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(baixar, codigos)

            

def criar_pasta(pai, nome) -> str:
    pasta = os.path.join(pai, nome)
    os.makedirs(pasta, exist_ok=True)

    return pasta

end_dir = criar_pasta(os.getcwd(), "DADOS_ESTACOES/")

if __name__ == "__main__":
    main()



