import requests
import zipfile
import io
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


from snirh import headers


def baixar(enumeracao, ignorar_fontes: set[str] = {}):
    global cuse_cached
    idx, codigo = enumeracao
    print(f"Baixando {codigo} -- ({idx+1})  ", end="\r")
    dir_estacao = criar_pasta(end_dir, str(codigo))

    if not "snirh" in ignorar_fontes and (
        (not (dir_estacao / "snirh").exists()) or cuse_cached == False
    ):
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

        dir_snirh = criar_pasta(dir_estacao, "snirh")

        arquivo_zip.extractall(dir_snirh)

    if not "sala_de_situacao" in ignorar_fontes and (
        (not (dir_estacao / "sala_de_situacao.xlsx").exists()) or cuse_cached == False
    ) :
        arquivo_sala_de_situação = requests.get(
            f"https://saladesituacao.rs.gov.br/api/station/ana/sheet/{codigo}",
        )

        if (
            arquivo_sala_de_situação.content
            != b'{"message":"Erro: Esta\\u00e7\\u00e3o sem dados"}\n'
        ):
            with open(os.path.join(dir_estacao, "sala_de_situacao.xlsx"), "wb") as f:
                f.write(arquivo_sala_de_situação.content)


def baixar_inmet(dir_inmet, year):
    print(f"\tBAIXANDO ANO {year}...   ", end="\r", flush=True)
    if (not (dir_inmet / str(year)).exists()) or cuse_cached == False:
        arquivo = requests.get(
            f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
        )
        
        arquivo_zip = zipfile.ZipFile(io.BytesIO(arquivo.content))
        if year >= 2020:
            dir_inmet = criar_pasta(dir_inmet, f"{year}")
        arquivo_zip.extractall(dir_inmet)


def baixar_todos(lista_estacoes=None, use_cached=True, ignorar_fontes: set[str] = {}):
    global cuse_cached
    cuse_cached = use_cached
    if lista_estacoes is None:
        lista_estacoes = "lista_estacoes.txt"

    with open(lista_estacoes, "r", encoding="utf-8") as f:
        estacoes = f.readlines()
        mensagem_baixando = f"Baixando dados das estações... Total: {len(estacoes)}   "
        print(mensagem_baixando)

        codigos = enumerate(estacao.strip() for estacao in estacoes)
        with ThreadPoolExecutor() as executor:
            executor.map(
                lambda enum: baixar(enumeracao=enum, ignorar_fontes=ignorar_fontes),
                codigos,
            )
    print(f"\033[A{mensagem_baixando}Pronto!")

    if not "inmet" in ignorar_fontes:
        print("Baixando dados do INMET...   ")
        dir_inmet = criar_pasta(end_dir, "INMET")
        anos = range(2000, 2025)
        with ThreadPoolExecutor() as executor:
            executor.map(lambda ano: baixar_inmet(dir_inmet, ano), anos)
        print("\033[ABaixando dados do INMET...   Pronto!")


def criar_pasta(pai: Path, nome: str) -> str:
    pasta = pai / nome
    pasta.mkdir(exist_ok=True)

    return pasta


end_dir = criar_pasta(Path.cwd(), "DADOS_ESTACOES/")
cuse_cached = True

if __name__ == "__main__":
    import parse_arguments

    args = parse_arguments.parse_args()
    baixar_todos(use_cached=not args.sem_cache, ignorar_fontes=args.sem_fontes)
