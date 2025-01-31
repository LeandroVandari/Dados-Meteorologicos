import requests
import zipfile
import io
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import datetime

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
    ):
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

def requisitar_hoje(email):
    email = email.replace("@", "%40")
    hoje = datetime.datetime.now().strftime("%Y-%m-%d")
    request_base = f"email={email}&tipo_dados=H&tipo_estacao=T&variaveis%5B%5D=I175&variaveis%5B%5D=I106&variaveis%5B%5D=I108&variaveis%5B%5D=I615&variaveis%5B%5D=I616&variaveis%5B%5D=I133&variaveis%5B%5D=I619&variaveis%5B%5D=I101&variaveis%5B%5D=I103&variaveis%5B%5D=I611&variaveis%5B%5D=I612&variaveis%5B%5D=I613&variaveis%5B%5D=I614&variaveis%5B%5D=I620&variaveis%5B%5D=I617&variaveis%5B%5D=I618&variaveis%5B%5D=I105&variaveis%5B%5D=I113&variaveis%5B%5D=I608&variaveis%5B%5D=I111"
    with open(Path("MODELO") / "estacoes_modelo.txt", "r") as f:
        estacoes = [estacao.strip() for estacao in f.readlines()]
        for estacao in estacoes:
            request_base += f"&estacoes%5B%5D={estacao}"
    request_base += f"&data_inicio={hoje}&data_fim={hoje}&tipo_pontuacao=V"
    r = requests.post(f"https://apibdmep.inmet.gov.br/requisicao", data=request_base, headers={
        "Sec-Ch-Ua-Platform": "\"Linux\"",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "*/*",
        "Sec-Ch-Ua": "\"Not?A_Brand\";v=\"99\", \"Chromium\";v=\"130\"",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Sec-Ch-Ua-Mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.59 Safari/537.36",
        "Origin": "https\":/\"/bdmep.inmet.gov.br",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https\":/\"/bdmep.inmet.gov.br/",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i",
        "Connection": "keep-alive",
    })

    print(r.text)
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
