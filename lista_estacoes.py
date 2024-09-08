import requests
from snirh import headers
from pathlib import Path

def baixar(arquivo: Path | None = None):
    if arquivo is None:
        arquivo = Path("lista_estacoes.txt")
    codigos = set()

    print("Baixando listas de estações...   ", end = "")
    sala_de_situacao = requests.get("https://saladesituacao.rs.gov.br/api/station/ana").json()
    hidroweb_convencional = requests.get("https://www.snirh.gov.br/hidroweb/rest/api/dadosHistoricos?estadoCodigo=24&tipoEstacao=F&size=10000&page=0&operando=1", headers=headers).json()

    for estacao in sala_de_situacao:
        codigo = estacao["id"]
        codigos.add(str(codigo))
    for estacao in hidroweb_convencional["content"]:
        codigo = estacao["id"]
        codigos.add(str(codigo))
    print("Pronto!")
    print("Salvando listas em arquivo...   ", end="")
    with open(arquivo, "w") as f:
        f.write("\n".join(list(codigos)))
    print("Pronto!")

if __name__ == "__main__":
    baixar()