import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Baixa e processa as estações fluviométricas no rs"
    )
    parser.add_argument(
        "--sem_cache",
        help="Essa opção fará com que todos os dados sejam baixados novamente, mesmo que já se encontrem na pasta.",
        action="store_true",
    )
    fontes = parser.add_argument_group("Fontes:")
    fontes.add_argument(
        "--sem_fontes",
        help="Não processar as fontes passadas",
        action="extend",
        nargs="+",
        choices=["snirh", "inmet", "sala_de_situacao"],
        default=[],
    )
    return parser.parse_args()
