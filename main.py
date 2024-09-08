import lista_estacoes, baixar_dados, processar
import argparse

parser = argparse.ArgumentParser(description="Baixa e processa as estações fluviométricas no rs")
parser.add_argument("--sem-cache", help="Essa opção fará com que todos os dados sejam baixados novamente, mesmo que já se encontrem na pasta.", default=False, action="store_const", const=True)
args = parser.parse_args()

lista_estacoes.baixar()

use_cached = not args.sem_cache
baixar_dados.baixar_todos(use_cached=use_cached)

processar.processar()