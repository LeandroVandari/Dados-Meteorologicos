import lista_estacoes, baixar_dados, processar, parse_arguments

args = parse_arguments.parse_args()
lista_estacoes.baixar()

use_cached = not args.sem_cache
baixar_dados.baixar_todos(use_cached=use_cached)
processar.processar(ignorar_fontes=set(args.sem_fontes))
