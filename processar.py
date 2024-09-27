from pathlib import Path
import re
import csv
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import polars
import altair
import time

altair.renderers.enable("browser")
altair.data_transformers.enable("vegafusion")


def sort_snirh(line, date_index):
    if line[0] == "EstacaoCodigo" or line[date_index] == "":
        return datetime(1, 1, 1, 0, 0)
    return datetime.strptime(line[date_index], "%d/%m/%Y")


def abrir_sala_de_situacoes(arquivo: (int, Path, int)) -> polars.DataFrame | None:
    idx, arquivo, total = arquivo
    print(f"\tCARREGANDO: {idx}/{total}    ", end="\r")
    if arquivo.exists():
        codigo_estacao = arquivo.parent.name
        nome_arquivo = arquivo
        arquivo = (
            polars.read_excel(
                arquivo,
                read_options={"header_row": 4},
                schema_overrides={
                    "Nível (cm)": polars.Float64,
                    "Vazão (m³/s)": polars.Float64,
                    "Chuva (mm)": polars.Float64,
                },
            )
            .with_columns(
                polars.col("Data").str.strptime(polars.Datetime, "%d/%m/%Y %H:%M:%S"),
                CodigoEstacao=polars.lit(codigo_estacao),
            )
            .set_sorted("Data", descending=True)
        )

        com_maximo_dia = arquivo.with_columns(
            polars.col("Nível (cm)")
            .max()
            .over(polars.col("Data").dt.date())
            .alias("NivelMaximoDia")
        )
        """ print(f"Abrindo gráfico da estação {codigo_estacao}")
        grafico = com_maximo_dia.plot.line(x="Data", y="Vazão (m³/s)").properties(width=500).configure_scale(zero=False)
        grafico.show() """

        com_maximo_dia = com_maximo_dia.filter(
            polars.col("Nível (cm)").is_not_null()
            | polars.col("Vazão (m³/s)").is_not_null()
        )

        mediana_nivel = com_maximo_dia.select(
            polars.median("Nível (cm)").alias("Mediana")
        )[0, 0]
        nivel_filtrado = com_maximo_dia.filter(
            (abs(polars.col("Nível (cm)") - mediana_nivel) <= 1000)
            | polars.col("Nível (cm)").is_null()
        )
        normalizado = nivel_filtrado.with_columns(
            (
                (polars.col("Nível (cm)") - polars.min("Nível (cm)"))
                / (polars.col("Nível (cm)").max() - polars.col("Nível (cm)").min())
            ).alias("Nível normalizado")
        )

        mediana_e_std_vazao = normalizado.select(
            polars.median("Vazão (m³/s)").alias("mediana"),
            polars.std("Vazão (m³/s)").alias("std"),
        )
        mediana_vazao = mediana_e_std_vazao[0, 0]
        std = mediana_e_std_vazao[0, 1]
        vazao_filtrada = normalizado.filter(
            (abs(polars.col("Vazão (m³/s)") - mediana_vazao) <= 3 * (std or 0))
            | polars.col("Vazão (m³/s)").is_null()
        )
        normalizado = vazao_filtrada.with_columns(
            (
                (polars.col("Vazão (m³/s)") - polars.min("Vazão (m³/s)"))
                / (polars.col("Vazão (m³/s)").max() - polars.col("Vazão (m³/s)").min())
            ).alias("Vazão normalizada")
        )

        return normalizado


def processar(
    pasta_estacoes: Path = Path("DADOS_ESTACOES/"),
    pasta_processados: Path | None = None,
    ignorar_fontes: set[str] = {},
):
    print("Processando dados baixados...   ", end="\n", flush=True)

    if pasta_processados is None:
        pasta_processados = Path("PROCESSADOS")
        pasta_processados.mkdir(exist_ok=True)

    # SNIRH
    if not "snirh" in ignorar_fontes:
        nomes_arquivos = set()
        print("\tProcessando arquivos SNIRH...   ", end="\n", flush=True)
        # Descobrir todos os tipos de arquivo que o snirh tem (Formato: {numero da estação}_{tipo do arquivo}.csv)
        for estacao in pasta_estacoes.iterdir():
            dir_snirh = estacao / "snirh/"
            if dir_snirh.exists():
                for arquivo in (estacao / "snirh/").iterdir():
                    nome_arquivo = re.sub("[0-9_]", "", arquivo.name)
                    nomes_arquivos.add(nome_arquivo)

        # Juntar todos os arquivos de mesmo tipo de todas as estaçãos em um só arquivo
        for nome in nomes_arquivos:
            print(f"\tPROCESSANDO ARQUIVO {nome}...            ", end="\r", flush=True)
            arquivo_juncao = pasta_processados / f"{nome}"
            linhas = []
            for estacao in pasta_estacoes.iterdir():
                arquivo = estacao / "snirh/" / f"{estacao.name}_{nome}"

                if arquivo.exists():
                    with arquivo.open(
                        mode="r", newline="", encoding="latin_1"
                    ) as arquivo:
                        reader = csv.reader(arquivo, delimiter=";")
                        for line in reader:
                            if len(line) > 0:
                                if line[0] == estacao.name or (
                                    len(linhas) == 0 and line[0] == "EstacaoCodigo"
                                ):
                                    linhas.append(line)
            linhas = sorted(linhas, key=lambda linha: sort_snirh(linha, 2))
            with arquivo_juncao.open(mode="w", newline="") as csvfile:
                writer = csv.writer(csvfile, delimiter=";")
                writer.writerows(linhas)

        print("\033[A\tProcessando arquivos SNIRH...   Pronto!")

    # SALA DE SITUAÇÃO
    if not "sala_de_situacao" in ignorar_fontes:
        pasta_sala_de_situacao = pasta_processados / "Sala de situação"
        pasta_sala_de_situacao.mkdir(exist_ok=True)

        print(" " * 20, "\r\tCarregando arquivos da sala de situações...   ")
        counter = 1
        total = len(list(pasta_estacoes.iterdir()))
        arquivos = (
            (tup[0], tup[1] / "sala_de_situacao.xlsx", total)
            for tup in enumerate(pasta_estacoes.iterdir())
        )
        with ProcessPoolExecutor() as executor:
            arquivos = filter(
                lambda i: i is not None, executor.map(abrir_sala_de_situacoes, arquivos)
            )
        print("\033[A\tCarregando arquivos da sala de situações...   Pronto!")

        print("\tOrdenando arquivos por data...   ", end="", flush=True)
        final = polars.concat(arquivos).sort("Data")
        print("Pronto!")
        print("\tSalvando em arquivo parquet...   ", end="", flush=True)
        final.write_parquet(pasta_sala_de_situacao / "sala_de_situacao.parquet")
        print("Pronto!")

    # INMET
    if not "inmet" in ignorar_fontes:
        pass

    print(" " * 100)


if __name__ == "__main__":
    import parse_arguments

    args = parse_arguments.parse_args()
    processar(ignorar_fontes=set(args.sem_fontes))
