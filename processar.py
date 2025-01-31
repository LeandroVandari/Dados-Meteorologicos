from pathlib import Path
import re
import csv
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import polars
import altair
import time
import itertools
import gc
import multiprocessing as mp

altair.renderers.enable("browser")
altair.data_transformers.enable("vegafusion")


def sort_snirh(line, date_index):
    if line[0] == "EstacaoCodigo" or line[date_index] == "":
        return datetime(1, 1, 1, 0, 0)
    return datetime.strptime(line[date_index], "%d/%m/%Y")


def abrir_sala_de_situacoes(arquivo: (int, Path, int)) -> polars.DataFrame | None:
    idx, arquivo, total = arquivo
    print(f"\t\tCARREGANDO: {idx}/{total}    ", end="\r")
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


def processar_inmet(enumeracao):
    idx, estacao, total = enumeracao
    with open(estacao, "r", encoding="iso-8859-1") as f:
        primeira_parte = f.read(1000)
        dados = dict(
            map(
                lambda linha: linha.split(";"),
                itertools.islice(primeira_parte.split("\n"), 8),
            )
        )
        if dados["UF:"] != "RS":
            return None
        codigo_estacao = dados["CODIGO (WMO):"]
        print(f"\t\tProcessando estação {codigo_estacao}...  ({idx}/{total})", end="\r")
    df = polars.read_csv(
        estacao,
        separator=";",
        decimal_comma=True,
        encoding="iso-8859-1",
        skip_rows=8,
        null_values="-9999",
        schema_overrides={
            "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": polars.Float64,
            "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": polars.Float64,
            "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": polars.Float64,
            "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": polars.Float64,
            "RADIACAO GLOBAL (KJ/m²)": polars.Float64,
            "RADIACAO GLOBAL (Kj/m²)": polars.Float64,
            "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": polars.Float64,
            "TEMPERATURA DO PONTO DE ORVALHO (°C)": polars.Float64,
            "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": polars.Float64,
            "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": polars.Float64,
            "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": polars.Float64,
            "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": polars.Float64,
            "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": polars.Float64,
            "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": polars.Float64,
            "UMIDADE RELATIVA DO AR, HORARIA (%)": polars.Float64,
            "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": polars.Float64,
            "VENTO, RAJADA MAXIMA (m/s)": polars.Float64,
            "VENTO, VELOCIDADE HORARIA (m/s)": polars.Float64,
        },
    )
    try:
        nome_dia, nome_hora = "DATA (YYYY-MM-DD)", "HORA (UTC)"
        df = df.with_columns(
            polars.col(nome_dia).str.strptime(polars.Date, "%Y-%m-%d").alias("Dia"),
            polars.col(nome_hora).str.strptime(polars.Time, "%H:%M").alias("Horario"),
        )
    except:
        nome_dia, nome_hora = "Data", "Hora UTC"
        df = df.with_columns(
            polars.col(nome_dia).str.strptime(polars.Date, "%Y/%m/%d").alias("Dia"),
            polars.col(nome_hora)
            .str.strptime(polars.Time, "%H%M UTC")
            .alias("Horario"),
        )
        if df.get_column("RADIACAO GLOBAL (Kj/m²)", default=None) is not None:
            df = df.rename({"RADIACAO GLOBAL (Kj/m²)": "RADIACAO GLOBAL (KJ/m²)"})

    df = df.with_columns(
        polars.col("Dia").dt.combine(polars.col("Horario")).alias("Timestamp"),
        CodigoEstacao=polars.lit(codigo_estacao),
    )
    df = df.drop([nome_dia, nome_hora, "Dia", "Horario", ""]).select(
        [polars.col("Timestamp").alias("Data"), polars.all().exclude("Timestamp")]
    )
    if df.get_column("Data")[0].year < 2023:
        return None
    df = df.drop_nulls()
    return (codigo_estacao, df)


def processar(
    pasta_estacoes: Path = Path("DADOS_ESTACOES/"),
    pasta_processados: Path | None = None,
    ignorar_fontes: list[str] = [],
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

        print("\t\tOrdenando arquivos por data...   ", end="", flush=True)
        final = polars.concat(arquivos).sort("Data")
        print("Pronto!")
        print("\t\tSalvando em arquivo parquet...   ", end="", flush=True)
        final.write_parquet(pasta_sala_de_situacao / "sala_de_situacao.parquet")
        print("Pronto!")

        del final
        del arquivos
        gc.collect()

    # INMET
    if not "inmet" in ignorar_fontes:
        pasta_inmet = pasta_processados / "INMET"
        pasta_inmet.mkdir(exist_ok=True)
        print(" " * 20, "\r\tCarregando arquivos do INMET...   ")
        arquivos = [
            arquivo
            for ano in (pasta_estacoes / "INMET").iterdir()
            for arquivo in ano.iterdir()
        ]
        total = len(arquivos)
        arquivos = [(tup[0], tup[1], total) for tup in enumerate(arquivos)]
        with ProcessPoolExecutor() as executor:
            dfs = filter(
                lambda i: i is not None, executor.map(processar_inmet, arquivos)
            )
        dfs = [*dfs]
        dfs_final = []
        indices_foi = []
        def rename_columns(col: str, i: int) -> str:
            if col == "Data":
                return col
            return col + str(i)
        for (i, (codigo, df)) in enumerate(dfs):
            print(i, codigo)
            for (j, (codigo_outra, df_outra)) in enumerate(dfs):
                if i != j and codigo == codigo_outra and i not in indices_foi and j not in indices_foi:
                    indices_foi.extend([i, j])
                    new = df.extend(df_outra)
                    new = new.rename(lambda col: rename_columns(col, i))
                    dfs_final.append(new)
            if not i in indices_foi:
                df = df.rename(lambda col: rename_columns(col, i))
                dfs_final.append(df)
                indices_foi.append(i)
        print(indices_foi)
        dfs = dfs_final
        print("\033[A\tCarregando arquivos do INMET...   Pronto!")
        df_final = dfs[0].lazy()
        print("\tOrdenando arquivos por data...    ", end="", flush=True)
        for (i, df) in enumerate(dfs):
            if df.is_empty() or i == 0:
                continue
            print(i, end="\r")

            df_final = df_final.join(df.lazy(), "Data", coalesce=True, how="full")
        df_final = df_final.collect()
        print(df_final.schema)
        print("Pronto!")
        print("\tSalvando em arquivo parquet...   ", end="", flush=True)

        df_final.write_parquet(pasta_inmet / "inmet.parquet")
        df_final.write_csv(pasta_inmet / "inmet.csv")
        print("Pronto!")

    print(" " * 100)


if __name__ == "__main__":
    import parse_arguments

    args = parse_arguments.parse_args()
    processar(ignorar_fontes=set(args.sem_fontes))
