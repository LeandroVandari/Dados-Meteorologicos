import polars as pl
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
import glob
import os

def LerArquivos(caminho_pasta):

    # Use glob para listar todos os arquivos CSV na pasta
    arquivos_csv = glob.glob(os.path.join(caminho_pasta, "*.csv"))

    i = 0
    dict_df = {}
    for arquivos in arquivos_csv:

        nome = os.path.splitext(os.path.basename(arquivos))[0]

        i += 1
        dict_df.update({str(i): nome})

    # Retorna um dicionario com o nome dos arquivos
    return dict_df

# Defina o caminho para a pasta onde estão os arquivos CSV
def treinar(caminho_pasta = "TESTE", dias_a_frente=0):
    dict_nomes = LerArquivos(caminho_pasta)


    def rename_cols(col: str) -> str:
        if col == "Data":
            return col
        return col + str(i)


    dict_df = {}
    df_final = pl.read_csv(
        caminho_pasta + "/" + dict_nomes["1"] + ".csv",
        separator=";",
        decimal_comma=True,
        skip_rows=10,
        null_values="null",
        schema_overrides={
            "Hora Medicao": str,
            "PRECIPITACAO TOTAL, HORARIO(mm)": pl.Float64,
            "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA(mB)": pl.Float64,
            "RADIACAO GLOBAL(Kj/m²)": pl.Float64,
            "TEMPERATURA DA CPU DA ESTACAO(°C)": pl.Float64,
            "UMIDADE REL. MIN. NA HORA ANT. (AUT)(%)": pl.Float64,
            "UMIDADE REL. MAX. NA HORA ANT. (AUT)(%)": pl.Float64,
            "VENTO, DIRECAO HORARIA (gr)(° (gr))": pl.Float64
        },
        try_parse_dates=False,
        low_memory=True
    )
    initial_schema = df_final.schema
    df_final = df_final.with_columns(
        pl.col("Hora Medicao").str.strptime(pl.Time, "%H%M"),
        pl.col("Data Medicao").str.strptime(pl.Date, "%Y-%m-%d"),
    )
    df_final = (
        df_final.drop(df_final.columns[-1])
        .with_columns(
            pl.col("Data Medicao").dt.combine(pl.col("Hora Medicao")).alias("Data")
        )
        .drop_nulls()
    )


    # Carregar os arquivos inmet
    for i in range(2, len(dict_nomes) + 1):
        file = dict_nomes[str(i)]
        caminho_arquivo = caminho_pasta + "/" + file + ".csv"

        print(caminho_arquivo)
        df = pl.read_csv(
            caminho_arquivo,
            separator=";",
            skip_rows=10,
            decimal_comma=True,
        schema=initial_schema,
            null_values="null",
            try_parse_dates=False,
            low_memory=True
        )
        df = df.with_columns(
            pl.col("Hora Medicao").str.strptime(pl.Time, "%H%M"),
            pl.col("Data Medicao").str.strptime(pl.Date, "%Y-%m-%d"),
        )
        df = df.drop(df.columns[-1])
        df = df.with_columns(
            pl.col("Data Medicao").dt.combine(pl.col("Hora Medicao")).alias("Data")
        )

        df = df.filter(~pl.all_horizontal(pl.all().is_null()))
        df = df[[s.name for s in df if not (s.null_count() == df.height)]]

        df = df.rename(rename_cols)

        df_final = df_final.join(df, "Data", coalesce=True, how="full")
    # filter rows where all values are null
    df_final = df_final.filter(~pl.all_horizontal(pl.all().is_null()))
    # filter columns where all values are null
    df_final = df_final[[s.name for s in df_final if not (s.null_count() == df_final.height)]]

    #df_final = df_final.drop_nulls()
    df_final = df_final.select(pl.all().exclude("Data Medicao"))
    nomes_colunas_final = df_final.columns
    features = nomes_colunas_final

    df_cota = pl.read_csv("cota.csv", separator=";", skip_rows=4, skip_rows_after_header=4, columns=["Data", "Nível (cm)"])
    df_cota = df_cota.with_columns(pl.col("Data").str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S")).drop_nulls()
    df_cota = df_cota.with_columns(pl.col("Data") - pl.duration(days=int(dias_a_frente)))
    df_final = df_final.sort("Data")
    cota = df_cota.sort("Data")
    df_junto = df_final.join_asof(cota, on="Data", tolerance="1h").drop_nulls()
    X= df_junto.select(pl.all().exclude(["Nível (cm)", "Data", "Hora Medicao"]))
    y = df_junto.get_column("Nível (cm)")

    # Divisão dos dados em treino e teste (80% treino, 20% teste)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Normalizar as features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    y_min = y_train.min()
    y_max = y_train.max()
    epsilon = 1e-8  # A small constant to prevent division by zero

    y_train_normalizado = (y_train - y_min) / (y_max - y_min + epsilon)
    y_test_normalizado = (y_test - y_min) / (y_max - y_min + epsilon)


    # Construir o modelo da rede neural
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(X_train_scaled.shape[1],)),
            tf.keras.layers.Dense(32, activation="relu", kernel_initializer="he_normal"),
            tf.keras.layers.Dense(16, activation="relu", kernel_initializer="he_normal"),
            tf.keras.layers.Dense(1),
        ]
    )

    # Compilar o modelo
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
        loss="mean_squared_error",
        metrics=["mae"],
    )
    # Treinar o modelo
    history = model.fit(
        X_train_scaled, y_train_normalizado, epochs=20, batch_size=16, validation_split=0.2
    )

    #print(list(map(lambda a: a[0] * (y_max - y_min) + y_min, model.predict(X_test_scaled))), y_test_normalizado, y_test, sep="\n\n")

    # Avaliar o modelo nos dados de teste
    test_loss, test_mae = model.evaluate(X_test_scaled, y_test_normalizado)
    print(f"Erro médio absoluto nos dados de teste: {test_mae:.2f}")
    return model

if __name__ == "__main__":
    model = treinar()
    model.save("MODELO.keras")