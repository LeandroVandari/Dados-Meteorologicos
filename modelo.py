import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
import glob
import os

#Defina o caminho para a pasta onde estão os arquivos CSV
caminho_pasta = 'Dados'

def LerArquivos():

    global caminho_pasta

    # Use glob para listar todos os arquivos CSV na pasta
    arquivos_csv = glob.glob(os.path.join(caminho_pasta, "*.csv"))

    i=0
    dict_df={}
    for arquivos in arquivos_csv:
        
        nome= os.path.splitext(os.path.basename(arquivos))[0]
        
        i+=1
        dict_df.update({str(i):nome})
    
    # Retorna um dicionario com o nome dos arquivos
    return dict_df

dict_nomes=LerArquivos()

dict_df={}
df_final=pd.DataFrame()

# Carregar os arquivos inmet
for i in range(1,len(dict_nomes)+1):
    file = dict_nomes[str(i)]
    caminho_arquivo=caminho_pasta+"/"+file+".csv"

    print(caminho_arquivo)
    df = pd.read_csv(caminho_arquivo, delimiter=';', skiprows=10)
    
    for j in range(len(df)):
        a=str(df.at[j,"Hora Medicao"])

        if len(a)==1:
            df.at[j,"Hora Medicao"]=3*str(0)+str(a)

        elif len(a)==3:
            df.at[j,"Hora Medicao"]=str(0)+str(a)
    df["Hora Medicao"]=pd.to_datetime(df["Hora Medicao"],format="%H%M").dt.time
    df = df.drop(columns=df.columns[-1])
   
    df.insert(0,"indice",[(str(df.at[a,"Data Medicao"])+" "+str(df.at[a,"Hora Medicao"])) for a in range(len(df))])
    df = df.set_index('indice')
    
    df = df.dropna(axis=0, how="all")
    nomes_colunas = df.columns.tolist()
    for k in range(len(nomes_colunas)):
        df=df.rename(columns={nomes_colunas[k]:nomes_colunas[k]+str(i)})
    
    df_final=pd.concat([df_final,df],axis=1, ignore_index=False)
print(df_final)
df_final = df_final.dropna(axis=0, how="all")
df_final = df_final.dropna(axis=1, how="all")
df_final = df_final.dropna(axis=0)
df_final=df_final.drop(columns=df_final.columns[0])

nomes_colunas_final = df_final.columns.tolist()
features = nomes_colunas_final

df_cota = pd.read_csv('cota.csv', delimiter=';', skiprows=4)
df_cota = df_cota.drop(columns=df_cota.columns[-2:])
df_cota = df_cota.drop(df_cota.index[:4])
df_cota['Data'] = pd.to_datetime(df_cota['Data'], format='%d/%m/%Y %H:%M:%S')
df_cota['Data'] = df_cota['Data'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_cota = df_cota.set_index('Data')
df_cota=df_cota.dropna(axis=0)
df_junto = pd.merge(df_final, df_cota, left_index=True, right_index=True, how='inner')
print(df_junto)

X = df_junto.drop(columns=['Nível (cm)'])
print(X)
X.columns = X.columns.astype(str)
y = df_junto['Nível (cm)']

# Divisão dos dados em treino e teste (80% treino, 20% teste)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train = X_train.select_dtypes(exclude=['object', 'datetime'])
X_test = X_test.select_dtypes(exclude=['object', 'datetime'])
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
model = tf.keras.Sequential([
    tf.keras.layers.InputLayer(input_shape=(X_train_scaled.shape[1],)),
    tf.keras.layers.Dense(32, activation='relu', kernel_initializer='he_normal'),
    tf.keras.layers.Dense(16, activation='relu', kernel_initializer='he_normal'),
    tf.keras.layers.Dense(1)
])

# Compilar o modelo
model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.001), loss='mean_squared_error', metrics=['mae'])
# Treinar o modelo
history = model.fit(X_train_scaled, y_train_normalizado, epochs=20, batch_size=16, validation_split=0.2)

print(list(map(lambda a: a[0] * (y_max - y_min) + y_min, model.predict(X_test_scaled))), y_test_normalizado, y_test, sep="\n\n")

# Avaliar o modelo nos dados de teste
test_loss, test_mae = model.evaluate(X_test_scaled, y_test_normalizado)
print(f'Erro médio absoluto nos dados de teste: {test_mae:.2f}')
model.save("MODELO.keras")