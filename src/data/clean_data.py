def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.
    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:
    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional
    Este archivo contiene toda la información del 1997 a 2021.
    """
    ## A continuación se realiza la limpieza de los datos de los archivos que se encuentran en 
    ## data_lake/raw/*.csv con el fin que todos estos archivos contengan las columnas fecha, hora, precio para 
    ## los años de 1997 al 2021

    import pandas as pd

    dfLista = list()
    for year in range(1995, 2022): 
        #Limpiar archivos de 1995 a 2000    
        if year in range(1995, 2000): 
            dato_csv = pd.read_csv('data_lake/raw/{}.csv'.format(year), header=None)
            dato_year = dato_csv.iloc[4:,:]
            dfLista.append(dato_year)
        #Limpiar datos del 2017 y 2018 
        elif year in range(2000, 2018): 
            dato_csv = pd.read_csv('data_lake/raw/{}.csv'.format(year), header=None)
            dato_year = dato_csv.iloc[3:,:]
            dfLista.append(dato_year)
         #Limpiar los demas archivos
        elif year in range(2018, 2022):
            dato_csv = pd.read_csv('data_lake/raw/{}.csv'.format(year), header=None)
            dato_year = dato_csv.iloc[3:,:]
            dfLista.append(dato_year)
    
    df = pd.concat(dfLista, ignore_index=True, axis=0)
    
    df = df.iloc[:, 0:25]

    df.rename(columns={0:'fecha', 1:'00', 2:'01', 3:'02', 4:'03', 5:'04', 6:'05', 7:'06', 
                        8:'07', 9:'08', 10:'09', 11:'10', 12:'11', 13:'12', 14:'13', 15:'14', 16:'15',
                        17:'16', 18:'17', 19:'18', 20:'19', 21:'20', 22:'21', 23:'22', 24:'23'},
               inplace=True)

    df = pd.melt(df, id_vars=['fecha'], value_vars=[
        '00',
        '01',
        '02',
        '03',
        '04',
        '05',
        '06',
        '07',
        '08',
        '09',
        '10',
        '11',
        '12',
        '13',
        '14',
        '15',
        '16',
        '17',
        '18',
        '19',
        '20',
        '21',
        '22',
        '23'
    ])
    
    df = df.where(df.notna(), 0, axis=0)
    df = df[df['fecha'] != 0]

    df = df.rename(columns={'fecha':'fecha', 'variable':'hora', 'value':'precio'})

    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')
    df['precio'] = df['precio'].astype(float)

    df.to_csv('data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=False)


    #raise NotImplementedError("Implementar esta función")



if __name__ == "__main__":
    import doctest

    doctest.testmod()

    clean_data()