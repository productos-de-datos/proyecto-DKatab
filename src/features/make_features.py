def make_features():
    """Prepara datos para pronóstico.

    Cree el archivo data_lake/business/features/precios-diarios.csv. Este
    archivo contiene la información para pronosticar los precios diarios de la
    electricidad con base en los precios de los días pasados. Las columnas
    correspoden a las variables explicativas del modelo, y debe incluir,
    adicionalmente, la fecha del precio que se desea pronosticar y el precio
    que se desea pronosticar (variable dependiente).

    En la carpeta notebooks/ cree los notebooks de jupyter necesarios para
    analizar y determinar las variables explicativas del modelo.

    """
    ## A continuación se crea el archivo precios-diarios.csv el cual es almacenado en 
    # data_lake/business/features/precios-diarios.csv. Este archivo continue las variables explicativas 
    # del modelo con las cuales se pronosticarán los precios diarios de la electricidad 
    # teniendo en cuenta el histórico de precios. La explicación del modelo se encuentra almacenado en 
    # notebooks/Analisis.ipnyb

    import pandas as pd
    
    precios_diarios = pd.read_csv('data_lake/business/precios-diarios.csv')
    precios_diarios['Variable_Dependiente'] = 0
    for i in range(9393,9409):
        precios_diarios.iloc[i,2] = precios_diarios.iloc[i,1]
        precios_diarios.iloc[i,1] = 0

    precios_diarios.to_csv('data_lake/business/features/precios_diarios.csv', index = False, encoding='utf-8')

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_features()