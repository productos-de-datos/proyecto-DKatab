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
    import pandas as pd

    df = pd.read_csv('../../data_lake/business/precios-diarios.csv')
    df["Fecha"] = pd.to_datetime(df["Fecha"], format='%Y-%m-%d')
    df["Diferencia_precio"] = df["Precio"] - df["Precio"].shift(1)
    df["Dia_semana"] = df["Fecha"].dt.dayofweek
    df["Dia_mes"] = df["Fecha"].dt.day
    df["Precio_ayer"] = df["Precio"].shift(1)

    df.to_csv('../../data_lake/business/features/precios-diarios.csv', encoding='utf-8', index=False)

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_features()