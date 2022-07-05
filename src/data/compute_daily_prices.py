def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



    """
     ## A continuación se realiza el calculo de los precios promedios diarios utilizando la data que se 
    ## encuentra en data_lake/cleansed/precios-horarios.csv

    import pandas as pd

    datos = pd.read_csv('data_lake/cleansed/precios-horarios.csv')
    df = datos.groupby(by="fecha",as_index=False).agg({"precio":"mean"})
    df.to_csv('data_lake/business/precios-diarios.csv', encoding='utf-8', index=False)

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    compute_daily_prices()