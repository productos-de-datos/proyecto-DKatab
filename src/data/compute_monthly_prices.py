from pandas import to_datetime


def compute_monthly_prices():
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
    import pandas as pd

    datos = pd.read_csv('../../data_lake/cleansed/precios-horarios.csv')
    datos["Fecha"] = pd.to_datetime(datos["Fecha"], format='%Y-%m-%d').dt.to_period("M").dt.to_timestamp()
    df = datos.groupby(by="Fecha",as_index=False).agg({"Precio":"mean"})
    df.to_csv('../../data_lake/business/precios-mensuales.csv', encoding='utf-8', index=False)

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    compute_monthly_prices()
