def make_forecasts():
    """Construya los pronosticos con el modelo entrenado final.

    Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:

    * La fecha.

    * El precio promedio real de la electricidad.

    * El pronóstico del precio promedio real.


    """
    import pandas as pd
    from sklearn.metrics import r2_score
    import pickle

    df = pd.read_csv('../../data_lake/business/features/precios-diarios.csv', index_col=None)
    df['Fecha'] = pd.to_datetime(df['Fecha'], format= '%Y-%m-%d')
    df['year'] = df['Fecha'].dt.year
    df['month'] = df['Fecha'].dt.month
    df['day'] = df['Fecha'].dt.day

    df.dropna(inplace=True)

    x_complete = df.copy().drop(columns = 'Fecha')
    y_complete = x_complete.pop('Precio')

    regression = pickle.load(open('../models/precios-diarios.pkl', 'rb'))
    prediction = regression.predict(x_complete)

    r2_score(y_complete,regression.predict(x_complete))

    df['Prediction'] = prediction

    df[['Fecha', 'Precio', 'Prediction']].to_csv(
        '../../data_lake/business/forecasts/precios-diarios.csv', index=None)

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_forecasts()