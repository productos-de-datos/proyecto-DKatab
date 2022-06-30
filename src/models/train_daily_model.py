from pandas import to_pickle


def train_daily_model():
    """Entrena el modelo de pronóstico de precios diarios.

    Con las features entrene el modelo de proóstico de precios diarios y
    salvelo en models/precios-diarios.pkl


    """
    from cgi import test
    import pandas as pd
    from sklearn import linear_model
    import pickle
   



    # Lea el archivo `insurance.csv` y asignelo al DataFrame `df`
    df = pd.read_csv('../../data_lake/business/features/precios-diarios.csv')
    df = df.dropna()

    # Asigne la columna `charges` a la variable `y`.
    y = df['Precio']
    X = df[['Diferencia_precio','Dia_semana','Dia_mes','Precio_ayer']]

    regr = linear_model.LinearRegression()
    regr.fit(X, y)
    pickle.dump(regr, open('../models/precios-diarios.pkl','wb'))


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    train_daily_model()