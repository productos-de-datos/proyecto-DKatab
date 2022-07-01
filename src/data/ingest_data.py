"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------

"""


def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.

    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.

    """
    # De la URL datasets/precio_bolsa_nacional/xls obtenemos la información de 27 archivos en excel con el cual 
    # realizaremos la ingesta de datos. Esta información se depositará en la carpta landing que creamos en el 
    # anterior.
    
     
    import requests as req

    for year in range(1995, 2022):
        if year in range(2016, 2018):
            url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xls?raw=true'.format(year)
            file = req.get(url, allow_redirects=True)
            open('data_lake/landing/{}.xls'.format(year), 'wb').write(file.content)
        else:
            url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true'.format(year)
            file = req.get(url, allow_redirects=True)
            open('data_lake/landing/{}.xlsx'.format(year), 'wb').write(file.content)

    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()

    ingest_data()