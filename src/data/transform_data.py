def transform_data():
    """Transforme los archivos xls a csv.
    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.
    """
   #La siguiente función permite transformar la data depositada en la carpeta landig a csv

    import pandas as pd

    for year in range(1995, 2022):
        if year in range(2016, 2018):
            busqueda = 'data_lake/landing/{}.xls'.format(year)
            destino = 'data_lake/raw/{}.csv'.format(year)
            datos_xls = pd.read_excel(busqueda, index_col=None, header=None)
            datos_xls.to_csv(destino, encoding='utf-8', index=False, header=False)
        else:
            busqueda = 'data_lake/landing/{}.xlsx'.format(year)
            destino = 'data_lake/raw/{}.csv'.format(year)
            datos_xls = pd.read_excel(busqueda, index_col=None, header=None)
            datos_xls.to_csv(destino, encoding='utf-8', index=False, header=False)

    
    #raise NotImplementedError("Implementar esta función")

def test_answer():
    import os

    assert os.path.isfile("data_lake/raw/1995.csv") is True
    assert os.path.isfile("data_lake/raw/1996.csv") is True
    assert os.path.isfile("data_lake/raw/1997.csv") is True
    assert os.path.isfile("data_lake/raw/1998.csv") is True
    assert os.path.isfile("data_lake/raw/1999.csv") is True
    assert os.path.isfile("data_lake/raw/2000.csv") is True
    assert os.path.isfile("data_lake/raw/2001.csv") is True
    assert os.path.isfile("data_lake/raw/2002.csv") is True
    assert os.path.isfile("data_lake/raw/2003.csv") is True
    assert os.path.isfile("data_lake/raw/2004.csv") is True
    assert os.path.isfile("data_lake/raw/2005.csv") is True
    assert os.path.isfile("data_lake/raw/2006.csv") is True
    assert os.path.isfile("data_lake/raw/2007.csv") is True
    assert os.path.isfile("data_lake/raw/2008.csv") is True
    assert os.path.isfile("data_lake/raw/2009.csv") is True
    assert os.path.isfile("data_lake/raw/2010.csv") is True
    assert os.path.isfile("data_lake/raw/2011.csv") is True
    assert os.path.isfile("data_lake/raw/2012.csv") is True
    assert os.path.isfile("data_lake/raw/2013.csv") is True
    assert os.path.isfile("data_lake/raw/2014.csv") is True
    assert os.path.isfile("data_lake/raw/2015.csv") is True
    assert os.path.isfile("data_lake/raw/2016.csv") is True
    assert os.path.isfile("data_lake/raw/2017.csv") is True
    assert os.path.isfile("data_lake/raw/2018.csv") is True
    assert os.path.isfile("data_lake/raw/2019.csv") is True
    assert os.path.isfile("data_lake/raw/2020.csv") is True
    assert os.path.isfile("data_lake/raw/2021.csv") is True

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    transform_data()