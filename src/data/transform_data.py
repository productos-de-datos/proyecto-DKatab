from sqlalchemy import column


def transform_data():
    """Transforme los archivos xls a csv.
    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.
    """
   #La siguiente función permite transformar la data depositada en la carpeta landig a csv

    import pandas as pd

    

    for num in range(1995, 2022):
        if num < 2000:
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=None, header=3)
        elif num in (2016, 2017):
            data_xls = pd.read_excel('data_lake/landing/{}.xls'.format(num), index_col=None, header=2)
        elif num >= 2018:
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=None)
        else:
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=None, header=2)
        
        data_xls = data_xls.iloc[:,0:25]

        data_xls.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8', index=False)

 

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    
    transform_data()