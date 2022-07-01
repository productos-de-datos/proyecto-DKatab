"""
Construya un pipeline de Luigi que:
* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales
En luigi llame las funciones que ya creo.
"""

"""
Se implemente Luigi como orquestador de las tareas previamente definidas.
"""

import luigi
from luigi import Task, LocalTarget

class IngestarDato(Task):
    def output(self):
        return LocalTarget('../../data_lake/Importar_dato.txt')

    def run(self):
        from ingest_data import ingest_data
        with self.output().open('w') as archivos:
            ingest_data()


class TransformarDato(Task):
    def requires(self):
        return IngestarDato()

    def output(self):
        return LocalTarget('../../data_lake/Transformar_dato.txt')

    def run(self):
        from transform_data import transform_data
        with self.output().open('w') as archivos:
            transform_data()


class LimpiarDato(Task):
    def requires(self):
        return TransformarDato()

    def output(self):
        return LocalTarget('../../data_lake/Limpiar_dato.txt')

    def run(self):
        from clean_data import clean_data
        with self.output().open('w') as archivos:
            clean_data()


class ComputarPrecioDiario(Task):
    def requires(self):
        return LimpiarDato()

    def output(self):
        return LocalTarget('../../data_lake/Computar_Precio_diario.txt')

    def run(self):
        from compute_daily_prices import compute_daily_prices
        with self.output().open('w') as archivos:
            compute_daily_prices()


class ComputarPrecioMensual(Task):
    def requires(self):
        return ComputarPrecioDiario()

    def output(self):
        return LocalTarget('../../data_lake/Computar_Precio_Mensual.txt')

    def run(self):
        from compute_monthly_prices import compute_monthly_prices
        with self.output().open('w') as archivos:
            compute_monthly_prices()

if __name__ == "__main__":

    luigi.run(["ComputarPrecioMensual", "--local-scheduler"])
    
    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()