"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.


"""
import luigi
from luigi import Task, LocalTarget


class ProcessOrders(Task):
    def output(self):
        return LocalTarget('orders.csv')

    def run(self):
        with self.output().open('w') as file:
            print('May,100', file=file)
            print('May,100', file=file)
            print('Jun,200', file=file)
            print('Jun,150', file=file)

class GenerateReport(Task):
    def requires(self):
        #
        # Dependencia de la tarea anterior
        #
        return ProcessOrders()


    def output(self):
        return LocalTarget('report.csv')

    def run(self):
        report = {}
        for line in self.input().open():
            month, amount=line.split(',')
            if month in report:
                report[month] += float(amount)
            else:
                report[month] = float(amount)

        with self.output().open('w') as file:
            for month in report:
                print(month+',' + str(report[month]), file=file)



if __name__ == '__main__':
    luigi.run(["GenerateReport", "--local-scheduler"])




if __name__ == "__main__":

    raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()
