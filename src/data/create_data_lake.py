""" 
Módulo de creación Data Lake. 
------------------------------------------------------------------------------- 
Se crea un Data Lake con sus carpetas y subcarpetas: 
 
     
    . 
    | 
    \___ data_lake/ 
         |_ landing/ 
         |_ raw/ 
         |_ cleansed/ 
         \___ business/ 
              |_ reports/ 
              |    |_ figures/ 
              |_ features/ 
              |_ forecasts/ 
 
     
 
""" 
import os 
 
 
def create_data_lake(): 
    try: 
        os.mkdir('./data_lake/') 
        parent_dir = 'data_lake/' 
        carpetas = ['landing', 'raw', 'cleansed', 'business'] 
        [os.mkdir(os.path.join(parent_dir, c)) for c in carpetas] 
        parent_dir = 'data_lake/business/' 
        carpetas = ['reports', 'features', 'forecasts'] 
        [os.mkdir(os.path.join(parent_dir, c)) for c in carpetas] 
        parent_dir = 'data_lake/business/reports/' 
        directory = 'figures' 
        os.mkdir(os.path.join(parent_dir, directory)) 
    except: 
        raise NotImplementedError("Implementar esta función") 
 
 
if __name__ == "__main__": 
 
    import doctest 
 
    doctest.testmod() 
create_data_lake()