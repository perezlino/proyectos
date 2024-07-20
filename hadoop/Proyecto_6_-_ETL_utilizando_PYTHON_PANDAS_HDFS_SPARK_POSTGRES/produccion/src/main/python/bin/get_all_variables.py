import os

# Set Environment Variables
os.environ['environ'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'airflow'
os.environ['password'] = 'airflow'

# Get Environment Variables
environ = os.environ['environ']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

# Set Other Variables
appName = "USA Prescriber Research Report"

# La variable "current_path" tomará el valor del "Directorio" desde donde se encuentra el
# archivo "run_presc_pipeline.py", porque desde ahi se llamará a esta variable. Entonces
# current_path seria igual a la ruta:
# current_path = "C:\PySpark_Installed\PyCharm_Community_Edition_2024.1.4\Proyectos\PrescriberAnalytics\src\main\python\bin"
current_path = os.getcwd()

##################################### RUTAS utilizadas como TEST en PYCHARM #####################################

# Como estamos llamando a esta variable desde el archivo "run_presc_pipeline.py",
# current_path toma el valor de la ruta antes indicada. Es por esto que se le agrega el '\..\'
# para que retroceda un directorio hacia atrás. Por tanto, la variable "staging_dim_city" es igual a:
# staging_city_dim = "C:\PySpark_Installed\PyCharm_Community_Edition_2024.1.4\Proyectos\PrescriberAnalytics\src\main\python\staging\dimension_city"

# staging_dim_city = current_path + r'\..\staging\dimension_city'

# Y la variable "staging_fact" queda de la siguiente manera:
# staging_fact = "C:\PySpark_Installed\PyCharm_Community_Edition_2024.1.4\Proyectos\PrescriberAnalytics\src\main\python\staging\fact"

# staging_fact = current_path + r'\..\staging\fact'

##################################################################################################################

##################################### RUTAS utilizadas en PRODUCCION en HDFS #####################################

staging_dim_city = '/proyectos/PrescPipeline/staging/dimension_city'

staging_fact = '/proyectos/PrescPipeline/staging/fact'

output_city = '/proyectos/PrescPipeline/output/dimension_city'

output_fact = '/proyectos/PrescPipeline/output/presc'

##################################################################################################################

