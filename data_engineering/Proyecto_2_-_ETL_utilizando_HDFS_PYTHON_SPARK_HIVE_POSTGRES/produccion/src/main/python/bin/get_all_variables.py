import os

# Establecer variables de entorno
os.environ['environ'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'admin'
os.environ['password'] = 'admin'

# Obtener variables de entorno
environ = os.environ['environ']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

# Establecer otras variables
appName = "USA Prescriber Research Report"

##################################### RUTAS utilizadas en PRODUCCION en HDFS #####################################

staging_dim_city = '/proyectos/PrescPipeline/staging/dimension_city'

staging_fact = '/proyectos/PrescPipeline/staging/fact'

output_city = '/proyectos/PrescPipeline/output/dimension_city'

output_fact = '/proyectos/PrescPipeline/output/presc'

##################################################################################################################

