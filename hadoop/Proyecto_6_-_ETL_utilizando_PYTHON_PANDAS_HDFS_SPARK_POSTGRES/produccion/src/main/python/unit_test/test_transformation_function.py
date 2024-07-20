import unittest
import os, sys
sys.path.insert(0, "C:\PySpark_Installed\PyCharm_Community_Edition_2021.2.1\Projects\PrescPipeline\src\main\python\\bin")
from presc_run_data_transform import city_report
from pyspark.sql import SparkSession


# Define a class
class TransformTest(unittest.TestCase):
    def test_city_report_zip_trx_count_check(self):

        #Create a Spark Object
        spark = SparkSession \
            .builder \
            .master('local') \
            .appName('Testing City Report Function for Zip Counts and trx counts') \
            .getOrCreate()

        df_city_sel=spark.read.load('city_data_riverside.csv',format='csv',header=True)
        df_fact_sel=spark.read.load('USA_Presc_Medicare_Data_2021.csv',format='csv',header=True)
        df_city_sel.show(truncate=False)
        df_fact_sel.show(truncate=False)

        # Call the City Report Function
        df_city_final = city_report(df_city_sel,df_fact_sel)
        df_city_final.show(truncate=False)

        #Extract the zip count and trx count
        zip_cnt=df_city_final.select("zip_counts").first().zip_counts
        trx_cnt=df_city_final.select("trx_counts").first().trx_counts

        # Calculate the Zip count and Trx count Manually from the files
            # Zip Count: 14
            # Trx Cnt: 45

        # Perform Unit Testing
        self.assertEqual([14,45],[zip_cnt,trx_cnt])

if __name__ == '__main__':
    unittest.main()

