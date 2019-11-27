from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkConf
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, asc

from bio_spark.io.fasta_reader import FASTAReade

if __name__ == "__main__":
    sConf = SparkConf("spark://localhost:7077")
    sc = SparkContext(conf=sConf)
    spark = SparkSession(sc)

    plain_df = sc.textFile("../data/SP1.fq").map(lambda x: Row(row=x)).toDF()
 
    reader = FASTAReade(sc)
    parsedDF = reader.read(plain_df)
    print(parsedDF.show(2))

