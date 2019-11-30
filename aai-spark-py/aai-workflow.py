from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkConf
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, asc
from bio_spark.io.fasta_reader import FASTAReade

def seq2kmer(id_, seq_, k):
    num_kmers = len(seq_) - k + 1
    kmers_list = [(id_, seq_[n*k:k*(n+1)]) for n in range(0, num_kmers)]
    return kmers_list

if __name__ == "__main__":
    sConf = SparkConf("spark://localhost:7077")
    sc = SparkContext(conf=sConf)
    spark = SparkSession(sc)

    plain_df = sc.textFile("../data/SP1.fq").map(lambda x: Row(row=x)).toDF()
 
    reader = FASTAReade(sc)
    parsedDF = reader.read(plain_df)
    parsedDF.coalesce(4)

    grouped_by_seq_rdd = parsedDF.groupby("seqID")

    ## finding kmers
    print(type(parsedDF.rdd))
    kmers_rdd = parsedDF.select("seqID", "seq").rdd\
                .flatMap(lambda c: seq2kmer(c.seqID[0], c.seq[0],3))                

    print(kmers_rdd.toDF().show())  
    reduced_rdd = kmers_rdd.reduce(lambda   )

    print(reduced_rdd.toDF().show())  
    # print(kmers_rdd.take(5))

