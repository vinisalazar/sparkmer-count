from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkConf
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from operator import add
from functools import reduce
from bio_spark.io.fasta_reader import FASTAReade
import collections
import numpy as np

def seq2kmer(seq_):
    value = seq_[0].strip()    
    k = 3
    num_kmers = len(value) - k + 1
    kmers_list = [value[n*k:k*(n+1)] for n in range(0, num_kmers)]
    
    # return len(value)
    return kmers_list

seq2kmer_udf = udf(seq2kmer)

def kmers_list2kmers_freq_dict(kmers_list):
    kmers_list_np = np.array(kmers_list)
    flatten_data = kmers_list_np.flatten()
    unique, counts = np.unique(flatten_data, return_counts=True)
    kmers_list = list(zip(unique.tolist(), counts.tolist()))
    return kmers_list

kmers_list2kmers_freq_dict_udf = udf(kmers_list2kmers_freq_dict)

if __name__ == "__main__":
    sConf = SparkConf("spark://localhost:7077")
    sc = SparkContext(conf=sConf)
    spark = SparkSession(sc)

    plain_df = sc.textFile("/home/thiago/Dados/sparkAAI-1/data/SP1.fq").map(lambda x: Row(row=x)).toDF()
 
    reader = FASTAReade(sc)
    parsedDF = reader.read(plain_df)
    parsedDF.coalesce(4)

    kmers_of_seqs_df = parsedDF\
            .withColumn("kmers", seq2kmer_udf("seq"))\

    print(kmers_of_seqs_df.show())

    grouped_by_seq_df = kmers_of_seqs_df\
            .groupby("seqID")\
            .agg(F.collect_list('kmers').alias('kmers_list'))\
            .withColumn('kmers_freq', kmers_list2kmers_freq_dict_udf('kmers_list'))
            # .count()
            # .agg(F.count)

    result = grouped_by_seq_df.select("kmers_freq").take(2)
    print(result[0][0])
    # print(grouped_by_seq_df.show())
    