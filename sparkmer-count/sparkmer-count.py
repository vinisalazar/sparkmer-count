import sys
import collections
import argparse
import time
import datetime
import numpy as np
import logging
from functools import reduce
from operator import add
from pathlib import Path
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession, Window, types
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from bio_spark.io.fasta_reader import FASTAReader


"""
Use the following functions like:

start = get_time()
<code block>
end = get_time()
delta()
"""


def get_time():
    return time.time()


def delta(start):
    end = get_time()
    delta = str(datetime.timedelta(seconds=end - start))
    print(f"Took {delta}.\n")


def stablish_spark_connection():
    sConf = SparkConf("spark://localhost:7077")
    sc = SparkContext(conf=sConf)
    spark = SparkSession(sc)

    return sConf, sc, spark


def fasta_plain_df():
    fasta_plain_df = (
        sc.textFile(",".join(files_to_process))
        .map(lambda x: Row(row=x))
        .zipWithIndex()
        .toDF(["row", "idx"])
    )
    return fasta_plain_df, fasta_plain_df.count()


def seq2kmer(seq_, k):
    value = seq_[0].strip()
    num_kmers = len(value) - k + 1
    kmers_list = [value[n * k : k * (n + 1)] for n in range(0, num_kmers)]
    # return len(value)
    return kmers_list


def parser_fasta_id_line(line):
    """
    Desejamos extrair os IDs das sequências da linhas que começarem pelo caracter ''>'. Pelo padrão
    FASTA, o ID é a primeira palavra e é um campo composto por ID.CONTIG

    Input:
        line: Uma linha de um arquivo FASTA
    Return:
        ID: da sequência ignorando o número de contigs, ou None caso não seja uma linha de ID
    """
    if line[0][0] == ">":
        header_splits = line[0][1:].split(" ")[0]
        seq_id_split = header_splits.split(".")
        return seq_id_split[0]
    else:
        return


def kmers_list2kmers_freq_dict(kmers_list):
    """
    Cálcula as frequências absolutas de cada kmer no dataframe
    Retorna:
        Um onjeto map("kmer" -> número de ocorrências ) para cada sequência
    """
    unique, counts = np.unique(kmers_list[0], return_counts=True)
    kmers_map = {str(k): int(v) for k, v in zip(unique, counts) if k}
    return kmers_map


if __name__ == "__main__":
    # Start counting time
    global_start, kmer_start = get_time(), get_time()

    # Argument block
    parser = argparse.ArgumentParser(description="Script to run kmer count with Spark.")
    parser.add_argument(
        "-i",
        "--input",
        help="Input directory containing genome contigs file.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output file with kmer counts. Default is kmer_count_out.txt",
        default="kmer_count_out.txt",
    )
    parser.add_argument(
        "-k", "--kmer", help="Size of kmer. Default is 3", default=3, type=int
    )
    args = parser.parse_args()

    # Connection block
    start_conn = get_time()
    print("### STEP 0: CONNECTING TO SPARK CLUSTER ###\n")
    sConf, sc, spark = stablish_spark_connection()
    print("Done!")
    delta_ = delta(start_conn)

    """
    Step 1: Kmer counting with Spark
        1.1 Input FASTA into dataframes
        1.2 Dataframe processing
        1.3 Kmer counting
        1.4 Sequence analysis
        1.5 Writing to disc

    """

    print("### STARTING STEP 1: KMER COUNTING ###\n")

    # 1.1 FASTA input
    start_input = get_time()

    input_dir_path = Path(args.input)
    files_to_process = [str(f) for f in input_dir_path.iterdir()]

    print(f"Your input directory is {input_dir_path}.")
    print(f"You have {len(files_to_process)} files to process.\n")
    print("Creating initial FASTA dataframe...\n")

    fasta_plain_df, count = fasta_plain_df()
    print(f"Done! You have {count} raw lines to process.")

    delta_ = delta(start_input)

    # 1.2 Dataframe processing
    start_process = get_time()

    print("Starting Spark dataframe processing.")
    seq2kmer_udf = F.udf(parser_fasta_id_line, types.StringType())
    fasta_null_ids_df = fasta_plain_df.withColumn("seqID_wNull", seq2kmer_udf("row"))
    num_ids = fasta_null_ids_df.where(F.col("seqID_wNull").isNotNull()).count()

    print(f"You have {num_ids} sequences to process.")
    fasta_n_filter_df = fasta_null_ids_df.withColumn(
        "seqID",
        F.last("seqID_wNull", ignorenulls=True).over(
            Window.orderBy("idx").rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )
        ),
    )
    fasta_df = (
        fasta_n_filter_df.where(F.col("seqID_wNull").isNull())
        .select("seqID", "row")
        .toDF("seqID", "seq")
    )

    print("Your sequence df has the following schema:\n")
    fasta_df.printSchema()

    delta_ = delta(start_process)

    # 1.3 Kmer counts
    start_kmer = get_time()

    print("Starting k-mer count.")
    k = args.kmer
    seq2kmerT = types.ArrayType(types.StringType())
    seq2kmer_udf = F.udf(lambda x: seq2kmer(x, k), seq2kmerT)
    fasta_kmers_df = fasta_df.withColumn("kmers", seq2kmer_udf("seq"))

    print("Your kmers df has the following schema:\n")
    fasta_kmers_df.printSchema()
    n_kmers_df = fasta_kmers_df.withColumn("n_kmers", F.size(F.col("kmers"))).select(
        "n_kmers"
    )
    n_kmers_df.describe().show()

    delta_ = delta(start_kmer)

    # 1.4 Sequence analysis
    start_seqs = get_time()

    print("Starting sequence processing.")
    KmerFreqTuple = types.MapType(types.StringType(), types.IntegerType())
    kmers_list2kmers_freq_dict_udf = F.udf(kmers_list2kmers_freq_dict)
    kmers_profile_df = (
        fasta_kmers_df.groupby("seqID")
        .agg(F.collect_list("kmers").alias("kmers_list"))
        .withColumn("kmers_freq", kmers_list2kmers_freq_dict_udf("kmers_list"))
    )

    print("Your kmer profile df has the following schema:\n")
    kmers_profile_df.printSchema()
    print(f"Writing profile to {args.output}")
    kmers_profile_df.toPandas().to_csv(args.output)

    # 1.5 Writing kmers profile to disc
    print("Creating kmers profile df.")
    kmers_profile_df = (
        fasta_kmers_df.rdd.map(lambda r: (r.seqID, r.kmers))
        .reduceByKey(lambda x, y: x + y)
        .toDF(["seqID", "kmers_list"])
    )
    kmers_profile_df.show()

    delta_ = delta(start_seqs)

    print(f"Finished step 1! Wrote dataframe with kmer profile to {args.output}")
    delta_ = delta(kmer_start)

    print("### STARTING STEP 2: CLUSTERING AND CROSS-VALIDATION ###")

    """
    Step 2: Machine learning
        2.1 Feature extraction
        2.2 Clustering
    """

    ml_start = get_time()

    # 2.1 Feature extraction
    start_features = get_time()

    print(
        "Starting feature extraction from kmers with CountVectorizer.",
        f"Feature space = 4^{k} = {4**k}",
    )
    cv = CountVectorizer(inputCol="kmers_list", outputCol="features")
    model = cv.fit(kmers_profile_df)
    features_df = model.transform(kmers_profile_df)
    print(f"Counting {4**k} features.")
    unique_features_count = features_df.select("features").distinct().count()
    print(f"Found {unique_features_count} unique features")
    print(f"{unique_features_count} of {num_ids} have unique features.")

    delta_ = delta(start_features)

    # 2.2 Clustering
    start_clustering = get_time()
    print("Starting clustering step.")
    bkm = BisectingKMeans()
    print("Fitting data to Bisecting K Means model")
    model = bkm.fit(features_df)
    clustering_pipeline = Pipeline(stages=[bkm])
    print("Building grid for cross-validation")
    paramGrid = ParamGridBuilder().addGrid(bkm.k, [2, 5, 10, 20, 50, 70, 100]).build()
    print("Starting cross-validation")
    crossval = CrossValidator(
        estimator=clustering_pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=ClusteringEvaluator(),
        numFolds=3,
    )
    cvModel = crossval.fit(features_df)
    cluster_df = cvModel.transform(features_df)
    cluster_df.select("prediction").describe().show()
    print("Finished step 2.")
    delta(ml_start)

    # End execution
    print("All steps complete.")
    delta_ = delta(global_start)
