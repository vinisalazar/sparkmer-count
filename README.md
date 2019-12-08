```shell
# Set up docker env
$ docker-compose up -d

# Run application
$ python sparkmer-count/sparkmer-count.py -i data/synechococcus -o synechococcus.csv -k 2

usage: sparkmer-count.py [-h] -i INPUT [-o OUTPUT] [-k KMER]

Script to run kmer count with Spark.

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Input directory containing genome contigs file.
  -o OUTPUT, --output OUTPUT
                        Output file with kmer counts. Default is
                        kmer_count_out.txt
  -k KMER, --kmer KMER  Size of kmer. Default is 3
```
