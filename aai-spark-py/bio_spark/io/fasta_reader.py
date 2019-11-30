from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, asc

class FASTAReade():

    def __init__(self, sc):
        self.setSparkCtx(sc)

    def setSparkCtx(self, sc):
        self._sc = sc
        return self
    
    def read(self, iDF):
        plain_df_idx = iDF.rdd\
                    .zipWithIndex().toDF(["row","idx"])\
                    .orderBy(asc("idx"))\
                    .coalesce(10)

        Windowspec=Window.orderBy("idx")
        oDF = plain_df_idx\
                .withColumn("quality", F.lead("row",count=3).over(Windowspec))\
                .withColumn("+", F.lead("row",count=2).over(Windowspec))\
                .withColumn("seq", F.lead("row",count=1).over(Windowspec))\
                .withColumn("seqID", F.lead("row",count=0).over(Windowspec))

        parsedDF = oDF.filter(F.col("idx") % 4 == 0).select("seqID", "seq", "+", "quality")
        return parsedDF

if __name__ == "__main__":
    pass