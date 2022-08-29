import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
object StreamStaticJoin {

  val spark: SparkSession = SparkSession.builder()
    .appName("streamStaticInnerJoin")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  import spark.implicits._

//  val rateData = df.as[RateData]
  val rateData = df.select($"value".as("value"), $"timestamp".as("timestamp"))

  val streamingEmployeeDS = rateData.where("value % 10 != 0")
    .withColumn("firstName",  concat(lit("firstName"),rateData.col("value")))
    .withColumn("lastName",  concat(lit("lastName"),rateData.col("value")))
    .withColumn("departmentId", lit(floor(rateData.col("value")%10)))

  val staticDepartmentDS = spark.read
    .format("csv")
    .option("header","true")
    .load("src/main/resources/department.csv")

  val innerJoinDS =  streamingEmployeeDS.join(
    staticDepartmentDS, $"departmentId" === $"id"
    //,   joinType = "leftOuter"      // can be "inner", "leftOuter", "rightOuter", "fullOuter", "leftSemi"
  )

  val innerJoinStream = innerJoinDS.writeStream
    .format("console")
    .queryName("InnerJoin")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\streamStaticInnerJoin\\cp1")
    .start()

  spark.streams.awaitAnyTermination()
}
