import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.logging.log4j.{LogManager, Logger}

object RealEstateAnalysis {
  private val logger: Logger = LogManager.getLogger(RealEstateAnalysis.getClass)
  def main(args: Array[String]): Unit = {
    // ensure our log4j2 is used
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")

    val spark = SparkSession.builder
      .appName("Real Estate Analysis")
      .config("spark.master", "local")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    val realEstateDF = loadData(spark, "src/main/resources/RealEstateUnitedStates.csv")

    // Clean data
    val cleanedDF = cleanData(realEstateDF)

    // Beginning analyses
    val avgHomePriceByRegionYear = calculateAvgHomePriceByRegionYear(cleanedDF)
    avgHomePriceByRegionYear.show()

    val medianIncomeByRegionYear = calculateMedianIncomeByRegionYear(cleanedDF)
    medianIncomeByRegionYear.show()

    val homePriceCorrelation = calculateCorrelation(cleanedDF, "Average Sales Price", "Median Income - Current Dollars")
    println(s"Correlation between Average Sales Price and Median Income (Current Dollars): $homePriceCorrelation")

    val homePriceWithGrowth = calculateHomePriceGrowth(cleanedDF)
    homePriceWithGrowth.show()

    val avgHomePriceByRegion = calculateAvgHomePriceByRegion(cleanedDF)
    avgHomePriceByRegion.show()

    val incomeDistribution = calculateIncomeDistribution(cleanedDF)
    incomeDistribution.show()

    spark.stop()
  }

  def loadData(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }

  def cleanData(df: DataFrame): DataFrame = {
    df.na.fill(0)
  }

  def calculateAvgHomePriceByRegionYear(df: DataFrame): DataFrame = {
    df.groupBy("Year", "Region")
      .agg(round(avg("Average Sales Price"), 2).alias("Avg Home Price"))
      .orderBy("Year", "Region")
  }

  def calculateMedianIncomeByRegionYear(df: DataFrame): DataFrame = {
    df.groupBy("Year", "Region")
      .agg(
        round(avg("Median Income - Current Dollars"), 2).alias("Avg Median Income Current"),
        round(avg("Median Income - 2022 Dollars"), 2).alias("Avg Median Income 2022")
      )
      .orderBy("Year", "Region")
  }

  def calculateCorrelation(df: DataFrame, col1: String, col2: String): Double = {
    df.stat.corr(col1, col2)
  }

  def calculateHomePriceGrowth(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("Region").orderBy("Year")

    df.withColumn("Previous Year Price", lag("Average Sales Price", 1).over(windowSpec))
      .withColumn("Price Growth Rate",
        round((col("Average Sales Price") - col("Previous Year Price")) / col("Previous Year Price") * 100, 2))
      .orderBy("Region", "Year")
  }

  def calculateAvgHomePriceByRegion(df: DataFrame): DataFrame = {
    df.groupBy("Region")
      .agg(round(avg("Average Sales Price"), 2).alias("Avg Home Price"))
      .orderBy(desc("Avg Home Price"))
  }


  def calculateIncomeDistribution(df: DataFrame): DataFrame = {
    df.groupBy("Region")
      .agg(
        round(avg("Median Income - Current Dollars"), 2).alias("Avg Median Income Current"),
        round(avg("Median Income - 2022 Dollars"), 2).alias("Avg Median Income 2022"),
        round(avg("Mean Income - Current Dollars"), 2).alias("Avg Mean Income Current"),
        round(avg("Mean Income - 2022 Dollars"), 2).alias("Avg Mean Income 2022")
      )
      .orderBy("Region")
  }
}