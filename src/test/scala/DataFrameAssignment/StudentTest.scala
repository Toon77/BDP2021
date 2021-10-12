package DataFrameAssignment

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.functions.{col, isnull, rand}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import utils.{Commit, Loader}
import windows.check.init

import scala.reflect.io.Path

/**
 * This class contains the necessary boilerplate code for testing with Spark. This contains the bare minimum to run
 * Spark, change as you like! It is highly advised to write your own tests to test your solution, as it can give you
 * more insight in your solution. You can also use the
 */
class StudentTest extends FunSuite with BeforeAndAfterAll {

  init("C://winutils//bin//winutils.exe")

  //Set logger level (Warn excludes info)
  //Logger.getLogger("org").setLevel(Level.WARN)
  //Logger.getLogger("akka").setLevel(Level.WARN)

  //  This is mostly boilerplate code, read the docs if you are interested!
  val spark: SparkSession = SparkSession
    .builder
    .appName("Spark-Assignment")
    .master("local[*]")
    .getOrCreate()

  implicit val sql: SQLContext = spark.sqlContext

  import spark.implicits._

  val commitDF: DataFrame = Loader.loadJSON(Path("data/data_raw.json"))
  commitDF.cache()

  val commitRDD = commitDF.as[Commit].rdd
  commitRDD.cache()

  test("Assert DF assignment 1") {
    val studentResult = DFAssignment.assignment_1(commitDF.orderBy(rand(0L)))
    val expectedSet = Set(("dirstart", "2019-05-23T12:27:01.000Z", 14),
      ("jbo-ads", "2019-05-10T09:42:31.000Z", 25))
    assertResult(expectedSet) {
      studentResult.as[(String, String, Int)].collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert DF assignment 2") {
    val studentResult = DFAssignment.assignment_2(commitDF, Seq("GitHub"))
    val expectedSet: Set[(String, String)] = Set(("GitHub","775ed90986380e2eadcc7504c1234fcd956c84a3"),
      ("GitHub","3fc47ee518e4b9690521d30d80e7df5cce094a35"))
    assertResult(expectedSet) {
      studentResult.as[(String, String)].collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert DF assignment 3") {
    val studentResult = DFAssignment.assignment_3(commitDF)
    val expectedSet = Set[(String, String, Int, BigInt)](("react-weight-tracker", "purplebubblegum",2019,5))
    assertResult(expectedSet) {
      studentResult.as[(String, String, Int, BigInt)].collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert DF assignment 4") {
    val studentResult = DFAssignment.assignment_4(commitDF)
    val expectedSet = Set(
      ("44e2a297657e4a817535834d37abedf905d8b869","Tue"),
      ("f0fc1ed67e8715cd086860ff3c087c3557d15bd7","Fri")
    )
    assertResult(expectedSet) {
      studentResult.select("sha", "day").as[(String, String)].collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert DF assignment 5") {
    val studentResult = DFAssignment.assignment_5(commitDF, "amazinggirl")
    val expectedSet: Set[(String, String, String, String)] = Set(("5ce692dd6480fd0f119694b4",
      "2019-05-23T12:22:13.000Z","2019-05-23T12:26:23.000Z","2019-05-23T12:27:11.000Z"))
    assertResult(expectedSet) {
      studentResult.as[(String,String,String,String)].collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert DF assignment 6") {
      val studentResult = DFAssignment.assignment_6(commitDF, "amazinggirl")
      val expectedSet = Set[(String, String, BigInt, BigInt)](("5ce691b06480fd1565a33660","2019-05-23T12:22:13.000Z",0,0),
        ("5ce692dd6480fd0f119694b4","2019-05-23T12:26:23.000Z",0,4))
      assertResult(expectedSet) {
        studentResult.as[(String, String, BigInt, BigInt)].collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert DF assignment 7") {
    val studentResult = DFAssignment.assignment_7(commitDF.sample(0.2, 0L))
    val expectedSet = Set[(String, Int, BigInt)](("giphbar",5,1))
    assertResult(expectedSet) {
      studentResult.where(col("repository").equalTo("giphbar"))
        .as[(String, Int, BigInt)].collect().toSet.intersect(expectedSet)
    }
  }


  test("Assert DF assignment 8") {
    val studentResult = DFAssignment.assignment_8(commitDF)
    val expectedSet = Set[(String, BigInt)](("Kitware Robot",52), ("qhua0008",12))
    assertResult(expectedSet) {
      studentResult.as[(String, BigInt)].collect().toSet.intersect(expectedSet)
    }
  }

  override def afterAll(): Unit = {
    //    Uncomment the line beneath if you want to inspect the Spark GUI in your browser, the url should be printed
    //    in the console during the start-up of the driver.
    //    Thread.sleep(9999999)
    //    You can uncomment the line below. Doing so will cause errors with `maven test`
    //    spark.close()
  }

}