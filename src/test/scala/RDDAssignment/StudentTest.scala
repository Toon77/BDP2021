package RDDAssignment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import utils.{Commit, File, Loader, Stats}
import windows.check.init

import java.sql.Timestamp
import java.time.Instant
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

  test("Assert RDD assignment 1") {
    assertResult(10000L) {
      RDDAssignment.assignment_1(commitRDD)
    }
  }

  test("Assert RDD assignment 2") {
    val studentResult: RDD[(String, Long)] = RDDAssignment.assignment_2(commitRDD)

    val expectedSet = Set(("outlook.com",59L), ("akcjademokracja.pl",1L))
    assertResult(expectedSet) {
      studentResult.collect().toSet.intersect(expectedSet)
    }
  }


  test("Assert RDD assignment 3") {
    val studentResult: (String, Long) = RDDAssignment.assignment_3(commitRDD)
    assertResult(studentResult) {
      ("GdsFileToBmp/TestFile/MHD.txt",2561994L)
    }
  }

  test("Assert RDD assignment 4") {
    val studentResult: RDD[(Long, String, Long)] = RDDAssignment.assignment_4(commitRDD)
    val expectedSet = Set((0L,"Mateusz Kossakowski",5L))
    assertResult(expectedSet) {
      studentResult.collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert RDD assignment 5") {
    val expectedSubset = Set(("js",Stats(2429903,2151766,278137)), ("md",Stats(160629,118296,42333)))
    assertResult(expectedSubset) {
      RDDAssignment.assignment_5(commitRDD, List("js", "md")).collect().toSet.intersect(expectedSubset)
    }
  }

  test("Assert RDD assignment 6") {
    val studentResult: RDD[(String)] = RDDAssignment.assignment_6(commitRDD)
    val expectedSet = Set("IcyBiscuit","Purple-CSGO","finkj","Kasugaccho","addisonVota","marcbryan",
      "highest-booker", "vishalkale74", "CornerDesign", "paulohscwb","paulohscwb", "aleasweb",
      "SHshzik", "ceubri", "zhouganglin")
    assertResult(expectedSet) {
      studentResult.collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert RDD assignment 7") {
    val studentResult: RDD[(String, Double)] = RDDAssignment.assignment_7(commitRDD)
    val expectedSet = Set(("android_packages_apps_DU-Tweaks",0.2), ("fp-formidling",0.25))
    assertResult(expectedSet) {
      studentResult.collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert RDD assignment 8") {
    val studentResult: RDD[(String, Iterable[String], Long)] = RDDAssignment.assignment_8(commitRDD)
    val expected = ("David",Iterable("biolearn_torch", "canvas-demo", "Make-Some-Code", "Javascript", "crayon"),
      7L)
    assertResult(1) {
      studentResult.filter(x => x._1==expected._1)
        .filter(x => x._2.toSet.union(expected._2.toSet).equals(expected._2.toSet) && x._3 ==expected._3).count()
    }
  }


  test("Assert RDD assignment 9") {
    val studentResult: RDD[(String, Iterable[(Timestamp, String)])] = RDDAssignment.assignment_9(commitRDD)
    val t = Timestamp.from(Instant.parse("2019-05-23T12:27:11.00Z"))
    val expected = ("blamer-vs", (t, "BeauAgst"))
    assertResult(Set(expected._2)) {
      studentResult.filter(x => x._1 == expected._1).map(x => x._2).first().toSet
        .intersect(Set(expected._2))
    }
  }

  test("Assert RDD assignment 10") {
    val studentResult: RDD[(String, List[(String, Stats)])] = RDDAssignment
      .assignment_10(commitRDD, "mysite2")
    val expectedSet = Set(("src/main/java/com/cafe24/mysite/dto/JSONResult.java",List(("Sowon Park",Stats(3,0,3)))),
      ("src/main/java/com/cafe24/mysite/service/AdminService.java",List(("Bking625",Stats(20,20,0)))))
    assertResult(expectedSet) {
      studentResult.collect().toSet.intersect(expectedSet)
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