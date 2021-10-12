package windows

import java.nio.file.{FileSystems, Files, Paths}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object check {

  /**
   * Initialize winutils for windows.
   * @param filename filename
   * @return env variable
   */
  def init(filename: String):Any = {

    val osName = System.getProperty("os.name").toLowerCase();
    if (osName.contains("windows")) {

      val winPath = Paths.get(getClass.getResource("/winutils.exe").toURI)

      val systemDrive = System.getenv("SystemDrive")
      val path = Paths.get(s"$systemDrive//winutils//bin")

      if (!(Files.exists(path) && Files.isDirectory(path))) {
        Files.createDirectories(path)
        Files.copy(winPath, Paths.get(filename))
      }

      System.setProperty("hadoop.home.dir", s"$systemDrive//winutils")
    }
  }
}
