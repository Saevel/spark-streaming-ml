package prv.saevel.spark.streaming.ml.utils

import java.io.File
import java.nio.file.{Files, Paths}

trait FileUtils {

  protected def withEmptyDirectory[T](path: String)(f: => T): T = {
    val directoryPath = Paths.get(path)
    Files.deleteIfExists(directoryPath)
    Files.createDirectories(directoryPath)
    f
  }

  protected def without[T](path: String)(f: => T): T = {
    val mainPath = Paths.get(path)
    deleteRecursive(mainPath.toFile)
    f
  }

  protected def deleteRecursive(file: File): Unit = {
    if(file.exists){
      if(file.isDirectory){
        file.listFiles().foreach(deleteRecursive)
      }
      file.delete()
    }
  }
}
