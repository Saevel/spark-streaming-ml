package prv.saevel.spark.streaming.ml.utils

import java.io.File
import java.nio.file.{Files, Paths}

/**
  * Basic utils for file manipulation in tests.
  */
trait FileUtils {
  /**
    * Executes a function within a context of a certain directory existing and being completely empty.
    * @param path the path to the directory.
    * @param f the function to be executed in context.
    * @tparam T returned data type.
    * @return whatever the <code>f</code> function returns.
    */
  protected def withEmptyDirectory[T](path: String)(f: => T): T = {
    val directoryPath = Paths.get(path)
    Files.deleteIfExists(directoryPath)
    Files.createDirectories(directoryPath)
    f
  }

  /**
    * Executes a function within the context of a certain file or directory not existing.
    * @param path the path to the file / directory.
    * @param f the function to execute.
    * @tparam T returned data type.
    * @return whatever the <code>f</code> function returns.
    */
  protected def without[T](path: String)(f: => T): T = {
    val mainPath = Paths.get(path)
    deleteRecursive(mainPath.toFile)
    f
  }

  /**
    * Recursively deletes a given file.
    * @param file the <code>File</code> to delete.
    */
  protected def deleteRecursive(file: File): Unit = {
    if(file.exists){
      if(file.isDirectory){
        file.listFiles().foreach(deleteRecursive)
      }
      file.delete()
    }
  }
}
