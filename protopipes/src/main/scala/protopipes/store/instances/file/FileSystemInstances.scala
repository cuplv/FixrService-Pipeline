package protopipes.store.instances.file

import java.io.{BufferedWriter, File, FileWriter}

import protopipes.data.{I, Identifiable}
import protopipes.store.{DataMap, DataMultiMap}

import scala.io.Source

/**
  * Created by chanceroberts on 8/29/17.
  */
object FileSystemInstances {
  ???
}

class FileSystemDataMap[Input <: Identifiable[Input], Output <: I[String]](subdirectory: String) extends DataMap[Input, Output] {
  private def getOutputOutOfDirectory(subdirectory: String, willDelete: Boolean = false): Seq[Output] = {
    val subdir = new File(subdirectory)
    subdir.listFiles().flatMap{
      file =>
        val fileName = file.getName
        val seq = if (file.isDirectory) getOutputOutOfDirectory(s"$subdirectory/$fileName")
        else {
          List(I(Source.fromFile(file).getLines().foldRight(""){
            case (line, fileContent) => s"$line\n$fileContent"
          }).asInstanceOf[Output])
        }
        if (willDelete) file.delete()
        seq
    }.toSeq
  }

  override def put_(data: Seq[Output]): Unit = data.foreach(put_(???, _))

  override def put_(key: Input, data: Output): Unit = {
    def mkDirs(path: String, currPath: String = ""): Unit = {
      path.indexOf("/") match {
        case -1 => ()
        case 1 if path.charAt(0) == '/' => ()
        case 2 if path.substring(0, 2).equals("..") => ()
        case x =>
          val file = new File(s"$currPath${path.substring(0, x)}")
          file.mkdir()
          mkDirs(path.substring(x+1), s"$currPath${path.substring(0, x+1)}")
      }
    }
    mkDirs(s"$subdirectory/${key.getId()}")
    val writer = new BufferedWriter(new FileWriter(s"$subdirectory/${key.getId()}", false))
    writer.write(data.a)
    writer.close()
  }

  override def remove(key: Input): Unit = new File(s"$subdirectory/${key.getId()}").delete()

  override def remove(keys: Seq[Input]): Unit =
    keys.foreach(key => new File(s"$subdirectory/${key.getId()}").delete())

  override def iterator(): Iterator[Output] = ???

  override def get(key: Input): Option[Output] = {
    try {
      if (new File(key.getId()).exists()) {
        val str = Source.fromFile(s"$subdirectory/${key.getId()}").getLines().foldRight("") {
          case (line, fileContent) => s"$line\n$fileContent"
        }
        Some(I(str).asInstanceOf[Output])
      } else None
    } catch {
      case _: Exception => None
    }
  }

  override def all(): Seq[Output] = getOutputOutOfDirectory(subdirectory)

  override def contains(key: Input): Boolean =
    try {
      new File(key.getId()).exists()
    } catch {
      case _: Exception => false
    }

  override def extract(): Seq[Output] = getOutputOutOfDirectory(subdirectory, willDelete = true)

  override def size(): Int = {
    def numberOfFiles(subdirectory: String): Int = {
      val subdir = new File(subdirectory)
      subdir.listFiles().foldLeft(0){
        case (numOfFiles, file) =>
          val fileName = file.getName
          if (file.isDirectory) numOfFiles+numberOfFiles(s"$subdirectory/$fileName")
          else numOfFiles+1
      }
    }
    numberOfFiles(subdirectory)
  }
}

class FileSystemDataMultiMap[Input <: Identifiable[Input], Output <: I[String]](subdirectory: String) extends DataMultiMap[Input, Output]{
  override def put_(data: Seq[Set[Output]]): Unit = ???

  override def put_(key: Input, data: Set[Output]): Unit = ???

  override def remove(keys: Seq[Input]): Unit = ???

  override def remove(data: Set[Output]): Unit = ???

  override def remove(key: Input): Unit = ???

  override def remove(key: Input, data: Set[Output]): Unit = ???

  override def iterator(): Iterator[Set[Output]] = ???

  override def iterator(key: Input): Iterator[Output] = ???

  override def get(key: Input): Set[Output] = ???

  override def all(): Seq[Set[Output]] = ???

  override def contains(key: Input, data: Set[Output]): Boolean = ???

  override def extract(): Seq[Set[Output]] = ???

  override def size(): Int = ???
}

class FileSystemIterator[Output <: I[String]](subdirectory: String) extends Iterator[Output]{
  var files: List[String] = developQueue(subdirectory)

  private def developQueue(subdir: String): List[String] = {
    new File(subdir).listFiles().toList.flatMap{
      file =>
        if (file.isDirectory) developQueue(s"$subdir/${file.getName}")
        else List(file.getName)
    }
  }

  override def next(): Output = files match{
    case Nil => throw new Exception("File System Iterator does not have any values left.")
    case head :: rest =>
      val ret = Source.fromFile(head).getLines().foldRight("") {
        case (line, fileContent) => s"$line\n$fileContent"
      }
      files = rest
      I(ret).asInstanceOf[Output]
  }

  override def hasNext: Boolean = files.nonEmpty
}
