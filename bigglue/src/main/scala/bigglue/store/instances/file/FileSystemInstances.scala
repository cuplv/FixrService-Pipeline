package bigglue.store.instances.file

import java.io.{BufferedWriter, File, FileWriter}

import bigglue.data.{I, Identifiable, Identity}
import bigglue.store.{DataMap, DataMultiMap, IdDataMap}

import scala.io.Source

/**
  * Created by chanceroberts on 8/29/17.
  */
object FileSystemInstances {
}

class FileSystemDataMap[Input <: Identifiable[Input], Output <: I[String]](subdirectory: String) extends DataMap[Input, Output] {
  private def getOutputOutOfDirectory(subdirectory: String, willDelete: Boolean = false): Seq[Output] = {
    val subdir = new File(subdirectory)
    subdir.listFiles().flatMap{
      file =>
        val fileName = file.getName
        val seq = if (file.isDirectory) getOutputOutOfDirectory(s"$subdirectory/$fileName")
        else {
          List(I(Source.fromFile(file).getLines().foldRight("") {
            case (line, fileContent) => s"$line\n$fileContent"
          }).asInstanceOf[Output])
        }
        if (willDelete) file.delete()
        seq
    }.toSeq
  }

  private def mkDirs(path: String, currPath: String = ""): Unit = {
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

  override def put_(data: Seq[Output]): Unit = data.foreach{
    output => if (output.a.endsWith("/")){
      mkDirs(output.a)
    } else {
      put_(???, output)
    }
  }

  override def put_(key: Input, data: Output): Unit = {
    mkDirs(s"$subdirectory/${key.getId()}")
    if (key.getId().endsWith("/")) {
      val file = new File(s"$subdirectory/${key.getId()}/${data.a}")
      file.createNewFile()
    } else {
      val writer = new BufferedWriter(new FileWriter(s"$subdirectory/${key.getId()}", false))
      writer.write(data.a)
      writer.close()
    }
  }

  override def remove(key: Input): Unit = new File(s"$subdirectory/${key.getId()}").delete()

  override def remove(keys: Seq[Input]): Unit =
    keys.foreach(key => new File(s"$subdirectory/${key.getId()}").delete())

  override def iterator(): Iterator[Output] = new FileSystemIterator[Output](subdirectory)

  private def getFiles(direct: File, subdir: String = "", firstTime: Boolean = true): List[String] = {
    val name: String = if (firstTime) s"$subdir" else s"$subdir${direct.getName}/"
    if (direct.isDirectory){
      direct.listFiles.toList.flatMap(file => getFiles(file,name,firstTime = false))
    } else {
      List(name.substring(0, name.length-1))
    }
  }

  override def get(key: Input): Option[Output] = {
    try {
      val file = new File(s"$subdirectory/${key.getId()}")
      if (file.exists()){
        val str = (if (file.isDirectory){
          getFiles(file, key.getId())
        } else {
          Source.fromFile(s"$subdirectory/${key.getId()}").getLines()
        }).foldRight("") {
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

class FileSystemIdDataMap extends IdDataMap[I[String]]{
  val fileMap: FileSystemDataMap[I[String], I[String]] =
    new FileSystemDataMap[I[String], I[String]](name)

  override def put_(data: Seq[I[String]]): Unit = data.foreach(dat => put_(dat.identity(), dat))

  override def put_(key: Identity[I[String]], data: I[String]): Unit = fileMap.put_(I(key.getId()), data)

  override def remove(key: Identity[I[String]]): Unit = fileMap.remove(I(key.getId()))

  override def remove(keys: Seq[Identity[I[String]]]): Unit = keys.foreach(key => fileMap.remove(I(key.getId())))

  override def iterator(): Iterator[I[String]] = fileMap.iterator()

  override def get(key: Identity[I[String]]): Option[I[String]] = fileMap.get(I(key.getId()))

  override def all(): Seq[I[String]] = fileMap.all()

  override def contains(key: Identity[I[String]]): Boolean = fileMap.contains(I(key.getId()))

  override def extract(): Seq[I[String]] = fileMap.extract()

  override def size(): Int = fileMap.size()
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
