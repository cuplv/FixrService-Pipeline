package bigglue.store.instances.file

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source
import bigglue.data.I
import bigglue.data.serializers.{BasicSerializer, IStringBasicSerializer}
import bigglue.store.DataMap

/**
  * Created by chanceroberts on 9/5/17.
  */

/**
  * A Simple Data Map that takes the form of a Text File
  * It's Line Number to String, so it the key is [[I]][Int], and the value is [[I]][String]
  * @param name The name of the text file to open.
  */
class TextFileDataMap(name: String) extends DataMap[I[Int], I[String]]{
  //override val serializerOpt: Option[BasicSerializer[I[String]]] = Some(IStringBasicSerializer)
  val file = new File(name)
  override def put_(data: Seq[I[String]]): Unit = {
    try {
      val writer = new BufferedWriter(new FileWriter(file, true))
      data.foreach(dta => writer.append(s"${dta.a}\n"))
      writer.close()
    }
    catch{
      case e: Exception => throw new Exception("File is not open.")
    }
  }

  override def put_(key: I[Int], data: I[String]): Unit = ???

  override def remove(key: I[Int]): Unit = ???

  override def remove(keys: Seq[I[Int]]): Unit = ???

  override def iterator(): Iterator[I[String]] = new TextFileIterator(name)

  override def get(key: I[Int]): Option[I[String]] = {
    try{
      def getLineNumber(lineIterator: Iterator[String], lineNumber: Int = 1): Option[I[String]] = (lineIterator.hasNext, lineNumber) match{
        case (true, x) if x == key.a => Some(I(lineIterator.next()))
        case (false, _) => None
        case (true, _) =>
          lineIterator.next()
          getLineNumber(lineIterator, lineNumber+1)
      }
      getLineNumber(Source.fromFile(file).getLines())
    } catch {
      case _: Exception => None
    }
  }

  override def all(): Seq[I[String]] =
    try {
      Source.fromFile(file).getLines().foldRight(List[I[String]]()) {
        case (str, list) => I(str) :: list
      }
    } catch{
      case _: Exception => Seq()
    }

  override def contains(key: I[Int]): Boolean = {
    try{
      def lineNumberExists(lineIterator: Iterator[String], lineNumber: Int = 1): Boolean = (lineIterator.hasNext, lineNumber) match{
        case (true, x) if x == key.a => true
        case (false, _) => false
        case (true, _) =>
          lineIterator.next()
          lineNumberExists(lineIterator, lineNumber+1)
      }
      lineNumberExists(Source.fromFile(file).getLines())
    }
    catch{
      case _: Exception => false
    }
  }

  override def extract(): Seq[I[String]] = {
    val ret = all()
    val writer = new BufferedWriter(new FileWriter(file, false))
    writer.close()
    ret
  }

  override def size(): Int = Source.fromFile(file).getLines().foldLeft(0){
    case (num, _) => num+1
  }
}

class TextFileIterator(name: String) extends Iterator[I[String]]{
  val file = new File(name)
  val trueIterator = Source.fromFile(file).getLines()
  override def hasNext: Boolean = trueIterator.hasNext

  override def next(): I[String] = I(trueIterator.next())
}