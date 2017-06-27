import edu.colorado.plv.fixr.fixrservice.pipeline.{BatchProduct, CombineTwoDataMaps, ComputeStep, HeapMap}
import org.scalatest.FlatSpec

/**
  * Created by chanceroberts on 6/26/17.
  */
class CombineTwoDataMapsTest extends FlatSpec {
  "BatchProduct" should "join two maps together to get every possibile combination." in {
    val dMapOne = new HeapMap[String, Int]
    val dMapTwo = new HeapMap[String, Int]
    dMapOne.put("a", 10)
    dMapOne.put("b", 20)
    dMapOne.put("c", 30)
    dMapTwo.put("Amazing", 404)
    dMapTwo.put("Beautiful", 1010)
    dMapTwo.put("Cool", 55)
    val bProduct = new BatchProduct(dMapOne, dMapTwo)
    assert(bProduct.dMap.get("a-:**:-Amazing").contains((10, 404)))
    assert(bProduct.dMap.get("b-:**:-Beautiful").contains((20, 1010)))
    assert(bProduct.dMap.get("c-:**:-Cool").contains((30, 55)))
    assert(bProduct.dMap.get("a-:**:-Beautiful").contains((10, 1010)))
  }

  it should "work with ComputeStep" in {
    val dMapOne = new HeapMap[String, Int]
    val dMapTwo = new HeapMap[String, Int]
    dMapOne.put("a", 10)
    dMapOne.put("b", 20)
    dMapOne.put("c", 30)
    dMapTwo.put("Amazing", 404)
    dMapTwo.put("Beautiful", 1010)
    dMapTwo.put("Cool", 55)
    val bProduct = new BatchProduct(dMapOne, dMapTwo)
    val computeStep = new ComputeStep[(Int, Int), Int]({case (a, b) => a+b}, "", dMap = Some(bProduct.dMap), sMap = Some(bProduct.sMap))
    computeStep.IncrementalCompute(blocking = true)
    assert(computeStep.AToBMap.get((10, 404)).contains(414))
    assert(computeStep.AToBMap.get((20, 1010)).contains(1030))
    assert(computeStep.AToBMap.get((30, 55)).contains(85))
    assert(computeStep.AToBMap.get((30, 404)).contains(434))
  }
}
