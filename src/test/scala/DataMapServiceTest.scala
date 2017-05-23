/**
  * Created by chanceroberts on 5/23/17.
  */
import org.scalatest._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import edu.colorado.plv.fixrservice.pipeline._

class DataMapServiceTest extends FlatSpec with ScalatestRouteTest {
  "DataMapService" should " work with post and get" in {
    //Post("/put?{\"key\":\"test\",\"value\":\"hello, world!\"})") ~> ???
    /*Get("/get?{\"key\":\"test\"}") ~> ??? ~> check {
      responseAs[String].equals("{ \"succ\": true, \"key\": \"test\", \"value\": \"hello, world!\" }")
    }*/
    //val dMapService = new DataMapService
    //dMapService.main(Array("Heap"))

  }
}
