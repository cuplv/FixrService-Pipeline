package bigglue.configurations

import bigglue.computations.CartesianProduct
import bigglue.data.Identifiable

/**
  * Created by edmundlam on 8/11/17.
  */
object Compute {

  def cartesianProduct[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR]]: CartesianProduct[InputL,InputR] = {
    new CartesianProduct[InputL,InputR]()
  }

}
