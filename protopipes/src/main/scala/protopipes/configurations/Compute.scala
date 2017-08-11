package protopipes.configurations

import protopipes.computations.CartesianProduct
import protopipes.data.Identifiable

/**
  * Created by edmundlam on 8/11/17.
  */
object Compute {

  def cartesianProduct[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR]](implicit builder: PlatformBuilder): CartesianProduct[InputL,InputR] = {
    new CartesianProduct[InputL,InputR]()
  }

}
