package bigglue.checkers

import com.typesafe.config.Config
import bigglue.configurations.PipeConfig
import bigglue.store.DataStore

/**
  * Created by edmundlam on 8/18/17.
  *
  * This module contains traits which implement basic checking routines targeted at config files and
  * input and output store types. These checking routines are expected to be ran before initialization of
  * the sub-components of the pipes, to weed out malformed configs and store type incompatibility
  * (between input/output DataStores and Computations/Platforms). The generic checks essentially imposes
  * no constraints, hence does nothing. Override the appropriate checking routines to implement specific
  * constraints on config, input and output data stores.
  *
  * A note on 'type compatibility' check: the design choice of making this a 'dynamic' check was not
  * taken lightly. While I love to attempt to make this statically typed (e.g., by annotating Computation
  * class with DataStore types) but it would introduce too many type parameter clutter to the
  * Computation/Platform classes. I thought that this (below) would be a fair compromise.
  *
  */

trait ConfigChecker {

  /**
    * Check that paths in configuration file relevant to host class are well-formed. Default does nothing.
    *
    * @param conf the config file to check
    */
  def checkConfig(conf: PipeConfig): Unit = { }

}

trait InputStoreChecker[Input] {

  /**
    * Check that input store is compatible with the host class. Default does nothing.
    *
    * @param inputMap the input store to check
    */
  def checkInput(inputMap: DataStore[Input]): Unit = { }

}

trait InputStoresChecker[InputL, InputR] {

  /**
    * Check that left input store is compatible with the host class. Default does nothing.
    *
    * @param inputLMap the input store to check
    */
  def checkInputL(inputLMap: DataStore[InputL]): Unit = { }

  /**
    * Check that right input store is compatible with the host class. Default does nothing.
    *
    * @param inputRMap the input store to check
    */
  def checkInputR(inputRMap: DataStore[InputR]): Unit = { }

}

trait OutputStoreChecker[Output] {

  /**
    * Check that output store is compatible with the host class. Default does nothing.
    *
    * @param outputMap the output store to check
    */
  def checkOutput(outputMap: DataStore[Output]): Unit = { }

}

trait UnaryChecker[Input, Output] extends ConfigChecker with InputStoreChecker[Input] with OutputStoreChecker[Output] {

  /**
    * Check routine for unary data processors
    *
    * @param conf the config file to check
    * @param inputMap the input store to check
    * @param outputMap the output store to check
    */
  def check(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
     checkConfig(conf)
     checkInput(inputMap)
     checkOutput(outputMap)
  }

}

trait BinaryChecker[InputL, InputR, Output] extends ConfigChecker with InputStoresChecker[InputL,InputR] with OutputStoreChecker[Output] {

  /**
    * Check routine for binary data processors
    *
    * @param conf the config file to check
    * @param inputLMap the left input store to check
    * @param inputRMap the right input store to check
    * @param outputMap the output store to check
    */
  def check(conf: PipeConfig, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output]): Unit = {
    checkConfig(conf)
    checkInputL(inputLMap)
    checkInputR(inputRMap)
    checkOutput(outputMap)
  }

}