package com.openjaw.locs.updater

import scala.xml.Node

object UtsHelper {

  def getNameAndStateUSA(utsCityName: String, utsCountryCode: String) = {
    var index = -1
    var stateInUSA: String = ""

    if (utsCountryCode == 138) {
      //todo: split based on regex for "(XX)"
      index = utsCityName.indexOf("(")
      stateInUSA = {
        if (index != -1) utsCityName.substring(index) else ""
      }
      stateInUSA = stateInUSA.replace("(", "")
      stateInUSA = stateInUSA.replace(")", "").trim
    }
    val cityName = {
      if (stateInUSA != "") utsCityName.substring(0, index)
      else utsCityName
    }

    (cityName.trim, stateInUSA.trim)
  }

  def extractSupplierInfo(supplierNode: Node) = {

    val utsCityName = supplierNode.text
    val utsCityNameRU = (supplierNode \ "@name_ru").text

    val ustCodeCity = (supplierNode \ "@id").text
    val utsCountryCode = (supplierNode \ "@country").text

    var cityName = utsCityName
    var stateInUSA = ""
    //for USA remove from name info about state (e.g. "Aspen (CO)")
    if (utsCountryCode == "138") {
    cityName = getNameAndStateUSA(utsCityName, utsCountryCode)._1 //without info about state
    stateInUSA = getNameAndStateUSA(utsCityName, utsCountryCode)._2 // state code only for USA
    }

    (utsCityName, utsCityNameRU, ustCodeCity, utsCountryCode, cityName, stateInUSA)

  }


}
