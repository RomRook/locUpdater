package com.openjaw.locs.updater.algorithms

import akka.actor.ActorRef
import com.openjaw.locs.updater.DefaultHierarchyHelper
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.Node

trait Algorithm {
  def run(): Node
}

object Algorithm  {
  val logger = Logger(LoggerFactory.getLogger("Algorithm"))

  def apply(locHelper: DefaultHierarchyHelper,config: Config, actor: Option[ActorRef]): Algorithm = {

    val algorithmType = config.algorithm
    logger.info(s"Provided algorithm type: $algorithmType")

    val algorithm = {
      algorithmType  match {
        case "TEMPLATE" => MappingTemplate(locHelper, config)
        case "UTS_COUNTRY" => Countries(locHelper, config)
//        case "CITY" => Cities (locHelper, config, actor)
//        case "CITY_UTS" => CitiesFromUTS(locHelper, config)
//        case "DEPOT" => CitiesFromUTS(locHelper, config)
//        case "UTS_COUNTRY" =>
//        case "UTS_CITY" =>
//        case "AIRPORT_GEO_UPDATE" =>
//        case "DEL_INCORRECT_IDS" =>
//        case "GET_LOCATION_BY_ID" =>
        case "SET_PRIMARY_GATEWAY" => SetPrimaryGateway(locHelper, config, actor)
        case "SET_SEARCHABLE" => SetLocsAsSearchable(locHelper, config, actor)
        case "GET_CITY_GEO_FROM_GOOGLE" => GetCityCoordinatesFromGoogle(locHelper, config)
        case "CITY_GEO_UPDATE" => UpdateCityCoordinates(locHelper, config, actor)
        case "DEL_DUP_UTS_CITY" => DuplicatedCitiesFromUTS(locHelper, config, actor)
        case "REMOVE_LOCATION" =>  RemoveLocationById(locHelper, config, actor)
//        case "UTS_CITY_AS_AIRPORT" =>
        case other => {
          logger.info(s"Provided algorithm type: $other not handled")
          throw new Exception(s"Provided algorithm type: $other not handled")
        }
      }
    }
    algorithm
  }
}

case class Config(xml: Node) {
  val defaultHierarchy =  (xml \\ "default_hierarchy").text
  val airportCoordinates = (xml \\ "airport_coordinates").text

  val inDir = (xml \\ "data_dir").text + "in/"
  val outDir = (xml \\ "data_dir").text + "out/"
  val utsEndpoint = (xml \\ "uts_endpoint").text
  val utsUser = (xml \\ "uts_user").text
  val utsPassword = (xml \\ "uts_password").text
  val rcEndpoint = (xml \\ "rc_endpoint").text
  val rcUser = (xml \\ "rc_user").text
  val rcPassword = (xml \\ "rc_password").text
  val distributorEndpoint = (xml \\ "xdist_endpoint").text
  val distributorTimeout = (xml \\ "xdist_timeout").text
  val distributorWorkers = (xml \\ "number_of_workers").text
  val distributorRequestLimit = (xml \\ "xdist_request_limit").text
  var countyIsoCode = (xml \\ "iso_country").text
  var locationId = (xml \\ "location_id").text
  var algorithm = ""
  private val modeStr = (xml \\ "mode").text
  var mode = modeStr match {
    case "FILE_ONLY" => Mode.SaveToFile
    case "SEND_RQ" => Mode.SendRequest
    case _ => Mode.None
  }
  val slash = System.getProperty("file.separator")
}

object Mode extends Enumeration {
  type Mode = Value
  val SaveToFile, SendRequest, None = Value
}

//used by UTS algorithm
//case class LocationInfoUTS(locationId: String,
//                        locationType: String,
//                        locationName: String,
//                        utsCode: String,
//                        isoCode: String,
//                        stateInUSA: String,
//                        node: Node)

case class LocationInfo(generalInfo: LocationGeneralInfo, codesInfo: LocationCodesInfo)


case class LocationGeneralInfo(
                             locationId: String,
                             locationType: String,
                             locationNameEN: String,
                             locationNameRU: String,
                             locationAjaxNameEN: String,
                             locationAjaxNameRU: String,
                             node: Node)

case class LocationCodesInfo(
                isoCountryCode: String,
                stateCodeInUSA: String,
                rcCountryCode: String,
                rcCityCode: String,
                utsCode: Seq[String])



//based on uts_countrues_mappinig.xml file
case class UTSCountryInfo(locationId: String,
                          locationType: String,
                          locationName: String,
                          continent: String,
                          tz: String,
                          isoCountryCode: String,
                          stateCodeInUSA: String,
                          cityMergeType: String,
                          ignored: String,
                          utsCode: String,
                          utsCountryName: String)


/////////////////
//case class LocationInfo(generalInfo: LocationGeneralInfo, codesInfo: LocationCodesInfo)
//
//case class RentalCarsLocationInfo(locationId: String,
//                                  locationType: String,
//                                  locationName: String,
//                                  isoCountryCode: String,
//                                  stateCodeInUSA: String,
//                                  keyType: String,
//                                  node: Node)
//
//case class LocationGeneralInfo(
//                                locationId: String,
//                                locationType: String,
//                                locationNameEN: String,
//                                locationNameRU: String,
//                                locationAjaxNameEN: String,
//                                locationAjaxNameRU: String,
//                                node: Node)
//
//case class LocationCodesInfo(
//                              isoCountryCode: String,
//                              stateCodeInUSA: String,
//                              rcCountryCode: String,
//                              rcCityCode: String,
//                              utsCode: Seq[String])
//
