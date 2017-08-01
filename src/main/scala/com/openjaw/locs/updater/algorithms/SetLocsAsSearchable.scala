package com.openjaw.locs.updater.algorithms

import akka.actor.ActorRef
import com.openjaw.connectors.DistributorConnector
import com.openjaw.locs.updater.{DefaultHierarchyHelper, Supervisor}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq}

/**
 *    This algorithm is used to set 'Searchable' attribute to true for those locations
 *    which are not searchable currently and have hotel capability defined
 *    The location of type 'city' is only processed by this algorithm but it might be extended to other types of locations
 */

case class SetLocsAsSearchable(locHelper: DefaultHierarchyHelper, config: Config, mainActor: Option[ActorRef]) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("SetLocsAsSearchable"))
  
  //from DefaultHierarchy (current configuration)
  val countries = locHelper.countries
  val cities = locHelper.cities
  val airports = locHelper.airports
  val locations = locHelper.locationNodes

  val handledLocationTypes = List("city") // other types are as follows: country, city, airport

  override def run() = {
        
    val toBeUpdated = cities
      .filter(node => {
          val locationInfo = locHelper.extractLocation(node)
          val searchable = node.attribute("Searchable").get.toString.toBoolean
          //val id = (node \ "@Id").text
          val (_, hotel,_ ) = locHelper.extractLocationCapabilities(node)

          val result = (locationInfo.generalInfo.locationId != ""
            && !searchable
            && hotel
            && handledLocationTypes.contains(locationInfo.generalInfo.locationType))
           result
        })
      .map(node => locHelper.setLocationAsSearchable(node))

    config.mode match   {
      case Mode.SaveToFile => saveToFiles(toBeUpdated)
      case Mode.SendRequest => sendRequests(toBeUpdated, mainActor.get)
    }

    val citiesToBeUpdate = extractCities(toBeUpdated)
    <Report>
      <Cities currentCount={cities.size.toString}  updatedCount={citiesToBeUpdate.size.toString}>
        {citiesToBeUpdate.sortBy(n => (n.attribute("isoCountryCode") + n.text))}
   	  </Cities>
    <Config> {config.xml}</Config>
    </Report>
  }

  def saveToFiles(toBeUpdated: NodeSeq) = {
    val groupedRequest = toBeUpdated.grouped(5000)
    var index = 0
    groupedRequest.foreach(n => {
      index +=1
      val requests = generateRequests(n)
      saveToFile("toBeUpdatedRQ" + "_" + index, requests)
    })
    logger.info(s"All files saved")
  }

  def saveToFile(fileName: String, content: Node ) = {
    val printer = new scala.xml.PrettyPrinter(180, 2)
    val fileNamePath = config.outDir + fileName + ".xml"
    //scala.xml.XML.save(fileNamePath, XML.loadString(printer.format(content)), "UTF-8")
    scala.xml.XML.save(fileNamePath, content, "UTF-8")
    logger.info(s"File saved $fileNamePath")
  }

  def generateRequests(toBeUpdated: NodeSeq) = {
    val toBeUpdatedRQ = DistributorConnector.createLocationHierarchyRQ(toBeUpdated)
    toBeUpdatedRQ
  }

  def sendRequests(toBeUpdated: NodeSeq, mainActor: ActorRef) = {
    if (toBeUpdated.size > 0) {
      logger.info(s"About to send requests to xDist [size: ${toBeUpdated.size}]")
      mainActor ! Supervisor("toBeUpdated", countryName = "ALL", requestType = DistributorConnector.REQUEST_TYPE_LOCATION, toBeUpdated)
    }
  }

  def extractCities(toBeUpdated: NodeSeq) = {
    toBeUpdated
      .filter(n => {
        val locationInfo = locHelper.extractLocation(n)
        locationInfo.generalInfo.locationType == "city"
      })
      .map( n => {
        val locationInfo = locHelper.extractLocation(n)
        <City isoCountryCode={locationInfo.codesInfo.isoCountryCode} Id={locationInfo.generalInfo.locationId} LocationType={locationInfo.generalInfo.locationType} LocationNameRU={locationInfo.generalInfo.locationNameRU}>{locationInfo.generalInfo.locationNameEN}</City>
      })
  }

  def extractAirports(toBeUpdated: NodeSeq) = {
    toBeUpdated
      .filter(n => {
        val locationInfo = locHelper.extractLocation(n)
        locationInfo.generalInfo.locationType == "airport"
      })
      .map( n => {
        val locationInfo = locHelper.extractLocation(n)
        <Airport isoCountryCode={locationInfo.codesInfo.isoCountryCode} Id={locationInfo.generalInfo.locationId} LocationType={locationInfo.generalInfo.locationType} LocationNameRU={locationInfo.generalInfo.locationNameRU}>{locationInfo.generalInfo.locationNameEN}</Airport>
    })
  }

  def extractCountries(toBeUpdated: NodeSeq) = {
    toBeUpdated
      .filter(n => {
        val locationInfo = locHelper.extractLocation(n)
        locationInfo.generalInfo.locationType == "country"
      })
      .map( n => {
        val locationInfo = locHelper.extractLocation(n)
        <Country isoCountryCode={locationInfo.codesInfo.isoCountryCode} Id={locationInfo.generalInfo.locationId} LocationType={locationInfo.generalInfo.locationType} LocationNameRU={locationInfo.generalInfo.locationNameRU}>{locationInfo.generalInfo.locationNameEN}</Country>
    })
  }

}