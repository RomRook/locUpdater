package com.openjaw.locs.updater.algorithms

import akka.actor.ActorRef
import com.openjaw.connectors.DistributorConnector
import com.openjaw.locs.updater.{DefaultHierarchyHelper, Supervisor}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq}

/**
  *    This algorithm is used to add PrimaryGateway capability
  *    If location contains at least one AssociatedLocations then this type of capability will be added
  *    If PrimaryGateway is already defined the location is not updated
  *
  *    PrimaryGateway is going to be used in Flight Search on front-end
  *    to check if location should be displayed or not
  */

case class SetPrimaryGateway(locHelper: DefaultHierarchyHelper, config: Config, mainActor: Option[ActorRef]) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("SetPrimaryGateway"))
  
  //from DefaultHierarchy (current configuration)
  val countries = locHelper.countries
  val cities = locHelper.cities
  val airports = locHelper.airports
  val locations = locHelper.locationNodes

  val locationTypes = List("country", "city", "airport")

  override def run() = {
        
    val toBeUpdated = cities
      .filter(node => {
          val associatedNodes = node \\ "AssociatedLocations" \ "LocationReference"
          //val id = (node \ "@Id").text
           val (_, _, primaryGateway) = locHelper.extractLocationCapabilities(node)
          (associatedNodes.size > 0) && (primaryGateway)
        })
      .map(node =>
      //locHelper.addPrimaryGatewayCapability(node)
      locHelper.removePrimaryGatewayCapability(node) )

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
      val searchable = n.attribute("Searchable").get
      <City ISOCountryCode={locationInfo.codesInfo.isoCountryCode} Searchable={searchable} Id={locationInfo.generalInfo.locationId} LocationType={locationInfo.generalInfo.locationType} LocationNameRU={locationInfo.generalInfo.locationNameRU}>{locationInfo.generalInfo.locationNameEN}</City>
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