package com.openjaw.locs.updater.algorithms

import akka.actor.ActorRef
import com.openjaw.connectors.DistributorConnector
import com.openjaw.locs.updater.{DefaultHierarchyHelper, Supervisor}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{XML, Node, NodeSeq}

/**
 *    This algorithm is used to delete one or multiple locations
 *    from DefaultHierarchy based on specified locationIds separated by pipe, e.g "CITY_MOV_RU|AIR_MUC_DE"
 */

case class RemoveLocationById(locHelper: DefaultHierarchyHelper, config: Config, mainActor: Option[ActorRef]) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("RemoveLocationById"))
  
  //from DefaultHierarchy (current configuration)
  val locations = locHelper.locationNodes

  override def run() = {

    val locationIdsSeq = config.locationId.split("\\|")

    val toBeRemoved = locations
      .filter(node => {
          val locationInfo = locHelper.extractLocation(node)
          locationIdsSeq.contains(locationInfo.generalInfo.locationId)
        })
      .map(node => locHelper.setLocationActionAsDelete(node))

    config.mode match   {
      case Mode.SaveToFile => saveToFiles(toBeRemoved)
      case Mode.SendRequest => sendRequests(toBeRemoved, mainActor.get)
    }

    <Report>
      <Locations removedCount={toBeRemoved.size.toString}>
        {toBeRemoved.map(n => {
          val locationInfo = locHelper.extractLocation(n)
          val generalInfo = locationInfo.generalInfo
          val codesInfo = locationInfo.codesInfo
        <Location locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString(",")}>{generalInfo.locationNameEN}</Location>
      })}
   	  </Locations>
    <Config> {config.xml}</Config>q
    </Report>
  }

  def saveToFiles(toBeUpdated: NodeSeq) = {
    val groupedRequest = toBeUpdated.grouped(5000)
    var index = 0
    groupedRequest.foreach(n => {
      index +=1
      val requests = generateRequests(n)
      saveToFile("toBeRemovedRQ" + "_" + index, requests)
    })
    logger.info(s"All files saved")
  }

  def saveToFile(fileName: String, content: Node ) = {
    val printer = new scala.xml.PrettyPrinter(180, 2)
    val fileNamePath = config.outDir + fileName + ".xml"
    scala.xml.XML.save(fileNamePath, XML.loadString(printer.format(content)), "UTF-8")
    //scala.xml.XML.save(fileNamePath, content, "UTF-8")
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

}