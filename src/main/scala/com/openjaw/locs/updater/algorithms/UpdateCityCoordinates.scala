package com.openjaw.locs.updater.algorithms

import com.openjaw.connectors.DistributorConnector
import com.openjaw.locs.updater.{Supervisor, DefaultHierarchyHelper}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import akka.actor.ActorRef

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode
import scala.xml.{Elem, Node, NodeSeq, XML}


case class UpdateCityCoordinates(locHelper: DefaultHierarchyHelper, config: Config, mainActor: Option[ActorRef]) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("UpdateCityCoordinates"))
  
  case class Row(locationId: String, latitude: String, longitude: String)
  case class Position(latitude: BigDecimal, longitude: BigDecimal)

  //from DefaultHierarchy (current configuration)
  val cities = locHelper.cities
  val airports = locHelper.airports
  val locations = locHelper.locationNodes
  
  // key - locationId
  var mapDefinitionFromCSV: Map[String, Position] = Map()
  var mapDefinitionFromLocation: Map[String, Position] = Map()

  logger.info(s"Number of cities to be processed ${cities.size}")

  override def run() = {
        
    val fileName = config.inDir +  "CityCoordinates.csv"
    logger.info(s"Input file $fileName")
    
    val rows = populateList(fileName)
    populateMap(rows.toSet)
    logger.info(s"Number of all rows defined in CSV file ${rows.size}")
    logger.info(s"Number of unique rows defined in CSV file ${rows.toSet.size}")
    
    val notDefinedInCSV = findMissinigCities
    logger.info(s"Number of missing cities in CSV file ${notDefinedInCSV.size} (DefaultHierarchy contains more cites)")

    val toBeUpdated = cities
      .filterNot(node => {
          val info = extractInfo(node)
          val locationId = info._1
          notDefinedInCSV.contains(locationId)
        })
      .filter(node => {
          val info = extractInfo(node)
          val locationId = info._1
          mapDefinitionFromCSV.isDefinedAt(locationId)
        })
      .filterNot(node => {
          val info = extractInfo(node)
          val locationId = info._1
          val position = info._10
        positionEquals(position, mapDefinitionFromCSV(locationId))
        })
      .map(node => updateCityWithNewPosition(node))

    config.mode match   {
      case Mode.SaveToFile => {
        val groupedRequest = toBeUpdated.grouped(1000)
        var index = 0
        groupedRequest.foreach(n => {
          index +=1
          val requests = generateRequests(n)
          saveToFile("toBeUpdatedRQ" + "_" + index, requests)
        })
      }
      case Mode.SendRequest => {
        sendRequests(toBeUpdated, mainActor.get)
      }
    }
    <Report>
      <Cities currentCount={cities.size.toString} notDefinedInCSVCount={notDefinedInCSV.size.toString} updatedCount={toBeUpdated.size.toString}>
        <ToBeUpdated>{toBeUpdated
          .sortWith((a,b) => {
            //sort by locationName
            val aLocationName = extractInfo(a)._3
            val bLocationName = extractInfo(b)._3
            aLocationName < bLocationName
            })
          .map( n => {
            val(locationId, _, locationName, _, _, _, _, _, _, position) = extractInfo(n)
            val oldLatitude = mapDefinitionFromLocation(locationId).latitude
            val oldLongitude = mapDefinitionFromLocation(locationId).longitude
            <City locationId={locationId} newLatitude = {position.latitude.toString()} oldLatitude = {oldLatitude.toString()} newLongitude  = {position.longitude.toString()} oldLongitude  = {oldLongitude.toString()} >
              {locationName}
            </City>
        })}</ToBeUpdated>
        <NotDefinedInCSV>{notDefinedInCSV
         .filter(item => !item.isEmpty)
         .sorted
         .map(item =>
         <City locationId={item}></City>)}
        </NotDefinedInCSV>
   	</Cities>
    <Config> {config.xml}</Config>
    </Report>
  }

  def findMissinigCities() = {
    var listBuffer = new ListBuffer[String]()
    cities.foreach(n => {
      val locationId = extractInfo(n)._1
      val locationName = extractInfo(n)._3
      if (!(mapDefinitionFromCSV.isDefinedAt(locationId)))
        listBuffer += locationId
    })
    listBuffer.toList
  }

  def populateList(csvFileName: String): List[Row] = {
    var rows: List[Row] = List.empty
    try {
      val lines = io.Source.fromFile(csvFileName).getLines.drop(1)
      logger.info(s"Input data read from: $csvFileName")
      
      // newLatitude|newLongitude|oldLatitude|oldLongitude|locationName|UTSCode|locationId|ISOCode|StateInUSA
      // 50.8503396|4.3517103|50.83705|4.367612|Brussels|380|CITY_BRU_BE|BE|

      lines.foreach(l => {
        val columns = l.split("\\|")        
        val latitude = columns(0).trim
        val longitude = columns(1).trim
        val locationId = columns(6).trim     
        //logger.debug(s"$locationId [$latitude $longitude]")
        rows =  Row(locationId, latitude,  longitude) :: rows
      })
    } catch  {
      case e: java.io.FileNotFoundException => {
        logger.error(s"Input file with mappinigs not found! Expected file: $csvFileName")        
        System.exit(0)
      }
    }
    rows
  }

  def populateMap(listOfRows: Set[Row]) = {
    listOfRows.foreach(row => {
      val locationId = row.locationId
      val position = Position(BigDecimal(row.latitude) ,BigDecimal(row.longitude))
      if (mapDefinitionFromCSV.isDefinedAt(locationId)) {
        //throw new Exception("Duplicated locationId in CSV: " + locationId)
        logger.error(s"Duplicated locationId in CSV for '$locationId'")
      }
      mapDefinitionFromCSV += (locationId -> position)
    })

    cities.foreach(node => {
      val info = extractInfo(node)
      val locationId = info._1
      val position = info._10
      if (mapDefinitionFromLocation.isDefinedAt(locationId)) {
        //throw new Exception("Duplicated locationId in xLocation: " + locationId)
        logger.error(s"Duplicated locationId in DefaultHierarchy for '$locationId'")
      }
      mapDefinitionFromLocation += (locationId -> position)
    })
  }


  def saveToFile(fileName: String, content: Node ) = {
    val printer = new scala.xml.PrettyPrinter(180, 2)
    val fileNamePath = config.outDir + fileName + ".xml"
    scala.xml.XML.save(fileNamePath, XML.loadString(printer.format(content)), "UTF-8")
    logger.info(s"File saved $fileNamePath")
  }

  def positionEquals(p1: Position, p2: Position) = {
    // p2 is what we compare with
    val p1_latitude =  p1.latitude.setScale(p2.latitude.scale, RoundingMode.DOWN)
    val p1_longitude = p1.longitude.setScale(p2.longitude.scale, RoundingMode.DOWN)

    val result = (p1_latitude.equals(p2.latitude) && p1_longitude.equals(p2.longitude))
    result

  }

  def extractInfo(locationNode: Node) = {
    try {
      val locationId = (locationNode \ "@Id").text
      val locationType = (locationNode \ "@LocationType").text

      //location name
      val namesNode = locationNode \\ "Location" \ "Names" \ "Name"
      //first english name (might be more then one name defined in english that's way take the first one)
      val firstEnglishName = namesNode.filter(name => {
        (name \ "@Language").text == "en"
      }).head
      val locationName = (firstEnglishName \ "@Value").text

      //IATA Code
      val codesNode = locationNode \\ "Location" \ "Codes" \ "Code"
      val iataCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "IATA"
      })
      val iataCode = (iataCodeNode \ "@Value").text

      //ISO code
      val isoCountryCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "ISOCountryCode"
      })
      val isoCode = (isoCountryCodeNode \ "@Value").text

      //Position
      val positionNode = (locationNode \\ "Location" \ "Position").head
      val latitude = (positionNode \ "@Latitude").text
      val longitude = (positionNode \ "@Longitude").text
      val radius = (positionNode \ "@Radius").text
      val zoom= (positionNode \ "@Zoom").text

      val position = Position(BigDecimal(latitude),BigDecimal(longitude) )

      (locationId, locationType, locationName, iataCode, isoCode, latitude, longitude, radius, zoom, position)

    } catch {
      case e: Exception => throw new Exception(e.getMessage + " locationNode " + locationNode)
    }

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

  def updateCityWithNewPosition(node: Node) = {
    val(locationId, _, _, _, _, _, _, oldRadius, oldZoom, position) = extractInfo(node)

    val newlatitude = mapDefinitionFromCSV(locationId).latitude.toString()
    val newlongitude = mapDefinitionFromCSV(locationId).longitude.toString()

    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)

    originalElem match {
      case e @ Elem(_, _, _, _, childCodes @ _*) => {
        val changedNodes = childCodes.map {
          case <Position>{ values @ _* }</Position> => {
            <Position Latitude={newlatitude} Longitude={newlongitude}  Radius={oldRadius} Zoom={oldZoom}>{values}</Position>
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }

  }


}