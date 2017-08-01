package com.openjaw.locs.updater.algorithms

import java.io.File

import com.openjaw.connectors.{DistributorConnector, UTSConnector}
import com.openjaw.locs.updater.DefaultHierarchyHelper
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq, XML}


case class Countries(locHelper: DefaultHierarchyHelper, config: Config) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("Countries"))

  //DefaultHierarchy locations
  val locations = locHelper.locationNodes
  val countries = locHelper.countries
  val continents = locHelper.continents

  //helper maps
  val locationLookup = locHelper.locationLookup

  //Name of continents provided by S7 mapped to xLocation codes
  var continentLookup: Map[String, String] = Map(
    "Europe" -> "CONT_EUROPE",
    "Carribbean" -> "CONT_CARIBBEAN",
    "Asia" -> "CONT_ASIA",
    "Indian Ocean" -> "CONT_INDIANOCEA",
    "Africa" -> "CONT_AFRICA",
    "Oceania" -> "CONT_AUSTRALASI",
    "North America" -> "CONT_NORTHAMERI",
    "South America" -> "CONT_SouthCent")

  logger.info("continentLookup size: " + continentLookup.size)
  logger.info("continentLookup: " + continentLookup.toString())

  val utsConnector = new UTSConnector(config.utsEndpoint, config.utsUser, config.utsPassword)

  //function's alias
  def extract(n: Node) = locHelper.extractLocation(n)

  override def run() = {
    val toBeProcessed = utsConnector.getCountries()
    logger.info(s"toBeProcessed: ${toBeProcessed.size}")

    val defaultHierarchyLookup = Countries.getDefaultHierarchyKeyLookup(countries,extract)

    val (alreadyDefined, toBeAdded) = toBeProcessed.partition(n => Countries.isAlreadyDefined(n,defaultHierarchyLookup))
    logger.info(s"alreadyDefined: ${alreadyDefined.size}")
    logger.info(s"toBeAdded: ${toBeAdded.size}")

    val fileWithMapping = config.inDir + Countries.COUNTRY_MAPPING_FILENAME
    val countriesMappings = Countries.userDefinedMapping(skipIgnored = true, fileWithMapping)

    val toBeAddedXML  = getCountriesToBeAdded(toBeAdded, countriesMappings)
    //val toBeAddedXML  = <Empty></Empty>

    //for each new country its parent (continent) should be updated
    //val parentsToBeUpdatedXML = getContinentsToBeUpdated(toBeAddedXML)
    val parentsToBeUpdatedXML = <Empty></Empty>

    val (toBeAddedRQ, continentsToBeUpdatedRQ) = Countries.generateRequests(toBeAddedXML,parentsToBeUpdatedXML)

    config.mode match   {
      case Mode.SaveToFile => {
        Countries.saveToFiles(config,toBeAddedRQ, continentsToBeUpdatedRQ)
      }
      case Mode.SendRequest => {
        Countries.saveToFiles(config,toBeAddedRQ, continentsToBeUpdatedRQ)
        Countries.sendRequests(toBeAddedRQ, continentsToBeUpdatedRQ)
      }
    }

    //generate summary report
    val toBeAddedList = toBeAddedXML.map(n => locHelper.extractLocation(n).generalInfo.locationNameEN)
    val parentsToBeUpdatedList = parentsToBeUpdatedXML.map(n => locHelper.extractLocation(n).generalInfo.locationNameEN)
    <Raport>
      <Countries currentCount={countries.size.toString} toBeProcessedCount={toBeProcessed.size.toString} toBeAddedCount={toBeAdded.size.toString}>
        <ToBeAdded>{toBeAddedList.sorted.map(item => <Country>{item}</Country>)}</ToBeAdded>
      </Countries>
      <Parents>
        <ToBeUpdated>{parentsToBeUpdatedList.sorted.map(item => <Location>{item}</Location>)}</ToBeUpdated>
      </Parents>
      <Config> {config.xml}</Config>
    </Raport>
  }

  def getCountriesToBeAdded(list: NodeSeq, countryMap: collection.mutable.LinkedHashMap[String,UTSCountryInfo]) = {
    val result = list
      .filter(n => {
        val countryName = n.text.trim
        val key = Countries.getKey(countryName)
        if (countryMap.isDefinedAt(key)) {
          //if locationId is empty then create new definition for this country
          val locationId =  countryMap(key).locationId
          (locationId.length == 0)
        } else
          false
      })
      .map(n => {
        val countryName = n.text.trim
        val key = Countries.getKey(countryName)
        val userDefinedData = countryMap(key)
        val continentName = userDefinedData.continent
        val continentId = Countries.getContinentId(continentName,countryName)
        val isoCountryCode = userDefinedData.isoCountryCode
        val timeZone = userDefinedData.tz
        Countries.newDefinition(countryName, continentId, isoCountryCode, timeZone)
      })
    result
  }
}

object Countries {

  val logger = Logger(LoggerFactory.getLogger("Countries"))

  val COUNTRY_MAPPING_FILENAME = "uts_countries_mappings.xml"

  //Name of continents from DefaultHierarchy
  var continentLookup: Map[String, String] = Map(
    "Europe" -> "CONT_EUROPE",
    "Carribbean" -> "CONT_CARIBBEAN",
    "Asia" -> "CONT_ASIA",
    "Indian Ocean" -> "CONT_INDIANOCEA",
    "Africa" -> "CONT_AFRICA",
    "Oceania" -> "CONT_AUSTRALASI",
    "North America" -> "CONT_NORTHAMERI",
    "South America" -> "CONT_SouthCent")
  logger.info("continentLookup size: " + continentLookup.size)
  logger.info("continentLookup: " + continentLookup.toString())

  def userDefinedMapping(skipIgnored: Boolean = true, file: String) = {
    //val lookup = collection.mutable.Map.empty[String, RentalCarsLocationInfo]
    val lookup = collection.mutable.LinkedHashMap.empty[String, UTSCountryInfo]

    val temp = new File(file)
    if (!temp.exists)
      throw new RuntimeException(s"File '$file' not exists, please prepare file with country mappings and try again")

    val xml = XML.loadFile(file)
    val nodes = (xml \\ "Country")
    logger.info(s"Number of countries from file: ${nodes.size}")

    nodes.sortBy(_.text.trim).foreach(n => {
      val country = n.text.trim
      val key = getKey(country)
      if (lookup.isDefinedAt(key))
        logger.error(s"Duplicated country in file for key '$key'")
      else {
        val ignored = (n \ "@Ignored").text
        val utsCode = (n \ "@UtsCode").text
        val isoCountryCode = (n \ "@ISOCountryCode").text
        val locationId = (n \ "@LocationId").text
        val locationType = (n \ "@LocationType").text
        val continent = (n \ "@Continent").text
        val tz = (n \ "@TZ").text
        val locationNameEN = (n \ "@CurrentName").text
        val stateCodeInUSA = (n \ "@StateCode").text
        val cityMergeType = (n \ "@CityMergeType").text

        if (skipIgnored && !(ignored == "true")) {
          val utsCountryInfo = UTSCountryInfo(locationId,locationType,locationNameEN,continent, tz, isoCountryCode,stateCodeInUSA,cityMergeType, ignored, utsCode, utsCountryName = country)
          lookup.put(key, utsCountryInfo)
        }
      }
    })
    logger.info(s"Map was populated and its size:  ${lookup.size}")
    lookup
  }

  def getKey(countryName: String) = {
    countryName.trim
  }

  def newDefinition(countryName: String, continentId: String, isoCountryCode: String, timeZone: String) = {
    val countryId = "COUNTRY_" + isoCountryCode
    <Location OnSale="false" ignoreDiff="true" TZ={timeZone} HideName="false" LocationType="country" Version="1" Searchable="true" IndexNumber="5" Id={countryId}>
      <ParentLocations>
        <LocationReference Index="2" Id={continentId}>
        </LocationReference>
      </ParentLocations>
      <Names>
        <Name AjaxString={countryName} Value={countryName} Language="en" AlternativeSpelling="FALSE">
        </Name>
      </Names>
      <Codes>
        <Code Value={isoCountryCode} Context="ISOCountryCode"></Code>
      </Codes>
      <Capabilities>
        <Capability Type="hotel" Value=" "/>
      </Capabilities>
    </Location>
  }

  def generateRequests(toBeAdded: NodeSeq, continentsToBeUpdated: NodeSeq ) = {
    logger.info("Preparing requests to be sent to xDist ")
    val toBeAddedRQ = DistributorConnector.createLocationHierarchyRQ(<Locations>{toBeAdded}</Locations>)
    val continentsToBeUpdatedRQ = DistributorConnector.createLocationHierarchyRQ(<Locations>{continentsToBeUpdated}</Locations>)

    (toBeAddedRQ,continentsToBeUpdatedRQ)
  }

  def saveToFiles(config: Config, toBeAddedRQ: Node,continentsToBeUpdatedRQ: Node ) = {
    logger.info("About to save files which contain xDist request")
    val outDir = config.outDir
    XML.save(filename = {outDir + "toBeAddedRQ_Countries.xml"}, toBeAddedRQ, "UTF-8")
    XML.save(filename = {outDir + "toBeUpdatedRQ_Continents.xml"}, continentsToBeUpdatedRQ, "UTF-8")
    logger.info("Files were saved in directory " + outDir)
  }

  def sendRequests(toBeAddedRQ: Node, continentsToBeUpdatedRQ: Node ) = {
    throw new RuntimeException("sendRequests with countries not implemented!")
  }

  def getDefaultHierarchyKeyLookup(countries: NodeSeq, extract: (Node) => LocationInfo) = {
    logger.info("About to populate map that contains key: CountryName, value: RentalCarsLocationInfo")
    val keyLookup = collection.mutable.Map.empty[String, LocationInfo]
    countries.foreach(n => {
      val locationInfo = extract(n)
      val generalInfo = locationInfo.generalInfo
      val locationNameEN = generalInfo.locationNameEN
      val locationType = generalInfo.locationType

      val key = getKey(locationNameEN)
      if (keyLookup.isDefinedAt(key)) {
        logger.error(s"Duplicated location for key '$key' " +
          s"current:[locationName: $locationNameEN, locationType: $locationType] " +
          s"alreadyInMap:[${keyLookup(key).generalInfo.locationNameEN}, ${keyLookup(key).generalInfo.locationType}]")
      } else
      {
        keyLookup.put(key, locationInfo)
      }
    })
    logger.info(s"Map was populated and its size:  ${keyLookup.size}")
    keyLookup.toMap
  }


  def isAlreadyDefined(supplierNode: Node, defaultHierarchyLookup: Map[String, LocationInfo]) = {
    val country = supplierNode.text.trim
    val key = getKey(country)
    //check if country from supplier is already defined in DefaultHierarchy
    defaultHierarchyLookup.isDefinedAt(key)
  }

  def getContinentId(continentName: String, countryName: String) = {
    if (continentName == "")
      throw new RuntimeException(s"Provide continent for country '$countryName'")

    if (!continentLookup.isDefinedAt(continentName))
      throw new RuntimeException(s"There is no such continent: '$continentName'")

    continentLookup(continentName)
  }

  def isLocationIdEmpty(node: Node) = {
    val locationId = (node \ "@LocationId").text
    val a = locationId.length
    (locationId.length == 0)
  }

  def isIgnored(node: Node) = {
    val ignored = (node \ "@Ignored").text
    val result = if (ignored == "false") false else true
    result
  }

  //param might be passed as 'PL|DE|RU' (pipe separates each iso code)
  def getList(inputParam: String) = {
    val isoCountryCodeList = {
      if (inputParam.isEmpty) {
        logger.warn(s"all countries will be processed")
        List.empty[String]
      }
      else {
        val list = inputParam.split("\\|").toList
        logger.info(s"Number of countries to be processed: ${list.size}")
        list
      }
    }
    logger.info(s"isoCountryCodeList: $isoCountryCodeList")
    isoCountryCodeList
  }

}



