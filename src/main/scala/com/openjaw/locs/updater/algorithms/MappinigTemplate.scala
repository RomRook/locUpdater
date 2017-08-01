package com.openjaw.locs.updater.algorithms

import com.openjaw.connectors.{UTSConnector}
import com.openjaw.locs.updater.DefaultHierarchyHelper
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq, XML}


case class MappingTemplate(locHelper: DefaultHierarchyHelper, config: Config) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("Countries"))

  //DefaultHierarchy locations
  val countries = locHelper.countries

  //helper maps
  val defaultHierarchyLookup = getDefaultHierarchyKeyLookup()

  //external systems
  val utsConnector = new UTSConnector(config.utsEndpoint, config.utsUser, config.utsPassword)

  //function's alias
  def extract(n: Node) = locHelper.extractLocation(n)

  override def run() = {

    val toBeProcessed = utsConnector.getCountries()
    logger.info(s"Number of countries from UTS: ${toBeProcessed.size}")
    logger.info(s"toBeProcessed: ${toBeProcessed.size}")

    val (alreadyDefined, toBeAdded) = toBeProcessed.partition(isAlreadyDefined)
    logger.info(s"alreadyDefined: ${alreadyDefined.size}")
    logger.info(s"toBeAdded: ${toBeAdded.size}")

    val mappingTemplate = createTemplate(alreadyDefined, toBeAdded)
    saveTemplate(<Countries>{mappingTemplate}</Countries>)

    <Raport>
      <Countries currentCount={countries.size.toString} toBeProcessedCount={toBeProcessed.size.toString} toBeAddedCount={toBeAdded.size.toString}>
        {mappingTemplate}
      </Countries>
      <Config> {config.xml}</Config>
    </Raport>
  }

  def getDefaultHierarchyKeyLookup() = {
    logger.info("About to populate map that contains key: CountryName, value: RentalCarsLocationInfo")
    val keyLookup = collection.mutable.Map.empty[String, LocationInfo]
    countries.foreach(n => {
      val locationInfo = locHelper.extractLocation(n)
      val generalInfo = locationInfo.generalInfo
      val locationNameEN = generalInfo.locationNameEN
      val locationType = generalInfo.locationType

      val key = Countries.getKey(locationNameEN)
      if (keyLookup.isDefinedAt(key)) {
          logger.error(s"Duplicated location for key '$key' " +
            s"current:[locationName: $locationNameEN, locationType: $locationType] " +
            s"alreadyInMap:[${keyLookup(key).generalInfo.locationNameEN}, ${keyLookup(key).generalInfo.locationType}]")
       } else
       {
          keyLookup.put(key, locationInfo)
       }

      })
    logger.info(s"Map was populated and its size: ${keyLookup.size}")
    keyLookup.toMap
  }

  def isAlreadyDefined(supplierNode: Node) = {
    val country = supplierNode.text.trim
    val key = Countries.getKey(country)
    //check if country from supplier is already defined in DefaultHierarchy
    defaultHierarchyLookup.isDefinedAt(key)
  }

  def saveTemplate(xml: Node) = {
    val name = "uts_countries_mappings_template.xml"
    val printer = new scala.xml.PrettyPrinter(500, 2)
    val outDir = config.outDir
    XML.save(filename = {outDir + name}, XML.loadString(printer.format(xml)), "UTF-8")
    logger.info(s"File '$name' was saved in directory: $outDir")
  }

  def createTemplate(alreadyDefined: NodeSeq, toBeAdded: NodeSeq) = {
    val alreadyDefinedList = alreadyDefined.map(n => {
      val country = n.text.trim
      val ustCode = n.attribute("id").get
      val key = Countries.getKey(country)
      val location = defaultHierarchyLookup(key)
      <Country LocationId={location.generalInfo.locationId}
               LocationType={location.generalInfo.locationType}
               CurrentName={location.generalInfo.locationNameEN}
               ISOCountryCode={location.codesInfo.isoCountryCode}
               StateCode={location.codesInfo.stateCodeInUSA}
               CityMergeType="city"
               UtsCode={ustCode}>{country}</Country>
    }).sortBy(n => n.text)
    val toBeAddedList = toBeAdded.map(n => {
      val country = n.text.trim
      val ustCode = n.attribute("id").get
      <Country Ignored="true"
               LocationId=""
               LocationType=""
               CurrentName=""
               ISOCountryCode=""
               Continent=""
               TZ=""
               CityMergeType=""
               StateCode=""
               UtsCode={ustCode}>{country}</Country>
    }).sortBy(n => n.text)
    (alreadyDefinedList ++ toBeAddedList)
  }

}
