package com.openjaw.locs.updater.algorithms

import com.openjaw.connectors.UTSConnector
import com.openjaw.locs.updater.{DefaultHierarchyHelper, UtsHelper}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq, XML}


case class CitiesFromUTS(locHelper: DefaultHierarchyHelper, config: Config) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("CitiesFromUTS"))

  private val KEY_TYPE_CITY_AND_STATE = "city#state"
  private val KEY_TYPE_CITY_ONLY = "city"

  private val LOCATION_TYPE_COUNTRY = "country"
  private val LOCATION_TYPE_CITY = "city"
  private val LOCATION_TYPE_AIRPORT = "airport"

  val locations = locHelper.locationNodes
  val countries = locHelper.countries
  val cities = locHelper.cities
  val airports = locHelper.airports

  val mapLocationToItsCountry = locHelper.getLocationToItsCountryMap
  val locationLookup = locHelper.locationLookup
  //val defaultHierarchyKeyLookup = Map("id" -> LocationInfo("","","","","","",config.xml ))
  //val defaultHierarchyKeyLookup = getDefaultHierarchyKeyLookup
  //logger.info(s"Number of keys based on DefaultHierarchy: ${defaultHierarchyKeyLookup.size}")

  val utsConnector = new UTSConnector(config.utsEndpoint, config.utsUser, config.utsPassword)
  val utsCountries = utsConnector.getCountries()
  logger.info(s"Number of countries from UTS: ${utsCountries.size}")
  //val utsCities = utsConnector.getCities()
  //logger.info(s"Number of cities from UTS: ${utsCities.size}")

  //logger.info(s"Saving requests to files")
  //val outDir = config.outDir
  //XML.save(filename = {outDir + "UTS_cites"}, <Cities>{utsCities}</Cities>, "UTF-8")

  //val distributorConnector = DistributorConnector(config.distributorEndpoint , config.distributorTimeout.toInt)

  def extract(n: Node) = locHelper.extractLocationInfoUTS(n)
  def extractUTS(n: Node) = UtsHelper.extractSupplierInfo(n)

//  val (mapUTSCityToLocationId, mapLocationIdToUtsCity) = locHelper.populateMapsUtsCityCode
//  logger.info(s"mapUTSCityToLocationId.size: ${mapUTSCityToLocationId.size}")
//  logger.info(s"mapLocationIdToUtsCity.size: ${mapLocationIdToUtsCity.size}")
//
//  val (mapUTSCountryToLocationId, mapLocationIdToUtsCountry) = locHelper.populateMapsUtsCountryCode()
//  logger.info(s"mapUTSCountryToLocationId.size: ${mapUTSCountryToLocationId.size}")
//  logger.info(s"mapLocationIdToUtsCountry.size: ${mapLocationIdToUtsCountry.size}")

  //todo: refactor this code below
  //Main.populateMaps(locHelper)

  override def run() = {


    val isoCountryCode = config.countyIsoCode
    logger.info(s"Provided param for ISOCountryCode: $isoCountryCode")
    val isoCountryCodeList = isoCountryCode.split("\\|").toList
    logger.info(s"Countries to be processed: ${isoCountryCodeList.toString}")

    logger.info(isoCountryCodeList.toString())

    if (isoCountryCode.isEmpty)
      logger.warn(s"all countries will be processed")

    //function
    verifyCountries

    val raportXML =  <Raport>{countries
                        .sortBy(extract(_)._3) //sort by countryName
                        .filter(country => {
                            val currentIsoCode = extract(country)._6
                            (isoCountryCodeList.contains(currentIsoCode) || isoCountryCode.isEmpty)
                         })
                        .map(country => {
                            val(locationIdCountry, _, locationNameCountry, _, utsCodeCountrySeq, isoCodeCountry, _, _, _) = extract(country)
                            logger.info(s"Country: $locationNameCountry")

    val citiesAlreadyDefined = cities.filter(node => {
                                val isoCode = extract(node)._6
                                (isoCodeCountry == isoCode)})
    logger.info("citiesAlreadyDefined size: " + citiesAlreadyDefined.size)

    val airportsAlreadyDefined = airports.filter(node => {
        val isoCode = extract(node)._6
        (isoCodeCountry == isoCode)})
    logger.info("airportsAlreadyDefined size: " + airportsAlreadyDefined.size)

    val toBeProcessed = getToBeProcessed(utsCodeCountrySeq)
    logger.info("toBeProcessed: " + toBeProcessed.size)

    if (toBeProcessed.size == 0) {
      logger.error("Empty response from supplier")
      throw new RuntimeException("ERROR: Empty response from supplier!")
    }

    //val defaultHierarchyKeyLookup = getDefaultHierarchyKeyLookup(isoCodeCountry)

      //val defaultHierarchyLookup = getDefaultHierarchyKeyLookup(cities, locHelper.extractLocation, isoCountryCode, stateCodeInUSA, LOCATION_TYPE_CITY, keyType)


    verifyCities(toBeProcessed, citiesAlreadyDefined ++ airportsAlreadyDefined, utsCodeCountrySeq)


    //logger.debug(s"Validate location to be processed")
    //toBeProcessed.foreach(validateUTSNode)

    val toBeAdded = <Empty></Empty>
    //val toBeAdded = getToBeAdded(toBeProcessed, locationNameCountry)
    //logger.info("toBeAdded: " + toBeAdded.size)

    val parentsMap = collection.mutable.Map.empty[String, List[String]]
    //populateMap(toBeAdded, parentsMap)
    //logger.info("parentsMap: " + parentsMap.size)

    //val toBeUpdated = getToBeUpdated(toBeProcessed, locationNameCountry)
    val toBeUpdated = <Empty></Empty>
    logger.info("toBeUpdated: " + toBeUpdated.size)

    //populateMap(toBeUpdated, parentsMap)
    //logger.info("parentsMap: " + parentsMap.size)

      val parentsToBeUpdated  = <Empty></Empty>
      //val parentsToBeUpdated = getParentsToBeUpdated(parentsMap, toBeAdded, toBeUpdated )
    //logger.info("parentsToBeUpdated: " + parentsToBeUpdated.size)

//    val (toBeAddedRQ, toBeUpdatedRQ, parentsToBeUpdatedRQ) = generateRequests(toBeAdded,toBeUpdated,parentsToBeUpdated)

      config.mode match   {
      case Mode.SaveToFile => {
//        saveToFiles(toBeAddedRQ, toBeUpdatedRQ, parentsToBeUpdatedRQ, isoCodeCountry)
      }
      case Mode.SendRequest => {
//        saveToFiles(toBeAddedRQ, toBeUpdatedRQ, parentsToBeUpdatedRQ, isoCodeCountry)
//        sendRequests(toBeAdded,toBeUpdated,parentsToBeUpdated)
      }
    }

    <Country locationName={locationNameCountry} locationId={locationIdCountry} utsCode={utsCodeCountrySeq.mkString(",")} isoCode={isoCodeCountry}>
      <Cities currentCount={citiesAlreadyDefined.size.toString} toBeProcessedCount={toBeProcessed.size.toString} toBeAddedCount={toBeAdded.size.toString} toBeUpdatedCount={toBeUpdated.size.toString}>
        <ToBeAdded>{toBeAdded
          .sortBy(n => extract(n)._3)
          .map( n => {
          val(locationId, locationType, locationNameEN, locationNameRU, ustCode, _, _, _, _) = extract(n)
          <City locationId={locationId} locationType={locationType} nameRU={locationNameRU} ustCode={ustCode.mkString}>{locationNameEN}</City>
          })
         }</ToBeAdded>
        <ToBeUpdated>{toBeUpdated
          .sortBy(n => extract(n)._3)
          .map( n => {
          val(locationId, locationType, locationNameEN, locationNameRU, ustCode, _, _, _, _) = extract(n)
          <City locationId={locationId} locationType={locationType} nameRU={locationNameRU} ustCode={ustCode.mkString}>{locationNameEN}</City>
          })}
        </ToBeUpdated>
      </Cities>
      <Parents>
        <ToBeUpdated>{parentsToBeUpdated
          .sortBy(n => extract(n)._3)
          .map( n => {
          val(locationId, locationType, locationNameEN, locationNameRU, ustCode, _, _, _, _) = extract(n)
          <Location locationId={locationId} locationType={locationType} nameRU={locationNameRU} ustCode={ustCode.mkString}>{locationNameEN}</Location>
          })}
        </ToBeUpdated>
        <ParentsMap  count={parentsMap.size.toString()}>{parentsMap.map(item =>{
           val parentId = item._1
           val chiledList = item._2.mkString(",")
           <Parent locationId={parentId}>{chiledList}</Parent>
        }
        )}
        </ParentsMap>
      </Parents>
    </Country>
    })}
   <Config> {config.xml}</Config>
  </Raport>

  raportXML
  }

  private def getToBeProcessed(utsCodeCountrySeq: Seq[String]) = {

    logger.info(s"UtsCountryCodes: ${utsCodeCountrySeq.mkString(",")}")

    var result: NodeSeq = Nil
      utsCodeCountrySeq.foreach(utsCode => {
        logger.info(s"About to get cities from UtsCountryCode: $utsCode")
        val cities = utsConnector.getCities(utsCode)
        logger.info(s"Supplier returned ${cities.size} cities")
        result = cities ++ result
      })
    result
  }


//  private def getToBeAdded(list: NodeSeq, locationNameCountry: String) = {
//    list.filterNot(city => isUtsCityCodeAlreadyDefined(city))
//        .map(city => newDefinition(city, locationNameCountry))
//  }


//  private def getToBeUpdated(list: NodeSeq, locationNameCountry: String) = {
//    list
//      .filter(city => isCityDefined(city))
//      .map(city => {
//        val (_, utsCityNameRU, ustCodeCity, utsCountryCode, cityName, stateInUSA) = extractUTS(city)
//        val key = getKey(cityName, utsCountryCode, stateInUSA)
//        val cityNode = getLocationNodeByKey(key)
//        val ajaxStringRU = utsCityNameRU + ", " + locationNameCountry
//        val changed_1 = locHelper.addUTSCode(cityNode, ustCodeCity)
//        val changed_2 = locHelper.updateLocationAttr(changed_1)
//        val changed_3 = locHelper.removeNameRU(changed_2)
//        val changed_4 = locHelper.addNameRUIfNotDefined(changed_3, utsCityNameRU, ajaxStringRU)
//        locHelper.addHotelCapability(changed_4)
//    })
//  }

  private def getParentsToBeUpdated(parentsMap: collection.mutable.Map[String, List[String]], listToBeAdded: NodeSeq, listToBeUpdated: NodeSeq)  = {
    //parent node might be already updated
    def getRecentlyModified(id: String, node: Node) = {
      var result = Seq.empty[Node]
      result = listToBeAdded.filter(node => {
          val locationId = (node \ "@Id").text
          (locationId == id)
         })
      if (result.size == 0) {
        result = listToBeUpdated.filter(node => {
          val locationId = (node \ "@Id").text
          (locationId == id)
        })
      }
      if (result.size == 0) {
        result = node
      }
       result.head
    }


    //find parent node
    locations
      .filter(node => {
          val locationId = (node \ "@Id").text
          parentsMap.isDefinedAt(locationId)
        })
      .map(node => {
          val locationId = (node \ "@Id").text
          val recentlyModifiedNode = getRecentlyModified(locationId, node)
          val updatedNode = locHelper.updateLocationAttr(recentlyModifiedNode)
          locHelper.addMultipleChildLocation(originalXML = updatedNode, childCodeList = parentsMap(locationId))
      })
  }


//  private def getLocationNodeByKey(key: String) = {
//    if (!defaultHierarchyKeyLookup.isDefinedAt(key)) {
//      val message = s"No location in map: defaultHierarchyKeyLookup for key: $key"
//      logger.error(message)
//      throw new Exception(message)
//    }
//    else {
//      defaultHierarchyKeyLookup(key).node
//    }
//  }

  private def populateMap(list: NodeSeq, currentParentsMap: collection.mutable.Map[String, List[String]]) = {
    list.foreach(city => {

      val parentLocation = (city \\ "ParentLocations" \ "LocationReference").head
      val parentId = (parentLocation \ "@Id").text

      val cityId = (city \ "@Id").text

      if (parentId == "") {
        logger.error(s"Parent not found for location [locationNode: $city]")
      }

      if (currentParentsMap.isDefinedAt(parentId)) {
        val currentList = currentParentsMap(parentId)
        currentParentsMap.put(parentId, (cityId :: currentList))
      } else {
        currentParentsMap.put(parentId,List(cityId))
      }
    })
  }



//  private def isUtsCityCodeAlreadyDefined(supplierNode: Node) = {
//    // size == 0
//    def validate(size: Int) = {
//
//    }
//
//    val (utsCityNameEN, _, ustCodeCity, utsCountryCode, cityName, stateInUSA) = extractUTS(supplierNode)
//
//    val locationIdSeq = mapUTSCityToLocationId.get(ustCodeCity)
//
//    val result = locationIdSeq match {
//      case Some(seq) => {
//        if (seq.size > 1) {
//          logger.error(s"utsCityCode: $ustCodeCity defined multiple times for locations: ${locationIdSeq.mkString(",")}} ]")
//          true
//        }
//        else {
//          val foundLocationNameEN = locationLookup(seq.head).locationNameEN
//          val foundLocationId = locationLookup(seq.head).locationId
//          if (utsCityNameEN.trim.toUpperCase() == foundLocationNameEN.trim.toUpperCase()) {
//            true
//          } else {
//            logger.error(s"Found location which has different name [ustName: $utsCityNameEN, ustCodeCity: $ustCodeCity, utsCountryCode: $utsCountryCode,  foundLocationName: $foundLocationNameEN, foundLocationId: $foundLocationId]")
//            false
//          }
//        }
//      }
//      case None => false
//    }
//
//    result
//  }


//  private def Duplicated location(supplierNode: Node) = {
//    val(utsCityNameEN, _, ustCodeCity, utsCountryCode, cityName, stateInUSA) = extractUTS(supplierNode)
//
//    val key = getKey(cityName, utsCountryCode, stateInUSA)
//
//    val result = if (!defaultHierarchyKeyLookup.isDefinedAt(key)) {
//      logger.error(s"No location defined in map: defaultHierarchyKeyLookup for key: $key [supplierNode: ${supplierNode.toString()}]")
//      false
//    } else {
//      val locationInfo = defaultHierarchyKeyLookup(key)
//      val foundLocationId = locationInfo.locationId
//      val foundLocationName = locationInfo.locationName
//      if (utsCityNameEN.trim.toUpperCase() == foundLocationName.trim.toUpperCase()) {
//        true
//      } else {
//        logger.error(s"Found location which has different name [ustName: $utsCityNameEN, ustCodeCity: $ustCodeCity, utsCountryCode: $utsCountryCode,  foundLocationName: $foundLocationName, foundLocationId: $foundLocationId]")
//        false
//      }
//    }
//    result
//  }



  private def newDefinition(supplierNode: Node,countryName: String ) = {
    <Location></Location>
    }

   private  def getCityId(cityName: String, isoCode: String, utsCode: String ) = {
      val city = cityName.replaceAll( "\\W", "")
      val name = {if (city.length > 4)
        city.substring(0,4).trim
      else
        city}.toLowerCase.capitalize

      //S7_XLOCATION"."HIERARCHY_ROWS"."LOCATION_ID" maximum: 16
      val cityId = "Cit" + isoCode.toLowerCase.capitalize + name + utsCode
      if (cityId.length > 16)
        throw new Exception("Maximum length for locationId is 16 characters, current length is " + cityId.length)
      else
        cityId
    }



  def getKey(keyType: String, city: String, state: String ) = {
    val key = if (keyType == KEY_TYPE_CITY_AND_STATE) {
      s"${city.trim}#${state.trim}"
    } else {
      city.trim
    }
    key
  }

  def getCityNameAndState(supplierName: String) = {
    // for country: 'USA - Other' there is also stateCode provided in city name, e.g.
    //    <City>Augusta, GA</City>
    //    <City>Augusta, KS</City>
    //    <City>Augusta, ME</City>
    //    <City>Augusta, NJ</City>
    val list = supplierName.split(",")
    val city = if (list.isDefinedAt(0)) list(0).trim else ""
    val state = if (list.isDefinedAt(1)) list(1).trim else ""

    (city, state)

  }


//  private def getKey(locationNameCity: String, ustCodeCountry: String, stateInUSA: String ) = {
//      if (ustCodeCountry == "138" && stateInUSA != "")
//        (locationNameCity.toUpperCase + "#"  + ustCodeCountry + "#" + stateInUSA.toUpperCase)
//      else
//        (locationNameCity.toUpperCase + "#"  + ustCodeCountry)
//    }

//  private def generateRequests(toBeAdded: NodeSeq, toBeUpdated: NodeSeq, parentsToBeUpdated: NodeSeq) = {
//    logger.info("Preparing requests to be sent to xDist")
//    val xDist = distributorConnector
//    val toBeAddedRQ = xDist.createLocationHierarchyRQ(<Locations>{toBeAdded}</Locations>)
//    val toBeUpdatedRQ = xDist.createLocationHierarchyRQ(<Locations>{toBeUpdated}</Locations>)
//    val parentsToBeUpdetedRQ = xDist.createLocationHierarchyRQ(<Locations>{parentsToBeUpdated}</Locations>)
//
//    (toBeAddedRQ, toBeUpdatedRQ, parentsToBeUpdetedRQ)
//  }
//
//  private def saveToFiles(toBeAddedRQ: Node, toBeUpdatedRQ: Node,parentsToBeUpdatedRQ: Node, countryCode: String) = {
//    logger.info(s"Saving requests to files")
//    val outDir = config.outDir
//    val prefix = "Requests"
//    val suffix = s"_${countryCode}.xml"
//    XML.save(filename = {outDir + prefix + "ToBeAddedRQ" + suffix}, toBeAddedRQ, "UTF-8")
//    XML.save(filename = {outDir + prefix + "ToBeUpdatedRQ" + suffix}, toBeUpdatedRQ, "UTF-8")
//    XML.save(filename = {outDir + prefix + "ParentsToBeUpdated" + suffix}, parentsToBeUpdatedRQ, "UTF-8")
//  }
//
//  private def sendRequests(toBeAdded: NodeSeq, toBeUpdated: NodeSeq,parentsToBeUpdeted: NodeSeq) = {
//    val xDist = distributorConnector
//    toBeAdded.foreach(n => {
//      val info = extract(n)
//      val locationId = info._1
//      val locationName = info._3
//      logger.info(s"city add request for $locationName [$locationId]")
//      val req = xDist.createLocationHierarchyRQ(<Locations>{n}</Locations>)
//      xDist.sendSOAP(req)
//    })
//
//    toBeUpdated.foreach(n => {
//      val info = extract(n)
//      val locationId = info._1
//      val locationName = info._3
//      logger.info(s"city update request for $locationName [$locationId]")
//      val req = xDist.createLocationHierarchyRQ(<Locations>{n}</Locations>)
//      xDist.sendSOAP(req)
//    })
//
//    parentsToBeUpdeted.foreach(n => {
//      val info = extract(n)
//      val locationId = info._1
//      val locationName = info._3
//      logger.info(s"parent's city update request for $locationName [$locationId]")
//      val req = xDist.createLocationHierarchyRQ(<Locations>{n}</Locations>)
//      xDist.sendSOAP(req)
//    })
//
//  }


  def getDefaultHierarchyKeyLookup(locations: NodeSeq,
                                   extractInfoFunction: (Node) => LocationInfo,
                                   isoCountryCode: String,
                                   stateCodeInUSA: String,
                                   locationType: String,
                                   keyType: String) = {
    logger.info(s"About to populate map for isoCountry='$isoCountryCode' that contains [key: $locationType, value: RentalCarsLocationInfo]")
    val keyLookup = collection.mutable.Map.empty[String, LocationInfo]
    locations
      .filter(n => {
        val currentLocationType = extractInfoFunction(n).generalInfo.locationType
        val currentIsoCode = extractInfoFunction(n).codesInfo.isoCountryCode
        val currentStateCodeInUSA = extractInfoFunction(n).codesInfo.stateCodeInUSA
        ((currentLocationType == locationType)
          && (currentIsoCode == isoCountryCode)
          && (stateCodeInUSA == "" || currentStateCodeInUSA == stateCodeInUSA)
          )
      })
      .foreach(n => {
        val generalInfo = extractInfoFunction(n).generalInfo
        val codesInfo = extractInfoFunction(n).codesInfo

        val (city, state) = (generalInfo.locationNameEN, codesInfo.stateCodeInUSA)

        val key = getKey(keyType, city, state)
        val locationInfo = LocationInfo(generalInfo,codesInfo)
          if (keyLookup.isDefinedAt(key)) {
            logger.error(s"Duplicated location for key '$key' " +
              s"current:[locationId: ${generalInfo.locationId}, locationName: ${generalInfo.locationNameEN}, locationType: ${generalInfo.locationType}, isoCountryCode: ${codesInfo.isoCountryCode}] " +
              s"alreadyInMap:[${keyLookup(key).generalInfo.locationId}, ${keyLookup(key).generalInfo.locationNameEN}, ${keyLookup(key).generalInfo.locationType}, ${keyLookup(key).codesInfo.isoCountryCode}]")
          } else   {
            keyLookup.put(key, locationInfo)
          }
      })
    logger.info(s"Map was populated and its size: ${keyLookup.size}")
    keyLookup.toMap
  }

//  def getDefaultHierarchyKeyLookup(isoCountryCode: String) = {
//    val keyLookup = collection.mutable.Map.empty[String, LocationInfoUTS]
//
//    val listNonCountryType = List("city","_region","_airport") // without 'airport'
//    //non country (uts city codes might be mapped to: city, airport, region
//    locations
//      .filter(n => {
//        val (locationId, locationType, _, _, _, isoCode, _, _, _) = locHelper.extractLocationInfoUTS(n)
//        (locationId != "" && locationType != "" && listNonCountryType.contains(locationType) && isoCode.contains(isoCountryCode))
//        })
//      .foreach(n => {
//        val (locationId, locationType, locationName, _, utsCityCodeSeq, isoCode, stateInUSA, _, _) = locHelper.extractLocationInfoUTS(n)
//        //for City there is always one exactly UTS code defined (countries might ave multiple UTS codes)
//        val ustCityCode = if (utsCityCodeSeq.size > 0) utsCityCodeSeq.head else ""
//
//        val countryId = mapLocationToItsCountry.get(locationId)
//        val countryNode = countryId match {
//        case Some(id) => {
//          if (id == "") {
//            logger.warn(s"Country not defined for locationId $locationId ")
//            <Location></Location>
//          }
//          else locationLookup(id).generalInfo.node
//        }
//        case None => {
//          logger.warn(s"Country not defined for locationId $locationId ")
//          <Location></Location>
//        }
//      }
//      val utsCountryCodeSeq = locHelper.extractLocationInfoUTS(countryNode)._5
//
//      utsCountryCodeSeq.foreach(code => {
//        val key = getKey(locationName, code, stateInUSA)
//        val locationInfo = LocationInfoUTS(locationId, locationType, locationName, ustCityCode, isoCode, stateInUSA, n)
//
//        if (keyLookup.isDefinedAt(key)) {
//          logger.error(s"Duplicated location for key '$key' " +
//            s"current:[locationName: $locationName, locationType: $locationType, ustCityCode: $ustCityCode, isoCode: $isoCode] " +
//            s"alreadyInMap:[${keyLookup(key).locationName}, ${keyLookup(key).locationType}, ${keyLookup(key).utsCode}, ${keyLookup(key).isoCode}]")
//
//        } else
//        {
//          keyLookup.put(key, locationInfo)
//        }
//      })
//    })
//    keyLookup.toMap
//  }


//  //list countries and its Uts codes
//  verificationLogger.info("--------------Country verification---------------")
//  verificationLogger.info(s"All countries defined in DefaultHierarchy: ${countries.size.toString}")
//  val emptyUtsCode = countries.filter(n => { val ustCodeSeq = extract(n)._5
//    ustCodeSeq.size == 0
//  }).map(extract(_)._3)
//
//  XML.save(
//    filename = {config.outDir + "CountriesWithEmptyUtsCode.xml" },
//    <Countries>{emptyUtsCode.sorted.map(item => <Country>{item}</Country>)}</Countries>,
//    "UTF-8"
//  )
//
//  verificationLogger.info(s"Countries without UTS code:")
//  emptyUtsCode.foreach(item => verificationLogger.info(item))
//
//  verificationLogger.info(s"-----------Countries from DefaultHierarchy and utsCode------------------")
//  val countriesMap = countries
//    .filter(n => { val ustCodeSeq = extract(n)._5
//    ustCodeSeq.size > 0})
//    .sortBy(n => { val ustCodeSeq = extract(n)._5
//    ustCodeSeq.head.toInt
//  }) //sort by ustCode
//    .map(n => {
//    val(locationIdCountry, _, locationNameCountry, _, utsCodeCountrySeq, isoCodeCountry, _) = extract(n)
//    val ustCodes = utsCodeCountrySeq.mkString(",")
//    val cuntryNode = <Country id={ustCodes}>{locationNameCountry}</Country>
//    verificationLogger.info(cuntryNode.toString())
//    cuntryNode
//  })
//
//  XML.save(filename = {config.outDir + "CountriesFromDefaultHierarchy.xml" }, <Countries>{countriesMap}</Countries>, "UTF-8")
//
//  verificationLogger.info(s"-----------Countries from supplier and utsCode------------------------")
//  val  ustCountriesFromUts = utsCountries
//    .sortBy(n => {val ustCode =  (n \ "@id").text
//    ustCode.toInt})
//    .map(n => {
//    verificationLogger.info(n.toString())
//    n
//  })
//
//  XML.save(filename = {config.outDir + "CountriesFromUts.xml" }, <Countries>{ustCountriesFromUts}</Countries>, "UTF-8")



  def verifyCountries = {
    //list countries and its Uts codes
    val emptyUtsCode = countries
      .filter(n => { val ustCodeSeq = extract(n)._5
                    ustCodeSeq.size == 0})
      .map(extract(_)._3)

    XML.save(
      filename = {config.outDir + "CountriesWithEmptyUtsCode.xml" },
      <Countries>{emptyUtsCode.sorted.map(item => <Country>{item}</Country>)}</Countries>,
      "UTF-8"
    )

    val countriesMap = countries
      .filter(n => { val ustCodeSeq = extract(n)._5
            ustCodeSeq.size > 0})
      .sortBy(n => { val ustCodeSeq = extract(n)._5
            ustCodeSeq.head.toInt
            }) //sort by ustCode
      .map(n => {
          val(_, _, locationNameCountry, _, utsCodeCountrySeq, _, _, _, _) = extract(n)
          val ustCodes = utsCodeCountrySeq.mkString(",")
          val cuntryNode = <Country id={ustCodes}>{locationNameCountry}</Country>
          cuntryNode
      })
    XML.save(filename = {config.outDir + "CountriesFromDefaultHierarchy.xml" }, <Countries>{countriesMap}</Countries>, "UTF-8")

    val ustCountriesFromUts = utsCountries.sortBy(n => {val ustCode =  (n \ "@id").text
                                                        ustCode.toInt})
    XML.save(filename = {config.outDir + "CountriesFromUts.xml" }, <Countries>{ustCountriesFromUts}</Countries>, "UTF-8")
  }

  def verifyCities(fromSupplier: NodeSeq, fromDefaultHierarchy: NodeSeq, utsCodeCountrySeq: Seq[String]) = {

    var fileName = ""

    val citiesFromUts = fromSupplier
      .sortBy(n => {val ustCode =  (n \ "@id").text
          ustCode.toInt})
      .map(n => { val name = n.text
                  val ustCityCode = (n \ "@id").text
                  val ustCountryCode = (n \ "@country").text
      <City country={ustCountryCode} id={ustCityCode}>{name}</City>
    })

    fileName = config.outDir + "CitiesFromUts_" + utsCodeCountrySeq.mkString("_") + ".xml"
    XML.save(fileName, <Cities>{citiesFromUts}</Cities>, "UTF-8")

    val emptyUtsCode = fromDefaultHierarchy
      .filter(n => { val ustCodeSeq = extract(n)._5
      ustCodeSeq.size == 0})
      .map(extract(_)._3)

    fileName = config.outDir + "CitiesWithEmptyUtsCode_" + utsCodeCountrySeq.mkString("_") + ".xml"
    XML.save(fileName , <Cities>{emptyUtsCode.sorted.map(item => <Citiy>{item}</Citiy>)}</Cities>, "UTF-8")

    val citiesFromDefaultHierarchy = fromDefaultHierarchy
      .filter(n => { val ustCodeSeq = extract(n)._5
          ustCodeSeq.size > 0})
      .sortBy(n => { val ustCodeSeq = extract(n)._5
          ustCodeSeq.head.toInt
      }) //sort by ustCode
      .map(n => {
        val(_, _, locationName, _, utsCodeCitySeq, _, _, _, _) = extract(n)
        val ustCodes = utsCodeCitySeq.mkString(",")
        val cuntryNode = <City country={utsCodeCountrySeq.mkString(",")} id={ustCodes}>{locationName}</City>
        cuntryNode
    })
    fileName = config.outDir + "CitiesFromDefaultHierarchy_" + utsCodeCountrySeq.mkString("_") + ".xml"
    XML.save(fileName, <Cities>{citiesFromDefaultHierarchy}</Cities>, "UTF-8")
  }



  /*

  def getUtsKeyLookup(list: NodeSeq) = {
    val keyLookup = collection.mutable.Map.empty[String, Node]
    list.foreach(n => {
      val (utsCityNameEN, _, ustCodeCity, utsCountryCode, cityName, stateInUSA) = extractUTS(n)
      val key = Main.getKey(utsCityNameEN, utsCountryCode, stateInUSA)

      if (keyLookup.isDefinedAt(key)) {
        logger.error(s"Duplicated location for key '$key' \n currentCity:\n $n \nfromMap:\n ${keyLookup(key)}")
      } else
      {
        keyLookup.put(key, n)
      }
    })
    keyLookup.toMap
  }


  def validateUTSNode(node: Node) = {

    val (utsCityNameEN, utsCityNameRU, ustCodeCity, utsCountryCode, cityName, stateInUSA) = extractUTS(node)

    logger.debug(s"\nValidate '$utsCityNameEN' utsCountry: '$utsCountryCode', ustCityCode: $ustCodeCity")
    val key = getKey(cityName, utsCountryCode, stateInUSA)
    if (defaultHierarchyKeyLookup.isDefinedAt(key)) {
      val locationInfo = defaultHierarchyKeyLookup(key)
      val foundLocationId = locationInfo.locationId
      val foundLocationName = locationInfo.locationName
      logger.debug(s"Found location for key: $key [ustName: $utsCityNameEN, ustCodeCity: $ustCodeCity, utsCountryCode: $utsCountryCode,  foundLocationName: $foundLocationName, foundLocationId: $foundLocationId]")
    }
    else {
      logger.debug(s"No location found in defaultHierarchyKeyLookup for key: $key [supplierNode: ${node.toString()}]")
    }

    val city = mapUTSCityToLocationId.getOrElse(ustCodeCity,"")
    val country = mapUTSCountryToLocationId.getOrElse(utsCountryCode,"")
    if (country == "" || city == "")
      logger.debug(s"No mappings for UTS: [utsCityNameEN: $utsCityNameEN, utsCountryCode: $utsCountryCode, ustCodeCity: $ustCodeCity] mapped to [country: $country, city: $city]")
    else
      logger.debug(s"UTS: [utsCityNameEN: $utsCityNameEN, utsCountryCode: $utsCountryCode, ustCodeCity: $ustCodeCity] mapped to [country: $country, city: $city]")
  }
  */

}
