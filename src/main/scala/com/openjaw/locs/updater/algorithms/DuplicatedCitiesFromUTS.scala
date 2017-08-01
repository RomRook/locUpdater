package com.openjaw.locs.updater.algorithms

import akka.actor.ActorRef
import com.openjaw.connectors.{DistributorConnector, UTSConnector}
import com.openjaw.locs.updater.{Supervisor, DefaultHierarchyHelper}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{XML, NodeSeq, Node}


case class DuplicatedCitiesFromUTS(locHelper: DefaultHierarchyHelper, config: Config, mainActor: Option[ActorRef]) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("DuplicatedCitiesFromUTS"))

  private val LANGUAGE_EN = "en"
  private val LANGUAGE_RU = "ru"

  private val KEY_TYPE_CITY_AND_STATE = "city#state"
  private val KEY_TYPE_CITY_ONLY = "city"

  private val LOCATION_TYPE_COUNTRY = "country"
  private val LOCATION_TYPE_CITY = "city"
  private val LOCATION_TYPE_AIRPORT = "airport"

  val locations = locHelper.locationNodes
  val countries = locHelper.countries
  val cities = locHelper.cities
  val airports = locHelper.airports

  //helper maps
  val locationLookup = locHelper.locationLookup
  lazy val statesInUSALookup = locHelper.getStatesInUSALookup

  //external systems
  val utsConnector = new UTSConnector(config.utsEndpoint, config.utsUser, config.utsPassword)
  val utsCountries = utsConnector.getCountries()
  logger.info(s"Number of countries from UTS: ${utsCountries.size}")

  def extract(n: Node) = locHelper.extractLocation(n)

  override def run() = {

    val isoCountryCode = config.countyIsoCode
    val isoCountryCodeList = Countries.getList(config.countyIsoCode)

    // based on data provided by user we create structure to hold detailed info about countries being processed
    // important attributes:
    // ISOCountryCode - used to find matching city/airport already defined (it is possible to provide multiple codes, e.g. "PL|DE|RU")
    // LocationId - used as a parent location for cities being added into DefaultHierarchy

    val fileWithMapping = config.inDir + Countries.COUNTRY_MAPPING_FILENAME
    val countriesMappings = Countries.userDefinedMapping(skipIgnored = true, fileWithMapping)

    val report =
      <Report>
        {countriesMappings
        .filter(pair => {
        val utsCountryInfo = pair._2
        val currentIsoCode = utsCountryInfo.isoCountryCode
        (isoCountryCodeList.contains(currentIsoCode) || isoCountryCode.isEmpty)
        })
        .map(pair => {
        val supplierCountryName = pair._1
        val utsCountryInfo = pair._2
        val ustCountryCode = utsCountryInfo.utsCode
        val currentIsoCode = utsCountryInfo.isoCountryCode
        val stateCodeInUSA = utsCountryInfo.stateCodeInUSA
        val countryId = utsCountryInfo.locationId
        val cityMergeType = utsCountryInfo.cityMergeType
        val defaultHierarchyCountryName = utsCountryInfo.locationName //comes from attribute @CurrentName

        logger.info(s"==============================================================================================")
        logger.info(s"Processing cities from UTS for country '$supplierCountryName' started")
        logger.info(s"Name of the country defined in DefaultHierarchy '$defaultHierarchyCountryName'")

        //cities from UTS for current country
        val allToBeProcessed = utsConnector.getCities(ustCountryCode)

        // split cities from supplier into two groups:
        // 1) airports - in fact it is city that contains in its name string 'airport' (it will be merged with locationType='airport')
        // 2) cities - remaining locations (it will be merged with locationType='city')
        val (airportsToBeProcessed, citiesToBeProcessed) = allToBeProcessed.partition(n => isAirport(supplierCityName = n.text, supplierCountryName))
        logger.info("allToBeProcessedAll: " + allToBeProcessed.size)
        logger.info("airportsToBeProcessed: " + airportsToBeProcessed.size)
        logger.info("citiesToBeProcessed: " + citiesToBeProcessed.size)

        //Supplier
        val dupFromUTS = verifyCitiesFromUTS(citiesToBeProcessed, Seq(ustCountryCode))
        logger.info("Number of duplicated cities from UTS: " + dupFromUTS.size)

        //populate map with cities from DefaultHierarchy (used during merge process)
        val (_, duplicatedLookup, defaultHierarchyLookup) =
            getDefaultHierarchyKeyLookup(cities,
                                         locHelper.extractLocation,
                                         currentIsoCode,
                                         stateCodeInUSA,
                                         LOCATION_TYPE_CITY,
                                         cityMergeType)

        val toBeUpdatedXML = getToBeUpdated(dupFromUTS,cityMergeType,stateCodeInUSA,defaultHierarchyLookup)

//      getLocationWithIncorrectUtsCodes(citiesToBeProcessed, cities,locHelper.extractLocation, isoCountryCode,stateCodeInUSA,LOCATION_TYPE_CITY)

        val toBeRemovedXML = getToBeRemoved(duplicatedLookup)
        //replace removed cityCode with the one that was updated
        val sqlUpdateList = getSqlUpdateList(duplicatedLookup)

        val parentsMap = populateMap(toBeRemovedXML)
        val parentsToBeUpdatedXML = getParentsToBeUpdated(parentsMap, toBeRemovedXML)

        config.mode match   {
          case Mode.SaveToFile => {
            val groupedRequest = toBeUpdatedXML.grouped(1000)
            var index = 0
            groupedRequest.foreach(n => {
              index +=1
              val requests = generateRequests(n.toSeq)
              saveToFile("toBeUpdatedRQ" + "_" + index, requests)
            })

            val reqParentToBeUpdate = generateRequests(parentsToBeUpdatedXML)
            saveToFile("parentsToBeUpdatedRQ", reqParentToBeUpdate)

            val reqToBeREmoved = generateRequests(toBeRemovedXML)
            saveToFile("toBeRemoved", reqToBeREmoved)

          }
          case Mode.SendRequest => {
            sendRequests(toBeUpdatedXML, toBeRemovedXML, parentsToBeUpdatedXML, mainActor.get)
          }
        }

        val duplicatedToShowInReport = getDuplicatedInDefaultHierarchy(duplicatedLookup)

        logger.info(s"Processing country '$supplierCountryName' finished")

        <Country SupplierName={supplierCountryName} LocationName={defaultHierarchyCountryName} ISOCountryCode={isoCountryCode} LocationId={countryId} StateCode={stateCodeInUSA}>
          <Cities currentCount={defaultHierarchyLookup.size.toString} toBeProcessedCount={citiesToBeProcessed.size.toString} toBeUpdatedCount={toBeUpdatedXML.size.toString}>
            <ToBeUpdated>{toBeUpdatedXML
              //.sortBy(n => extract(n).generalInfo.locationNameEN)
              .map( n => {
              val locationInfo = locHelper.extractLocation(n)
              val generalInfo = locationInfo.generalInfo
              val codesInfo = locationInfo.codesInfo
              <City locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString(",")}>{generalInfo.locationNameEN}</City>
            })}
            </ToBeUpdated>
            <ToBeRemoved>{toBeRemovedXML
              //.sortBy(n => extract(n).generalInfo.locationNameEN)
              .map( n => {
              val locationInfo = locHelper.extractLocation(n)
              val generalInfo = locationInfo.generalInfo
              val codesInfo = locationInfo.codesInfo
              <City locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString(",")}>{generalInfo.locationNameEN}</City>
            })}
            </ToBeRemoved>
            <DuplicatedFromUTS>{dupFromUTS
              .map(pair => (pair._1, pair._2.mkString(",")))
              .map(tuple2 => {
                <City name={tuple2._1} ustCodesSeq={tuple2._2}></City>
              })}
             </DuplicatedFromUTS>
            <DuplicatedInDefaultHierachy>{duplicatedToShowInReport.map(pair => {
             val locationName=  pair._1
             val dups = pair._2.mkString(",")
             <Duplicated locationName={locationName}>{dups}</Duplicated>
             })
            }</DuplicatedInDefaultHierachy>
          </Cities>
         <Parents>
           <ToBeUpdated>{parentsToBeUpdatedXML
             .map( n => {
             val locationInfo = locHelper.extractLocation(n)
             val generalInfo = locationInfo.generalInfo
             val codesInfo = locationInfo.codesInfo
             <Parent locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString(",")}>
               {generalInfo.locationNameEN}
             </Parent>
           })}
           </ToBeUpdated>
         </Parents>

         <SQL>
           {sqlUpdateList.map(update => {update._2}.mkString("\n"))}
         </SQL>

        </Country>
      })}<Config> {config.xml}</Config>
      </Report>

      report

    }

    def isAirport(supplierCityName: String, supplierCountryName: String) = {
    //  logger.debug(s"City '$supplierCityName' in country '$supplierCountryName' from RentalCars will be treated as 'Airport' type")
      false
  }

  def getSqlUpdateList(duplicated: Map[String, List[LocationInfo]]) = {
    duplicated.map(pair => {
      val locationName = pair._1
      val dupsSeq = pair._2
      val locationToBeUpdated = dupsSeq.head
      val locationToBeRemoved = dupsSeq.tail
      val newCityCode = locationToBeUpdated.generalInfo.locationId

      val updatesList = locationToBeRemoved.map(locInfo => {
        val oldCityCode = locInfo.generalInfo.locationId
        s"update HOTELS set city_code = '$newCityCode' where city_code = '$oldCityCode';"
      })
      (locationName, updatesList)
    })
  }


  def getDuplicatedInDefaultHierarchy(duplicated: Map[String, List[LocationInfo]]) = {
    duplicated.map(pair => {
      val locationName = pair._1
      val dupsSeq = pair._2
      val dupLocationIdSeq = dupsSeq.map(locInfo => {
        locInfo.generalInfo.locationId
      })
      (locationName, dupLocationIdSeq)
    })
  }

  def getToBeUpdated(duplicatedInUTS: Map[String, Seq[String]],
                     cityMergeType: String,
                     stateCodeInUSA: String,
                     defaultHierarchyLookup: Map[String, List[LocationInfo]]) = {
    duplicatedInUTS
      .filter(p => {
        val cityName = p._1
        val ustCodeSeq = p._2
        val key = getKey(cityMergeType, cityName, stateCodeInUSA)
         val result = defaultHierarchyLookup.isDefinedAt(key)
         if (result == false) logger.error(s"City '$cityName' from supplier is not defied in Default Hierarchy [key: $key]")
         result
       })
      .map(p => {
        val cityName = p._1
        val ustCodeSeq = p._2
        val key = getKey(cityMergeType, cityName, stateCodeInUSA)

        //the first from the list with duplicates is used
        val locationInfo = defaultHierarchyLookup(key).headOption
        //val node = if (defaultHierarchyLookup.isDefinedAt(key)) Some(first.generalInfo.node)
        //else None

        (cityName, ustCodeSeq, locationInfo)
      })
      .filter(tuple => tuple._3.isDefined)
      .map(tuple => {
        val locInfo = tuple._3.get
        val node = locInfo.generalInfo.node
        val ustCodeSeq = tuple._2
        val changedNode: Node = locHelper.addUTSCodeSeq(node, ustCodeSeq)
        changedNode
    }).toSeq
  }

  def getToBeRemoved(duplicated: Map[String, List[LocationInfo]]) = {
    //we want to remove tail from the list with duplicates (the first was left and it was updated with seq of utsCodes)
    duplicated
      .map(pair => {
      val locationId = pair._1
      val list = pair._2
      list.tail
    })
      .flatten
      .map(loc => {
      val node = loc.generalInfo.node
      locHelper.setLocationActionAsDelete(node)
    })
      .toList
  }

  private def populateMap(list: NodeSeq) = {
    val parentsMap = collection.mutable.Map.empty[String, List[String]]
    list.foreach(city => {

      val parentsSeq = extractParentLocation(city)
      logger.debug(s"Parents ${parentsSeq.toString}")

      val cityId = (city \ "@Id").text

      if (parentsSeq.size == 0) {
        logger.error(s"Parent not found for location [locationNode: $city]")
      }

      parentsSeq.foreach(parentId => {
        if (parentsMap.isDefinedAt(parentId)) {
          val currentList = parentsMap(parentId)
          parentsMap.put(parentId, (cityId :: currentList))
        } else {
          parentsMap.put(parentId,List(cityId))
        }
      })
    })
    //immutable map is returned
    parentsMap.toMap
  }


  private def getParentsToBeUpdated(parentsMap: Map[String, List[String]], listToBeRemoved: NodeSeq)  = {
    parentsMap.filter(pair => {
      val parentId = pair._1
      val childSeq = pair._2
      val result = if (locationLookup.isDefinedAt(parentId))
          true
        else {
          logger.error(s"Parent not found in DefaultHierarchy [parentId: $parentId, childSeq: ${childSeq.mkString(",")} ]")
          false
        }
      result
      })
      .map(pair => {
        val parentId = pair._1
        val childSeq = pair._2
        val parentNode = locationLookup(parentId).generalInfo.node
        locHelper.removeChildLocations(parentNode,childSeq)
      }).toSeq
  }

//  private def NO_chage_getParentsToBeUpdated(parentsMap: Map[String, List[String]], listToBeRemoved: NodeSeq)  = {
//    parentsMap.filter(pair => {
//      val parentId = pair._1
//      val childSeq = pair._2
//      val result = if (locationLookup.isDefinedAt(parentId))
//        true
//      else {
//        logger.error(s"Parent not found in DefaultHierarchy [parentId: $parentId, childSeq: ${childSeq.mkString(",")} ]")
//        false
//      }
//      result
//    })
//      .map(pair => {
//      val parentId = pair._1
//      val childSeq = pair._2
//      val parentNode = locationLookup(parentId).generalInfo.node
//      parentNode
//    }).toSeq
//  }

  def getDefaultHierarchyKeyLookup(locations: NodeSeq,
                                   extractInfoFunction: (Node) => LocationInfo,
                                   isoCountryCode: String,
                                   stateCodeInUSA: String,
                                   locationType: String,
                                   cityMergeType: String,
                                   language: String = LANGUAGE_EN) = {
    logger.info(s"About to populate map for isoCountry='$isoCountryCode' that contains [key: $locationType, value: LocationInfo]")
    val keyLookup = collection.mutable.Map.empty[String, List[LocationInfo]]
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
      val locationInfo = extractInfoFunction(n)
      val generalInfo = locationInfo.generalInfo
      val codesInfo = locationInfo.codesInfo

      val (city, state) = if (language == LANGUAGE_EN)
        (generalInfo.locationNameEN, codesInfo.stateCodeInUSA)
      else
        (generalInfo.locationNameRU, codesInfo.stateCodeInUSA)

      val key = getKey(cityMergeType, city, state)
      if (key != "") {
        if (keyLookup.isDefinedAt(key)) {
          val alreadyInMap = keyLookup(key)
          logger.error(s"Duplicated location for key '$key' " +
            s"\ncurrent:     [locationId: ${generalInfo.locationId}, locationName: ${generalInfo.locationNameEN}, locationType: ${generalInfo.locationType}, isoCountryCode: ${codesInfo.isoCountryCode}] " +
            s"\nalreadyInMap:[locationId: ${alreadyInMap.map(_.generalInfo.locationId).mkString(",")}, locationName: ${alreadyInMap.map(_.generalInfo.locationNameEN).mkString(",")}, locationType: ${alreadyInMap.map(_.generalInfo.locationType).mkString(",")} , isoCountryCode: ${alreadyInMap.map(_.codesInfo.isoCountryCode).mkString(",")} ]")
          val currentList = keyLookup(key)
          keyLookup.put(key, currentList ++ List(locationInfo))
        } else {
          keyLookup.put(key, List(locationInfo))
        }
      }
    })
    logger.info(s"Based on DefaultHierarchy map was populated and its size: ${keyLookup.size}")

    //if size of the list for the same key is greater then 1 then it means location has duplications
    val (ok, duplicated) =  keyLookup.partition(pair => {
      val list = pair._2
      (list.size == 1)
    })
    (ok, duplicated.toMap, keyLookup.toMap)
  }

  def getKey(keyType: String, city: String, state: String ) = {
    val key = if (keyType == KEY_TYPE_CITY_AND_STATE) {
      s"${city.trim}#${state.trim}"
    } else {
      city.trim
    }
    key
  }

  def getUtsCodeForCity(fromSupplier: NodeSeq, cityName: String) = {
    fromSupplier.filter(n => {
      val currentCityName = n.text
      val ustCityCode = (n \ "@id").text
      (currentCityName == cityName)
    })
      .map(n => {
      val ustCityCode = (n \ "@id").text
      ustCityCode
    })
  }

  def verifyCitiesFromUTS(fromSupplier: NodeSeq, utsCodeCountrySeq: Seq[String]) = {

    //key - city name, value - number of occurrences
    val mapCityToItsOccurrences = collection.mutable.Map.empty[String, Seq[String]]
    val duplicatedCityMaps = collection.mutable.Map.empty[String, Node]

    fromSupplier
      .sortBy(n => {val ustCode =  (n \ "@id").text
            ustCode.toInt})
      .foreach(n => {
        val name = n.text
        val ustCityCode = (n \ "@id").text
        //val ustCountryCode = (n \ "@country").text
        if (mapCityToItsOccurrences.isDefinedAt(name)) {
          logger.debug(s"Duplicated city from UTS: '$name'")
//            s"\n current     : ${n.toString} " +
//            s"\n alreadyInMap: ${mapCityToItsOccurrences(name).toString}")
           val currentUtsCodes = mapCityToItsOccurrences(name)
          mapCityToItsOccurrences.put(name, currentUtsCodes ++ Seq(ustCityCode))
        }
        else
          mapCityToItsOccurrences.put(name, Seq(ustCityCode))
      })

    //leave only those cities that have more then one occurrence
    val duplicatedCities = mapCityToItsOccurrences.filter(pair => pair._2.size > 1)

    val fileName = config.outDir + "DuplicatedCitiesFromUts_" + utsCodeCountrySeq.mkString("_") + ".xml"
    XML.save(fileName,
      <Cities>{duplicatedCities
        .map(p =>
          <City ustCode={p._2.mkString("|")}>{p._1}</City>)}
      </Cities>, "UTF-8")

    logger.info(s"Duplicated cities from UTS (${duplicatedCities.size}):")
    duplicatedCities.foreach(p => {
      val cityName = p._1
      val ustCodeSeq = p._2
      logger.info(s"'$cityName' [ustCode: ${p._2.mkString(", ")}]")
    })

    duplicatedCities.toMap
  }


  def getLocationWithIncorrectUtsCodes(fromSupplier: NodeSeq, fromDefaultHierarchy: NodeSeq, extractInfoFunction: (Node) => LocationInfo, isoCountryCode: String, stateCodeInUSA: String = "", locationType: String = LOCATION_TYPE_CITY) = {

    fromDefaultHierarchy.filter(n => {
      val locationInfo = extractInfoFunction(n)
      val generalInfo = locationInfo.generalInfo
      val codesInfo = locationInfo.codesInfo

      val currentUtsCodeSeq = codesInfo.utsCode
      val currentIsoCode = codesInfo.isoCountryCode
      val currentLocationType = generalInfo.locationType
      val currentStateCodeInUSA = codesInfo.stateCodeInUSA

      ((currentLocationType == locationType)
        && (currentIsoCode == isoCountryCode)
        && (stateCodeInUSA == "" || currentStateCodeInUSA == stateCodeInUSA)
        && (currentUtsCodeSeq.size > 0)
        )
      })
      .filterNot(n => {
        val locationInfo = extractInfoFunction(n)
        val generalInfo = locationInfo.generalInfo
        val cityName = generalInfo.locationNameEN
        val alreadyDefiniedUtsCodeSeq = locationInfo.codesInfo.utsCode
        val utsCodeSeqFromSupplier = getUtsCodeForCity(fromSupplier,cityName)

        alreadyDefiniedUtsCodeSeq.forall(item => utsCodeSeqFromSupplier.contains(item))
    })

  }

  def verifyCitiesFromDefaultHierarchy(fromDefaultHierarchy: Seq[LocationInfo], isoCountryCode: String, stateCodeInUSA: String = "", locationType: String = LOCATION_TYPE_CITY) = {

    val mapCityToItsOccurrences = collection.mutable.Map.empty[String, Seq[String]]
    var fileName = ""

        val emptyUtsCode = fromDefaultHierarchy
          .filter(locationInfo => {
            val generalInfo = locationInfo.generalInfo
            val codesInfo = locationInfo.codesInfo

            val currentUtsCodeSeq = codesInfo.utsCode
            val currentIsoCode = codesInfo.isoCountryCode
            val currentLocationType = generalInfo.locationType
            val currentStateCodeInUSA = codesInfo.stateCodeInUSA

            ((currentLocationType == locationType)
            && (currentIsoCode == isoCountryCode)
            && (stateCodeInUSA == "" || currentStateCodeInUSA == stateCodeInUSA)
            && (currentUtsCodeSeq.size == 0)
            )

          })
          .map(locationInfo => {
            val generalInfo = locationInfo.generalInfo
            <City LocationId={generalInfo.locationId}>{generalInfo.locationNameEN}</City>
         })

        fileName = config.outDir + "CitiesWithEmptyUtsCode_" + isoCountryCode + ".xml"
        XML.save(fileName , <Cities>{emptyUtsCode.sortBy(n => n.text)}</Cities>, "UTF-8")

        fromDefaultHierarchy
          .filter(locationInfo => {
            val ustCodeSeq = locationInfo.codesInfo.utsCode
            ustCodeSeq.size > 0
            })
          .foreach(locationInfo => {
            val generalInfo = locationInfo.generalInfo
            val locationId = generalInfo.locationId
            val name = generalInfo.locationNameEN

            if (mapCityToItsOccurrences.isDefinedAt(name)) {
              logger.error(s"Duplicated city for DefaultHierarchy: '$name'")
              val currentLocationIds = mapCityToItsOccurrences(name)
              mapCityToItsOccurrences.put(name, currentLocationIds ++ Seq(locationId))
            }
            else
              mapCityToItsOccurrences.put(name, Seq(locationId))
            })

      //leave only those cities that have more then one occurrence
      val duplicatedCities = mapCityToItsOccurrences.filter(pair => pair._2.size > 1)

      fileName = config.outDir + "DuplicatedCitiesFromDefaultHierarchy_" + isoCountryCode + ".xml"
      XML.save(fileName,
        <Cities>{duplicatedCities
          .map(p =>
          <City locationId={p._2.mkString("|")}>{p._1}</City>)}
        </Cities>, "UTF-8")

     duplicatedCities
  }

  def isDefiniedForCountry(locationInfo: LocationInfo, locationType: String, isoCountryCode: String, stateCodeInUSA: String = "") = {
    val currentLocationType = locationInfo.generalInfo.locationType
    val currentIsoCode = locationInfo.codesInfo.isoCountryCode
    val currentStateCodeInUSA = locationInfo.codesInfo.stateCodeInUSA

    val result = ((currentLocationType == locationType)
      && (currentIsoCode == isoCountryCode)
      && (stateCodeInUSA == "" || currentStateCodeInUSA == stateCodeInUSA)
      )
    result
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

  def extractParentLocation(node: Node) = {
    val parentLocations = (node \\ "ParentLocations" \\ "LocationReference")
    // the first but non-continent location
    val parentLocation = parentLocations
      .map(n => {
        val parentId = (n \ "@Id").text
        parentId
      })
    parentLocation
  }

  def sendRequests(toBeUpdated: NodeSeq, toBeRemoved: NodeSeq, parentsToBeUpdated: NodeSeq, mainActor: ActorRef) = {

    if (toBeUpdated.size > 0) {
      logger.info(s"About to send toBeUpdated requests to xDist [size: ${toBeUpdated.size}]")
      mainActor ! Supervisor("toBeUpdated", countryName = "ALL", requestType = DistributorConnector.REQUEST_TYPE_LOCATION, toBeUpdated)
    }

    if (toBeRemoved.size > 0) {
      logger.info(s"About to send toBeRemoved requests to xDist [size: ${toBeRemoved.size}]")
      mainActor ! Supervisor("toBeRemoved", countryName = "ALL", requestType = DistributorConnector.REQUEST_TYPE_LOCATION, toBeRemoved)
    }

    if (parentsToBeUpdated.size > 0) {
      logger.info(s"About to send parentsToBeUpdated requests to xDist [size: ${parentsToBeUpdated.size}]")
      mainActor ! Supervisor("parentsToBeUpdated", countryName = "ALL", requestType = DistributorConnector.REQUEST_TYPE_LOCATION, parentsToBeUpdated)
    }

  }


}

