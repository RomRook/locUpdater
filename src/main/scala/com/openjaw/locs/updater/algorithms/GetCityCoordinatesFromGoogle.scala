package com.openjaw.locs.updater.algorithms

import java.util.concurrent.TimeUnit

import com.openjaw.locs.updater.DefaultHierarchyHelper
import com.openjaw.locs.updater.exceptions.GoogleAPIException
import GoogleAPIException.{GeocodingAPIException, QueryLimitException, ZeroResultsException}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node
import scalaj.http.{Http, HttpRequest}

/**
 *    This algorithm is used to retrieve city's coordinates from Google
 *    2.5k is the number of free requests per day (Google Maps API limit)
 *    e.g. http://maps.google.com/maps/api/geocode/xml?language=ru&address=Tortuguitas+AR
 *
 *    Result of response from Google is saved in CSV file in format:
 *    newLatitude|newLongitude|oldLatitude|oldLongitude|locationName|UTSCode|locationId|ISOCode|StateInUSA
 *    40.2467738|8.5184217|0.0|0.0|Tresnuraghes|52340|CitItTres52340|IT|
 *
 */
case class GetCityCoordinatesFromGoogle(locHelper: DefaultHierarchyHelper, config: Config) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("GetCityCoordinatesFromGoogle"))

  case class Position(latitude: BigDecimal, longitude: BigDecimal)
  case class Row(iataCode: String, latitude: String, longitude: String)

  val csvProcessedSuccessfully = Logger(LoggerFactory.getLogger("CityCoordinates"))
  val csvQueryLimit = Logger(LoggerFactory.getLogger("CityCoordinatesQueryLimit"))
  val csvZeroResult = Logger(LoggerFactory.getLogger("CityCoordinatesZeroResult"))
  val csvError = Logger(LoggerFactory.getLogger("CityCoordinatesErrors"))

  val isoCode = config.countyIsoCode
  val cities = locHelper.cities

  override def run() = {

    val processedSuccessfully = getProcessedCities("csv/CityCoordinates.csv")
    val processedZeroResult = getProcessedCities("csv/CityCoordinatesZeroResult.csv")
    val processedQueryLimit = getProcessedCities("csv/CityCoordinatesQueryLimit.csv")

    logger.info(s"isoCode $isoCode")

    logger.info(s"processed successfully so far: ${processedSuccessfully.size}")
    logger.info(s"processed with zero results so far: ${processedZeroResult.size}")

    val countryISOCodeList = isoCode.split("\\|")

    if (isoCode.isEmpty)
      logger.warn(s"all countries will be processed")

    var countProcessedSuccessfully = 0  // General counter to display in the console how many nodes were processed
    var countQueryLimit = 0       // Counter for cities for which query limit was exceeded
    var countZeroResult = 0       // Counter for cities for which zero result was returned
    var countError = 0       // Counter for other errors

    // 2 500 free request per day (Google Maps API limit)
    val processed = cities
      .filter(city => {
            val currentIsoCode = extract(city)._5
            (countryISOCodeList.contains(currentIsoCode) || isoCode.isEmpty)
            })
      .filterNot(city => {
              val currentCityId = extract(city)._1
              (processedSuccessfully.contains(currentCityId) || processedZeroResult.contains(currentCityId))
        })
      .map(city => {


      val (cityId, locationType, locationName, utsCode, isoCountryCode, stateInUSA, position) = extract(city)

      val oldLatitude = position.latitude
      val oldLongitude = position.longitude
      val cityName = locationName

      // Create query that works for cities
      val query = if (stateInUSA != "") (cityName + " " + stateInUSA + " " + isoCountryCode)
                      else (cityName + " " + isoCountryCode)

      var outputLine = s"NULL|NULL|$oldLatitude|$oldLongitude|$cityName|$utsCode|$cityId|$isoCountryCode|$stateInUSA"

      try {
        logger.info(s"About to get city coordinates for $cityName [$cityId]")

        // Obtain geocode by sending http request
        Thread.sleep(500) // This ensures that 50 queries per second limit is not exceeded
        val (newLatitude, newLongitude, noOfMatches) = getGEOcodesByHttp(query)

        outputLine = s"$newLatitude|$newLongitude|$oldLatitude|$oldLongitude|$cityName|$utsCode|$cityId|$isoCountryCode|$stateInUSA"
        csvProcessedSuccessfully.info(outputLine)
        countProcessedSuccessfully += 1

      } catch {
        case e: ZeroResultsException => {
          logger.error(e.getMessage)
          csvZeroResult.info(s"$outputLine")
          countZeroResult += 1
        }
        case e: QueryLimitException => {
          val message = e.getMessage
          logger.error(message + " The querying procedure will be suspend for 2 hour")
          csvQueryLimit.info(s"$outputLine")
          countQueryLimit += 1
          //Thread.sleep(3600*1000) // 1h
          TimeUnit.HOURS.sleep(2) // 2h
        }
        case e: GeocodingAPIException => {
          logger.error(e.getMessage)
          csvError.info(s"$outputLine")
          countError += 1
        }
        case other : Throwable => {
          logger.error("Unhandled error occurred - " + other.getMessage)
          csvError.info(s"$outputLine")
          countError += 1
        }

      } finally {
        logger.info(s"Finished for $cityName [$cityId]")
      }
      city
    })

    logger.info(s"All cities defined: ${cities.size}")
    logger.info(s"Processed (filter applied): ${processed.size}")
    logger.info(s"Number of correct: $countProcessedSuccessfully")
    logger.info(s"Number of queries which limit was exceeded: $countQueryLimit")
    logger.info(s"Number of cities with zero result returned: $countZeroResult")
    logger.info(s"Number of errors: $countError")
    logger.info(s"PROCESS FINISHED SUCCESSFULLY")

    <Cities allCount={cities.size.toString} processedCount={processed.size.toString} correctCount={countProcessedSuccessfully.toString} errorCount={countError.toString}>
      <Filter>{isoCode}</Filter>
    </Cities>

  }

  def extract(locationNode: Node) = {
    try {

      val locationId = (locationNode \ "@Id").text
      val locationType = (locationNode \ "@LocationType").text

      //location name
      val namesNode = locationNode \\ "Location" \ "Names" \ "Name"
      //firts english name (might be more then one name defined in english thats way take the firts one)
      val firstEnglishName = namesNode.filter(name => {
        (name \ "@Language").text == "en"
      }).head
      val locationName = (firstEnglishName \ "@Value").text

      //UTS Code
      val codesNode = locationNode \\ "Location" \ "Codes" \ "Code"
      val utsCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "UTS"
      })
      val utsCode = (utsCodeNode \ "@Value").text

      //ISO code
      val isoCountryCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "ISOCountryCode"
      })
      val isoCode = (isoCountryCodeNode \ "@Value").text

      //State should be used only for USA
      val stateInUSA = {
        if (isoCode == "US") {
          val stateNode = codesNode.filter(code => {
            (code \ "@Context").text == "StateCode"
          })
          (stateNode \ "@Value").text
        } else {
          ""
        }
      }

      //Position
      val positionNode = (locationNode \\ "Location" \ "Position").head
      val latitude = (positionNode \ "@Latitude").text
      val longitude = (positionNode \ "@Longitude").text
      val radius = (positionNode \ "@Radius").text
      val zoom= (positionNode \ "@Zoom").text

      val position = Position(BigDecimal(latitude),BigDecimal(longitude) )

      (locationId, locationType, locationName, utsCode, isoCode, stateInUSA, position )

    } catch {
      case e: Exception => throw new Exception(e.getMessage + " locationNode " + locationNode)
    }

  }

  def getGEOcodesByHttp(query: String) = {
    val baseUrl = "http://maps.google.com/maps/api/geocode/xml?address="
    val fullUrl = baseUrl + query.replaceAll("\\s", "+")

    logger.debug(s"HTTP REQUEST: $fullUrl")
    val response = sendRequest(fullUrl)
    val status = (response \ "status").text
    logger.debug(s"RESPONSE STATUS: $status")

    status match {
      case "OK" =>
      case "ZERO_RESULTS" => throw ZeroResultsException(s"No coordinates found for '$query'")
      case "OVER_QUERY_LIMIT" => throw QueryLimitException("Daily query limit of 2500 was reached.")
      case otherStatus => throw GeocodingAPIException("Google returned this message for: " + query + " - " + otherStatus)
    }

    val latitude = (response \ "result" \ "geometry" \ "location" \ "lat").head.text
    val longitude = (response \ "result" \ "geometry" \ "location" \ "lng").head.text
    (latitude, longitude, (response \ "result").size)
  }

  def sendRequest(url: String) = {
    val request: HttpRequest = Http(url)
      .timeout(connTimeoutMs = 1000, readTimeoutMs = 1000)

    if (request.asString.isSuccess) {
      val response = request.execute(parser = {inputStream => scala.xml.XML.load(inputStream)})
      response.body
    }
    else
      throw new Exception("Error: " + request.asString)
  }

  def getProcessedCities(csvFileName: String) = {
    val str = io.Source.fromFile(csvFileName).mkString
    val lines = str.split("\\n")
    var processedCities = ArrayBuffer[String]()

    lines.foreach(l => {
      if (l.length > 0) {
        val elements = l.split("\\|")
        val cityId = elements(6).trim
        processedCities += cityId
      }
    })
    processedCities.toSet
  }

}
