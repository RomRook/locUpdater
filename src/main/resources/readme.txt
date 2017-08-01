usage:
java -jar <rc.contentload> -Dlog.level=info
 -algo <arg>     implemented algorithms are as follows: TEMPLATE, UTS_COUNTRY, UTS_CITY, CITE_GEO_......
 -config <arg>   path and filename to configuration file, e.g. /home/user/locs_config.xml
 -iso <arg>      1. e.g. for one country: RU
                 2. multiple countries : RU|PL|FR|IT
                 3. If value left empty or option not specified at all then all countries will be processed
 -mode <arg>     mode option:
                 FILE_ONLY - files with requests will be saved,
                 SEND_RQ - requests to xDist will be sent


e.g.
java -jar com.openjaw.locs.updater-1.0.0-SNAPSHOT-jar-with-dependencies.jar
-config "src/main/resources/in/locs_config.xml"
-algo UTS_CITY
-mode FILE_ONLY
-iso "RU|PL"




