package io.alphash

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

case class Config(
  druidHost:  String = "",
  druidPort:  Int = 8082,
  batchSize:  Int = 256,
  dataSource: String = "foo",
  startTime:  String = "2017-01-01T00:00:00Z",
  endTime:    String = "2017-01-01T01:00:00Z",
  filters:    Seq[(String, String)] = List[(String, String)](),
  columns:    Seq[String] = List[String](),
  interval:   Int = 60,
  ftpHost:    String = "localhost",
  ftpUser:    String = "username",
  ftpPasswd:  String = "password",
  directory:  String = ".",
  overwrite:  Boolean = true,
  buffer:     Int = 4194303,
  config:     String = "",
  query:      String = ""
)

object CommandParser {
  def parseCommand(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("exportctl") {
      head("exportctl", "INTERNAL DEMO USE ONLY")

      opt[String]('s', "server").required().action( (x, c) ⇒ c.copy(druidHost = x) )
        .text("Druid server hostname or IP address")
      opt[Int]('p', "port").action( (x, c) ⇒ c.copy(druidPort = x) )
        .validate { x ⇒
          if (x > 0 && x < 65535) success
          else failure("Option --port must be > 0 and < 65535")
        }.text("Druid server port, default: 8082")
      opt[String]('d', "data-source").action( (x, c) ⇒ c.copy(dataSource = x) )
        .text("Data source name, default: 'foo'")
      opt[Int]('b', "batch-size").action( (x, c) ⇒ c.copy(batchSize = x) )
        .validate { x ⇒
          if (x > 0 && x <= 4096) success
          else failure("Option --batch-size must be > 0 and <= 4096")
        }.text("Number of rows to send per batch by Druid, default: 256")
      opt[String]('S', "start").action( (x, c) ⇒ c.copy(startTime = x) )
        .text("Start time, ISO-8601 timestamp in UTC (yyyy-MM-ddTHH:mmZ), "
              + "default: '2017-01-01T00:00:00Z'")
      opt[String]('E', "end").action( (x, c) ⇒ c.copy(endTime = x) )
        .text("End time, ISO-8601 timestamp in UTC (yyyy-MM-ddTHH:mmZ), "
              + "default: '2017-01-01T00:00:00Z'")
      opt[Seq[(String,String)]]('f', "filter").valueName("k1=v1,k2=v2...").action( (x, c) ⇒
          c.copy(filters = x) ).text("Filtering option, e.g., system=foo, default: no filter")
      opt[Seq[String]]('c', "columns").valueName("<column1>,<column2>...").action( (x,c) ⇒
          c.copy(columns = x) ).text("Columns (i.e., dimensions) to export, default: all columns")
      opt[Int]('i', "interval").action( (x, c) ⇒ c.copy(interval = x) )
        .validate { x ⇒
          if (x >= 0) success
          else failure("Option --interval must be >= 0")
        }.text("The interval (minutes) used to split results into multiple files, default: 60")
      opt[String]('H', "host").action( (x, c) ⇒ c.copy(ftpHost = x) )
        .text("FTP server hostname or IP address, default: 'localhost'")
      opt[String]('U', "user").action( (x, c) ⇒ c.copy(ftpUser = x) )
        .text("FTP server login username, default: 'username'")
      opt[String]('P', "password").action( (x, c) ⇒ c.copy(ftpPasswd = x) )
        .text("FTP server login password, default: 'password'")
      opt[String]('D', "directory").action( (x, c) ⇒ c.copy(directory = x) )
        .text("Base directory in remote FTP server, default: '.'")
      opt[Boolean]('o', "overwrite").action( (x, c) ⇒ c.copy(overwrite = x) )
        .text("Overwrite if file exists in FTP server, default: 'true'")
      opt[Int]('B', "buffer-size").action( (x, c) ⇒ c.copy(buffer = x) )
        .validate { x ⇒
          if (x > 0) success
          else failure("Option --buffer-size must be > 0")
        }.text("Local buffer size in bytes, default: 4194303")
      opt[String]('C', "configuration").action( (x, c) ⇒ c.copy(config = x) )
        .text("Use configuration file. This file invalidates -q and overwrites"
              + "other options accordingly")
      opt[String]('q', "query").action( (x, c) ⇒ c.copy(query = x) )
        .text("Use Druid query request JSON file. "
              + "Options in this file overwrites other options accordingly")

      help("help").text("Print this usage text")
      version("version").text("Print version info")
    }

    parser.parse(args, Config())
  }
}

object DateTimeHelper {
  def formatter: DateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis.withZone(DateTimeZone.UTC)
  def toISO8601String(dt: DateTime): String = formatter.print(dt)
}
