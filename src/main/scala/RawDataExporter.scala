package io.alphash

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{Sink ⇒ AkkaSink}
import akka.stream.alpakka.ftp._
import akka.stream.alpakka.ftp.FtpCredentials.NonAnonFtpCredentials
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.util.ByteString
import fs2.{Sink, Stream, Task, io, text}

import java.io.InputStream
import java.net.InetAddress

import org.apache.commons.net.ftp.FTPClient
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.io.Source._
import scalaj._
import scalaj.http._

import streamz.converter._

import _root_.io.circe.{Json, Printer}
import _root_.io.circe.fs2.{decoder, stringArrayParser}
import _root_.io.circe.generic.auto._
import _root_.io.circe.parser._
import _root_.io.circe.syntax._

sealed trait QueryConfig {
  def dataSource: String
  def startTime: String
  def endTime: String
  def filter: Option[Filter]
  def batchSize: Int
  def columns: Seq[String]
}

case class Field(dimension: String, values: Seq[String], `type`: String = "in")
case class Filter(fields: Seq[Field], `type`: String = "and")

case class QuerySourceConfig (
  dataSource: String,
  startTime: String,
  endTime: String,
  filter: Option[Filter],
  batchSize: Int = 256,
  columns: Seq[String] = List()
) extends QueryConfig

case class SourceConfig(
  dataSource: String,
  startTime: String,
  endTime: String,
  filters: Option[Json],
  batchSize: Int = 256,
  columns: Seq[String] = List()
) extends QueryConfig {
  def filter: Option[Filter] = {
    val filter: Option[Filter] = filters match {
      case Some(x) ⇒
        val xs = x.asObject.get.keys.map { key ⇒
          Field(key, x.hcursor.get[List[String]](key).right.get)
        }
        Some(Filter(xs.toList))
      case _ ⇒ None
    }
    filter
  }
}

case class DestinationConfig(
  host:      String,
  user:      String,
  password:  String,
  directory: String = ".",
  overwrite: Boolean = true,
  interval:  Int = 60
) {
  def ftpSettings = FtpSettings (
    host        = InetAddress.getByName(host),
    credentials = new NonAnonFtpCredentials(user, password),
    binary      = true,
    passiveMode = false
  )
}

case class DataExportConfig(source: SourceConfig, destination: DestinationConfig)

case class ScanQuery(
  dataSource:   String,
  intervals:    Seq[String],
  queryType:    String = "scan",
  resultFormat: String = "compactedList",
  columns:      Seq[String] = List(),
  batchSize:    Int = 256,
  filter:       Option[Filter] = None
) extends QueryConfig {
  def startTime = intervals.take(1).mkString.split("/")(0)
  def endTime = intervals.take(1).mkString.split("/")(1)
}

case class ScanQueryResult(columns: Seq[String], events: Seq[Json])
case class QueryStatistics(var rows: Long, var batches: Long, var bytes: Long)

case class DruidConfig(host: String, port: Int) {
  def url = s"http://${host}:${port}/druid/v2"
}

object RawDataExporter {

  def prepareQuery(config: QueryConfig, interval: Int = 60): Seq[ScanQuery] = {
    import DateTimeHelper._

    def mkScanQuery(intervals: Seq[String]): ScanQuery =
      ScanQuery(config.dataSource, intervals, filter = config.filter, columns = config.columns)

    @tailrec
    def generate(start: DateTime, end: DateTime, acc: Seq[ScanQuery]): Seq[ScanQuery] = {
      def f: DateTime ⇒ String = toISO8601String

      end.isAfter(start) || end.isEqual(start) match {
        case false ⇒ throw new Exception(s"End time ${f(end)} is behind start time ${f(start)}")
        case _ ⇒ // do nothing
      }

      interval > 0 match {
        case true ⇒
          val next = start.plusMinutes(interval)
          end.isAfter(next) match {
            case true ⇒
              val intervals = List(s"${f(start)}/${f(next)}")
              generate(next, end, acc ++ List(mkScanQuery(intervals)))
            case _ ⇒
              val intervals = List(s"${f(start)}/${f(end)}")
              acc ++ List(mkScanQuery(intervals))
          }
        case _ ⇒
          val intervals = List(s"${f(start)}/${f(end)}")
          acc ++ List(mkScanQuery(intervals))
      }
    }

    val start: DateTime = formatter.parseDateTime(config.startTime)
    val end: DateTime   = formatter.parseDateTime(config.endTime)
    generate(start, end, List())
  }

  def getRemoteDirectory(query: ScanQuery, basedir: String = "."): String = {
    import DateTimeHelper.formatter
    val start  = query.intervals.mkString.split("/")(0)
    val subdir = formatter.parseDateTime(start).toYearMonthDay.toString
    s"${basedir}/${query.dataSource}/${subdir}"
  }


  def getFilename(query: ScanQuery): String =
    s"${query.dataSource}_${query.intervals.mkString.replaceAll("/", "_")}.csv"

  def toCsv(result: ScanQueryResult, stat: QueryStatistics): String = {
    import _root_.io.circe._
    import _root_.io.circe.parser._
    stat.batches += 1
    val strlist = result.events.map { json ⇒
      stat.rows += 1
      print(s"  #Rows: ${stat.rows} #Batch: ${stat.batches}\r")
      json.asArray.get.map { x ⇒
        if (x.isArray) s""""${x.asArray.get.map(_.as[String].right.get).mkString(",")}""""
        else x.noSpaces
      }.mkString(",")
    }
    if (stat.batches != 1) {
      val csvRow = s"${strlist.mkString("\n")}\n"
      stat.bytes += csvRow.getBytes.length
      csvRow
    } else {
      val header = result.columns.mkString(",")
      val csvRow = s"${(List(header) ++ strlist).mkString("\n")}\n"
      stat.bytes += (csvRow.getBytes.length + header.getBytes.length)
      csvRow
    }
  }

  def streamToFtp(
    inStream: InputStream,
    settings: FtpSettings,
    path: String,
    isAppended: Boolean = false,
    bufferSize: Int = 4194304
  ): Unit = {
    implicit val system = ActorSystem("DataExportDemo")
    implicit val materializer = ActorMaterializer()
    implicit val ec: ExecutionContext = system.dispatcher

    def ftpSink(settings: FtpSettings, path: String) =
      Ftp.toPath(path, settings, append = isAppended).toSink()

    val streams: Stream[Task, ScanQueryResult] =
      fs2.io.readInputStream[Task](Task.now(inStream), bufferSize)
        .through(text.utf8Decode)
        .through(stringArrayParser)
        .through(decoder[Task, ScanQueryResult])

    val stat = QueryStatistics(0, 0, 0)
    try {
      streams.map(toCsv(_, stat))
        .map(ByteString(_))
        .to(ftpSink(settings, path))
        .onFinalize(Task.delay{print("\nDone!\n")}).run.unsafeRun
    } catch {
      case e: Exception ⇒ e.printStackTrace()
    } finally {
      system.shutdown
    }
    println(s"Exported ${stat.rows} rows, total ${stat.bytes} bytes")
  }

  def makeDirectories(settings: FtpSettings, path: String): Unit = {
    val client = new FTPClient
    try {
      client.connect(settings.host.getHostAddress)
      client.login(settings.credentials.username, settings.credentials.password)
      path.split("/").map { dir ⇒
        client.changeWorkingDirectory(dir) match {
          case true ⇒ // directory exists, do nothing
          case _    ⇒
            client.makeDirectory(dir) match {
              case true ⇒ client.changeWorkingDirectory(dir)
              case _ ⇒ throw new Exception(s"Failed to create directory '${dir}'")
            }
        }
      }
    } catch {
      case e: Exception ⇒ e.printStackTrace
    } finally {
      client.logout
      client.disconnect
    }
  }

  def export(
    url: String,
    query: ScanQuery,
    settings: FtpSettings,
    basedir: String = ".",
    overwrite: Boolean = true,
    bufferSize: Int = 4194304
  ): Unit = {
    println(s"\nStart exporting '${query.dataSource}' of '${query.intervals.mkString}' to FTP...")
    val remoteBaseDir = getRemoteDirectory(query, basedir)
    makeDirectories(settings, remoteBaseDir)
    val dst = s"${remoteBaseDir}/${getFilename(query)}"
    val requestBody = query.asJson.pretty(Printer(true, true, ""))
    Http(url).postData(requestBody)
      .header("Content-Type", "application/json")
      .execute(x ⇒ streamToFtp(x, settings, dst, isAppended = !overwrite, bufferSize = bufferSize))
    println(s"End of exporting '${query.dataSource}' of '${query.intervals.mkString}' to FTP...")
  }

  def timer[R](block: ⇒ R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + ((t1 - t0) / 1000) + " seconds")
    result
  }

  def parseFilters(s: Seq[(String, String)]): Filter = {
    val m: Map[String, Seq[String]] = s.groupBy(_._1).mapValues(_.map(_._2))
    val fields = for {
      (k, v) <- m
    } yield Field(k, v.toList)
    Filter(fields.toList)
  }

  def main(args: Array[String]): Unit = {
    import CommandParser._
    parseCommand(args) match {
      case Some(c) ⇒
        val dconf = DruidConfig(c.druidHost, c.druidPort)
        c.config match {
          case "" ⇒
            val dest = DestinationConfig(
              c.ftpHost, c.ftpUser, c.ftpPasswd, c.directory, c.overwrite, c.interval
            )
            c.query match {
              case "" ⇒
                val filter = parseFilters(c.filters)
                val src = QuerySourceConfig (
                  dataSource = c.dataSource,
                  startTime = c.startTime,
                  endTime = c.endTime,
                  filter = Some(filter),
                  batchSize = c.batchSize,
                  columns = c.columns.toList
                )
                queryByArguments(dconf.url, src, dest, c.buffer)
              case _  ⇒ queryByJsonRequest(dconf.url, c.query, dest, c.buffer)
            }
          case _ ⇒ queryByConfiguration(dconf.url, c.config, c.buffer)
        }
      case None ⇒ System.exit(1)
    }

    def queryByArguments(url: String, src: QueryConfig, dest: DestinationConfig, buff: Int)  = {
      val queries = prepareQuery(src, dest.interval)
      timer {
        queries foreach { x ⇒
          export(url, x, dest.ftpSettings, dest.directory, dest.overwrite, buff)
        }
      }
    }

    def queryByJsonRequest(url:  String, path: String, dest: DestinationConfig, buff: Int): Unit = {
      def parseConfig(path: String): ScanQuery = {
       val query = decode[ScanQuery](fromFile(path).mkString) match {
         case Right(x) ⇒ x
         case Left(y)  ⇒ throw new Exception(s"Invalid query: $y")
       }
       query
      }
       val queries = prepareQuery(parseConfig(path))
       timer {
         queries foreach { x ⇒
           export(url, x, dest.ftpSettings, dest.directory, dest.overwrite, buff)
         }
       }
    }

    def queryByConfiguration(url: String, path: String, buff: Int): Unit = {
      def parseConfig(path: String): DataExportConfig = {
        import _root_.io.circe.yaml.parser
        val json   = parser.parse(fromFile(path).mkString).right.get
        val config = decode[DataExportConfig](json.noSpaces) match {
          case Right(x) ⇒ x
          case Left(y)  ⇒ throw new Exception(s"Invalid configuration: $y")
        }
        config
      }

      val config: DataExportConfig = parseConfig(path)
      val dest: DestinationConfig  = config.destination
      val queries = prepareQuery(config.source, dest.interval)
      timer {
        queries foreach { x ⇒
          export(url, x, dest.ftpSettings, dest.directory, dest.overwrite, buff)
        }
      }
    }

  } // end of main
}
