package moe.karla.handler.default



import moe.karla.config.AppConfig
import moe.karla.service.TaskHelper
import moe.karla.metadata.*


import zio.*
import zio.stream.*

import zio.config.*
import zio.config.typesafe.*
import zio.config.magnolia.deriveConfig

import zio.http.*
import zio.http.Client
import zio.http.Header.AcceptEncoding


import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill

import org.jsoup.Jsoup
import org.jsoup.nodes.Element

import org.apache.commons.compress.archivers.examples.Expander

import okhttp3.RequestBody

import scala.concurrent.duration.DurationInt

import java.io.IOException
import java.nio.file.*
import java.sql.SQLException
import java.util.UUID
import java.net.MalformedURLException




object HentaiMangaHandler:
  
  object HentaiMangaConfig:
    
    final case class DownloadConfig(

      decompress: Boolean
    )

    def load =
      ConfigProvider.fromHoconFile:
          new java.io.File("config/hentaimanga.conf")
        .load:
          deriveConfig[HentaiMangaConfig].mapKey(toKebabCase)
        .tapError:
          e => ZIO.logError(e.getMessage())


    def layer = ZLayer.fromZIO(load)
    

  end HentaiMangaConfig
  

  import HentaiMangaConfig.DownloadConfig


  final case class HentaiMangaConfig(

    agent: Option[String],

    patterns: List[String],

    download: DownloadConfig
  )


  given Config[HentaiMangaConfig] = deriveConfig[HentaiMangaConfig]


  final case class ParsingError(message: String) extends RuntimeException(message)

  final case class ServerError(message: String) extends RuntimeException(message)

  final case class BypassError(message: String) extends RuntimeException(message)

  final case class InvalidDownloadLinkError(causeUrl: String, message: String) extends RuntimeException(message)

  final case class DownloadStreamError(causeUrl: String, message: String) extends RuntimeException(message)


  trait IdleIndicator:

    def isIdle: UIO[Boolean]

    def setIdle(value: Boolean): UIO[Unit]
  
  end IdleIndicator


  final class IdleIndicatorLive(ref: Ref[Boolean]) extends IdleIndicator: 

    def isIdle = ref.get

    def setIdle(value: Boolean) = ref.set(value)

  end IdleIndicatorLive



  val layer: ZLayer[TaskHelper & AppConfig & CommonConfig, Nothing, HentaiMangaHandler] =
    ZLayer {
      for
        hentaiMangaConfig <- HentaiMangaConfig.load.orDie
        appConfig <- ZIO.service[AppConfig]
        commonConfig <- ZIO.service[CommonConfig]
        taskHelper <- ZIO.service[TaskHelper]
        idleIndicator <- Ref.make(true).map(IdleIndicatorLive(_))
      yield 
        HentaiMangaHandler(
          hentaiMangaConfig, 
          appConfig.downPath, 
          appConfig.cachePath, 
          commonConfig.defaultUserAgent, 
          idleIndicator, 
          taskHelper
        )
    }
    

end HentaiMangaHandler


final class HentaiMangaHandler(
  
  config: HentaiMangaHandler.HentaiMangaConfig,

  downPath: String,
  
  cachePath: String,

  defaultUserAgent: String,

  idleIndicator: HentaiMangaHandler.IdleIndicator,

  taskHelper: TaskHelper,

) extends moe.karla.handler.Handler:

  def canHandle(url: String) =
    ZIO.succeed(config.patterns.exists(url.matches(_)))


  def isIdle = idleIndicator.isIdle
  

  private val expander = new Expander


  private def configureRequest(url: URL) =
    Request.get(url)
      .removeHeader("User-Agent")
      .addHeader("User-Agent", config.agent.getOrElse(defaultUserAgent))
      .addHeader(AcceptEncoding(AcceptEncoding.GZip(), AcceptEncoding.Deflate()))
  

  import HentaiMangaHandler.*


  extension [R, E <: Throwable, A] (mo: ZIO[R, E, A])
    private def defaultRetry = 
      mo.retry(defaultRetryFrequency && Schedule.recurWhile[Throwable] {
        case _: (BypassError | ParsingError) => false
        case _ => true
      })



  private def parseAndRetrievePages(meta: MangaMeta)
    : RIO[zio.http.Client & Scope, (MangaMeta, List[MangaPage])] =
    for
      client <- ZIO.service[Client]

      _ <- ZIO.log(s"Parsing: '${meta.galleryUri}'")

      url <- ZIO.fromEither(URL.decode(meta.galleryUri))
      
      body <-
        client.request:
            configureRequest(url)
          .flatMap: resp =>
            if (resp.status.code == 403) 
              ZIO.fail(BypassError(s"Cloudflare blocking presents while parsing: '${url}'"))
            else if (resp.status.code == 404)
              ZIO.fail(ParsingError(s"404 not found presents while parsing: '${url}'"))
            else if (resp.status.code > 500)
              ZIO.fail(ServerError(s"50x presents while parsing: '${url}'")) 
            else resp.body.asString
          .tapSomeError:
            case e if !e.isInstanceOf[BypassError | ParsingError] => 
              ZIO.logError(s"Exception is raised while parsing: '${url}': ${e.getMessage}")
          .defaultRetry
          .tapSomeError:
            case e if !e.isInstanceOf[BypassError | ParsingError] =>
              ZIO.logError(s"Maximum number of retries has been reached while parsing: '${url}'. => abort.")
          .map:
            Jsoup.parse(_).body

      titleString <- 
        ZIO.fromOption:
            Option(body.selectFirst("div#bodywrap > h2"))
              .map(_.wholeText.filtered)
              .collect { case s: String if s.size > 0 => s }
          .mapError: _ => 
            ParsingError(s"Extracting title failed while parsing: '${url}'")


      downloadPage <- 
        ZIO
          .fromOption:
            Option(body.selectFirst("div.download_btns > a.btn")).map(_.attr("href")).map(path => 
              s"${url.scheme.get.encode}://${url.host.get}${path}")
          .mapError: _ => 
            ParsingError(s"Extracting download page failed while parsing: '${url}'")


      parsedMeta = meta.copy(title = Some(titleString), totalPages = 1, isParsed = true)

      parsedPages =
        List:
          MangaPage(
            0,
            parsedMeta.id,
            downloadPage,
            1,
            s"${titleString.filtered}",
            MangaPage.State.Pending.code,
          )
      
    yield (parsedMeta, parsedPages)

  end parseAndRetrievePages



  private def downloadSinglePage(page: MangaPage): RIO[zio.http.Client & Scope, Long] = 
    {
      def processFileExtensionName(bytes: List[Byte]): Either[List[Byte], String] =
        bytes match
          //0x50 = 0101 0000 -> self = 80
          //0x4B = 0100 1011 -> self = 75
          //0x03 = 0000 0011 -> self = 3
          case 80 :: 75 :: 3 :: _ => Right("zip")
          //0x52 = 0101 0010 -> self = 82
          //0x61 = 0110 0001 -> self = 97
          //0x72 = 0111 0010 -> self = 114
          case 82 :: 97 :: 114 :: _ => Right("rar")
          //0x37 = 0011 0110 -> self = 55
          //0x7A = 0111 1010 -> self = 122
          //0xBC = 1011 1100 -> 1100 0011 -> 1100 0100 = -68
          case 55 :: 122 :: -68 :: _ => Right("7z")
          
          case _ => Left(bytes)

      end processFileExtensionName

      case class LinkGenConfig(
        WORKER_API: String,
        FILE_KEY: String,
        FILE_NAME: String,
      )
      case class LinkGenReq(
        file_key: String,
        file_name: String,
      )
      case class LinkGenResponse(
        success: Boolean,
        url: String,
      )

      import zio.schema.Schema
      import zio.schema.DeriveSchema
      import zio.schema.codec.DecodeError
      import zio.schema.codec.JsonCodec.*

      given lgSchema: Schema[LinkGenConfig] = DeriveSchema.gen[LinkGenConfig]
      given lrSchema: Schema[LinkGenReq] = DeriveSchema.gen[LinkGenReq]
      given lpSchema: Schema[LinkGenResponse] = DeriveSchema.gen[LinkGenResponse]

      import scala.jdk.CollectionConverters.*

      import okhttp3.OkHttpClient

      def getLink(linkGenUrl: URL, req: LinkGenReq): ZIO[OkHttpClient & Scope, Exception, String] =
        for
          okhttpCli <- ZIO.service[OkHttpClient]
          /*
          host <-
            ZIO.attempt:
                s"${linkGenUrl.host.get}${
                  linkGenUrl.port.map(i => if (i == 443 || i == 80) "" else s":${i}").getOrElse("")
                }"
              .mapError: _ =>
                ParsingError(s"Extracting base url failed while parsing '${linkGenUrl}'")
          //_ <- ZIO.log(s"Host: ${host}")
          //_ <- ZIO.log(s"User-Agent: ${config.agent.getOrElse(defaultUserAgent)}")
          */
          linkReq = 
            okhttp3.Request.Builder()
              .url(linkGenUrl.encode)
              .post(RequestBody.create(jsonEncoder(lrSchema).encodeJson(req).toString(), okhttp3.MediaType.get("application/json")))
              .removeHeader("Accept")
              .addHeader("Accept", "*/*")
              .removeHeader("User-Agent")
              .addHeader("User-Agent", config.agent.getOrElse(defaultUserAgent))
              /*
              .removeHeader("Referer")
              .addHeader("Referer", linkGenUrl.encode)
              .removeHeader("Host")
              .addHeader("Host", host)
              */
              .removeHeader("Accecpt-Language")
              .addHeader("Accept-Language", "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5")
              .build()

          resp <-
            ZIO.fromAutoCloseable:
              ZIO.attemptBlockingIO:
                okhttpCli.newCall(linkReq).execute

          _ <-
            ZIO.whenCaseDiscard(resp.code()) {
              case 403 =>
                ZIO.fail:
                  BypassError(s"Cloudflare blocking presents while parsing: '${linkGenUrl.encode}'")
              case 404 =>
                ZIO.fail:
                  ParsingError(s"404 not found presents while parsing: '${linkGenUrl.encode}'")
              case i if i >= 500 =>
                ZIO.fail:
                  ServerError(s"50x presents while parsing: '${linkGenUrl.encode}'")
            }

          linkResp <- 
            ZIO.fromEither:
                resp.to[LinkGenResponse]
              .mapError: str =>
                ParsingError(s"Extracting download link from json failed: '${str}'")

        yield linkResp.url


      def getStream(archiveUri: String): ZIO[OkHttpClient & Scope, Exception, ZStream[Any, IOException, Byte]] =
        for
          okhttpCli <- ZIO.service[OkHttpClient]

          req = 
            okhttp3.Request.Builder()
              .url(archiveUri)
              .removeHeader("Accept")
              .addHeader("Accept", "*/*")
              .removeHeader("User-Agent")
              .addHeader("User-Agent", config.agent.getOrElse(defaultUserAgent))
              /*
              .removeHeader("Referer")
              .addHeader("Referer", linkGenUrl.encode)
              .removeHeader("Host")
              .addHeader("Host", host)
              */
              .removeHeader("Accecpt-Language")
              .addHeader("Accept-Language", "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5")
              .build()

          resp <-
            ZIO.fromAutoCloseable:
              ZIO.attemptBlockingIO:
                okhttpCli.newCall(req).execute

          _ <-
            ZIO.whenCaseDiscard(resp.code()) {
              case 404 =>
                ZIO.fail(InvalidDownloadLinkError(archiveUri, s"404 not found presents while trying to download: '${archiveUri}'"))
              case 403 =>
                ZIO.fail(InvalidDownloadLinkError(archiveUri, s"403 Invalid Signature: '${archiveUri}'"))
              case 410 =>
                ZIO.fail(InvalidDownloadLinkError(archiveUri, s"410 Link is expired: '${archiveUri}'"))
              case cod if cod > 500 =>
                ZIO.fail(ServerError(s"50x presents while trying to download: '${archiveUri}'"))
            }
          
        yield 
          ZStream.fromInputStreamZIO:
            ZIO.attemptBlockingIO(resp.body().byteStream())


      for
        _ <- ZIO.log(s"Parsing: '${page.pageUri}'")

        pageUrl <- ZIO.fromEither(URL.decode(page.pageUri))

        client <- ZIO.service[Client]

        body <-
          client
            .request(configureRequest(pageUrl))
          .flatMap: resp =>
            resp.status.code match
              case 403 =>
                ZIO.fail(BypassError(s"Cloudflare blocking presents while parsing: '${pageUrl}'"))
              case 404 =>
                ZIO.fail(ParsingError(s"404 not found presents while parsing: '${pageUrl}'"))
              case cod if cod > 500 =>
                ZIO.fail(ServerError(s"50x presents presents while parsing: '${pageUrl}'")) 
              case _ => 
                resp.body.asString
          .tapSomeError:
            case e if !e.isInstanceOf[BypassError | ParsingError] => 
              ZIO.logError(s"Exception is raised when parsing: '${pageUrl}': ${e.getMessage()}")
          .defaultRetry
          .tapSomeError:
            case e if !e.isInstanceOf[BypassError | ParsingError] => 
              ZIO.logError(s"Maximum number of retries has been reached while parsing: '${pageUrl}'. => abort.")
          .map:
            Jsoup.parse(_).body
        
        configPattern = "const CONFIG = \\{(.)*\"(.)*\\};".r

        lc <- 
          (
            for 
              lc <- 
                ZIO.fromEither:
                    zio.schema.codec.JsonCodec.jsonDecoder(lgSchema).decodeJson(
                      body.getElementsByTag("script").asList.asScala.map: ele =>
                          configPattern.findFirstIn(ele.data.replace('\n', ' '))
                        .flatten
                        .head
                        .replace("const CONFIG = {", "{")
                        .replace("WORKER_API:", "\"WORKER_API\":")
                        .replace("FILE_KEY:", "\"FILE_KEY\":")
                        .replace("FILE_NAME:", "\"FILE_NAME\":")
                        .replace(';', ' ')
                    )
              linkGenApi <- ZIO.fromEither(URL.decode(lc.WORKER_API))
            yield (linkGenApi, lc.FILE_KEY, lc.FILE_NAME)
          )
          .mapError: _ =>
            ParsingError(s"Invalid link configuration presents while parsing: '${pageUrl}'")

        (linkGenUrl, _key, _name) = lc

        //_ <- ZIO.debug(s"url: '${linkGenUrl}', key: '${_key}', name: '${_name}'")

        _ <- ZIO.sleep(2 seconds)

        _ <- ZIO.log(s"Retrieving download link from: '${linkGenUrl}'")

    
        archiveUri <-
          getLink(linkGenUrl, LinkGenReq(_key, _name))
          /*
          client.request(
              configureRequest(linkGenUrl).copy(method = Method.POST, body = Body.from(LinkGenReq(_key, _name)))
            )
            .flatMap: resp =>
              resp.status.code match
                case 403 =>
                  ZIO.fail(BypassError(s"Cloudflare blocking presents while retrieving download link: '${linkGenUrl}'"))
                case 404 =>
                  ZIO.fail(ParsingError(s"404 not found presents while retrieving download link: '${linkGenUrl}'"))
                case cod if cod > 500 =>
                  ZIO.fail(ServerError(s"50x presents while retrieving download link: '${linkGenUrl}'")) 
                case _ =>
                  resp.body
                    .to[LinkGenResponse]
                    .mapError:
                      case e: DecodeError =>
                        ParsingError(e.message)
                    .map(_.url)
            */
            .tapSomeError:
              case ServerError(mesg) =>
                ZIO.logError(mesg)
              case e if !e.isInstanceOf[BypassError | ParsingError] => 
                ZIO.logError(s"Exception is raised while retrieving download link: '${linkGenUrl}': ${e.getMessage()}")
            .defaultRetry
            .tapSomeError:
              case e if !e.isInstanceOf[BypassError | ParsingError] => 
                ZIO.logError(s"Maximum number of retries has been reached while retrieving download link: '${linkGenUrl}'. => abort.")


        ref <- Ref.make(Either.cond[List[Byte], String](true, "unknownfile", List[Byte]()))

        fireSink = ZSink.collectAllN[Byte](6).map(_.toList).mapZIO(bytes => ref.set(processFileExtensionName(bytes)))

        cfile = s"${cachePath}/${UUID.randomUUID().toString()}.tmp"

        _ <- ZIO.log(s"To download: '${archiveUri}'")

        length <-
          getStream(archiveUri)
          /*
          client.request(Request.get(archiveUri))
            .flatMap { resp =>
              resp.status.code match
                case 404 =>
                  //ZIO.fail(ParsingError(s"404 not found presents while trying to download: '${archiveUri}'"))
                  ZIO.fail(InvalidDownloadLinkError(archiveUri, s"404 not found presents while trying to download: '${archiveUri}'"))
                case 403 =>
                  ZIO.fail(InvalidDownloadLinkError(archiveUri, s"403 Invalid Signature: '${archiveUri}'"))
                case 410 =>
                  ZIO.fail(InvalidDownloadLinkError(archiveUri, s"410 Link is expired: '${archiveUri}'"))
                case cod if cod > 500 =>
                  ZIO.fail(ServerError(s"50x presents while trying to download: '${archiveUri}'"))
                case _ =>
                  ZIO.succeed(resp.body.asStream)
            }
            */
            .flatMap(
              _.timeout(6 hours).tapSink(fireSink).run(ZSink.fromFileName(cfile))
                .mapError: thr =>
                  DownloadStreamError(archiveUri, s"Exception is raised while downloading '${archiveUri}': ${thr.getMessage()}")
            )
            .tapError: thr =>
              ZIO.logError(thr.getMessage())
            .onInterrupt: _ =>
              defaultRemoveFile(cfile).forkDaemon
            .retry(
              Schedule.recurs(2) && 
              Schedule.spaced(3 seconds) &&
              Schedule.recurWhile[Throwable] {
                case ServerError(_) => true
                case _ => false 
              }
            )

        preExt <- ref.get

        _ <- ZIO.when(preExt.isLeft)(
          ZIO.logWarning(s"Unknown file signature is detected, archive uri: '${archiveUri}', page uri: '${page.pageUri}'")
        )

        ext = preExt.getOrElse("unknownfile")

        savedPathRef <- Ref.make(s"${downPath}/${page.title}.${ext}")

        _ <-
          if (config.download.decompress) {
            ext match
              case "zip" => 
                ZIO.attemptBlockingIO:
                    expander.expand(Paths.get(cfile), Paths.get(s"${downPath}/${page.title}"))
                  .catchAll: _ => 
                    ZIO.logWarning(s"Decompress failure presents: '${cfile}', fall back to compressed file.") *>
                      savedPathRef.get.flatMap: p =>
                        defaultMoveFile(cfile, p)
                  .flatMap: _ =>
                    ZIO.attemptBlockingIO(Files.deleteIfExists(Paths.get(cfile))) *>
                      savedPathRef.set(s"${downPath}/${page.title}")
              case _ =>
                ZIO.logWarning(s"Decompress supports only zip archives currently: '${cfile}'") *>
                  savedPathRef.get.flatMap: p =>
                    defaultMoveFile(cfile, p)
          }
          else
            savedPathRef.get.flatMap: p =>
              defaultMoveFile(cfile, p)
        
        savedPath <- savedPathRef.get

        _u = {
          val loc = archiveUri.indexOf('&')
          if (loc != -1)
            archiveUri.substring(0, loc)
          else archiveUri
        }

        _ <- ZIO.log(s"Saved: '${_u}' as '${savedPath}', downloaded size: ${String.format("%.2f", length.toFloat / 1024 / 1024)} MB")
        
      yield length
    }
    .defaultRetry
    .tapSomeError:
      case DownloadStreamError(url, _) =>
        ZIO.logError(s"Maximum number of retries has been reached while downloading, current download url: '${url}'. => abort.")
      case InvalidDownloadLinkError(url, _) =>
        ZIO.logError(s"Maximum number of retries has been reached while retrieving valid download link, last link is: '${url}'. => abort.")
    .provideSomeLayer:
      moe.karla.handler.default.OkHttpClient.live

  end downloadSinglePage



  def handle(meta: MangaMeta): URIO[zio.http.Client & Scope, Unit] =
  (
    for
      _ <- idleIndicator.setIdle(false)
      _ <-
        ZIO.whenCaseDiscard(meta.isParsed) {
          case true => {
            for
              p <- taskHelper.acquirePage(meta.id).map(_.get)
              _ <- 
                downloadSinglePage(p).tapError: _ =>
                  taskHelper.failPage(p.id)
              _ <- taskHelper.completePage(meta.id, p.id)
            yield ()
          }
          case false => {
            for
              t <- parseAndRetrievePages(meta)
              (m, ps) = t
              _ <- taskHelper.submitParsed(m, ps)
              p <- taskHelper.acquirePage(meta.id).map(_.get)
              _ <-
                downloadSinglePage(p).tapError: _ =>
                  taskHelper.failPage(p.id)
              _ <- taskHelper.completePage(meta.id, p.id)
            yield ()
          }
        }
        .tapError: e => 
          taskHelper.failManga(meta.id, e.getMessage())
        .tapSomeError:
          case e: (BypassError | ParsingError | MalformedURLException) => 
            ZIO.logError(s"${e.getMessage()}. => abort.")

      _ <- taskHelper.completeManga(meta.id)

      _ <- ZIO.sleep(7 seconds)

      _ <- idleIndicator.setIdle(true)
    yield ()
  )
  .catchAll: _ =>
    idleIndicator.setIdle(true)
  /*
    .onError { cause =>
      val msg = cause.dieOption.map(_.getMessage()).getOrElse("???")
      taskHelper.failManga(meta.id, msg).ignore
        *> ZIO.logError(s"Fatal error: ${msg}. => die.")
    }
  */
  .onError: cause =>
    ZIO.logError(s"HentaiMangaHandler: ${cause.dieOption.map(_.getMessage()).getOrElse("???")}. => die.")
  
  end handle