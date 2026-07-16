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
import zio.http.Request
import zio.http.Header.AcceptEncoding

import zio.schema.codec.DecodeError

import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill

import okhttp3.OkHttpClient

import scala.concurrent.duration.DurationInt

import java.nio.file.*
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util.UUID
import java.net.CookieManager
import java.net.MalformedURLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter




object PawchiveHandler:
  
  object PawchiveConfig:

    final case class FanboxDownloadConfig(
      renameImages: Boolean,
    )

    final case class CloudflareConfig(

      strategy: String,

      providedCookies: String
    )
    
    final case class DownloadConfig(

      ignoreArchive: Boolean,

      ignoreLeftIfArchiveExists: Boolean,

      fanbox: FanboxDownloadConfig
    )

    def load =
      ConfigProvider.fromHoconFile:
          new java.io.File("config/pawchive.conf")
        .load:
          deriveConfig[PawchiveConfig].mapKey(toKebabCase)
        .tapError:
          e => ZIO.logError(e.getMessage())


    def layer = ZLayer.fromZIO(load)
   

  end PawchiveConfig
  

  import PawchiveConfig.DownloadConfig
  import PawchiveConfig.CloudflareConfig

  final case class PawchiveConfig(

    agent: Option[String],

    patterns: List[String],

    titlePattern: String,

    cloudflare: CloudflareConfig,

    download: DownloadConfig
  )


  given Config[PawchiveConfig] = deriveConfig[PawchiveConfig]


  final case class ParsingError(message: String) extends RuntimeException(message)

  final case class ServerError(message: String) extends RuntimeException(message)

  final case class BypassError(message: String) extends RuntimeException(message)

  final case class LostMetaError(message: String) extends RuntimeException(message)

  final case class TemporaryError(message: String) extends RuntimeException(message)


  trait IdleIndicator:

    def isIdle: UIO[Boolean]

    def setIdle(value: Boolean): UIO[Unit]
  
  end IdleIndicator


  final class IdleIndicatorLive(ref: Ref[Boolean]) extends IdleIndicator: 

    def isIdle = ref.get

    def setIdle(value: Boolean) = ref.set(value)

  end IdleIndicatorLive

  /*
  trait CookieInitializedIndicator:

    def isInitializd: UIO[Boolean]

    def setInitializd(value: Boolean): UIO[Unit]
  
  end CookieInitializedIndicator


  final class CookieInitializedIndicatorLive(ref: Ref[Boolean]) extends CookieInitializedIndicator: 

    def isInitializd = ref.get

    def setInitializd(value: Boolean) = ref.set(value)

  end CookieInitializedIndicatorLive
  */

  val layer: ZLayer[TaskHelper & AppConfig & CommonConfig, Nothing, PawchiveHandler] =
    ZLayer {
      for
        pawchiveConfig <- PawchiveConfig.load.orDie
        appConfig <- ZIO.service[AppConfig]
        commonConfig <- ZIO.service[CommonConfig]
        idleIndicator <- Ref.make(true).map(IdleIndicatorLive(_))
        //cookiesHolder <- Ref.make(Chunk.empty[Cookie.Request]).map(RequestCookiesHolderLive(_))
        cookieInitialized <- Ref.make(false)
        taskHelper <- ZIO.service[TaskHelper]
      yield 
        PawchiveHandler(
          commonConfig,
          pawchiveConfig,
          appConfig.downPath, 
          appConfig.cachePath,
          idleIndicator,
          cookieInitialized,
          taskHelper
        )
    }
  

  final case class FileMeta(
    name: Option[String],
    path: Option[String],
  )

  final case class DefiniteFileMeta(
    name: String, path: String,
  ):
    private[PawchiveHandler] def isCompressed = 
      name.endsWith(".zip") ||
      name.endsWith(".rar") ||
      name.endsWith(".7z")
    /*
    private [PawchiveHandler] def isMedia = 
      name.endsWith(".mp4") ||
      name.endsWith(".mkv") ||
      name.endsWith(".wav") ||
      name.endsWith(".flac")
    */
    private[PawchiveHandler] def isImage = 
      name.endsWith(".jpg") ||
      name.endsWith(".jpeg") ||
      name.endsWith(".png") ||
      name.endsWith(".webp")

  end DefiniteFileMeta

  final case class LesserPostMeta(
    id: String,
    user: String,
    title: String,
    service: String,
    published: LocalDateTime,
    file: FileMeta,
    attachments: List[FileMeta]
  )


  type ApiPostResp = LesserPostMeta

  type ApiPostListResp = List[LesserPostMeta]
  

  final case class ApiProfileResp(
    name: String,
    public_id: Option[String],
    service: String,
  )

end PawchiveHandler



final class PawchiveHandler(
  
  commonConfig: CommonConfig,

  pawchiveConfig: PawchiveHandler.PawchiveConfig,

  downPath: String,
  
  cachePath: String,

  idleIndicator: PawchiveHandler.IdleIndicator,

  cookieInitialized: Ref[Boolean],

  taskHelper: TaskHelper,

) extends moe.karla.handler.Handler:


  def canHandle(url: String) =
    ZIO.succeed(pawchiveConfig.patterns.exists(url.matches(_)))


  def isIdle = idleIndicator.isIdle


  extension [A <: String] (str: A)
    private def mayClip =
      if (commonConfig.titleTruncation && str.getBytes(StandardCharsets.UTF_8).length > 254)
        s"${str.substring(0, 84)}…"
      else str


  private def configureRequest(url: URL, cookie: String = "") =
    if (pawchiveConfig.cloudflare.strategy == "provided-cookies")
      for
        host <-
          ZIO.attempt:
              s"${url.host.get}${
                url.port.map(i => if (i == 443 || i == 80) "" else s":${i}").getOrElse("")
              }"
            .mapError: _ =>
              MalformedURLException(s"Extracting base url failed while parsing '${url}'")
        //_ <- ZIO.log(s"Host: ${host}")
        //_ <- ZIO.log(s"User-Agent: ${config.agent.getOrElse(defaultUserAgent)}")
        builder = okhttp3.Request.Builder()
        _ = {
          if (cookie != "")
            builder.addHeader("Cookie", cookie)
        }
      yield
        builder
          .url(url.encode)
          .removeHeader("Accept")
          .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
          .removeHeader("User-Agent")
          .addHeader("User-Agent", pawchiveConfig.agent.getOrElse(commonConfig.defaultUserAgent))
          //.removeHeader("Referer")
          //.addHeader("Referer", url.encode)
          .removeHeader("Host")
          .addHeader("Host", host)
          .removeHeader("Accecpt-Language")
          .addHeader("Accept-Language", "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5")
          .build()
    else
      ZIO.fail:
        UnsupportedOperationException("Current strategy for bypassing cloudflare is not implemented")

  /*
  private def configureRequest(url: URL) =
    Request.get(url)
      .removeHeader("User-Agent")
      .addHeader("User-Agent", pawchiveConfig.agent.getOrElse(commonConfig.defaultUserAgent))
      .removeHeader("Accept-Encoding")
      .addHeader(AcceptEncoding(AcceptEncoding.GZip(), AcceptEncoding.Deflate()))
      .removeHeader("Accept")
  */

  import PawchiveHandler.*


  extension [R, E <: Throwable, A] (mo: ZIO[R, E, A])
    private def defaultRetry = 
      mo.retry(defaultRetryFrequency && Schedule.recurWhile[Throwable] {
        case _: (BypassError | ParsingError | DecodeError) => false
        case _ => true
      })



  private def parseAndRetrievePages(meta: MangaMeta)
    : RIO[zio.http.Client & Scope, (MangaMeta, List[MangaPage])] = {

      import zio.schema.Schema
      import zio.schema.DeriveSchema
      import zio.schema.codec.JsonCodec.*


      given postSchema: Schema[ApiPostResp] = DeriveSchema.gen[ApiPostResp]

      given postListSchema: Schema[ApiPostListResp] = DeriveSchema.gen[ApiPostListResp]

      given profileSchema: Schema[ApiProfileResp] = DeriveSchema.gen[ApiProfileResp]

      for
        endpointsInfo <- (
            for 
              galleryUrl <- 
                ZIO.fromEither(URL.decode(meta.galleryUri))
              /*
              base <-
                ZIO.attempt:
                    s"${galleryUrl.scheme.get.encode}://${galleryUrl.host.get}${
                      galleryUrl.port.map(i => if (i == 443 || i == 80) "" else s":${i}").getOrElse("")
                    }"
                  .mapError: _ => 
                    ParsingError(s"Error extracting base url while parsing '${galleryUrl}'")
              */
              base = galleryUrl.path(zio.http.Path.empty).removeQueryParam("o")

              tup <-
                if (galleryUrl.encode.contains("/post/"))
                  //ZIO.fromEither(URL.decode(meta.galleryUri.replace(base, s"${base}/api/v1/")))
                  ZIO.fromEither(URL.decode(s"${base}/api/v1${galleryUrl.path.encode}"))
                    .flatMap: url => 
                      ZIO.fromEither(URL.decode(url.encode.replaceAll("post/(\\d+)", "profile")))
                        .map((_, url, true))
                else {
                  //ZIO.fromEither(URL.decode(meta.galleryUri.replace(base, s"${base}/api/v1/") + "/posts"))
                  ZIO.fromEither(URL.decode(s"${base}/api/v1${galleryUrl.path.encode}/posts${galleryUrl.queryParams.encode}"))
                    .flatMap: url => 
                      ZIO.fromEither(URL.decode(url.removeQueryParam("o").encode.replace("posts", "profile")))
                        .map((_, url, false))
                }

            yield (base, tup._1, tup._2, tup._3)
          )
          .tapError: e =>
            ZIO.logError(s"${e.getMessage}. => abort.")
        
        (base, profileApiEndpoint, postApiEndpoint, postFlag) = endpointsInfo


        okClient <- ZIO.service[OkHttpClient]

        cookied <- cookieInitialized.get

        providedCookies = pawchiveConfig.cloudflare.providedCookies

        _ <- ZIO.when(!cookied && providedCookies != "")(
          ZIO.log(s"Sending request to '${base}' for ensuring initial cookies(${providedCookies}) are valid.") *>
            configureRequest(base, providedCookies)
              .flatMap: req =>
                ZIO.fromAutoCloseable:
                  ZIO.attemptBlockingIO:
                    okClient.newCall(req).execute()
              .flatMap: resp =>
                ZIO.when(resp.code() == 403)(
                  ZIO.fail(BypassError(s"Anti-bots blocking presents on: '${base}'. => abort."))
                )
              *> cookieInitialized.set(true)
        )

        //client<- ZIO.service[Client]
        
        _ <- ZIO.log(s"Requesting api: '${profileApiEndpoint}'.")

        profileMeta <- 
          configureRequest(profileApiEndpoint)
            .flatMap: req =>
              ZIO.fromAutoCloseable:
                ZIO.attemptBlockingIO:
                  okClient.newCall(req).execute()
            .flatMap: resp =>
              resp.code() match
                case 403 =>
                  ZIO.fail(BypassError(s"Anti-bots blocking presents when parsing: '${profileApiEndpoint}'"))
                case 404 =>
                  ZIO.fail(ParsingError(s"404 not found presents when parsing: '${profileApiEndpoint}'"))
                case i if i > 500 =>
                  ZIO.fail(ServerError(s"50x presents presents when parsing: '${profileApiEndpoint}'"))
                case _ =>
                  ZIO.fromEither:
                    resp.to[ApiProfileResp].left.map(ParsingError(_))
            .tapError:
              case bp: (BypassError | ParsingError | DecodeError) =>
                ZIO.logError(s"${bp.getMessage}. => abort.")
              case t =>
                ZIO.logError(s"Exception is raised when parsing: '${profileApiEndpoint}': ${t.getMessage}")
            .defaultRetry
            .tapSomeError:
              case e if !e.isInstanceOf[BypassError | ParsingError | DecodeError] => 
                ZIO.logError(s"Maximum number of retries has been reached while parsing: '${profileApiEndpoint}'. => abort.")
        
        tup <- {
          def fetchSchema[T](siteURL: URL)(using sc: Schema[T]): RIO[Scope, T] =
            configureRequest(siteURL)
              .flatMap: req =>
                ZIO.fromAutoCloseable:
                  ZIO.attemptBlockingIO:
                    okClient.newCall(req).execute
              .flatMap: resp =>
                resp.code() match
                  case 403 =>
                    ZIO.fail(BypassError(s"Anti-bots blocking presents when parsing: '${siteURL}'"))
                  case 404 =>
                    ZIO.fail(ParsingError(s"404 not found presents when parsing: '${siteURL}'"))
                  case i if i > 500 =>
                    ZIO.fail(ServerError(s"50x presents presents when parsing: '${siteURL}'"))
                  case _ =>
                    ZIO.fromEither:
                      //zio.schema.codec.JsonCodec.jsonDecoder(sc).decodeJson(resp.body().string()).left.map(str => ParsingError(str))
                      resp.to[T].left.map(str => ParsingError(str))
              .tapError:
                case bp: (BypassError | ParsingError | DecodeError) =>
                  ZIO.logError(s"${bp.getMessage}. => abort.")
                case t =>
                  ZIO.logError(s"Exception is raised when parsing: '${siteURL}': ${t.getMessage}")
              .defaultRetry
              .tapSomeError:
                case e if !e.isInstanceOf[BypassError | ParsingError | DecodeError] => 
                  ZIO.logError(s"Maximum number of retries has been reached while parsing: '${siteURL}'. => abort.")
          end fetchSchema

          def buildTitle(postMeta: LesserPostMeta) =
              pawchiveConfig.titlePattern
                .replace(":title", postMeta.title)
                .replace(":artist", profileMeta.name)
                .replace(":service", postMeta.service)
                .replace(":time", postMeta.published.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")))
                .replace(":date", postMeta.published.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
                .filtered
                .mayClip

          def buildAttachs(postMeta: LesserPostMeta) = {
            val attachs = 
              (postMeta.file :: postMeta.attachments)
                .filter:
                  _.path.isDefined
                .distinct
                .map: mt =>
                  (mt.name, mt.path.get)
                .map: (nameOption, mp) =>
                  DefiniteFileMeta(nameOption.getOrElse(mp.substring(mp.lastIndexOf('/') + 1)), mp)
            (
              if (pawchiveConfig.download.ignoreArchive)
                attachs.filterNot:
                  _.isCompressed
              else if (
                pawchiveConfig.download.ignoreLeftIfArchiveExists && 
                attachs.exists(_.isCompressed)
              )
                attachs.filter:
                  _.isCompressed
              else
                if (pawchiveConfig.download.fanbox.renameImages && postMeta.service == "fanbox") {
                  val (a, b) = attachs.partition(_.isImage)
                  b ::: a
                }
                else
                  attachs
            )
            .zipWithIndex
            .map: (ath, index) =>
              {
                val athName = if (
                  pawchiveConfig.download.fanbox.renameImages && ath.isImage
                )
                  s"${index + 1}${ath.name.substring(ath.name.lastIndexOf("."))}".filtered
                else
                  ath.name.filtered

                MangaPage(
                  0,
                  meta.id,
                  s"${base.encode.replace("https://", "https://file.")}/data/${ath.path}",
                  index + 1,
                  if (postFlag)
                    athName
                  else
                    s"${buildTitle(postMeta)}/${athName}"
                  ,
                  MangaPage.State.Pending.code,
                )
              }
          }

          for
            _ <- ZIO.log(s"Requesting api: '${postApiEndpoint}'.")

            ttap <-
              ZIO.whenCase(postApiEndpoint.encode) {
                case postURL if postURL.matches("(.)+/(\\w+)/user/(\\d+)/post/(\\d+)") =>
                  fetchSchema[ApiPostResp](postApiEndpoint).map: postMeta =>
                    (buildTitle(postMeta), buildAttachs(postMeta))

                case userURL if userURL.matches(s"(.)+/(\\w+)/user/(\\d+)/posts(\\?o=(\\d+))?") =>
                  fetchSchema[ApiPostListResp](postApiEndpoint).map: postMetaList =>
                    (s"Pawchive：${profileMeta.name.filtered}", postMetaList.map(buildAttachs).filter(a => a.length > 0).flatten)
              }
          yield ttap
        }
        .someOrElseZIO:
          ZIO.logError(s"Not in case matching: '${postApiEndpoint.encode}'. => abort.") *>
            ZIO.fail:
              ParsingError(s"Not in case matching: '${postApiEndpoint.encode}'")

        (title, parsedPages) = tup

        _ <- ZIO.when(parsedPages.length < 1)(
          ZIO.logError(s"No available posts were found while parsing: '${postApiEndpoint}'") *>
            ZIO.fail:
              ParsingError(s"No available posts were found while parsing: '${postApiEndpoint}'")
        )

        parsedMeta =
          meta.copy(
            title = Some(title),
            totalPages = parsedPages.length, 
            isParsed = true
          )
        
      yield (parsedMeta, parsedPages)
    }
    .provideSomeLayer:
      moe.karla.handler.default.OkHttpClient.live


  private def downloadSinglePage(title: String, page: MangaPage): RIO[zio.http.Client & Scope, Long] = (
    for
      pageUrl <-
        ZIO.fromEither:
            URL.decode(page.pageUri)
          .tapError: e =>
              ZIO.logError(s"${e.getMessage}. => abort.")

      _ <- ZIO.log(s"Downloading: '${pageUrl}'")

      /*
      _client <- ZIO.service[Client]

      client = 
        _client @@ ZClientAspect.followRedirects(2)((resp, message) => 
          ZIO.logError(s"Redirect Error: ${message}").as(resp)
        )
      */
      okClient <- ZIO.service[OkHttpClient]

      cfile = s"${cachePath}/${UUID.randomUUID().toString()}.tmp"

      fireSink =
        ZSink.collectAllN[Byte](20).mapZIO: chk =>
          ZIO.when(String(chk.toArray).startsWith("<html>"))(
            ZIO.fail(ServerError(s"Unexpected html presents when downloading: '${pageUrl}'")) 
          )

      length <-
        configureRequest(pageUrl)
          .flatMap: req =>
            ZIO.fromAutoCloseable:
              ZIO.attemptBlockingIO:
                okClient.newCall(req).execute
          .flatMap: resp =>
            resp.code() match 
              case 403 =>
                ZIO.fail(BypassError(s"Anti-bots blocking presents when downloading: '${pageUrl}'"))
              case 404 =>
                ZIO.fail(ParsingError(s"404 not found presents when downloading: '${pageUrl}'"))
              case i if i > 500 =>
                ZIO.fail(ServerError(s"50x presents presents when downloading: '${pageUrl}'"))
              case _ =>
                ZStream.fromInputStream(resp.body.byteStream).tapSink(fireSink).timeout(4 hours).run(ZSink.fromFileName(cfile))
          .onInterrupt: _ =>
            defaultRemoveFile(cfile).forkDaemon
          .tapError:
            case bp: (BypassError | ParsingError) =>
              ZIO.logError(s"${bp.getMessage}. => abort.")
            case t =>
              ZIO.logError(s"Exception is raised when downloading: '${pageUrl}': ${t.toString}")
          .defaultRetry
          .tapSomeError:
            case e if !e.isInstanceOf[BypassError | ParsingError] => 
              ZIO.logError(s"Maximum number of retries has been reached while downloading: '${pageUrl}'. => abort.")
                *> defaultRemoveFile(cfile).forkDaemon
          .mapError:
            case ep: (BypassError | ParsingError) =>
              ep
            case o =>
              TemporaryError(o.toString)

      savedPath = s"${downPath}/${title}/${page.title}"

      _ <-
        (
          ZIO.ifZIO(ZIO.succeed(page.title.contains('/')))(
            onTrue = ZIO.attemptBlockingIO(Files.createDirectories(Paths.get(downPath, title, page.title.split('/').head))),
            onFalse = ZIO.attemptBlockingIO(Files.createDirectories(Paths.get(downPath, title)))
          ) *>
            defaultMoveFile(cfile, savedPath)
        )
        .tapError: e =>
          ZIO.logError(s"IOException was raised: '${e.getMessage}'. => abort.")

      _ <- ZIO.log(s"Saved: '${pageUrl}' as '${savedPath}', downloaded size: ${String.format("%.2f", length.toFloat / 1024 / 1024)} MB")
      
    yield length
  )
  .provideSomeLayer(moe.karla.handler.default.OkHttpClient.live)
    

  end downloadSinglePage




  def handle(meta: MangaMeta): URIO[zio.http.Client & Scope, Unit] =
  (
    for
      _ <- idleIndicator.setIdle(false)
      _ <-
        ZIO.whenCaseDiscard(meta.isParsed) {
          case true =>
            Ref.make(meta.completedPages).flatMap: cp =>
              ZStream.fromIterable(meta.completedPages to meta.totalPages)
                .runForeach(_ =>
                    taskHelper.acquirePage(meta.id).flatMap:
                      case Some(p) =>
                        downloadSinglePage(meta.title.get, p)
                          .tapError: e =>
                            taskHelper.failPage(p.id)
                          .flatMap: _ =>
                            taskHelper.completePage(meta.id, p.id)
                      case None =>
                        ZIO.ifZIO(taskHelper.checkMetaExists(meta.id))(
                          onTrue =
                            taskHelper.completeManga(meta.id)
                              *> ZIO.log(s"Completed: '${meta.galleryUri}' as '${meta.title.get}'")
                          ,
                          onFalse = ZIO.fail(LostMetaError(s"Meta (id = ${meta.id}) may be removed"))
                        )
                  )
                *> idleIndicator.setIdle(true)
          case false => 
            for
              t <- parseAndRetrievePages(meta)
              (m, ps) = t
              _ <- taskHelper.submitParsed(m, ps)
              _ <- ZIO.log(s"Parsed: '${m.galleryUri}' as '${m.title.get}'")
              _ <- handle(m)
            yield ()
        }
      _ <- ZIO.sleep(3 seconds)
    yield ()
  )
  .tapError: 
    case l: LostMetaError =>
      ZIO.logError(s"PawchiveHandler: ${l.message}. => abort.")
    case e =>
      taskHelper.failManga(meta.id, e.getMessage())
  .catchAll: _ =>
    idleIndicator.setIdle(true)
  .onError: cause =>
    ZIO.logError(s"PawchiveHandler: ${cause.dieOption.map(_.getMessage()).getOrElse("???")}. => die.")
  
  end handle