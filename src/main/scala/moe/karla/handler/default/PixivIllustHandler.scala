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
import zio.http.Header.SetCookie
import zio.http.Header.AcceptEncoding

import zio.schema.Schema
import zio.schema.codec.DecodeError

import scala.concurrent.duration.DurationInt

import java.nio.file.*
import java.util.UUID
import java.net.MalformedURLException




object PixivIllustHandler:
  
  object PixivConfig:
    
    //final case class CloudflareConfig(
    //  //strategy: String,
    //  providedCookies: String
    //)


    def load =
      ConfigProvider.fromHoconFile:
          new java.io.File("config/pixiv-illust.conf")
        .load:
          deriveConfig[PixivConfig].mapKey(toKebabCase)
        .tapError:
          e => ZIO.logError(e.getMessage())


    def layer = ZLayer.fromZIO(load)
    

  end PixivConfig


  final case class PixivConfig(

    agent: Option[String],

    patterns: List[String],

    defaultSite: String,

    titlePattern: String,

    //cloudflare: CloudflareConfig,
    providedCookies: String
  )


  given Config[PixivConfig] = deriveConfig[PixivConfig]


  final case class ParsingError(message: String) extends RuntimeException(message)

  final case class ServerError(message: String) extends RuntimeException(message)

  final case class BypassError(message: String) extends RuntimeException(message)

  final case class LostMetaError(message: String) extends RuntimeException(message)


  trait IdleIndicator:

    def isIdle: UIO[Boolean]

    def setIdle(value: Boolean): UIO[Unit]
  
  end IdleIndicator


  final class IdleIndicatorLive(ref: Ref[Boolean]) extends IdleIndicator: 

    def isIdle = ref.get

    def setIdle(value: Boolean) = ref.set(value)

  end IdleIndicatorLive

  trait RequestCookiesHolder:

    def getCookies: UIO[Chunk[Cookie.Request]]

    def setCookies(cookies: Chunk[Cookie.Request]): UIO[Unit]
  
  end RequestCookiesHolder

  
  final class RequestCookiesHolderLive(cookiesRef: Ref[Chunk[Cookie.Request]]) extends RequestCookiesHolder:

    def getCookies: UIO[Chunk[Cookie.Request]] = cookiesRef.get

    def setCookies(cookies: Chunk[Cookie.Request]): UIO[Unit] = 
      cookiesRef.set(cookies)

  val layer: ZLayer[TaskHelper & AppConfig & CommonConfig, Nothing, PixivIllustHandler] =
    ZLayer {
      for
        pixivConfig <- PixivConfig.load.orDie
        appConfig <- ZIO.service[AppConfig]
        commonConfig <- ZIO.service[CommonConfig]
        idleIndicator <- Ref.make(true).map(IdleIndicatorLive(_))
        initCookies <- 
          ZIO.fromEither(
            Cookie.decodeRequest(pixivConfig.providedCookies)
          ).orDie
        cookiesHolder <- Ref.make(initCookies).map(RequestCookiesHolderLive(_))
        taskHelper <- ZIO.service[TaskHelper]
      yield 
        PixivIllustHandler(
          pixivConfig,
          appConfig.downPath, 
          appConfig.cachePath, 
          commonConfig.defaultUserAgent, 
          idleIndicator, 
          cookiesHolder,
          taskHelper
        )
    }
  

  final case class ImageUrlMeta(
    small: String,
    regular: String,
    original: String,
  )

  final case class PageMeta(
    urls: ImageUrlMeta,
    width: Int,
    height: Int
  )

  final case class IllustMeta(
    title: String,
    illustType: Int,
    userId: String,
    userName: String,
    userAccount: String,
  )

  import zio.schema.derived

  sealed trait ApiResp derives Schema

  final case class ApiIllustResp(
    error: Boolean,
    message: String,
    body: IllustMeta
  ) extends ApiResp derives Schema

  final case class ApiPagesResp(
    error: Boolean,
    message: String,
    body: List[PageMeta]
  ) extends ApiResp derives Schema

end PixivIllustHandler



final class PixivIllustHandler(
  
  config: PixivIllustHandler.PixivConfig,

  downPath: String,
  
  cachePath: String,

  defaultUserAgent: String,

  idleIndicator: PixivIllustHandler.IdleIndicator,

  cookiesHolder: PixivIllustHandler.RequestCookiesHolder,

  taskHelper: TaskHelper,

) extends moe.karla.handler.Handler:


  def canHandle(url: String) =
    ZIO.succeed:
      config.patterns.exists(url.matches(_)) ||
      url.matches("pixiv://(\\d+)")


  def isIdle = idleIndicator.isIdle


  extension (request: Request)
    private def preconfigure =
      request
        .removeHeader("User-Agent")
        .addHeader("User-Agent", config.agent.getOrElse(defaultUserAgent))
        .removeHeader("Accecpt-Language")
        .addHeader("Accept-Language", "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2")
        .removeHeader("Accept-Encoding")
        .addHeader(AcceptEncoding(AcceptEncoding.GZip(), AcceptEncoding.Deflate()))
  

  import PixivIllustHandler.*


  extension [R, E <: Throwable, A] (mo: ZIO[R, E, A])
    private def defaultRetry = 
      mo.retry(defaultRetryFrequency && Schedule.recurWhile[Throwable] {
        case _: (BypassError | ParsingError | DecodeError) => false
        case _ => true
      })


  private inline def makeURL(uriString: String): IO[MalformedURLException, URL] = 
    ZIO.fromEither(URL.decode(
      uriString match
        case u if u.matches("pixiv://(\\d+)") =>
          s"${config.defaultSite}/artworks/${uriString.replace("pixiv://", "")}"
        case _ => uriString
    ))
    


  private def parseAndRetrievePages(meta: MangaMeta)
    : RIO[zio.http.Client & Scope, (MangaMeta, List[MangaPage])] =
      for
        endpoints <- 
          ZIO.ifZIO(ZIO.succeed(meta.galleryUri.matches("pixiv://(\\d+)")))(
            onTrue = {
              val pid = meta.galleryUri.replace("pixiv://", "")
              for
                galleryUrl <- 
                  ZIO.fromEither:
                    URL.decode(s"${config.defaultSite}/artworks/${pid}")
                illustApiEndpoint <- 
                  ZIO.fromEither:
                    URL.decode:
                      s"${config.defaultSite}/ajax/illust/${pid}"
                pagesApiEndpoint <- 
                  ZIO.fromEither:
                    URL.decode:
                      s"${config.defaultSite}/ajax/illust/${pid}/pages?lang=zh"
              yield (pid, galleryUrl, illustApiEndpoint, pagesApiEndpoint)
            },
            onFalse = {
              for
                galleryUrl <- 
                  ZIO.fromEither(URL.decode(meta.galleryUri))
                base <-
                  ZIO.attempt:
                    s"${galleryUrl.scheme.get.encode}://${galleryUrl.host.get}${
                      galleryUrl.port.map(i => if (i == 443 || i == 80) "" else s":${i}").getOrElse("")
                    }"
                  .mapError: _ =>
                    ParsingError(s"Extracting base url failed while parsing '${galleryUrl}'")
                
                pid = galleryUrl.encode.replace(s"${base}/artworks/", "")
                illustApiEndpoint <- 
                  ZIO.fromEither:
                    URL.decode:
                      s"${base}/ajax/illust/${pid}"
                pagesApiEndpoint <- 
                  ZIO.fromEither:
                    URL.decode:
                      s"${base}/ajax/illust/${pid}/pages?lang=zh"
              yield (pid, galleryUrl, illustApiEndpoint, pagesApiEndpoint)
            }
          )
          .tapError: e =>
            ZIO.logError(s"${e.getMessage}. => abort.")


        (pid, galleryUrl, illustApiEndpoint, pagesApiEndpoint) = endpoints

        rc <- cookiesHolder.getCookies

        client <- ZIO.service[Client]

        _resp <- {
          def fetch_[T](siteURL: URL)(using sc: Schema[T]): RIO[Scope, T] =
            client.request:
              Request.get(siteURL)
                .preconfigure
                .removeHeader("Accept")
                .addHeader("Accept", "application/json")
                .removeHeader("Cookie")
                .addHeader(Header.Cookie(NonEmptyChunk.fromChunk(rc).get))
            .tap: re =>
              ZIO.unless(re.status.code == 403)(
                ZIO.succeed(re.headers.getAll(SetCookie)).flatMap: sc =>
                  cookiesHolder.setCookies:
                    rc.map { cc =>
                      val c = sc.filter(_.value.name == cc.name)
                      if (c.length > 0)
                        cc.content(c.head.value.content)
                      else cc
                    }
              )
            .flatMap: resp =>
              if (resp.status.code == 403)
                ZIO.fail(BypassError(s"Anti-robots blocking presents while parsing: '${siteURL}'"))
              else if (resp.status.code == 404)
                ZIO.fail(ParsingError(s"404 not found presents while parsing: '${siteURL}'"))
              else if (resp.status.code > 500)
                ZIO.fail(ServerError(s"50x presents while parsing: '${siteURL}'"))
              else resp.body.asString.flatMap: str =>
                ZIO.fromEither:
                  zio.schema.codec.JsonCodec.jsonDecoder(sc).decodeJson(str).left.map(ParsingError(_))
            .tapError:
              case bp: (BypassError | ParsingError | DecodeError) =>
                ZIO.logError(s"${bp.getMessage}. => abort.")
              case t =>
                ZIO.logError(s"Exception is raised when parsing: '${siteURL}': ${t.getMessage}")
            .defaultRetry
            .tapSomeError:
              case e if !e.isInstanceOf[BypassError | ParsingError | DecodeError] => 
                ZIO.logError(s"Maximum number of retries has been reached while parsing: '${siteURL}'. => abort.")

          for 
            _ <- ZIO.log(s"Requesting api: '${illustApiEndpoint}'.")

            illust <- fetch_[ApiIllustResp](illustApiEndpoint)
            _ <- 
              ZIO.whenCaseDiscard(illust) {
                case ApiIllustResp(true, _, _) =>
                  ZIO.fail(ServerError(s"Parsing was interrupted because error field in api response is true: '${illustApiEndpoint}'"))
                case r if (r.body.illustType == 2) =>
                  ZIO.fail(UnsupportedOperationException(s"Animations are currently unsupported: '${illustApiEndpoint}'")) 
                case _ =>
                  ZIO.unit
              }
              .tapError: e =>
                ZIO.logError(s"${e.getMessage}. => abort.")

            _ <- ZIO.log(s"Requesting api: '${pagesApiEndpoint}'.")

            pages <- fetch_[ApiPagesResp](pagesApiEndpoint)

          yield (illust.body, pages.body)
        }

        (illustMeta, pagesMeta) = _resp

        title = 
          config.titlePattern
            .replace(":title", illustMeta.title)
            .replace(":artist", illustMeta.userName)
            .replace(":pid", pid)
            .filtered

        parsedPages =
          pagesMeta.map: p => 
              p.urls.original
            .zipWithIndex
            .map: (urlString, index) =>
              MangaPage(
                0,
                meta.id,
                urlString,
                index + 1,
                urlString.substring(urlString.lastIndexOf('/') + 1).filtered,
                MangaPage.State.Pending.code,
              )

        _ <- ZIO.when(parsedPages.length < 1)(
          ZIO.fail(ParsingError(s"No available attachments were found while parsing: '${pagesApiEndpoint}'"))
        )

        parsedMeta = 
          meta.copy(
            galleryUri = galleryUrl.encode,
            title = Some(title),
            totalPages = parsedPages.length,
            isParsed = true
          )
        
      yield (parsedMeta, parsedPages)

    end parseAndRetrievePages

    

  private def downloadSinglePage(title: String, page: MangaPage): RIO[zio.http.Client & Scope, Long] = 
    for
      pageUrl <-
        ZIO.fromEither:
            URL.decode(page.pageUri)
          .tapError: e =>
              ZIO.logError(s"${e.getMessage}. => abort.")

      _ <- ZIO.log(s"Downloading: '${pageUrl}'")

      client <- ZIO.service[Client]

      cfile = s"${cachePath}/${UUID.randomUUID().toString()}.tmp"

      length <-
        client.request:
          Request.get(pageUrl)
            .preconfigure
            .removeHeader("Accept")
            .addHeader("Accept", "image/avif,image/webp,image/png,image/svg+xml,image/*;q=0.8,*/*;q=0.5")
            .removeHeader("Cookie")
            .removeHeader("Referer")
            .addHeader("Referer", "https://www.pixiv.net/")
        .flatMap: resp =>
          resp.status.code match 
            case 403 =>
              ZIO.fail(BypassError(s"Anti-bots blocking presents when downloading: '${pageUrl}'"))
            case 404 =>
              ZIO.fail(ParsingError(s"404 not found presents when downloading: '${pageUrl}'"))
            case i if i > 500 =>
              ZIO.fail(ServerError(s"50x presents presents when downloading: '${pageUrl}'"))
            case _ =>
              resp.body.asStream.timeout(4 hours).run(ZSink.fromFileName(cfile))
        .onInterrupt: _ =>
          defaultRemoveFile(cfile).forkDaemon
        .tapError:
          case bp: (BypassError | ParsingError) =>
            ZIO.logError(s"${bp.getMessage}. => abort.")
          case t =>
            ZIO.logError(s"Exception is raised when downloading: '${pageUrl}': ${t.getMessage}")
        .defaultRetry
        .tapSomeError:
          case e if !e.isInstanceOf[BypassError | ParsingError] => 
            ZIO.logError(s"Maximum number of retries has been reached while downloading: '${pageUrl}'. => abort.")
              *> defaultRemoveFile(cfile).forkDaemon

      savedPath = s"${downPath}/${title}/${page.title}"

      _ <- ZIO.attemptBlockingIO(Files.createDirectories(Paths.get(s"${downPath}/${title}")))
        
      _ <- defaultMoveFile(cfile, savedPath)

      _ <- ZIO.log(s"Saved: '${pageUrl}' as '${savedPath}', downloaded size: ${String.format("%.2f", length.toFloat / 1024 / 1024)} MB")
      
    yield length

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
        ZIO.logError(s"PixivIllustHandler: ${l.message}. => abort.")
      case e =>
        taskHelper.failManga(meta.id, e.getMessage())
    .catchAll: _ =>
      idleIndicator.setIdle(true)
    .onError: cause =>
      ZIO.logError(s"PixivIllustHandler: ${cause.dieOption.map(_.getMessage()).getOrElse("???")}. => die.")
    
  end handle