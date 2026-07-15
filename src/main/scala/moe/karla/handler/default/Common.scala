package moe.karla.handler.default


import zio.*

import zio.config.*
import zio.config.typesafe.*
import zio.config.magnolia.*
import zio.config.refined.*

import org.jsoup.nodes.Element

import java.nio.file.*



final case class CommonConfig(
  
  defaultUserAgent: String,

  titleTruncation: Boolean,
)


object CommonConfig:

  def load = 
    ConfigProvider.fromHoconFile(new java.io.File("config/common.conf"))
      .load(deriveConfig[CommonConfig].mapKey(toKebabCase))  
      .tapError: e =>
        ZIO.logError(e.getMessage())
      .orDie

  val layer = ZLayer.fromZIO(load)


import okhttp3.OkHttpClient
import okhttp3.ConnectionSpec
import okhttp3.TlsVersion
import okhttp3.CipherSuite


object OkHttpClient:

  private val spec = ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
    .tlsVersions(TlsVersion.TLS_1_2, TlsVersion.TLS_1_3)
    .cipherSuites(
      CipherSuite.TLS_AES_128_GCM_SHA256,
      CipherSuite.TLS_AES_256_GCM_SHA384,
      CipherSuite.TLS_CHACHA20_POLY1305_SHA256,
    )
    .build()

  val live = ZLayer {
    ZIO.succeed:
      okhttp3.OkHttpClient.Builder()
        .connectionSpecs(java.util.Collections.singletonList(spec))
        .cookieJar(new okhttp3.JavaNetCookieJar(new java.net.CookieManager()))
        .build()
  }


extension [A <: String] (str: A)
  private[default] def filtered = 
    str.strip
      .replace('?', '？')
      .replace('（', '(')
      .replace('）', ')')
      .replace(":", "：")
      .replace("/", "／")
      .replace("\\", "＼")
      .replace("*", "＊")
      .replaceAll("[\\\\/:*?\"<>|\t]", " ")


extension [A <: Element] (element: A)
  private[default] def map[B](f: A => B): B = f(element)


private[default] def defaultRemoveFile(path: String) =
  ZIO.attemptBlockingIO(
    Files.deleteIfExists(Paths.get(path))
  )
  .retry(Schedule.recurs(3) && Schedule.spaced(2 seconds))


private[default] def defaultMoveFile(src: String, to: String) =
  ZIO.attemptBlockingIO(
    Files.move(
      Paths.get(src), 
      Paths.get(to), 
      StandardCopyOption.REPLACE_EXISTING
    )
  )
  .retry(Schedule.recurs(3) && Schedule.spaced(2 seconds))


private[default] val defaultRetryFrequency = 
  Schedule.recurs(4) && 
  Schedule.spaced(7 seconds) && 
  Schedule.fibonacci(800 millis)


import zio.schema.Schema
import zio.schema.codec.JsonCodec.*

extension (resp: okhttp3.Response)
  private[default] def to[T](using entitySchema: Schema[T]) =
    zio.schema.codec.JsonCodec.jsonDecoder(entitySchema).decodeJson(resp.body.string())