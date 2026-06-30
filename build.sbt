ThisBuild / scalaVersion     := "3.3.8"
ThisBuild / version          := "0.0.1"
ThisBuild / organization     := "moe.karla"



//val zioVersion = "2.1.19"
//val quillVersion = "4.8.6"
//val zioHttpVersion = "3.3.3"
//val zioConfigVersion = "4.0.4"
//val zioLoggingVersion = "2.5.0"
val zioVersion = "2.1.26"
val quillVersion = "4.8.6"
//val zioHttpVersion = "3.7.4"
val zioHttpVersion = "3.8.1"
//val zioConfigVersion = "4.0.6"
val zioConfigVersion = "4.0.7"
val zioLoggingVersion = "2.5.3"


assembly / assemblyJarName := "hdjdown-play.jar"

//enablePlugins(JavaAppPackaging)

//enablePlugins(GraalVMNativeImagePlugin)


ThisBuild / assemblyMergeStrategy := {
  case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.contains("getquill")  => MergeStrategy.preferProject
  case PathList(ps @ _*) if ps.contains("jline")  => MergeStrategy.preferProject
  case PathList(ps @ _*) if ps.contains("scala") => MergeStrategy.preferProject
  case PathList(ps @ _*) if ps.contains("compiler.properties") => MergeStrategy.preferProject
  case PathList(ps @ _*) if ps.contains("OSGI-IN") => MergeStrategy.first
  case PathList(ps @ _*) if ps.last == "io.netty.versions.properties"  => MergeStrategy.first
  case PathList("META-INF", "versions", "9", "OSGI-INF", "MANIFEST.MF") => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}


lazy val root = (project in file("."))
  .settings(
    name := "hdjdown-play",
    scalaVersion := "3.3.8",
    scalacOptions ++= Seq("-language:postfixOps", "-deprecation"),
    javacOptions ++= Seq("-source", "21", "-target", "21"),
    //javaOptions ++= Seq("-Dlogback.configurationFile=bootstrap/logback.xml"),
    libraryDependencies ++= Seq(

      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,

      //"io.getquill" %% "quill-jdbc" % quillVersion,
      "io.getquill" %% "quill-jdbc-zio" % quillVersion,
      

      "dev.zio" %% "zio-http" % zioHttpVersion,

      "dev.zio" %% "zio-config" % zioConfigVersion,
      "dev.zio" %% "zio-config-typesafe" % zioConfigVersion,
      "dev.zio" %% "zio-config-magnolia" % zioConfigVersion,
      "dev.zio" %% "zio-config-refined"% zioConfigVersion,

      "dev.zio" %% "zio-logging" % zioLoggingVersion,
      //"dev.zio" %% "zio-logging-slf4j2" % zioLoggingVersion,

      //"dev.zio" %% "zio-json" % "0.7.39",
      "dev.zio" %% "zio-json" % "0.7.45",
      
      //"com.zaxxer" % "HikariCP" % "7.0.2",
      "org.vibur" % "vibur-dbcp" % "26.0",
      //"org.xerial" % "sqlite-jdbc" % "3.49.1.0",
      //"org.xerial" % "sqlite-jdbc" % "3.51.1.0",
      "org.xerial" % "sqlite-jdbc" % "3.53.2.0",
      //"com.h2database" % "h2" % "2.3.232",
      //"com.codenameone" % "sqlite-jdbc" % "7.0.178",
      //"com.codenameone" % "sqlite-jdbc" % "7.0.212",
      //"org.flywaydb" % "flyway-core" % "10.22.0",
      "org.flywaydb" % "flyway-core" % "11.13.3",
      //"org.apache.commons" % "commons-compress" % "1.27.1",
      "org.apache.commons" % "commons-compress" % "1.28.0",
      //"com.github.junrar" % "junrar" % "7.5.5",
      //"com.microsoft.playwright" % "playwright" % "1.51.0",
      //"org.slf4j" % "slf4j-simple" % "2.0.17",
      "org.slf4j" % "slf4j-simple" % "2.0.18",
      //"org.jsoup" % "jsoup" % "1.19.1",
      //"org.jsoup" % "jsoup" % "1.22.1",
      "org.jsoup" % "jsoup" % "1.22.2",
      //"org.reflections" % "reflections" % "0.10.2"
      "com.squareup.okhttp3" % "okhttp-jvm" % "5.4.0"
    ),
    assembly / mainClass := Some("moe.karla.NativeMain")
    //assembly / mainClass := Some("moe.karla.AppMain")
  )
