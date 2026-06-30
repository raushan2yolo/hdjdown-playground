package moe.karla.miscellaneous


import zio.* 

import org.flywaydb.core.Flyway
import org.flywaydb.core.api.FlywayException
import org.flywaydb.core.api.output.MigrateResult

//import com.zaxxer.hikari.HikariDataSource
import org.vibur.dbcp.ViburDBCPDataSource

import javax.sql.DataSource
import java.io.IOException



object FlywayMigration:

  /*
  def runMigrate: ZIO[DataSource, IOException, MigrateResult] =
    ZIO.serviceWithZIO(ds => 
      ZIO.attemptBlockingIO(
        Flyway.configure().dataSource(ds).baselineOnMigrate(true).cleanDisabled(true).outOfOrder(false).load().migrate()
      )
      .tapError(e => ZIO.logError(e.getMessage()))
    )
  */
  
  def runMigrate(locations: String*): IO[IOException, MigrateResult] = {

    def acquire = ZIO.attemptBlockingIO(Storage.dataSource(poolSize = 3))
    
    def release(ds: ViburDBCPDataSource) = ZIO.attemptBlockingIO(ds.close()).orDie

    ZIO.acquireReleaseWith(acquire)(release)(ds => ZIO.attemptBlockingIO(
        Flyway.configure()
          .locations(locations*)
          .dataSource(ds)
          .baselineOnMigrate(true)
          .cleanDisabled(true)
          .outOfOrder(false)
          .load()
          .migrate()
      )
    )
    .tapError: e => 
      ZIO.logError(e.getMessage)
  }
  
    

