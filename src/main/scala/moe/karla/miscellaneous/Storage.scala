package moe.karla.miscellaneous



import io.getquill.*
import io.getquill.jdbczio.Quill

import org.vibur.dbcp.ViburDBCPDataSource



object Storage:
  
  
  def dataSource(poolSize: Int) = {
    val ds = ViburDBCPDataSource()
    //config.setJdbcUrl("jdbc:h2:./data")
    //config.setDriverClassName("org.sqlite.JDBC")
    //config.setDataSourceClassName("org.sqlite.SQLiteDataSource")
    ds.setJdbcUrl("jdbc:sqlite:./data.db")
    ds.setDriverClassName("org.sqlite.JDBC")
    ds.setUsername("")
    ds.setPassword("")
    //config.setUsername("sa")
    //ds.setDefaultTransactionIsolation("TRANSACTION_SERIALIZABLE")
    ds.setDefaultTransactionIsolation("SERIALIZABLE")
    ds.setPoolInitialSize(poolSize)
    ds.setPoolMaxSize(poolSize)
    ds.setCollectQueryStatistics(-1)
    ds.start()
    ds
  }
  

  val dataSourceLayer = Quill.DataSource.fromDataSource(dataSource(poolSize = 1))