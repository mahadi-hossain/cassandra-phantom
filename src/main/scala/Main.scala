import com.datastax.driver.core.{PlainTextAuthProvider, SocketOptions}
import com.outworkers.phantom.dsl._
import com.datastax.driver.core.PagingState

object Connector {

  val default: CassandraConnection = ContactPoint.local
    .withClusterBuilder(
      _.withSocketOptions(
        new SocketOptions()
          .setConnectTimeoutMillis(20000)
          .setReadTimeoutMillis(20000)
      )
  ).keySpace(
    KeySpace("store").ifNotExists().`with`(
      replication eqs SimpleStrategy.replication_factor(1)
    )
  )
}


case class SampleRow(
  id:Int,
  year: Int,
  geo_count: Int,
  ec_count: Int
)

abstract class Sample extends Table[Sample, SampleRow] {
  object year extends IntColumn with PartitionKey
  object id extends IntColumn with ClusteringOrder with Descending
  object geo_count extends IntColumn
  object ec_count extends IntColumn
}


class BasicDatabase(override val connector: CassandraConnection) extends Database[BasicDatabase](connector) {
  object sampleTable extends Sample with Connector
}

object db extends BasicDatabase(Connector.default)




import java.util.UUID
import scala.concurrent.Future

trait SelectExamples extends db.Connector {
  val year = 2019
  // This is a select * query, selecting the entire record
  
  def selectAll(): Future[ListResult[SampleRow]] = {
    db.sampleTable.select.where(_.year eqs year).paginateRecord(_.setPagingState(
      PagingState.fromString("00120010000600040000001bf07ffffff5f07ffffff58e223fef390347792ea005b35a1ff0730004")
    ).setFetchSize(10))
  }

  
}


object Main extends App{
  println("Connect me......")

  new SelectExamples(){}.selectAll.foreach{
    listResult =>
      println(listResult.state)
      println(listResult.records)
  }
}