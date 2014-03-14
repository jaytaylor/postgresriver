package com.github.jaytaylor.postgresriver.indexer

import java.sql.Timestamp
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.indices.IndexMissingException
import com.github.jaytaylor.postgresriver.model.{ElasticSearchIndexed, ElasticSearchIndexable}
import com.github.jaytaylor.postgresriver.service.ElasticSearchService
import com.threatstream.util.driver.MyPostgresDriver.simple._
import com.threatstream.util.DatabaseUtil._


/**
 * @author Jay Taylor [@jtaylor]
 * @date 2012-09-19
 */

/**
 * Generic indexer management facility.
 * @tparam T1
 * @tparam T2
 */
trait IndexerManager[T1 <: ElasticSearchIndexable, T2 <: Indexer[T1]] {

    protected def _instantiateIndexerInstance: T2

    protected def _companion: ElasticSearchIndexed[T1]

    protected def _getSearchService: ElasticSearchService[T1]

    protected def _instantiatePeriodicIndexerInstance: PeriodicCommitIndexer[T1]

    val createAliases: Boolean = false //Conf getBoolean "elasticsearch.indexer.createAliases"

    lazy val indexer = {
        val i = _instantiateIndexerInstance
        i.initService
        i
    }

    /**
     * Specialized indexer for Threads received via the API.
     */
    private lazy val _addIndexer = _instantiatePeriodicIndexerInstance

    def reindex {
        Logger.info(indexer.indexAlias + " indexer :: reindex starting..")
        val newIndexName = indexer.indexAlias + "_" + (DateTimeFormat forPattern "yyyyMMddHHmmss" print (new DateTime))
        Logger.info(indexer.indexAlias + " indexer :: New index name will be: " + newIndexName)
        val oldIndexName = indexer.indexName

        try {
            indexer.indexName = newIndexName
            val latestIndex = indexer.filterIndices(_.startsWith(indexer.indexAlias + "_")).sorted.headOption
            indexer.createIndex(_companion.ES_SCHEMA_MAPPING, _companion.ES_ANALYZER)
            val start = Some(new Timestamp(new DateTime().getMillis))
            var num = 0
            def addToIndexWithCounter(t: T1): Unit = {
                indexer addToIndex t
                num += 1
            }

            try {

                getDatabaseConnection withSession { implicit session: Session =>
                //val lastId: Long = _companion applyToAllRecords addToIndexWithCounter //indexer.addToIndex
                    indexer.commitBulk
                    Logger.info("Indexer finished, added " + num + " documents")
                    //SyncTracking.set(indexer.indexAlias, ts=start, lastId=lastId)
                }

            } catch {
                case e: Throwable =>
                    Logger.error(
                        "IndexerManager.reindex :: Caught exception " + e.getMessage +
                            " with connectionName="// + connectionName
                    )
                    e.printStackTrace()
            }

            if (createAliases) {
                // Link ES index alias.
                Logger.info("Indexer now creating alias..")
                try {
                    latestIndex match {
                        case Some(oldIndexName) =>
                            try {
                                indexer.swapAlias(oldIndexName, newIndexName)
                            } catch {
                                // If there was no index, attempt to create the alias. 1/3
                                case e: RemoteTransportException => indexer createAlias newIndexName
                            }

                        // If there was no index, attempt to create the alias. 2/3
                        case None => indexer createAlias newIndexName
                    }

                    // Simple ES test query.
                    _getSearchService.numRecords
                } catch {
                    // If there was no index, attempt to create the alias. 3/3
                    case e: IndexMissingException => indexer createAlias newIndexName
                }
            } else {
                Logger.warn("Indexer alias creation disabled by configuration")
            }

            indexer.commitBulk

        } catch {
            case e: Throwable =>
                Logger error e.getMessage
            //Logger error new RichException(e).getStackTraceAsString

            //case e: RichException => Logger error e.getStackTraceAsString

        } finally {
            // Switch the index name back no matter what.
            indexer.indexName = oldIndexName
        }

        //indexer.close
        Logger.info(indexer.indexAlias + " indexer :: reindex finished")
    }

    def refresh {
        Logger.info(indexer.indexAlias + " indexer :: refresh starting..")

        try {

            getDatabaseConnection withSession { implicit session: Session =>
                def cb(lastRefreshTs: Timestamp, currentLastId: Long): Long = {
                    //val newLastId = _companion.modifiedFromDatabase(lastRefreshTs, currentLastId)(indexer.addToIndex)
                    indexer.commitBulk
                    //newLastId
                    0L
                }

                //SyncTracking.refresh(indexer.indexAlias)(cb _)
            }

        } catch {
            case e: Throwable =>
                Logger.error(
                    "IndexerManager.refresh :: Caught exception " + e.getMessage +
                        " with connectionName=" //+ connectionName
                )
                e.printStackTrace()
        }

        Logger.info(indexer.indexAlias + " indexer :: refresh finished")
    }

    /**
     * Backfill a certain percentage of the T1 table.
     *
     * @param percent Percentage of total records to backfill, a double > 0.0 and <= 1.0.
     */
    def backfillPercentage(percent: Double): Unit =
        percent match {
            case p if p <= 0.0 || p > 1.0 =>
                throw new Exception(indexer.indexAlias + " indexer: The perecentage must be > 0.0 and <= 1.0")

            case _ =>
                try {

                    getDatabaseConnection withSession { implicit session: Session =>
                        val numToRetrieve = 1000 //(_companion.countAll * percent).toInt
                        Logger.info(indexer.indexAlias + " indexer :: Backfilling " + numToRetrieve + " records")
                        _companion.applyToLastNRecords(numToRetrieve)(indexer.addToIndex)
                    }

                } catch {
                    case e: Throwable =>
                        Logger.error(
                            "IndexerManager.refresh :: Caught exception " + e.getMessage +
                                " with connectionName=" //+ connectionName
                        )
                        e.printStackTrace()
                }
        }

    def add(o: T1) {
        Logger.info(indexer.indexAlias + " indexer :: Queueing object to be inserted into the index: " + o.toString)
        _addIndexer addToIndex o
    }

    def add(objects: Seq[T1]) {
        Logger.info(
            indexer.indexAlias + " indexer :: Queueing " + objects.length + " objects to be inserted into the index"
        )
        objects map { _addIndexer addToIndex _ }
    }
}
