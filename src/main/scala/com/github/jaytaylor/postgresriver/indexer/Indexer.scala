package com.github.jaytaylor.postgresriver.indexer

import org.elasticsearch.common.logging.Loggers

import java.util.{ArrayList => JArrayList, List => JList}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import org.elasticsearch.{ElasticsearchException, ElasticsearchTimeoutException}
import org.elasticsearch.action.{ActionFuture, ListenableActionFuture, WriteConsistencyLevel}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder
import org.elasticsearch.action.admin.indices.status.{IndicesStatusRequestBuilder, IndicesStatusResponse}
import org.elasticsearch.action.bulk.{BulkRequestBuilder, BulkResponse}
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.client.Requests
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.{ImmutableSettings, Settings}
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.unit.TimeValue.timeValueSeconds
import org.elasticsearch.common.xcontent.XContentBuilder
import com.github.jaytaylor.postgresriver.model.{ElasticSearchIndexable, ElasticSearchIndexed}
import com.github.jaytaylor.postgresriver.service.ElasticSearchService


object Logger {
    def info(s: Any) {
        println("INFO: " + s)
    }
    def error(s: Any) {
        println("ERROR: " + s)
    }
    def warn(s: Any) {
        println("WARN: " + s)
    }
}

trait Indexer[T1 <: ElasticSearchIndexable] {

    def clusterName: String
    var indexName: String
    def indexAlias: String
    def subjectType: String

    //implicit def toXContentBuilder[T3: Manifest[T1]](t: T1): XContentBuilder = companionOf[T3].collect { case t2: T2.type => t2 }

    protected def _companion: ElasticSearchIndexed[T1]

    implicit def toXContentBuilder(t: T1): XContentBuilder = _companion toXContentBuilder t

    def addToIndex(t: T1): Unit = {
        val builder: IndexRequestBuilder = client
            .prepareIndex(indexName, subjectType, t.id.toString)
            .setSource(t: XContentBuilder)

        bulkIndexRequest(builder)
    }

    val CORE_POOL_SIZE: Int = 1 //Conf getInt "elasticsearch.indexer.corePoolSize"
    val MAX_POOL_SIZE: Int = 5 //Conf getInt "elasticsearch.indexer.maxPoolSize"

    val BATCH_SIZE: Int = 2500 //Conf getInt "elasticsearch.indexer.batchSize"

    val KEEP_ALIVE_TIME: Long = 10000 //Conf getLong "elasticsearch.indexer.keepAliveMillis"

    val MERGE_FACTOR: Int = 6 //Conf getInt "elasticsearch.indexer.mergeFactor"
    val NUMBER_OF_REPLICAS: Int = 1 //Conf getInt "elasticsearch.indexer.numberOfReplicas"

    val RESPONSE_TIMEOUT_MILLIS: Long = 10000 // Conf getLong "elasticsearch.indexer.responseTimeoutMillis"
    val SHUTDOWN_TIME_SECONDS: Long = 90 //Conf getLong "elasticsearch.indexer.shutdownTimeSeconds"

    val STATUS_DELAY: TimeValue = timeValueSeconds(30)
    val CACHE_FILTER_MAX_SIZE: Int = 500
    val CACHE_FIELD_EXPIRE: String = "30m"
    val MAYBE_INDEX_REFRESH_INTERVAL: String = "-1"
    val BULK_LOADING_INDEX_REFRESH_INTERVAL: String = "3600s"

    /**
     * Port number to connect to.
     */
    val CLUSTER_PORT: Int = 9300 //Conf getInt "elasticsearch.indexer.port"

    val timeout: TimeValue = TimeValue.timeValueMillis(10000) //Conf getInt "elasticsearch.client.timeout")

    val client: TransportClient = getClient

    // Indexer resources:
    // 2x the queue size to be safe and avoid "queue is full" types of exceptions.
    var pendingRequests: BlockingQueue[IndexRequestBuilder] =
        new ArrayBlockingQueue[IndexRequestBuilder](BATCH_SIZE * 2)

    /**
     * This is kind of funky but necessary b/c the client might not yet be ready.
     */
    def bulkRequest: BulkRequestBuilder = {
        _bulkRequest match {
            case Some(br) => br
            case None =>
                _bulkRequest = Some(client.prepareBulk)
                _bulkRequest.head
        }
    }

    protected var _bulkRequest: Option[BulkRequestBuilder] = None

    var responseQueue: BlockingQueue[Runnable] = new ArrayBlockingQueue[Runnable](BATCH_SIZE)
    var service: ExecutorService = null

    /**
     * Initialize ElasticSearch client transport
     */
    def getClient: TransportClient = {
        Logger.info("Setting up connection to cluster: " + clusterName)
        /*val settings: Settings = ImmutableSettings.settingsBuilder
            .put("cluster.name", clusterName)
            .put(NetworkService.TcpSettings.TCP_CONNECT_TIMEOUT, timeout.getMillis)
            .put(NetworkService.TcpSettings.TCP_KEEP_ALIVE, "true")
            .build*/
        //var settings: Settings = ImmutableSettings.settingsBuilder.put("cluster.name", clusterName).build//put("ping_timeout", 10).build
        val c = new TransportClient()//settings)

        Logger.info("ES INDEXER HOSTS: ...")// + (Conf getString "elasticsearch.indexer.hosts"))

        val hosts = "localhost" // Conf getString "elasticsearch.indexer.hosts"
        hosts split "," map { hostName =>
            c.addTransportAddress(new InetSocketTransportAddress(hostName, CLUSTER_PORT))
        }

        c
    }

    /**
     * Initialize ElasticSearch client transport and service.
     */
    def initService = {
        if (service != null && service.isShutdown) {
            Logger.info("Had to recreate an executor after shutdown")
        }

        if (service == null || service.isShutdown) {
            service = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                KEEP_ALIVE_TIME,
                TimeUnit.MILLISECONDS,
                responseQueue,
                new ThreadFactory {
                    var threadNumber: AtomicInteger = new AtomicInteger(0)
                    def newThread(runnable: Runnable): Thread =
                        new Thread(runnable, "Bulk Response Handler: " + threadNumber.incrementAndGet)
                },
                new ThreadPoolExecutor.CallerRunsPolicy
            )
        }

        this
    }

    def createIndex(mappingSchema: String, analyzerSchema: String) {
        val status: IndicesStatusRequestBuilder = client.admin.indices prepareStatus indexName

        val responseActionFuture: ActionFuture[IndicesStatusResponse] = status.execute

        var createIndex: Boolean = try {
            var response: IndicesStatusResponse = responseActionFuture.actionGet(STATUS_DELAY)
            !response.getIndices.containsKey(indexName)
        } catch {
            case e: ElasticsearchTimeoutException =>
                throw new RuntimeException(String.format("Unable to contact ElasticSearch cluster %s", clusterName))

            case e: ElasticsearchException => true
        }

        if (createIndex) {
            val settings = ImmutableSettings
                .settingsBuilder
                .loadFromSource(analyzerSchema)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, NUMBER_OF_REPLICAS)
                //.put("index.refresh_interval", MAYBE_INDEX_REFRESH_INTERVAL)
                .put("index.merge.policy.merge_factor", MERGE_FACTOR)
            //.put("index.cache.filter.max_size", CACHE_FILTER_MAX_SIZE)
            //.put("index.cache.field.expire", CACHE_FIELD_EXPIRE)
            //.put("analysis", analyzerSchema)

            //settings.

            val builder: CreateIndexRequestBuilder = client.admin.indices prepareCreate indexName

            builder.addMapping(subjectType, mappingSchema)

            builder setSettings settings.build

            //            builder.
            //
            ////            builder.addMapping("index", """{
            ////               "index": {
            ////                   "analysis":
            ////                    """ + analyzerSchema + """
            ////               }
            ////            }""")
            //
            Logger info settings.internalMap.toString

            builder.execute actionGet STATUS_DELAY

        } else {
            val builder: UpdateSettingsRequestBuilder = client.admin.indices prepareUpdateSettings indexName
            val settings = ImmutableSettings
                .settingsBuilder
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, NUMBER_OF_REPLICAS)
                //.put("index.refresh_interval", MAYBE_INDEX_REFRESH_INTERVAL)
                .put("index.merge.policy.merge_factor", MERGE_FACTOR)
                //.put("index.cache.filter.max_size", CACHE_FILTER_MAX_SIZE)
                //.put("index.cache.field.expire", CACHE_FIELD_EXPIRE)
                .put("analysis", analyzerSchema)
            //.build
            builder setSettings settings.build

            Logger info settings.internalMap.toString
            builder.execute actionGet STATUS_DELAY

            //builder.execute.actionGet(STATUS_DELAY)
            val mappingRequestBuilder: PutMappingRequestBuilder = client.admin.indices preparePutMapping indexName
            mappingRequestBuilder setIndices indexName
            mappingRequestBuilder setSource mappingSchema
            mappingRequestBuilder setType subjectType
            mappingRequestBuilder.execute actionGet STATUS_DELAY
            //mappingRequestBuilder.setMasterNodeTimeout(STATUS_DELAY)
            //mappingRequestBuilder.execute.actionGet(STATUS_DELAY)
        }
    }

    def bulkIndexRequest(builder: IndexRequestBuilder) {
        builder setConsistencyLevel WriteConsistencyLevel.QUORUM

        pendingRequests add builder
        bulkRequest add builder

        if (pendingRequests.size >= BATCH_SIZE) {
            commitBulk
        }
    }

    def commitBulk {
        pendingRequests.size match {
            case 0 => //Logger.info("[commitBulk] No pending items right now")

            case _ =>
                var numActions: Int = bulkRequest.numberOfActions
                Logger.info("[commitBulk] Committing a bulk operation (numActions=" + numActions + ")")
                var resp: ListenableActionFuture[BulkResponse] = bulkRequest.execute
                var nextBatch: JList[IndexRequestBuilder] = new JArrayList[IndexRequestBuilder](numActions)

                pendingRequests.drainTo(nextBatch, numActions)
                service.submit(new RetrySendCallable(resp, nextBatch, RESPONSE_TIMEOUT_MILLIS))
                _bulkRequest = Some(client.prepareBulk)
        }
    }

    /**
     * Close operation.
     */
    def close: Unit = {
        commitBulk

        Logger.info("Finished indexing")

        try {
            service.shutdown
            service.awaitTermination(SHUTDOWN_TIME_SECONDS, TimeUnit.SECONDS)

        } catch {
            case e: InterruptedException =>
                Logger.info("Error, timed out waiting for indexing to complete\nForcing execution to shutdown")
                throw e
        } finally if (!service.isShutdown) {
            service.shutdownNow
        }

        client.close
    }

    /**
     * Callable which allows us to automatically re-try a particular indexing on executor pool.
     */
    protected class RetrySendCallable(
        val responseActionFuture: ActionFuture[BulkResponse],
        val pendingRequests: JList[IndexRequestBuilder],
        val timeout: Long = 0L
        ) extends Callable[BulkResponse] {
        /**
         * Bulk Response
         *
         * @see java.util.concurrent.Callable#call()
         */
        def call: BulkResponse = {
            import collection.JavaConversions._
            try {
                val response: BulkResponse = responseActionFuture.actionGet(this.timeout, TimeUnit.MILLISECONDS)
                if (response.hasFailures) {
                    Logger.info(response.buildFailureMessage)
                    for (builder <- pendingRequests) {
                        Indexer.this bulkIndexRequest builder
                    }
                } else {
                    Logger.info("Batch took millis: " + response.getTookInMillis)
                }
                response

            } catch {
                case e: ElasticsearchTimeoutException =>
                    Logger.info("Retrying request after timeout!")
                    for (builder <- pendingRequests) {
                        Indexer.this bulkIndexRequest builder
                    }
                    null
            }
        }
    }

    /**
     * Create an alias for an index.
     */
    def createAlias(indexName: String) {
        Logger.info("Creating alias " + indexAlias + " for index " + indexName)
        client.admin.indices.prepareAliases.addAlias(indexName, indexAlias).execute.get
    }

    /**
     * Remove an alias for an index.
     */
    def removeAlias(indexName: String) {
        Logger.info("Deleting alias " + indexAlias + " for index " + indexName)
        client.admin.indices.prepareAliases.removeAlias(indexName, indexAlias).execute.get
    }

    /**
     * Swap old index alias with new one.
     */
    def swapAlias(oldIndexName: String, newIndexName: String) {
        removeAlias(oldIndexName)
        createAlias(newIndexName)
    }

    /**
     * Retrieve the list of indices from the cluster.
     */
    def listIndices: Array[String] =
        Option(getClusterState.getMetaData.concreteAllIndices) match {
            case Some(indices) => indices
            case None => Array[String]()
        }

    /**
     * Filter indices.
     */
    def filterIndices(filter: (String => Boolean)): Array[String] = listIndices filter filter

    /**
     * Used for cluster operations.
     */
    def getClusterState: ClusterState = client.admin.cluster.state(Requests.clusterStateRequest).actionGet.getState
}
