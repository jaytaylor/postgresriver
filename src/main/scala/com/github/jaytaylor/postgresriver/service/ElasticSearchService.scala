package com.github.jaytaylor.postgresriver.service


import java.util.ArrayList
import java.util.List
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import org.elasticsearch.{ElasticsearchException, ElasticsearchTimeoutException}
import org.elasticsearch.action.ActionFuture
import org.elasticsearch.action.ListenableActionFuture
import org.elasticsearch.action.WriteConsistencyLevel
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequestBuilder
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue

import org.elasticsearch.index.query._

import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.index.query.QueryBuilders._
import collection.JavaConversions._
import org.elasticsearch.common.network.NetworkService

import com.github.jaytaylor.postgresriver.indexer.Logger

import org.elasticsearch.client.Requests
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.action.search.SearchRequestBuilder

import org.elasticsearch.search.SearchHit
import com.github.jaytaylor.postgresriver.model.ElasticSearchIndexable

/**
 * @author Jay Taylor [@jtaylor]
 * @date 2012-07-27
 */

object ElasticSearchService {
  val clusterName: String = "elasticsearch" // Conf getString "elasticsearch.client.cluster"
  val port: Int = 9300 //Conf getInt "elasticsearch.client.port"
  val host: String = "localhost" // Conf getString "elasticsearch.client.host"
  val timeout: TimeValue = TimeValue.timeValueMillis(10000) //Conf getInt "elasticsearch.client.timeout")

  def getClient: TransportClient = {
    Logger.info("Setting up connection to cluster: " + clusterName)
    val settings: Settings = ImmutableSettings.settingsBuilder
      .put("cluster.name", clusterName)
      .put(NetworkService.TcpSettings.TCP_CONNECT_TIMEOUT, timeout.getMillis)
      .put(NetworkService.TcpSettings.TCP_KEEP_ALIVE, "true")
      .build

    val client = new TransportClient(settings)

    // Add host.
    client.addTransportAddress(new InetSocketTransportAddress(host, port))
    client
  }

  val containsDigitRe = """^.*\d.*$""" r
  val nonAlphaNumericRe = """[^a-zA-Z0-9]+""" r
}

trait ElasticSearchService[T <: ElasticSearchIndexable] {
  import ElasticSearchService._

  def indexName: String
  def subjectType: String

  def client: TransportClient

  implicit def fromSearchHit(hit: SearchHit): T

  val defaultLimit: Int = 25 //Conf getInt "paging.default.limit"

  implicit def seqLongToSeqJavaLong(seq: Seq[Long]): Seq[java.lang.Long] = seq map { x => x: java.lang.Long }

  protected def _addTerm(baseQuery: BoolQueryBuilder, fieldName: String, term: String): QueryBuilder = {
    (term contains " ") || (term contains "+") match {
      case true =>
        val innerQuery = QueryBuilders.boolQuery
        term split " |\\+" foreach { t => innerQuery.must(QueryBuilders.prefixQuery(fieldName, t)) }
        baseQuery should innerQuery

      case false => baseQuery should QueryBuilders.prefixQuery(fieldName, term)
    }

    baseQuery minimumNumberShouldMatch 1
  }

  protected def _cleanQuery(q: Option[String]): Option[String] = q match {
    // Allow word characters, whitepace, digits, and unicode characters.
    case Some(q) => Some(("""[^\w&&\d&&\s&&\p{L}]+""".r replaceAllIn (q, " ")).replaceAll("""\s+""", " ").trim())
    case None => None
  }

  /* @TODO OPTIMIZE WITH FILTERS
  protected def _addTerm(baseFilter: BoolFilterBuilder, fieldName: String, term: String): QueryBuilder = {
      term contains " " match {
          case true =>
              val innerFilter = FilterBuilders.boolFilter
              term split " " foreach { t => innerFilter.must(FilterBuilders.prefixFilter(fieldName, t)) }
              baseFilter should innerFilter
              baseFilter.

          case false =>
              baseFilter should QueryBuilders.prefixQuery(fieldName, term)
      }

      baseFilter minimumNumberShouldMatch 1
  }

  protected def _addTerm[T <: AnyRef](baseQuery: BoolQueryBuilder, fieldName: String, values: Seq[T]): QueryBuilder = {
      FilterBuilders.inFilter(fieldName, values: _*)
      val innerQuery = QueryBuilders.inQuery(fieldName, values: _*)
      baseQuery should innerQuery
      baseQuery minimumNumberShouldMatch 1
  }*/

  protected def _addTerm[U <: AnyRef](baseQuery: BoolQueryBuilder, fieldName: String, values: Seq[U]): QueryBuilder = {
    val innerQuery = QueryBuilders.inQuery(fieldName, values: _*)
    baseQuery should innerQuery
    baseQuery minimumNumberShouldMatch 1
  }

  /**
   * @return The number of records contained in the current index.
   */
  def numRecords: Long = { 0L
    /*val request = Requests.countRequest(indexName)
    request.query(QueryBuilders.matchAllQuery)
    val future = client count request
    val result = future.actionGet.count
    result*/
  }

  def getId(userId: Long, id: String): Option[T] = getIds(userId, Seq(id)).headOption

  /**
   * Attempt to retrieve a specific id of type T from ElasticSearch or the database.
   */
  def getIds(userId: Long, ids: Seq[String]): Seq[T] =
    /*Conf getBoolean "elasticsearch.enabled"*/ true match {
      case true => _getIdsFromEs(userId, ids)
      case false => _getIdsFromDb(userId, ids)
    }

  /**
   * Attempt to retrieve a specific id of type T from the database.
   */
  protected def _getIdsFromDb(userId: Long, ids: Seq[String]): Seq[T]

  /**
   * Attempt to retrieve a specific id of type T from ElasticSearch.
   */
  private def _getIdsFromEs(userId: Long, ids: Seq[String]): Seq[T] =
    ids.length match {
      case 0 => Nil
      case _ =>
        val filter = QueryBuilders.filteredQuery(
          QueryBuilders.matchAllQuery,
          FilterBuilders.idsFilter().addIds(ids: _*)
        )

        val query = client.prepareSearch(indexName)
          .setSize(ids.length)
          .setTypes(subjectType)
          .setQuery(filter)
          .setTimeout(timeout)

        //val response = timeIt("elasticsearch group by id query") {
          val response = query.execute.actionGet
        //}
        val hits = response.getHits

        Logger.info(
          "[%s milliseconds] Found %d total hits for query '%s'".format(
            response.getTookInMillis,
            hits.getTotalHits,
            indexName
          )
        )

        hits.getHits map { hit => hit: T }
    }
}
