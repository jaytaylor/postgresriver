package com.github.jaytaylor.postgresriver.model

import org.elasticsearch.search.SearchHit
import spray.json._
import DefaultJsonProtocol._ // !!! IMPORTANT, else `convertTo` and `toJson` won't work correctly
import com.threatstream.util.RichSql
//import RichSql._
import org.elasticsearch.common.xcontent.XContentBuilder
import java.sql.Timestamp
import com.threatstream.util.driver.MyPostgresDriver.simple._


trait ElasticSearchIndexable {
    def id: Any
}

/**
 * Trait for models which will be routable within ElasticSearch.
 */
trait ElasticSearchRoutable extends ElasticSearchIndexable {
    def getRoute: String
}


/**
 * @author Jay Taylor [@jtaylor]
 * @date 2012-08-30
 */
trait ElasticSearchIndexed[T] {

    private val _containsNumericIdPattern = """^.*([^\\]"id" *:).*$""".r

    // For replacing string ids with numeric ones.
    private val _containsStringIdPattern = """^.*[^\\]("id" *: *"([1-9][0-9]*)").*$""".r

    private val _idIntegralPattern = """^([1-9][0-9]*)$""".r

    implicit val format: JsonFormat[T]

    /**
     * NB: If search hit result `id` field not present in json, then one will be inferred and inserted.
     */
    implicit def fromSearchHit(hit: SearchHit)(implicit manifest: Manifest[T]): T = {
        val source = hit.sourceAsString
        val cleanedSource: String = source match {

            case _containsStringIdPattern(chunk, id) => source.replaceFirst(chunk, """"id": """ + id.toLong)

            case _containsNumericIdPattern(_) => source

            case _ => source.replaceFirst("\\{", """{"id":""" + (
                hit.id.toString match {
                    case _idIntegralPattern(p) => hit.id.toLong + ","

                    case _ => hit.id.replaceAll("\"", "\\\"") + "\","
                }
            ))
        }

        cleanedSource.asJson.convertTo[T]
    }

    /**
     * Convert a db ResultSet into a T instance.
     */
    //implicit def fromResultSet(rs: RichResultSet): T

    /**
     * Convert a T instance to XContentBuilder for use with the ElasticSearch indexer.
     */
    implicit def toXContentBuilder(o: T): XContentBuilder

    /** Part of the ES Indexer interface */
    def ES_SCHEMA_MAPPING: String

    /**
     * Analyzer
     *
     * Part of the ES Indexer interface
     *
     * NB: Helpful gist- https://gist.github.com/1037563
     */
    val ES_ANALYZER: String = """{
        "analysis": {
        }
    }"""/*
            "analyzer": {
                "left": {
                    "type": "custom",
                    "tokenizer": "left_tokenizer",
                    "filter": [
                        "standard",
                        "lowercase",
                        "shingle_filter",
                        "stop"
                    ]
                }
            },
            "filter": {
                "shingle_filter": {
                    "type": "shingle",
                    "min_shingle_size": 2,
                    "max_shingle_size": 4,
                    "output_unigrams": true
                }
            },
            "tokenizer": {
                "left_tokenizer": {
                    "type": "edgeNGram",
                    "side": "front",
                    "min_gram": 1,
                    "max_gram": 20
                }
            }
        }
    }"""*/

    /** Part of the DB interface */
    def applyToAllRecords(fn: (T) => Unit)(implicit session: Session): Long

    /** Part of the DB interface */
    def modifiedFromDatabase(ts: Timestamp, lastId: Long)(fn: (T) => Unit)(implicit session: Session): Long

    /** Part of the DB interface */
    def applyToLastNRecords(limit: Int)(fn: (T) => Unit)(implicit session: Session): Long

    /** Part of the DB interface */
    def countAll(implicit session: Session): Long

    /** Part of the DB interface */
    def maxId(implicit session: Session): Long
}
