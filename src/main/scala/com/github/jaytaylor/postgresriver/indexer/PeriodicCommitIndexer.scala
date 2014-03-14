package com.github.jaytaylor.postgresriver.indexer

import java.util.{ArrayList => JArrayList, List => JList}
import org.elasticsearch.action.ListenableActionFuture
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequestBuilder

import akka.actor.{Actor, ActorSystem, Props}
import scala.concurrent.duration._
import com.github.jaytaylor.postgresriver.model.ElasticSearchIndexable

/**
 * @author Jay Taylor [@jtaylor]
 * @date 2012-08-31
 */

trait PeriodicCommitIndexer[T <: ElasticSearchIndexable] extends Indexer[T] {

    override def commitBulk {} // Disabled.

    def periodicCommit {
        pendingRequests.size match {
            case 0 => //Logger.info("[PERIODIC-commitBulk] No pending items right now")

            case _ =>
                Logger.info("[PERIODIC-commitBulk] Committing a bulk operation")
                var numActions: Int = bulkRequest.numberOfActions
                var resp: ListenableActionFuture[BulkResponse] = bulkRequest.execute
                var nextBatch: JList[IndexRequestBuilder] = new JArrayList[IndexRequestBuilder](numActions)

                pendingRequests.drainTo(nextBatch, numActions)
                service.submit(new RetrySendCallable(resp, nextBatch, RESPONSE_TIMEOUT_MILLIS))
                _bulkRequest = Some(client.prepareBulk)
        }
    }

    override def close {
        periodicCommit
        super.close
    }

    def start: PeriodicCommitIndexer[T] = {
        val system = ActorSystem()

        val Flush = "flush"

        val flushActor = system.actorOf(Props(new Actor {
            def receive = {
                case Flush ⇒ periodicCommit
            }
        }))

        // Use system's dispatcher as ExecutionContext
        import system.dispatcher

        //This will schedule to send the Flush-message to the flushActor.
        val cancellable = system.scheduler.schedule(5000 milliseconds, 2500 milliseconds, flushActor, Flush)

        //Global registerCancellable cancellable

        this
    }
}
