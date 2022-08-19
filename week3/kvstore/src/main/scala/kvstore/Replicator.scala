package kvstore

import akka.actor.{Actor, ActorRef, Props, Timers, actorRef2Scala}
import akka.event.Logging

import scala.concurrent.duration.*

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class SnapshotTimeout(seq: Long, forRetry: Snapshot)

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor with Timers:
  import Replicator.*
  import context.dispatcher

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq(): Long =
    val ret = _seqCounter
    _seqCounter += 1
    ret

  def receive: Receive =
    case Replicate(key, valueOption, id) =>
      val seqNumber = nextSeq()
      val snapshotMessage = Snapshot(key, valueOption, seqNumber)

      acks += (seqNumber -> (sender(), Replicate(key, valueOption, id)))
      pending = pending :+ snapshotMessage

      replica ! snapshotMessage
      timers.startTimerAtFixedRate(
        replicateTimeoutKey(seqNumber),
        SnapshotTimeout(seqNumber, snapshotMessage),
        200.millis // 200 millis for batch
      )
    case SnapshotAck(key, seq) =>
      timers.cancel(replicateTimeoutKey(seq))

      acks.get(seq) match {
        case Some((originalReplica, Replicate(key, _, id))) =>
          originalReplica ! Replicated(key, id)
          pending = pending.filterNot(_.seq == seq)
          acks -= seq
        case None => ()
      }
    case SnapshotTimeout(seqNumber, forRetry) =>
      replica ! forRetry
    case _ =>

  private def replicateTimeoutKey(seqNumber: Long) = s"timeoutReplicateSeq$seqNumber"