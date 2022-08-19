package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, Timers, actorRef2Scala}
import akka.event.Logging
import kvstore.Arbiter.*
import akka.pattern.{ask, pipe}

import scala.concurrent.duration.*
import akka.util.Timeout

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class ReplicationTimeout(opId: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers:
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.dispatcher

  val log = Logging(context.system, this)

//  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(withinTimeRange = 1.second) {
//    case _: Exception => SupervisorStrategy.Resume
//  }

  // current replica values
  var kv = Map.empty[String, String]

  // own persistence actor
  var persistence: ActorRef = context.actorOf(persistenceProps)
  // context.watch(persistence)

  def receive =
    case JoinedPrimary   =>
      context.become(leader)
    case JoinedSecondary =>
      context.become(replica)

  arbiter ! Join // Post join message to arbiter to receive replica role (primary or secondary)

  /*
  *
  * LEADER
  *
  * */

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // a map from update operation id to original sender, key, value, pending persistence and replication
  var updateOperationRequirements = Map.empty[Long, (ActorRef, String, Option[String], List[ActorRef], List[ActorRef])]

  val leader: Receive =
    case operation : Insert =>
      applyUpdateOperationAsLeader(sender(), operation)
    case operation: Remove =>
      applyUpdateOperationAsLeader(sender(), operation)
    case op: Get =>
      performGetOperation(sender(), op)
    case Replicas(replicas: Set[ActorRef]) =>
      val removedReplicas = secondaries.keySet -- replicas

      removedReplicas.foreach(deadReplica => {
        val replicator = secondaries(deadReplica)

        updateOperationRequirements
          .filter(_._2._5.contains(replicator))
          .foreach(op => {
            removeReplicationFromUpdateRequirements(op._1, replicator)
          })

        replicators -= replicator
        replicator ! PoisonPill
        secondaries -= deadReplica
      })

      replicas
        .filter(replica => replica != self)
        .filter(!secondaries.contains(_))
        .foreach(newReplica => {
          val replicator = context.actorOf(Replicator.props(newReplica))
          secondaries = secondaries + (newReplica -> replicator)
          replicators = replicators + replicator
          initialReplicatorSync(replicator)
        })
    case PersistTimeout(opId) =>
      updateOperationRequirements.get(opId) match {
        case Some(data) => persistence ! Persist(data._2, data._3, opId)
        case None => ()
      }
    case Persisted(key, opId) =>
      timers.cancel(persistenceTimeoutKey(opId))
      removePersistenceFromUpdateRequirements(opId)
    case Replicated(key, opId) =>
      removeReplicationFromUpdateRequirements(opId, sender())
    case ReplicationTimeout(opId) =>
      updateOperationRequirements.get(opId) match {
        case Some(data) => data._1 ! OperationFailed(opId)
        case None => ()
      }
    case _ =>

  private def applyUpdateOperationAsLeader(sender: ActorRef, operation: Operation): Unit = operation match
    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      setupReplicationAndPersistence(sender, id, key, Some(value))
    case Remove(key, id) =>
      kv = kv - key
      setupReplicationAndPersistence(sender, id, key, None)
    case _ => ()

  private def setupReplicationAndPersistence(sender: ActorRef, opId: Long, key: String, maybeValue: Option[String]): Unit = {
    persistence ! Persist(key, maybeValue, opId)
    timers.startTimerAtFixedRate(persistenceTimeoutKey(opId), PersistTimeout(opId), 100.millis)

    val replicationMessages = replicators.map(replicator => {
      replicator ! Replicate(key, maybeValue, opId)
      replicator
    })

    updateOperationRequirements = updateOperationRequirements + (opId -> (sender, key, maybeValue, List(persistence), replicationMessages.toList))

    timers.startSingleTimer(replicationTimeoutKey(opId), ReplicationTimeout(opId), 1.second)
  }

  private def onReplicationUpdate(opId: Long): Unit = {
    updateOperationRequirements.get(opId).foreach(data => {
      if (data._4.isEmpty && data._5.isEmpty) {
        timers.cancel(replicationTimeoutKey(opId))
        updateOperationRequirements = updateOperationRequirements - opId
        data._1 ! OperationAck(opId)
      }
    })
  }

  private def removePersistenceFromUpdateRequirements(opId: Long) = {
    updateOperationRequirements = updateOperationRequirements.map(entry => {
      if (entry._1 == opId) {
        (entry._1, (entry._2._1, entry._2._2, entry._2._3, List.empty[ActorRef], entry._2._5))
      } else {
        entry
      }
    })

    onReplicationUpdate(opId)
  }

  private def removeReplicationFromUpdateRequirements(opId: Long, replication: ActorRef) = {
    updateOperationRequirements = updateOperationRequirements.map(entry => {
      if (entry._1 == opId) {
        (entry._1, (entry._2._1, entry._2._2, entry._2._3, entry._2._4, entry._2._5.filter(ref => ref != replication)))
      } else {
        entry
      }
    })

    onReplicationUpdate(opId)
  }

  private def performGetOperation(sender: ActorRef, operation: Get): Unit = operation match {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }

  private def initialReplicatorSync(replicator: ActorRef) = {
    kv.foreach((key, value) => replicator ! Replicate(key, Some(value), 0))
  }

  private def replicationTimeoutKey(seqId: Long) = s"replicationTimeout$seqId"

  /*
  *
  * REPLICA
  *
  * */

  var nextExpectedSnapshot = 0L
  var pendingSnapshotAcks = Map.empty[Long, PersistRequest]

  val replica: Receive =
    case op: Get =>
      performGetOperation(sender(), op)
    case Snapshot(key, valueOption, seq) => seq match {
      case seq if seq == nextExpectedSnapshot =>
        applySnapshotAsSecondary(Snapshot(key, valueOption, seq))
        nextExpectedSnapshot = nextExpectedSnapshot + 1
      case seq if seq < nextExpectedSnapshot =>
        sender() ! SnapshotAck(key, seq)
      case _ =>
        ()
    }
    case Persisted(key, seq) =>
      timers.cancel(persistenceTimeoutKey(seq))
      pendingSnapshotAcks.get(seq) match {
        case Some(PersistRequest(sender, _)) =>
          pendingSnapshotAcks = pendingSnapshotAcks - seq
          sender ! SnapshotAck(key, seq)
        case None => ()
      }
    case PersistTimeout(seq) =>
      pendingSnapshotAcks.get(seq).foreach(request => persistence ! request.persist)
    case _ => ()

  private def applySnapshotAsSecondary(snapshot: Snapshot): Unit = snapshot match
    case Snapshot(key, valueOption, seq) =>
      valueOption match {
        case Some(value) =>
          kv = kv + (key -> value)
        case None =>
          kv = kv - key
      }
      val persistMessage = Persist(key, valueOption, seq)
      persistence ! persistMessage
      pendingSnapshotAcks = pendingSnapshotAcks + (seq -> PersistRequest(sender(), persistMessage))
      timers.startTimerAtFixedRate(persistenceTimeoutKey(seq), PersistTimeout(seq), 100.millis)

  private def persistenceTimeoutKey(seqId: Long) = s"persistenceTimeout$seqId"