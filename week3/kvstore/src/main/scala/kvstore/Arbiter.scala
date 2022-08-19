package kvstore

import akka.actor.{ActorRef, Actor, actorRef2Scala}

object Arbiter:
  case object Join

  case object JoinedPrimary
  case object JoinedSecondary

  case class Replicas(replicas: Set[ActorRef])

class Arbiter extends Actor:
  import Arbiter.*
  var leader: Option[ActorRef] = None
  var replicas = Set.empty[ActorRef]

  def receive =
    case Join =>
      if leader.isEmpty then
        leader = Some(sender())
        replicas += sender()
        sender() ! JoinedPrimary
      else
        replicas += sender()
        sender() ! JoinedSecondary
      leader foreach (_ ! Replicas(replicas))

