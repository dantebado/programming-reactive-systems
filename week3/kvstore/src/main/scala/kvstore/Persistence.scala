package kvstore

import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}

import scala.util.Random

object Persistence:
  case class Persist(key: String, valueOption: Option[String], id: Long)
  case class Persisted(key: String, id: Long)

  class PersistenceException extends Exception("Persistence failure")

  case class PersistRequest(sender: ActorRef, persist: Persist)
  case class PersistTimeout(id: Long)

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)

class Persistence(flaky: Boolean) extends Actor:
  import Persistence.*

  def receive =
    case Persist(key, _, id) =>
      if !flaky || Random.nextBoolean() then sender() ! Persisted(key, id)
      else throw PersistenceException()
