/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation =>
      root ! op
    case GC =>
      val newTreeRoot = createRoot
      root ! CopyTo(newTreeRoot)
      context.become(garbageCollecting(newTreeRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot
      context.become(normal)
      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty
    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)
  }


object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def positionFor(elem: Int): Position = if (elem < this.elem) Left else Right

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Contains(requester, id, elem) =>
      if (elem == this.elem)
        requester ! ContainsResult(id, !removed)
      else
        subtrees.get(positionFor(elem)) match {
          case Some(subtree) => subtree ! Contains(requester, id, elem)
          case None => requester ! ContainsResult(id, false)
        }
    case Insert(requester, id, elem) =>
      if (elem == this.elem) {
        removed = false
        requester ! OperationFinished(id)
      } else {
        val position = positionFor(elem)
        subtrees.get(position) match {
          case Some(subtree) => subtree ! Insert(requester, id, elem)
          case None =>
            subtrees += position -> context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
            requester ! OperationFinished(id)
        }
      }
    case Remove(requester, id, elem) =>
      if (elem == this.elem) {
        removed = true
        requester ! OperationFinished(id)
      } else {
        val position = positionFor(elem)
        subtrees.get(position) match {
          case Some(subtree) => subtree ! Remove(requester, id, elem)
          case None => requester ! OperationFinished(id)
        }
      }
    case CopyTo(newTreeRoot) =>
      if (!removed) {
        newTreeRoot ! Insert(self, copyOperationId, elem)
      }

      val childrenActors = subtrees.values.map(leaf => {
                              leaf ! CopyTo(newTreeRoot)
                              leaf
                            }).toSet

      val expecting = if (removed) childrenActors else childrenActors + self

      if (expecting.isEmpty) {
        context.parent ! CopyFinished
      } else {
        context.become(copying(expecting, false))
      }
  }

  private def copyOperationId = elem + 1000000

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) =>
      val newExpected = expected - self
      if (newExpected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
      } else {
        context.become(copying(newExpected, true))
      }
    case CopyFinished =>
      val newExpected = expected - sender()
      if (newExpected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
      } else {
        context.become(copying(newExpected, insertConfirmed))
      }
  }