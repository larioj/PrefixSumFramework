import scala.collection.mutable
import scala.util.Random

/**
  * Created by mesosphere on 6/27/16.
  */
class PrefixSumState(numbers: Array[Int]) {
  type Id = Int

  private case class Node(index: Int, var state: State.Value, var sum: Int, var fromLeft: Int)

  case class WorkItem(id: Id, x: Int, y: Int)

  private object State extends Enumeration {
    val Sum, FromLeft, Prefix, Final = Value
  }

  private val _identity = 0

  private val _originalLength = numbers.length
  require(_originalLength > 1 && _originalLength <= (1 << 28))

  private val _length = ceilPower2(_originalLength)
  private val _numbers = numbers ++ new Array[Int](_length - _originalLength)
  private val _storage = new Array[Node](2 * _length - 1)
  private val _work = mutable.Queue[Node]()
  private val _inProgress = mutable.Map[Id, Node]()
  private val _final = new Array[Int](_length)
  private var _nextId: Id = 0


  private def parentIdx(node: Node): Int = (node.index - 1) / 2

  private def leftChildIdx(node: Node): Int = 2 * node.index + 1

  private def rightChildIdx(node: Node): Int = leftChildIdx(node) + 1

  private def getParent(node: Node): Node =
    if (node.index < 1) throw new Error("trying to take parent of root")
    else _storage(parentIdx(node))

  private def getRightChild(node: Node): Node =
    if (isLeaf(node)) throw new Error("trying to take right child of leaf")
    else _storage(rightChildIdx(node))

  private def getLeftChild(node: Node): Node =
    if (isLeaf(node)) throw new Error("trying to take left child of leaf")
    else _storage(leftChildIdx(node))

  private def getSibling(node: Node): Node =
    if (isLeftChild(node)) getRightChild(getParent(node))
    else getLeftChild(getParent(node))

  private def isRoot(node: Node): Boolean = node.index == 0

  private def isSiblingInFromLeftState(node: Node): Boolean = getSibling(node).state == State.FromLeft

  private def isLeftChild(node: Node): Boolean = {
    def isOdd(i: Int): Boolean = i % 2 == 1
    if (isRoot(node)) throw new Error("trying to check if root is left child")
    else isOdd(node.index)
  }

  private def isLeaf(node: Node): Boolean = leftChildIdx(node) >= _storage.length

  private def nextWorkId(): Int = {
    val id = _nextId
    _nextId += 1
    id
  }

  private def setFinal(node: Node, result: Int) = {
    if (!isLeaf(node)) throw new Error("trying to set final on non-leaf")
    val i = node.index - (_length - 1)
    _final(i) = result
  }

  def isDone: Boolean = _work.isEmpty && _inProgress.isEmpty

  def getResult: Array[Int] = _final.dropRight(_length - _originalLength)

  def nextWorkItemOption(): Option[WorkItem] = {
    if (_work.isEmpty) return None

    val node = _work.dequeue()
    val wi: WorkItem = getWorkItem(node)
    _inProgress += (wi.id -> node)
    Some(wi)
  }

  private def getWorkItem(node: Node): WorkItem = {
    val wi = node.state match {
      case State.Sum =>
        WorkItem(nextWorkId(), getLeftChild(node).sum, getRightChild(node).sum)
      case State.FromLeft => if (isLeftChild(node)) {
        WorkItem(nextWorkId(), getParent(node).fromLeft, _identity)
      } else {
        WorkItem(nextWorkId(), getParent(node).fromLeft, getLeftChild(getParent(node)).sum)
      }
      case State.Prefix =>
        WorkItem(nextWorkId(), node.fromLeft, node.sum)
    }
    wi
  }

  private def getWorkNodes(node: Node): List[Node] =
    node.state match {
      case State.Sum if isRoot(node) => List(getLeftChild(node), getRightChild(node))
      case State.Sum if isSiblingInFromLeftState(node) => List(getParent(node))
      case State.FromLeft if isLeaf(node) => List(node)
      case State.FromLeft => List(getLeftChild(node), getRightChild(node))
      case _ => List()
    }

  private def updateState(node: Node, result: Int): Unit =
    node.state match {
      case State.Sum =>
        node.sum = result
        node.state = State.FromLeft
        if (isRoot(node)) {
          node.fromLeft = _identity
          node.state = State.Final
        }
      case State.FromLeft =>
        node.fromLeft = result
        node.state = State.Final
        if (isLeaf(node)) {
          node.state = State.Prefix
        }
      case State.Prefix =>
        node.state = State.Final
        setFinal(node, result)
    }


  def submitResult(id: Int, result: Int): Unit = {
    val node = _inProgress(id)

    _inProgress.remove(id)
    _work ++= getWorkNodes(node)
    updateState(node, result)
  }

  def hasWork: Boolean = _work.nonEmpty

  def nextWorkItem(): WorkItem = nextWorkItemOption().get

  private def createNodes(): Unit = _storage.indices foreach { i =>
    _storage(i) = Node(i, State.Sum, 0, 0)
  }

  private def initializeLeafs(): Unit = ((_length - 1) until _storage.length) foreach { i =>
    val j = i - (_length - 1)
    _storage(i).sum = _numbers(j)
    _storage(i).state = State.FromLeft
  }

  private def addInitialWork(): Unit = ((_length - 1) until _storage.length) foreach { i =>
    val node = _storage(i)
    if (isLeftChild(node)) {
      _work += getParent(node)
    }
  }

  // Initialize the state
  createNodes()
  initializeLeafs()
  addInitialWork()

  override def toString: String =
    s"""
       |{
       |  storage: ${_storage.toList}
       |  work: ${_work}
       |  in progress: ${_inProgress}
       |  next id: ${_nextId}
       |  final: ${_final.toList}
       |  is done: $isDone
       |}
     """.stripMargin

  def ceilPower2(x: Int) =
    if (x == 0) 0
    else {
      val p = 32 - Integer.numberOfLeadingZeros(x - 1)
      1 << p
    }
}

object PrefixSumState {
  def apply(numbers: Array[Int]): PrefixSumState = new PrefixSumState(numbers)

  def getSum(numbers: Array[Int]): Array[Int] = {
    val sumState = apply(numbers)

    var work = List[PrefixSumState#WorkItem]()
    def getWork() = {
      work = sumState.nextWorkItem() :: work
    }

    def submitWork() = {
      val i = Random.nextInt(work.length)
      val wi = work(i)
      work = work.patch(i, Nil, 1)
      sumState.submitResult(wi.id, wi.x + wi.y)
    }

    while (!sumState.isDone) {
      if (work.isEmpty) getWork()
      else if (!sumState.hasWork) submitWork()
      else {
        val get = Random.nextBoolean()
        if (get) getWork()
        else submitWork()
      }
    }
    sumState.getResult
  }

  def test(): Unit = {
    val sumState = apply(Array(1, 2, 3, 4))
    def p = println(sumState)
    p
    val n1 = sumState.nextWorkItem()
    p
    val n2 = sumState.nextWorkItem()
    p
    sumState.submitResult(n1.id, n1.x + n1.y)
    p
    sumState.submitResult(n2.id, n2.x + n2.y)
    p
    val n0 = sumState.nextWorkItem()
    p
    sumState.submitResult(n0.id, n0.x + n0.y)
    p
    val f1 = sumState.nextWorkItem()
    p
    val f2 = sumState.nextWorkItem()
    p
    sumState.submitResult(f1.id, f1.x + f1.y)
    p
    sumState.submitResult(f2.id, f2.x + f2.y)
    p

    val fs = (0 until 4).map(_ => {
      val wi = sumState.nextWorkItem()
      p
      wi
    })

    fs foreach { f =>
      sumState.submitResult(f.id, f.x + f.y); p
    }


    val ps = (0 until 4).map(_ => {
      val wi = sumState.nextWorkItem()
      p
      wi
    })

    ps foreach { f =>
      sumState.submitResult(f.id, f.x + f.y); p
    }
  }

}
