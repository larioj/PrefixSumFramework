import java.nio.ByteBuffer
import java.util

import com.google.protobuf.ByteString
import org.apache.mesos.Protos._
import org.apache.mesos._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by Jesus E. Larios Murillo on 6/24/16.
  */
class PrefixSumScheduler(numbers: Array[Int], executor: ExecutorInfo) extends Scheduler {

  private val _cpuPerTask = 1.0
  private val _memPerTask = 1024.0
  private val _sumState = PrefixSumState(numbers)
  private val _workIds = mutable.Map[String, Int]()


  private def getCpuCount(offer: Offer): Double = {
    val cpus = offer.getResourcesList.asScala.filter(_.getName == "cpus")
    assert(cpus.length == 1)
    val cpuCount = cpus.head.getScalar.getValue
    cpuCount
  }

  private def getTasks(offer: Offer): List[TaskInfo] = {

    def generateBytes(wi: PrefixSumState#WorkItem): ByteString = ByteString.copyFrom(ByteBuffer.allocate(8).putInt(0, wi.x).putInt(4, wi.y))
    
    def maxNumTasks(): Int = (getCpuCount(offer) / _cpuPerTask).toInt
    
    def generateTask(wi: PrefixSumState#WorkItem, offer: Offer): TaskInfo = {

      val id = TaskID.newBuilder.setValue("task" + System.currentTimeMillis() + "-" + wi.id)
      val name = s"SleepTask-${id.getValue}"
      val slaveId = offer.getSlaveId
      val cpu = Resource.newBuilder
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(_cpuPerTask))
      val mem = Resource.newBuilder
        .setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(_memPerTask))
      val data = generateBytes(wi)

      val task = TaskInfo.newBuilder
        .setData(data)
        .setExecutor(executor)
        .setName(name)
        .setTaskId(id)
        .setSlaveId(slaveId)
        .addResources(cpu)
        .addResources(mem)
        .build()

      _workIds(id.getValue) = wi.id // updates state

      task
    }
    
    def workItems = (1 to maxNumTasks()).map(_ => _sumState.nextWorkItemOption()).filter(_.isDefined)
    
    workItems.map(wi => generateTask(wi.get, offer)).toList
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    println(s"ResourceOffers: got some offers!")
    for (offer <- offers.asScala) {
      println(s"\tresource offer ${offer.getId.getValue}")

      if (_sumState.hasWork) {
        val tasks = getTasks(offer)
        println(s"\t launching ${tasks.length} tasks on ${getCpuCount(offer)} cpu")
        driver.launchTasks(List(offer.getId).asJava, tasks.asJava)
      } else  {
        println(s"\t no work available")
        driver.declineOffer(offer.getId)
      }
    }
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {

   if (status.getState == Protos.TaskState.TASK_FINISHED) {
      val taskId = status.getTaskId.getValue
      val workId = _workIds.remove(taskId).get
      val result = status.getData.asReadOnlyByteBuffer.getInt

      _sumState.submitResult(workId, result)
    } else {
      println(s"statusUpdate: Status update: Task ${status.getTaskId.getValue} is in state ${status.getState}")
    }

    if (_sumState.isDone) {
      println(
        s"""
          |Done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          |.............................................................................................................
          |
          |
          |${_sumState.getResult.toList}
          |
          |
        """.stripMargin)

      driver.stop()
    }
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit =
    println(s"offerRecinded: Offer ${offerId.getValue} has been rescinded")

  override def disconnected(driver: SchedulerDriver): Unit = {}
  println("disconnected: Disconnected from the mesos master")

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit =
    println("reregistered: Reregistered with the mesos master")

  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit =
    println(s"slaveLost: Slave ${slaveId.getValue} lost :(")

  override def error(driver: SchedulerDriver, message: String): Unit =
    println(s"error: Error: $message")

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Unit =
    println(s"frameworkMessage: Received message from executor ${executorId.getValue} at slave ${slaveId.getValue} with contents $data")

  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit =
    println(s"registered: Registered with mesos master ${masterInfo.getId} at ip ${masterInfo.getIp} with port ${masterInfo.getPort}")

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int): Unit = {}
    //println(s"executorLost: We lost the executor ${executorId.getValue} at slave ${slaveId.getValue}!!!")
}
