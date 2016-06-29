import java.util

import org.apache.mesos.Protos._
import org.apache.mesos._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by Jesus E. Larios Murillo on 6/24/16.
  */
class PrefixSumScheduler(numbers: Array[Int]) extends Scheduler {

  val _sumState = PrefixSumState(numbers)
  val _workIds = mutable.Map[String, Int]()

  def generateCommand(wi: PrefixSumState#WorkItem): String = "exit $((" + s" ${wi.x} + ${wi.y} ))"
  def parseResult(result: String): Int = {
    val num = result.substring(result.lastIndexOf(' ') + 1).trim
    num.toInt
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    println(s"ResourceOffers: got some offers!")
    for (offer <- offers.asScala) {
      println(s"\tresource offer ${offer.getId.getValue}")

      if (_sumState.hasWork) {
        val wi = _sumState.nextWorkItem()

        val command = CommandInfo.newBuilder.setValue(generateCommand(wi)).build()
        val id = TaskID.newBuilder.setValue("task" + System.currentTimeMillis())
        val name = s"SleepTask-${id.getValue}"
        val slaveId = offer.getSlaveId
        val cpu = Resource.newBuilder.setName("cpus").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(1.0))
        val mem = Resource.newBuilder.setName("mem").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(32))

        val task = TaskInfo.newBuilder
          .setCommand(command)
          .setName(name)
          .setTaskId(id)
          .setSlaveId(slaveId)
          .addResources(cpu)
          .addResources(mem)
          .build()

        _workIds(id.getValue) = wi.id
        driver.launchTasks(List(offer.getId).asJava, List(task).asJava)
      } else  {
        println(s"\t no work available")
        driver.declineOffer(offer.getId)
      }
    }
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
   if (status.getState == Protos.TaskState.TASK_FAILED) {
      val taskId = status.getTaskId.getValue
      val workId = _workIds.remove(taskId).get
      val result = parseResult(status.getMessage)

      _sumState.submitResult(workId, result)
    } else {
      println(s"statusUpdate: Status update: Task ${status.getTaskId.getValue} is in state ${status.getState}, msg: ${status.getMessage}")
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

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int): Unit =
    println(s"executorLost: We lost the executor ${executorId.getValue} at slave ${slaveId.getValue}!!!")
}
