import java.nio.ByteBuffer

import com.google.protobuf.ByteString
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver}
import org.apache.mesos.Protos._

/**
  * Created by mesosphere on 6/29/16.
  */
object PrefixSumExecutor extends Executor {
  override def shutdown(driver: ExecutorDriver): Unit = {
    println("Shutdown: starting")
  }

  override def disconnected(driver: ExecutorDriver): Unit = println("Disconnected: ")

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = println("Kill Task: ")

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = println("Reregistered: ")

  override def error(driver: ExecutorDriver, message: String): Unit = println("Error: in error mode")

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = println("Framework Message: ")

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = println("Registered: ")

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    print(
      s"""
         |Launch Task: ${task.getTaskId.getValue}
      """.stripMargin)

    val thread = new Thread {
      override def run(): Unit = {
        driver.sendStatusUpdate(TaskStatus.newBuilder
          .setTaskId(task.getTaskId)
          .setState(TaskState.TASK_RUNNING).build())

        val data: ByteString = task.getData
        val x = data.asReadOnlyByteBuffer().getInt(0)
        val y = data.asReadOnlyByteBuffer().getInt(4)
        val result = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(x + y))

        driver.sendStatusUpdate(TaskStatus.newBuilder
          .setTaskId(task.getTaskId)
          .setState(TaskState.TASK_FINISHED)
          .setData(result)
          .build())
      }
    }

    thread.start()
  }

  def main(args: Array[String]): Unit = {
    val driver = new MesosExecutorDriver(PrefixSumExecutor)
    driver.run()
  }
}
