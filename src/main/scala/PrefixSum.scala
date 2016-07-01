import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos._

/**
  * Created by Jesus E. Larios Murillo on 6/24/16.
  */
object PrefixSum {

  def main(args: Array[String]): Unit = {
    val name = "Prefix Sum Framework " + System.currentTimeMillis()
    val user = "" // take the default
    val checkpoint = false
    val timeout = 60.0
    val id = FrameworkID.newBuilder.setValue(name).build()

    val executorCommand = CommandInfo.newBuilder
      .setValue("/opt/mesosphere/bin/java -cp /vagrant/PrefixSumFramework-assembly-1.0.jar PrefixSumExecutor")
      .build()
    val executorId = ExecutorID.newBuilder.setValue("PrefixExecutor-" + System.currentTimeMillis())
    val executorName = "Prefix Executor"
    val source = "PrefixExecutor"


    val executor = ExecutorInfo.newBuilder
      .setCommand(executorCommand)
      .setExecutorId(executorId)
      .setName(executorName)
      .setSource(source)
      .build()

    val scheduler = new PrefixSumScheduler((1 to 128).toArray, executor)
    val framework = FrameworkInfo.newBuilder
      .setName(name)
      .setFailoverTimeout(timeout)
      .setCheckpoint(checkpoint)
      .setUser(user)
      .setId(id)
      .build()
    val mesosMaster = "192.168.65.90:5050"

    val driver = new MesosSchedulerDriver(scheduler, framework, mesosMaster)
    driver.run()
  }
}
