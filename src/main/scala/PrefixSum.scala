import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos.{FrameworkID, FrameworkInfo}

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

    val scheduler = new PrefixSumScheduler((1 to 64).toArray)
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
