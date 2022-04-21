package ai.xpress.testyarnresources


import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext, ContainerStatus, NodeId, NodeReport, UpdatedContainer}
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.util

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
import java.util.TimeZone
import java.util.logging.SimpleFormatter

/** This is our application master (AM).
 * It handles asynchronously the execution of our application TYRMain.
 * The AM needs to be submitted to to Yarn (RM) and is launched on an allocated container.
 *
 * Responsibilities of the AM_
 * - communicate with Yarn to negotiate/allocate resources for future containers
 * - communicate with Yarns NodeManagers (NM) to launch containers
 * -
 */
object TYRApplicationMaster  {

  //var nmClient: NMClientAsync

  def main(args: Array[String]): Unit = {

    //amClient.init(conf)

    //nmClient = new NMClientAsync.

    println("Test: If you can see this, the ApplicationMaster has been started.")

    val env = System.getenv()

    // get & print container id
    val containerIdString = env.get(ApplicationConstants.Environment.CONTAINER_ID.toString)
    println("Container ID is " + containerIdString)

    // get & print application submission time
    val appTimeStampStr = env.get(ApplicationConstants.APP_SUBMIT_TIME_ENV)
    val ts = appTimeStampStr.toLong
    val inst = Instant.ofEpochMilli(ts)
    val d = LocalDateTime.ofInstant(inst, ZoneId.systemDefault())

    println("Application was submitted at " + d.format(DateTimeFormatter.ISO_DATE_TIME))

    // get & print node manager details
    val nmHost = env.get(ApplicationConstants.Environment.NM_HOST.toString)
    val nmPort = env.get(ApplicationConstants.Environment.NM_PORT.toString)
    val nmHttpPort = env.get(ApplicationConstants.Environment.NM_HTTP_PORT.toString)
    println("NodeManager is running on host " + nmHost + ":" + nmPort.toString)
    println("with it's web interface at http://" + nmHost + ":" + nmHttpPort.toString)


  }
}
