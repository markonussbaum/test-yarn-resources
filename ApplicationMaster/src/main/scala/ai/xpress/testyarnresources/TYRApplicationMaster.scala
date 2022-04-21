package ai.xpress.testyarnresources


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext, ContainerStatus, FinalApplicationStatus, NodeId, NodeReport, Resource, UpdatedContainer}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync.AbstractCallbackHandler
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.util
import org.apache.hadoop.yarn.util.ConverterUtils

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
import java.util.{List, Map}

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

  var conf = new Configuration
  var appMasterRpcPort = 0
  var appMasterTrackingUrl = ""

  def main(args: Array[String]): Unit = {

    //amClient.init(conf)

    //nmClient = new NMClientAsync.

    println("Test: If you can see this, the ApplicationMaster has been started.")

    val env = System.getenv()

    // get & print container id
    val containerIdString = env.get(ApplicationConstants.Environment.CONTAINER_ID.toString)
    println("Container ID is " + containerIdString)
    val containerId = ConverterUtils.toContainerId(containerIdString)
    //val nodeId = NodeId.fromString(containerIdString)

    // get & print application submission time
    val appTimeStampStr = env.get(ApplicationConstants.APP_SUBMIT_TIME_ENV)
    val ts = appTimeStampStr.toLong
    val inst = Instant.ofEpochMilli(ts)
    val d = LocalDateTime.ofInstant(inst, ZoneId.systemDefault())

    println("Application was submitted at " + d.format(DateTimeFormatter.ISO_DATE_TIME))

    // get & print node manager details
    val nmHost = env.get(ApplicationConstants.Environment.NM_HOST.toString)
    val nmHttpPort = env.get(ApplicationConstants.Environment.NM_HTTP_PORT.toString)
    appMasterRpcPort = env.get(ApplicationConstants.Environment.NM_PORT.toString).toInt
    appMasterTrackingUrl = "http://" + nmHost + ":" + nmHttpPort
    println("NodeManager is running on host " + nmHost + ":" + appMasterRpcPort)
    println("with it's web interface at " + appMasterTrackingUrl)

    // important for communication with RM: ApplicationAttemptId
    val applicationAttemptId = containerId.getApplicationAttemptId

    copyApplicationToHDFS()

    startResourceManagerClient()
    startNodeManagerClient()


  }

  def copyApplicationToHDFS() = {
    // TODO
    println("TODO: Implement copyApplicationToHDFS")
  }

  def startResourceManagerClient(): Unit = {

    val allocListener = new AMRMClientAsync.AbstractCallbackHandler {

      override def onContainersCompleted(statuses: List[ContainerStatus]): Unit = {
        // deregister

        //rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Appmsg: Finished", "")
        println("container completeted with statuses:")
        statuses.forEach( (s: ContainerStatus) => {
          val cid = s.getContainerId
          val cs = s.getState
          println("Container with id " + cid.toString + " completed with state " + cs.toString)
        })
      }

      override def onContainersAllocated(containers: List[Container]): Unit = ???

      override def onContainersUpdated(containers: List[UpdatedContainer]): Unit = ???

      override def onShutdownRequest(): Unit = ???

      override def onNodesUpdated(updatedNodes: List[NodeReport]): Unit = ???

      override def getProgress: Float = {
        // TODO: Return actual progress of work
        1.0f
      }

      override def onError(e: Throwable): Unit = {
        println("an error occurred: " + e.getLocalizedMessage)
        println("Stack trace:\n" + e.getStackTrace)
      }
    }
    val rmClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)

    rmClient.init(conf)
    rmClient.start()

    // register heartbeat
    val appHostname = NetUtils.getHostname
    rmClient.registerApplicationMaster(appHostname, appMasterRpcPort, appMasterTrackingUrl)

  }

  def startNodeManagerClient(): Unit = {

    val nmClient = new NMClientAsyncImpl(new AbstractCallbackHandler {
      override def onContainerStarted(containerId: ContainerId, allServiceResponse: Map[String, ByteBuffer]): Unit = ???

      override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = ???

      override def onContainerStopped(containerId: ContainerId): Unit = ???

      override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = ???

      override def onContainerResourceIncreased(containerId: ContainerId, resource: Resource): Unit = ???

      override def onContainerResourceUpdated(containerId: ContainerId, resource: Resource): Unit = ???

      override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = ???

      override def onIncreaseContainerResourceError(containerId: ContainerId, t: Throwable): Unit = ???

      override def onUpdateContainerResourceError(containerId: ContainerId, t: Throwable): Unit = ???

      override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = ???
    } )
    nmClient.init(conf)
    nmClient.start()

  }

}
