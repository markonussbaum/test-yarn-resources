package ai.xpress.testyarnresources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext, ContainerStatus, FinalApplicationStatus, NodeReport, Priority, Resource, UpdatedContainer}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.NMClientAsync.AbstractCallbackHandler
import org.apache.hadoop.yarn.client.api.async.impl.{AMRMClientAsyncImpl, NMClientAsyncImpl}
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
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

    val rmClient = startResourceManagerClient()
    startNodeManagerClient()

    // request container for our app
    val prio = Records.newRecord(classOf[Priority])
    prio.setPriority(1)

    val capability = Records.newRecord(classOf[Resource])
    capability.setMemorySize(128)
    capability.setVirtualCores(1)
//    capability.setResourceValue("ve", 1) // wants one vector engine

    // we only ask for 1 container for now
    val req = new ContainerRequest(capability, null, null, prio)
    rmClient.addContainerRequest(req)

    // rest is done async, once the container arrives
  }

  def copyApplicationToHDFS() = {
    // TODO
    println("TODO: Implement copyApplicationToHDFS")
  }

  def startResourceManagerClient(): AMRMClientAsync[ContainerRequest] = {

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

      override def onContainersAllocated(containers: List[Container]): Unit = {
        println("containers allocated:")
        containers.forEach( (c: Container) => {
          println("container: " + c.toString)
        })
      }

      override def onContainersUpdated(containers: List[UpdatedContainer]): Unit = {
        println("Containers updated:")
        containers.forEach( (uc: UpdatedContainer) => {
          println("updated: " + uc.toString)
        })
      }

      override def onShutdownRequest(): Unit = {
        println("shutdown requested by RM")
      }

      override def onNodesUpdated(updatedNodes: List[NodeReport]): Unit = {
        println("onNodesUpdated")
        updatedNodes.forEach( (nr: NodeReport) => {
          println("Node with id: " + nr.getNodeId.toString + " is now in state " + nr.getNodeState.toString)
          println("Node resources: " + nr.getCapability.toString)
        })
      }

      override def getProgress: Float = {
        // TODO: Return actual progress of work
        println("RM requests progress, reporting 1.0")
        1.0f
      }

      override def onError(e: Throwable): Unit = {
        println("an error occurred: " + e.getLocalizedMessage)
        println("Stack trace:\n" + e.getStackTrace)
      }
    }
    val rmClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)

    rmClient.init(conf)
    rmClient.start()

    // register heartbeat
    val appHostname = NetUtils.getHostname
    rmClient.registerApplicationMaster(appHostname, appMasterRpcPort, appMasterTrackingUrl)
    rmClient
  }

  def startNodeManagerClient(): Unit = {

    val nmClient = new NMClientAsyncImpl(new AbstractCallbackHandler {
      override def onContainerStarted(containerId: ContainerId, allServiceResponse: Map[String, ByteBuffer]): Unit = {
        println("container with id " + containerId + " has been started! ")
        println("service response: " + allServiceResponse.toString)
      }

      override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = {
        println("received status for continer with id " + containerId + ":")
        println(containerStatus.toString)
      }

      override def onContainerStopped(containerId: ContainerId): Unit = {
        println("container with id " + containerId + " has been stopped.")
        println("=========================================")
      }

      override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = {
        println("error starting container with id " + containerId + ": " + t.toString)
      }

      override def onContainerResourceIncreased(containerId: ContainerId, resource: Resource): Unit = {
        println("container " + containerId.toString + " got more resources")
      }

      override def onContainerResourceUpdated(containerId: ContainerId, resource: Resource): Unit = {
        println("container " + containerId.toString + " got it's resources updated")
      }

      override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = {
        println("container " + containerId.toString + " had an error")
        println(t.printStackTrace())
      }

      override def onIncreaseContainerResourceError(containerId: ContainerId, t: Throwable): Unit = {
        println("container " + containerId.toString + " had an error while increasing resources")
        println(t.printStackTrace())
      }

      override def onUpdateContainerResourceError(containerId: ContainerId, t: Throwable): Unit = {
        println("container " + containerId.toString + " had an error while updating resources")
        println(t.printStackTrace())
      }

      override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = {
        println("container " + containerId.toString + " had an error while stopping")
        println(t.printStackTrace())
      }
    } )
    nmClient.init(conf)
    nmClient.start()

  }

}
