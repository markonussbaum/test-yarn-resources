package ai.xpress.testyarnresources

import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId, Container, ContainerExitStatus, ContainerId, ContainerLaunchContext, ContainerState, ContainerStatus, FinalApplicationStatus, NodeReport, Priority, Resource, UpdatedContainer}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.{AMRMClientAsyncImpl, NMClientAsyncImpl}
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.concurrent.ConcurrentHashMap


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

  /// Application start command for the app
  private val appCommand = List(
    "$JAVA_HOME/bin/java -Xmx256M" +
      " ai.xpress.testyarnresources.TYRMain" +
      " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
  )

  private val appPath = "Application/target/scala-2.13/tyrapp_2.13-0.1.0-SNAPSHOT.jar"
  private val appClassPath = ""

  private var conf = new YarnConfiguration()

  private var nmClientAsync: NMClientAsync = null
  private var containerListener: NMCallbackHandler = null

  private var appAttemptID: ApplicationAttemptId = null
  private var appID: ApplicationId = null

  private var containerId: ContainerId = null
  private var appSubmissionTime: LocalDateTime = null
  private var nmHost: String = null
  private var nmHttpPort: Int = 0
  private var nmPort: Int = 0
  private var applicationAttemptId: ApplicationAttemptId = null
  private var appMasterTrackingUrl: String = null

  // Container Information
  private val containerStartTimes =  new ConcurrentHashMap[ContainerId, Long]()

  def main(args: Array[String]): Unit = {

    println("The ApplicationMaster has been started.")

    evaluateEnv()

    copyApplicationToHDFS()

    val rmClient = startResourceManagerClient()
    val nmClient = startNodeManagerClient()

    requestContainersForApp(rmClient);

  }

  def requestContainersForApp(rmClient: AMRMClientAsync[ContainerRequest]): Unit = {
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
  }

  def evaluateEnv(): Unit = {
    val env = System.getenv()

    // helper function
    def evalMandatoryEnvVar(envVarName: String): String = {
      val result: String = env.get(envVarName)
      if (result == null) {
        throw new RuntimeException("environment variable "
          + envVarName + " needs to be set.")
      }
      result
    }

    // get & print container id
    val containerIdString = evalMandatoryEnvVar(ApplicationConstants.Environment.CONTAINER_ID.toString)
    println("Container ID is " + containerIdString)
    this.containerId = ConverterUtils.toContainerId(containerIdString)   // TODO: deprecated, but what is new?

    // get & print application submission time
    val appTimeStampStr = evalMandatoryEnvVar(ApplicationConstants.APP_SUBMIT_TIME_ENV)
    val ts = appTimeStampStr.toLong
    val inst = Instant.ofEpochMilli(ts)
    this.appSubmissionTime = LocalDateTime.ofInstant(inst, ZoneId.systemDefault())
    println("Application was submitted at " + this.appSubmissionTime.format(DateTimeFormatter.ISO_DATE_TIME))

    this.nmHost = evalMandatoryEnvVar(ApplicationConstants.Environment.NM_HOST.name())

    val httpPortStr = evalMandatoryEnvVar(ApplicationConstants.Environment.NM_HTTP_PORT.name())
    this.nmHttpPort = httpPortStr.toInt

    val nmPortStr = evalMandatoryEnvVar(ApplicationConstants.Environment.NM_PORT.name())
    this.nmPort = nmPortStr.toInt

    val nmTrackingUrl = "http://" + nmHost + ":" + nmHttpPort
    println("NodeManager is running on host " + nmHost + ":" + nmPort)
    println("with it's web interface at " + nmTrackingUrl)

    // important for communication with RM: ApplicationAttemptId
    this.appAttemptID = containerId.getApplicationAttemptId
    println("application attempt id: " + this.appAttemptID.toString)
  }


  def copyApplicationToHDFS() = {
    // TODO
    println("TODO: Implement copyApplicationToHDFS")
  }

  def startResourceManagerClient(): AMRMClientAsync[ContainerRequest] = {

    val rmClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler)

    rmClient.init(conf)
    rmClient.start()

    // register heartbeat
    val appMasterHostname = NetUtils.getHostname
    val appMasterRpcPort = -1     // ??? TODO: Research in src
    val appMasterTrackingUrl = "" // ???
    rmClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl)
    rmClient
  }

  private

  def startNodeManagerClient(): NMClientAsync = {
    val nmClient = new NMClientAsyncImpl(new NMCallbackHandler)
    nmClient.init(conf)
    nmClient.start()
    nmClient
  }

}
