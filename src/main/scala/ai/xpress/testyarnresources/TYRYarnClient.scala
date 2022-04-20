package ai.xpress.testyarnresources

import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationSubmissionContext, ContainerLaunchContext, LocalResource, Priority, Resource}
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}
import org.apache.hadoop.yarn.api.records.{LocalResourceType, LocalResourceVisibility}

import java.io.File
import java.nio.ByteBuffer
import java.util.logging.Logger
import scala.jdk.CollectionConverters._

/** This is the Yarn client, that requests access
 * to the cluster and configures and submits our application
 */
object TYRYarnClient {

  val yarnPath: String = "ApplicationMaster/target/scala-2.13/tyram_2.13-0.1.0-SNAPSHOT.jar"
  val applicationMasterJARPath: String = "ApplicationMaster/target/scala-2.13/TYRApplicationMaster-assembly.jar"
  val classPath: String = ""

  /// Application Master commands
  val applicationMasterCommand = List(
    "$JAVA_HOME/bin/java -Xmx256M" +
      " ai.xpress.testyarnresources.TYRApplicationMaster" +
      " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
  )

  /// Application Master Resources
  val applicationMasterResources = Map(
    "TYRYarnClient-assembly-0.1.jar" -> setupLocalResourceFromPath(yarnPath)
  )

  /// Application Master environment
  // will be exported in the start script
  val applicationMasterEnvironment = Map(
    "CLASSPATH" -> "TYRYarnClient-assembly-0.1.jar"
  )

  private val log = Logger.getLogger(TYRYarnClient.getClass.getName)

  def main(args: Array[String]): Unit = {

    val yarnClient = createYarnClient()
    val app = createClientApplication(yarnClient)
    val appId = getAppId(app)

    setupAppSubmissionContext(app)

    // finally
    yarnClient.submitApplication(app.getApplicationSubmissionContext)

    /*

    val fs = FileSystem.get(conf)

    // set jar of ApplicationMaster as a local resource
    val appMasterJar: LocalResource = Records.newRecord(classOf[LocalResource])
    var jarStatus: FileStatus = null;
    try {
      jarStatus = fs.getFileStatus(localJarPath)
    } catch {
      case e: Exception => {
        log.severe("could not find ApplicationMaster jar file at " + localJarPath)
      }
    }

    // copy bundle to HDFS
    val fs = FileSystem.get(conf)

    val localJarPath = new Path(args(0))
    val remotePath = fs.makeQualified(Path.mergePaths(new Path("/apps/"), localJarPath))
    fs.copyFromLocalFile(false, true, localJarPath, remotePath)

    val amContainerSpec = createContainerContext()

    val resource = Resource.newInstance(1024,2)
    val context = app.getApplicationSubmissionContext
    context.setAMContainerSpec(amContainerSpec)
    context.setApplicationName("MYRTA (Marko's Yarn Resources Test App)")
    context.setResource(resource)
    context.setPriority(prio)
    yarnClient.submitApplication(context)
*/
  }

  /// setup a local resources that are needed to run the container
  private def setupLocalResourceFromPath(str: String): LocalResource = {
    val res = Records.newRecord(classOf[LocalResource])

    val path = new Path(applicationMasterJARPath)
    val conf = new org.apache.hadoop.conf.Configuration()
    val fileStatus = path.getFileSystem(conf).getFileStatus(path)
    val timestamp =   (fileStatus.getModificationTime / 1000) * 1000  // make sure, the last 3 digits are 000
                                                                      // otherwise file is rejected as "changed"

    val pkgUrl = ConverterUtils.getYarnUrlFromPath(
      FileContext.getFileContext().makeQualified(path)
    )

    res.setResource(pkgUrl)
    res.setType(LocalResourceType.ARCHIVE)
    res.setVisibility(LocalResourceVisibility.APPLICATION)
    res.setTimestamp(timestamp)
    res
  }

  private def createYarnClient(): YarnClient = {
    val conf = new YarnConfiguration()
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(conf)
    yarnClient.start()
    log.info("Yarn client created")
    yarnClient
  }

  private def createClientApplication(yarnClient: YarnClient): YarnClientApplication = {
    var app: YarnClientApplication = null
    try {
      app = yarnClient.createApplication()
    } catch {
      case e: Exception => {
        log.severe("Could not create Yarn application. Aborting.")
        e.printStackTrace()
      }
    }
    app
  }

  private def getAppId(application: YarnClientApplication): ApplicationId = {
    val appResponse = application.getNewApplicationResponse
    log.info("Resources: " + appResponse.getMaximumResourceCapability.toString)
    appResponse.getApplicationId
  }


  private def setupAppSubmissionContext(app: YarnClientApplication): Unit = {
    val appContext: ApplicationSubmissionContext = app.getApplicationSubmissionContext
    appContext.setKeepContainersAcrossApplicationAttempts(false)
    appContext.setApplicationName("Yarn Resource Test")

    val resource = setupResource()
    val priority = setPriority()
    val containerSpec = setupContainerLaunchContext(app)

    appContext.setResource(resource)
    appContext.setPriority(priority)
    appContext.setAMContainerSpec(containerSpec)
  }

  private def setupResource(): Resource = {

    // Common resources:
    val mem: Long = 1024 // in MB
    val vCores: Int = 1 // we use only one core

    // Other resources: We want one Vector Engine
    val otherResources: Map[String, java.lang.Long] = Map(
      "ve" -> 1,
    )
    Resource.newInstance(mem, vCores, otherResources.asJava)
  }

  private def setPriority(): Priority = {
    val prio = Records.newRecord(classOf[Priority])
    prio.setPriority(1)
    prio
  }

  private def setupContainerLaunchContext(app: YarnClientApplication): ContainerLaunchContext = {
    val clc = Records.newRecord(classOf[ContainerLaunchContext])
    clc.setCommands(applicationMasterCommand.asJava)
    clc.setLocalResources(applicationMasterResources.asJava)
    clc.setEnvironment(applicationMasterEnvironment.asJava)
    clc
  }

}
