package ai.xpress.testyarnresources

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ContainerId, ContainerLaunchContext, ContainerStatus, Resource}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.util.Records
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

// Callbackhandler for Node Manager Client
private class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

  override def onContainerStarted(containerId: ContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]): Unit = {
    println("container with id " + containerId + " has been started! ")
    println("service response: " + allServiceResponse.toString)

    // create ContainerLaunchContext for the actual application, containing
    // - command to execute
    // - environment to execute the command
    // - binaries to localize all relevant security credentials
    val launchContext = Records.newRecord(classOf[ContainerLaunchContext])

    // TODO: complete start command
    val cmds = List(
      "$JAVA_HOME/bin/java -Xmx256M" +
        " ai.xpress.testyarnresources.TYRMain" +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    )

    launchContext.setCommands(cmds.asJava)
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
}
