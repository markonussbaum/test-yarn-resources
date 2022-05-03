package ai.xpress.testyarnresources

import org.apache.hadoop.yarn.api.records.{Container, ContainerExitStatus, ContainerState, ContainerStatus, NodeReport, UpdatedContainer}
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync

class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
  override def onContainersCompleted(statuses: java.util.List[ContainerStatus]): Unit = {
    // deregister

    //rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Appmsg: Finished", "")
    println("container completeted with statuses:")
    statuses.forEach( (s: ContainerStatus) => {
      val cid = s.getContainerId
      val cs = s.getState
      val exitStatus = s.getExitStatus
      val diag = s.getDiagnostics
      println("Container with id " + cid.toString + " completed with state " + cs.toString)

      // only completed containers should be here
      assert(cs == ContainerState.COMPLETE)

      if (exitStatus != 0) {

        if (exitStatus == ContainerExitStatus.ABORTED) {
          // TODO: App failed

        } else {

          // Container was killed by framework

        }

      } else {
        println("Container completed successfully")
      }

    })
  }

  override def onContainersAllocated(containers: java.util.List[Container]): Unit = {
    println("containers allocated:")
    containers.forEach( (c: Container) => {
      println("container: " + c.toString)

      /* TODO
      val runnableLaunchContainer = new LaunchContainerRunnable(
        c,  // container to run in
        this // NMCallbackHandler
      )

       */

    })

    //
  }

  override def onContainersUpdated(containers: java.util.List[UpdatedContainer]): Unit = {
    println("Containers updated:")
    containers.forEach( (uc: UpdatedContainer) => {
      println("updated: " + uc.toString)
    })
  }

  override def onShutdownRequest(): Unit = {
    println("shutdown requested by RM")
  }

  override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = {
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
