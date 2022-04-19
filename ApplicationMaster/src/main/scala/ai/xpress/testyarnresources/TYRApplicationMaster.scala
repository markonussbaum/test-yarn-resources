package ai.xpress.testyarnresources

import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext, ContainerStatus, NodeId, NodeReport, UpdatedContainer}
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.util

import java.util

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

  }
}
