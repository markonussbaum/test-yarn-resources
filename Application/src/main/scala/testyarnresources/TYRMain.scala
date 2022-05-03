package ai.xpress.testyarnresources

object TYRMain extends App {

  (1 until 10).foreach(no => {
    println("this should run in some container for the " + no + " time")
    Thread.sleep(1000)
  })

  // TODO: Test for resources

}
