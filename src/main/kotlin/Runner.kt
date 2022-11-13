import mu.KotlinLogging

object Runner{
    @JvmStatic
    fun main(args: Array<String>) {
        val MAX_TRIES = 4
        val logger = KotlinLogging.logger("Runner")
        logger.info { "Init process" }
        var crashes = 0
        while (!Thread.interrupted()){
            try{
                logger.info {
                    "Starting LoadBalancer..."
                }
                LoadBalancer.run()
                crashes = 0
            }catch (e: Exception){
                logger.error { "Error on load balancer: $e" }
                crashes++
                if(crashes == MAX_TRIES){
                    logger.error {
                        "Given up, crashed $crashes times"
                    }
                    break
                }

            }
        }
        logger.info {
            "Ending process..."
        }
    }
}
