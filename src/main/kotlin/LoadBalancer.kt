import mu.KotlinLogging
import org.zeromq.*
import org.zeromq.ZMQ.Socket

internal object LoadBalancer {
    private val logger = KotlinLogging.logger ("Load Balancer")

    object Constants {
        const val HEARTBEAT_LIVELINESS = 3
        const val HEARTBEAT_INTERVAL = 1000
        const val PPP_READY = "\u0001"
        const val PPP_HEARTBEAT = "\u0002"
        const val THREAD_NUMBERS = 4
    }

    private val workers = ArrayList<Worker>()
    private var heartBeat = 0L

    @JvmStatic
    fun run() {
        logger.info { "Creating context..." }
        ZContext(Constants.THREAD_NUMBERS).use { ctx ->
            val frontend = ctx.createSocket(SocketType.ROUTER)
            val backend = ctx.createSocket(SocketType.ROUTER)
            frontend.bind("tcp://*:5555")
            backend.bind("tcp://*:5556")
            val poller = ctx.createPoller(2)
            logger.info {
                "Creating sockets successfully"
            }
            heartBeat = System.currentTimeMillis() + Constants.HEARTBEAT_INTERVAL
            poller.register(backend, ZMQ.Poller.POLLIN)
            poller.register(frontend, ZMQ.Poller.POLLIN)
            logger.info {
                "Listening connections..."
            }
            while (!Thread.interrupted()) {
                val workersAvailable = workers.size > 0
                val rc = poller.poll(Constants.HEARTBEAT_INTERVAL.toLong())
                if (rc == -1)
                    break //  Interrupted

                if (poller.pollin(0)) {
                    backendAction(backend, frontend)
                }

                if (workersAvailable && poller.pollin(1)) {
                    frontendAction(backend, frontend)
                }
                checkHealth(backend)
                Worker.purge(workers)
            }
            logger.info { "Interrupted, Cleaning..." }
            workers.clear()
        }
        logger.info { "Ending LoadBalancer" }
    }


    @JvmStatic
    fun backendAction(backend: Socket, frontend: Socket) {
        val msg = ZMsg.recvMsg(backend) ?: return
        val address = msg.unwrap()
        val worker = Worker(address)
        worker.ready(workers)

        if (msg.size == 1) {
            val frame = msg.first
            val data = String(frame.data, ZMQ.CHARSET)
            if (data != Constants.PPP_READY && data != Constants.PPP_HEARTBEAT ) {
                logger.error {
                    "E: invalid message from worker"
                }
                msg.dump(System.out)
            }
            msg.destroy()
        } else
            msg.send(frontend)
    }

    @JvmStatic
    fun frontendAction(backend: Socket, frontend: Socket) {
        val msg = ZMsg.recvMsg(frontend) ?: return
        msg.push(Worker.next(workers))
        msg.send(backend)
    }
    @JvmStatic
    fun checkHealth(backend: Socket){
        if (System.currentTimeMillis() >= heartBeat) {
            for (worker in workers) {
                worker.address.send(
                    backend, ZFrame.REUSE + ZFrame.MORE
                )
                val frame = ZFrame(Constants.PPP_HEARTBEAT)
                frame.send(backend, 0)
            }
            val now = System.currentTimeMillis()
            heartBeat = now + Constants.HEARTBEAT_INTERVAL
        }
    }
}