import org.zeromq.ZFrame
import org.zeromq.ZMQ

internal class Worker(var address: ZFrame) {
    var identity = String(address.data, ZMQ.CHARSET)
    var expiry = System.currentTimeMillis() + LoadBalancer.Constants.HEARTBEAT_INTERVAL * LoadBalancer.Constants.HEARTBEAT_LIVELINESS

    fun ready(workers: ArrayList<Worker>) {
        val it = workers.iterator()
        while (it.hasNext()) {
            val worker = it.next()
            if (identity == worker.identity) {
                it.remove()
                break
            }
        }
        workers.add(this)
    }

    companion object {
        fun next(workers: ArrayList<Worker>): ZFrame {
            val worker = workers.removeAt(0)
            return worker.address
        }
        fun purge(workers: ArrayList<Worker>) {
            val it = workers.iterator()
            while (it.hasNext()) {
                val worker = it.next()
                if (System.currentTimeMillis() < worker.expiry) {
                    break
                }
                it.remove()
            }
        }
    }
}