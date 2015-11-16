package grails.plugin.jesque

import net.greghaines.jesque.worker.WorkerListener
import net.greghaines.jesque.worker.WorkerEvent
import net.greghaines.jesque.worker.Worker
import net.greghaines.jesque.Job
import com.newrelic.api.agent.Trace

class WorkerLifecycleListener implements WorkerListener {

    JesqueService jesqueService

    WorkerLifecycleListener(JesqueService jesqueService) {
        this.jesqueService = jesqueService
    }

    @Trace
    void onEvent(WorkerEvent workerEvent, Worker worker, String queue, Job job, Object runner, Object result, Exception ex) {
        log.debug("Processing worker event ${workerEvent.name()}")
        if( workerEvent == WorkerEvent.WORKER_STOP ) {
            jesqueService.removeWorkerFromLifecycleTracking(worker)
        }
    }
}
