package grails.plugin.jesque

import net.greghaines.jesque.Job
import net.greghaines.jesque.admin.Admin
import net.greghaines.jesque.admin.AdminClient
import net.greghaines.jesque.admin.AdminImpl
import net.greghaines.jesque.client.Client
import net.greghaines.jesque.meta.WorkerInfo
import net.greghaines.jesque.meta.dao.WorkerInfoDAO
import net.greghaines.jesque.worker.ExceptionHandler
import net.greghaines.jesque.worker.Worker
import net.greghaines.jesque.worker.WorkerEvent
import net.greghaines.jesque.worker.WorkerListener
import org.codehaus.groovy.grails.support.PersistenceContextInterceptor
import org.joda.time.DateTime
import org.springframework.beans.factory.DisposableBean
import com.newrelic.api.agent.Trace

class JesqueService implements DisposableBean {

    static transactional = false
    static scope = 'singleton'

    static final int DEFAULT_WORKER_POOL_SIZE = 3

    def grailsApplication
    def jesqueConfig
    def jesqueDelayedJobService
    PersistenceContextInterceptor persistenceInterceptor
    Client jesqueClient
    WorkerInfoDAO workerInfoDao
    List<Worker> workers = Collections.synchronizedList([])
    AdminClient jesqueAdminClient

    @Trace
    void enqueue(String queueName, Job job) {
        jesqueClient.enqueue(queueName, job)
    }

    @Trace
    void enqueue(String queueName, String jobName, List args) {
        enqueue(queueName, new Job(jobName, args))
    }

    @Trace
    void enqueue(String queueName, Class jobClazz, List args) {
        enqueue(queueName, jobClazz.simpleName, args)
    }

    @Trace
    void enqueue(String queueName, String jobName, Object... args) {
        enqueue(queueName, new Job(jobName, args))
    }

    @Trace
    void enqueue(String queueName, Class jobClazz, Object... args) {
        enqueue(queueName, jobClazz.simpleName, args)
    }

    @Trace
    void priorityEnqueue(String queueName, Job job) {
        jesqueClient.priorityEnqueue(queueName, job)
    }

    @Trace
    void priorityEnqueue(String queueName, String jobName, def args) {
        priorityEnqueue(queueName, new Job(jobName, args))
    }

    @Trace
    void priorityEnqueue(String queueName, Class jobClazz, def args) {
        priorityEnqueue(queueName, jobClazz.simpleName, args)
    }

    @Trace
    void enqueueAt(DateTime dateTime, String queueName, Job job) {
        jesqueDelayedJobService.enqueueAt(dateTime, queueName, job)
    }

    @Trace
    void enqueueAt(DateTime dateTime, String queueName, String jobName, Object... args) {
        enqueueAt( dateTime, queueName, new Job(jobName, args) )
    }

    @Trace
    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, Object... args) {
        enqueueAt( dateTime, queueName, jobClazz.simpleName, args)
    }

    @Trace
    void enqueueAt(DateTime dateTime, String queueName, String jobName, List args) {
        enqueueAt( dateTime, queueName, new Job(jobName, args) )
    }

    @Trace
    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, List args) {
        enqueueAt( dateTime, queueName, jobClazz.simpleName, args )
    }

    @Trace
    void enqueueIn(Integer millisecondDelay, String queueName, Job job) {
        enqueueAt( new DateTime().plusMillis(millisecondDelay), queueName, job )
    }

    @Trace
    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, Object... args) {
        enqueueIn( millisecondDelay, queueName, new Job(jobName, args) )
    }

    @Trace
    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, Object... args) {
        enqueueIn( millisecondDelay, queueName, jobClazz.simpleName, args )
    }

    @Trace
    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, List args) {
        enqueueIn( millisecondDelay, queueName, new Job(jobName, args) )
    }

    @Trace
    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, List args) {
        enqueueIn( millisecondDelay, queueName, jobClazz.simpleName, args )
    }

    @Trace
    Worker startWorker(String queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler = null, boolean paused = false){
        startWorker([queueName], [(jobName):jobClass], exceptionHandler, paused)
    }

    @Trace
    Worker startWorker(List queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler = null, boolean paused = false){
        startWorker(queueName, [(jobName):jobClass], exceptionHandler, paused)
    }

    @Trace
    Worker startWorker(String queueName, Map<String, Class> jobTypes, ExceptionHandler exceptionHandler = null, boolean paused = false){
        startWorker([queueName], jobTypes, exceptionHandler, paused)
    }

    @Trace
    Worker startWorker(List<String> queues, Map<String, Class> jobTypes, ExceptionHandler exceptionHandler = null, boolean paused = false){
        log.debug "Starting worker processing queueus: ${queues}"

        Class workerClass = GrailsWorkerImpl
        def customWorkerClass = grailsApplication.config.grails.jesque.custom.worker.clazz
        if(customWorkerClass) {
            if( customWorkerClass in GrailsWorkerImpl)
                workerClass = customWorkerClass
            else
                log.warn("The specified custom worker class ${customWorkerClass} does not extend GrailsWorkerImpl. Ignoring it")
        }
        Worker worker = (Worker)workerClass.newInstance(grailsApplication, jesqueConfig, queues, jobTypes)

        def customListenerClass = grailsApplication.config.grails.jesque.custom.listener.clazz
        if(customListenerClass) {
            if(customListenerClass in WorkerListener )
                worker.addListener(customListenerClass.newInstance() as WorkerListener)
            else
                log.warn("The specified custom listener class ${customListenerClass} does not implement WorkerListener. Ignoring it")
        }

        def customJobExceptionHandler = grailsApplication.config.grails.jesque.custom.jobExceptionHandler.clazz
        if (customJobExceptionHandler) {
            if (customJobExceptionHandler in JobExceptionHandler)
                worker.jobExceptionHandler = customJobExceptionHandler.newInstance() as JobExceptionHandler
            else
                log.warn("The specified custom job exception handler class does not implement JobExceptionHandler. Ignoring it")
        }

        if (exceptionHandler)
            worker.exceptionHandler = exceptionHandler

        if (paused) {
            worker.togglePause(paused)
        }

        workers.add(worker)

        // create an Admin for this worker (makes it possible to administer across a cluster)
        Admin admin = new AdminImpl(jesqueConfig)
        admin.setWorker(worker)

        def workerPersistenceListener = new WorkerPersistenceListener(persistenceInterceptor)
        worker.addListener(workerPersistenceListener, WorkerEvent.JOB_EXECUTE, WorkerEvent.JOB_SUCCESS, WorkerEvent.JOB_FAILURE)

        def workerLifeCycleListener = new WorkerLifecycleListener(this)
        worker.addListener(workerLifeCycleListener, WorkerEvent.WORKER_STOP)

        def workerThread = new Thread(worker)
        workerThread.start()

        def adminThread = new Thread(admin)
        adminThread.start()

        worker
    }

    @Trace
    void stopAllWorkers() {
        log.info "Stopping ${workers.size()} jesque workers"

        List<Worker> workersToRemove = workers.collect{ it }
        workersToRemove.each { Worker worker ->
            try{
                log.debug "Stopping worker processing queues: ${worker.queues}"
                worker.end(true)
                worker.join(5000)
            } catch(Exception exception) {
                log.error "Exception ending jesque worker", exception
            }
        }
    }

    @Trace
    void withWorker(String queueName, String jobName, Class jobClassName, Closure closure) {
        def worker = startWorker(queueName, jobName, jobClassName)
        try {
            closure()
        } finally {
            worker.end(true)
        }
    }

    @Trace
    void startWorkersFromConfig(ConfigObject jesqueConfigMap) {

        def startPaused = jesqueConfigMap.startPaused as boolean ?: false

        jesqueConfigMap.workers.each { String workerPoolName, value ->
            log.info "Starting workers for pool $workerPoolName"

            def workers = value.workers ? value.workers.toInteger() : DEFAULT_WORKER_POOL_SIZE
            def queueNames = value.queueNames
            def jobTypes = value.jobTypes

            if (!((queueNames instanceof String) || (queueNames instanceof List<String>)))
                throw new Exception("Invalid queueNames for pool $workerPoolName, expecting must be a String or a List<String>.")

            if (!(jobTypes instanceof Map))
                throw new Exception("Invalid jobTypes (${jobTypes}) for pool $workerPoolName, must be a map")

            workers.times {
                startWorker(queueNames, jobTypes, null, startPaused)
            }
        }
    }

    @Trace
    void pruneWorkers() {
        def hostName = InetAddress.localHost.hostName
        workerInfoDao.allWorkers?.each { WorkerInfo workerInfo ->
            if( workerInfo.host == hostName ) {
                log.debug "Removing stale worker $workerInfo.name"
                workerInfoDao.removeWorker(workerInfo.name)
            }
        }
    }

    @Trace
    public void removeWorkerFromLifecycleTracking(Worker worker) {
        log.debug "Removing worker ${worker.name} from lifecycle tracking"
        workers.remove(worker)
    }

    @Trace
    void destroy() throws Exception {
        this.stopAllWorkers()
    }

    @Trace
    void pauseAllWorkersOnThisNode() {
        log.info "Pausing all ${workers.size()} jesque workers on this node"

        List<Worker> workersToPause = workers.collect{ it }
        workersToPause.each { Worker worker ->
            log.debug "Pausing worker processing queues: ${worker.queues}"
            worker.togglePause(true)
        }
    }

    @Trace
    void resumeAllWorkersOnThisNode() {
        log.info "Resuming all ${workers.size()} jesque workers on this node"

        List<Worker> workersToPause = workers.collect{ it }
        workersToPause.each { Worker worker ->
            log.debug "Resuming worker processing queues: ${worker.queues}"
            worker.togglePause(false)
        }
    }

    @Trace
    void pauseAllWorkersInCluster() {
        log.debug "Pausing all workers in the cluster"
        jesqueAdminClient.togglePausedWorkers(true)
    }

    @Trace
    void resumeAllWorkersInCluster() {
        log.debug "Resuming all workers in the cluster"
        jesqueAdminClient.togglePausedWorkers(false)
    }

    @Trace
    void shutdownAllWorkersInCluster() {
        log.debug "Shutting down all workers in the cluster"
        jesqueAdminClient.shutdownWorkers(true)
    }

    @Trace
    boolean areAllWorkersInClusterPaused() {
        return workerInfoDao.getActiveWorkerCount() == 0
    }
}
