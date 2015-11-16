package grails.plugin.jesque

import org.joda.time.DateTime
import net.greghaines.jesque.Job
import net.greghaines.jesque.json.ObjectMapperFactory
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Jedis
import com.newrelic.api.agent.Trace

class JesqueDelayedJobService {

    static transactional = false

    def redisService
    def jesqueService

    protected static final String RESQUE_DELAYED_JOBS_PREFIX = 'resque:delayed'

    @Trace
    public void enqueueAt(DateTime at, String queueName, Job job) {
        String jobString = ObjectMapperFactory.get().writeValueAsString(job)

        redisService.withPipeline { Pipeline pipeline ->
            log.info "JesqueDelayedJobService enqueueAt"
            pipeline.rpush( "${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}:${at.millis}", jobString )
            log.info "JesqueDelayedJobService enqueueAt RPUSH: ${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}:${at.millis}"
            pipeline.zadd( "${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}", at.millis.doubleValue(), at.millis.toString() )
            log.info "JesqueDelayedJobService enqueueAt ZADD: ${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}:${at.millis}"
            pipeline.sadd( "${RESQUE_DELAYED_JOBS_PREFIX}:queues", queueName)
            log.info "JesqueDelayedJobService enqueueAt SADD: ${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}:${at.millis}"
        }
    }

    @Trace
    public void enqueueReadyJobs() {
        def maxScore = new DateTime().millis as double

        redisService.withRedis { Jedis jedis ->
            def queues = jedis.smembers("${RESQUE_DELAYED_JOBS_PREFIX}:queues")

            log.info "JesqueDelayedJobService enqueueReadyJobs queues: ${queues}"
            queues.each{ String queueName ->
                def zrangeByScoreList = jedis.zrangeByScore("${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}", 0, maxScore)
                if ( zrangeByScoreList.size() > 0 ) {
                    log.info "enqueueReadyJobs zrangeByScoreList maxScore: ${maxScore} - size: ${zrangeByScoreList.size()} - first: ${zrangeByScoreList?.first()} - last: ${zrangeByScoreList?.last()}"
                }
                zrangeByScoreList.each{ timestamp ->
                    def jobString = jedis.lpop("${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}:${timestamp}")
                    if( jobString ) {
                        Job job = ObjectMapperFactory.get().readValue(jobString, Job.class)
                        jesqueService.enqueue(queueName, job)
                    } else {
                        deleteQueueTimestampListIfEmpty(queueName, timestamp)
                    }
                }

                //check if "${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}" is empty now, if so, conditionally delete it and the entry from the queues key
                deleteDelayedQueueIfEmpty(queueName)
            }
        }
    }

    @Trace
    public DateTime nextFireTime() {
        def dateTime = redisService.withRedis { Jedis jedis ->
            def queues = jedis.smembers("${RESQUE_DELAYED_JOBS_PREFIX}:queues")
            def minTimestamp = queues.collect{ queueName ->
                def timestamps = jedis.zrangeByScore( "${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}", '0', 'inf', 0, 1 )
                timestamps ? timestamps.asList().first().toLong() : Long.MAX_VALUE
            }.min()
            minTimestamp ? new DateTime( minTimestamp.toLong() ) : DateTime.now().plusYears(1000)
        } as DateTime

        return dateTime
    }

    @Trace
    protected void deleteQueueTimestampListIfEmpty(String queueName, String timestamp) {
        String queueTimestampKey = "${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}:${timestamp}"
        redisService.withRedis { Jedis jedis ->
            jedis.watch( queueTimestampKey)
            def length = jedis.llen( queueTimestampKey)
            log.info "deleteQueueTimestampListIfEmpty length: ${length} - queueTimestampKey: ${queueTimestampKey}"
            if( length == 0 ) {
                def transaction = jedis.multi()
                transaction.del( queueTimestampKey)
                transaction.zrem("${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}", timestamp)
                transaction.exec()
            } else {
                jedis.unwatch()
            }
        }
    }

    @Trace
    protected void deleteDelayedQueueIfEmpty(String queueName) {
        String queueKey = "${RESQUE_DELAYED_JOBS_PREFIX}:${queueName}"
        redisService.withRedis { Jedis jedis ->
            jedis.watch( queueKey )
            def length = jedis.zcard( queueKey )
            log.info "deleteDelayedQueueIfEmpty length: ${length} - queueKey: ${queueKey}"
            if( length == 0) {
                def transaction = jedis.multi()
                transaction.del( queueKey )
                transaction.srem( "${RESQUE_DELAYED_JOBS_PREFIX}:queues", queueName )
                transaction.exec()
            } else {
                jedis.unwatch()
            }
        }
    }
}
