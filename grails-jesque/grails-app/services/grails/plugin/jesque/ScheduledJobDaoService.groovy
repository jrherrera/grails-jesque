package grails.plugin.jesque

import redis.clients.jedis.Jedis
import com.newrelic.api.agent.Trace

class ScheduledJobDaoService {

    def redisService

    @Trace
    void save( ScheduledJob scheduledJob ) {
        redisService.withRedis { Jedis redis ->
            save( redis, scheduledJob )
        }
    }

    @Trace
    void save( Jedis redis, ScheduledJob scheduledJob ) {
        redis.hmset(scheduledJob.redisKey, scheduledJob.toRedisHash())
        redis.sadd(ScheduledJob.JOB_INDEX, scheduledJob.name)
    }

    @Trace
    void delete( String name ) {
        redisService.withRedis { Jedis redis ->
            delete( redis, name )
        }
    }

    @Trace
    void delete( Jedis redis, String name) {
        redis.del(ScheduledJob.getRedisKeyForName(name))
        redis.srem(ScheduledJob.JOB_INDEX, name)
    }

    @Trace
    ScheduledJob findByName(String name) {
        redisService.withRedis { Jedis redis ->
            findByName(redis, name)
        } as ScheduledJob
    }

    @Trace
    ScheduledJob findByName(Jedis redis, String name) {
        def scheduledJob = ScheduledJob.fromRedisHash( redis.hgetAll( ScheduledJob.getRedisKeyForName(name) ) )
        scheduledJob.trigger = Trigger.fromRedisHash( redis.hgetAll( Trigger.getRedisKeyForJobName(name) ) )

        scheduledJob
    }

    @Trace
    List<ScheduledJob> getAll() {
        redisService.withRedis { Jedis redis ->
            redis.smembers(ScheduledJob.JOB_INDEX).collect{ jobName ->
                findByName(redis, jobName)
            }
        } as List<ScheduledJob>
    }
}

