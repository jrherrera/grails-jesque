package grails.plugin.jesque

import redis.clients.jedis.Jedis
import com.newrelic.api.agent.Trace

class TriggerDaoService {

    def redisService

    @Trace
    void save( Trigger trigger) {
        redisService.withRedis { Jedis redis ->
            save( redis, trigger )
        }
    }

    @Trace
    void save( Jedis redis, Trigger trigger) {
        redis.hmset(trigger.redisKey, trigger.toRedisHash())
    }

    @Trace
    void delete( String jobName ) {
        redisService.withRedis { Jedis redis ->
            delete( redis, jobName )
        }
    }

    @Trace
    void delete( Jedis redis, String jobName ) {
        redis.del(Trigger.getRedisKeyForJobName(jobName))
        redis.zrem(Trigger.TRIGGER_NEXTFIRE_INDEX, jobName)
    }

    @Trace
    Trigger findByJobName(String jobName) {
        redisService.withRedis { Jedis redis ->
            findByJobName(redis, jobName)
        } as Trigger
    }

    @Trace
    Trigger findByJobName(Jedis redis, String jobName) {
        Trigger.fromRedisHash( redis.hgetAll( Trigger.getRedisKeyForJobName(jobName) ) )
    }

}
