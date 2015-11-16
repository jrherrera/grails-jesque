package grails.plugin.jesque

import org.joda.time.DateTime
import com.newrelic.api.agent.Trace

class Trigger {
    public static final String REDIS_PREFIX = 'trigger'
    public static final String TRIGGER_NEXTFIRE_INDEX = 'trigger:nextFireTime:WAITING:sorted'

    String jobName
    DateTime nextFireTime
    TriggerState state
    String acquiredBy

    @Trace
    static Trigger fromRedisHash(Map<String, String> hash) {
        Trigger trigger = new Trigger()
        trigger.jobName = hash.jobName
        trigger.nextFireTime = new DateTime(hash.nextFireTime.toLong())
        trigger.state = TriggerState.findByName(hash.state)
        trigger.acquiredBy = hash.acquiredBy

        return trigger
    }

    @Trace
    Map<String, String> toRedisHash() {

        [jobName: jobName, nextFireTime:nextFireTime.millis.toString(), state:state.name, acquiredBy:acquiredBy]
    }

    @Trace
    String getRedisKey() {
        "$REDIS_PREFIX:$jobName"
    }

    @Trace
    static String getRedisKeyForJobName(String jobName) {
        "$REDIS_PREFIX:$jobName"
    }

    @Trace
    static String getAcquiredIndexByHostName(String hostName) {
        "$REDIS_PREFIX:state:${TriggerState.Acquired.name}:$hostName"
    }
}
