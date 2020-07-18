package pw.nullpointer.zcache.autoconfigure;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import pw.nullpointer.zcache.cache.AbstractCache;

/**
 * @author WeJan
 * @since 2020-07-18
 */
@Configuration
@ConditionalOnBean(RedisConnectionFactory.class)
@ConditionalOnProperty(prefix = "zcache", name = "enabled", matchIfMissing = false, havingValue = "true")
public class ZCacheAutoConfiguration {
    private static final String PATTERN = "__zcache_pubsub_channel__*";

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory connectionFactory) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(new AbstractCache.CacheMessageListener(), new PatternTopic(PATTERN));
        return container;
    }
}
