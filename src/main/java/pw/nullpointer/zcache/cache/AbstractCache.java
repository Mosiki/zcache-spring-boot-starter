package pw.nullpointer.zcache.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.data.redis.serializer.RedisSerializer;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author WeJan
 * @since 2020-07-18
 */
public abstract class AbstractCache<K, V> implements Cache<K, V> {
    static Logger logger = LoggerFactory.getLogger(AbstractCache.class);

    private static final String ALL_KEYS = "__zcache_pubsub_channel__allKeys__";
    private static final Map<String, AbstractCache<?, ?>> cacheMap = new ConcurrentHashMap<>(8);

    @Resource
    private RedisConnectionFactory redisConnectionFactory;

    public AbstractCache() {
        cacheMap.put(getCacheChannel(), this);
    }

    private String getCacheChannel() {
        return "__zcache_pubsub_channel__" + this.getClass().getTypeName();
    }

    @Nonnull
    protected abstract Cache<K, V> getCache();

    @Nonnull
    protected abstract Function<? super K, ? extends V> getLoadFunction();

    @CheckForNull
    public final V get(@Nonnull K key) {
        return get(key, getLoadFunction());
    }

    @CheckForNull
    @Override
    public final V getIfPresent(@Nonnull Object key) {
        return getCache().getIfPresent(key);
    }

    @CheckForNull
    @Override
    public final V get(@Nonnull K key, @Nonnull Function<? super K, ? extends V> mappingFunction) {
        return getCache().get(key, mappingFunction);
    }

    @Override
    public @NonNull Map<K, V> getAll(@NonNull Iterable<? extends K> keys, @NonNull Function<Iterable<? extends K>, Map<K, V>> mappingFunction) {
        return getCache().getAll(keys, mappingFunction);
    }

    @Nonnull
    @Override
    public final Map<K, V> getAllPresent(@Nonnull Iterable<?> keys) {
        return getCache().getAllPresent(keys);
    }

    @Override
    public final void put(@Nonnull K key, @Nonnull V value) {
        getCache().put(key, value);
    }

    @Override
    public final void putAll(@Nonnull Map<? extends K, ? extends V> map) {
        getCache().putAll(map);
    }

    @Override
    public final void invalidate(@Nonnull Object key) {
        RedisConnection connection = RedisConnectionUtils.getConnection(redisConnectionFactory);
        try {
            connection.publish(getChannel(), Objects.requireNonNull(RedisSerializer.java().serialize(key)));
        } finally {
            RedisConnectionUtils.releaseConnection(connection, redisConnectionFactory, false);
        }
        getCache().invalidate(key);
    }

    private byte[] getChannel() {
        return getCacheChannel().getBytes(UTF_8);
    }

    @Override
    public final void invalidateAll(@Nonnull Iterable<?> keys) {
        RedisConnection connection = RedisConnectionUtils.getConnection(redisConnectionFactory);
        try {
            connection.publish(getChannel(), Objects.requireNonNull(RedisSerializer.java().serialize(keys)));
        } finally {
            RedisConnectionUtils.releaseConnection(connection, redisConnectionFactory, false);
        }
        getCache().invalidateAll(keys);
    }

    @Override
    public final void invalidateAll() {
        RedisConnection connection = RedisConnectionUtils.getConnection(redisConnectionFactory);
        try {
            connection.publish(getChannel(), Objects.requireNonNull(RedisSerializer.java().serialize(ALL_KEYS)));
        } finally {
            RedisConnectionUtils.releaseConnection(connection, redisConnectionFactory, false);
        }
        getCache().invalidateAll();
    }

    @Override
    public final long estimatedSize() {
        return getCache().estimatedSize();
    }

    @Nonnull
    @Override
    public final CacheStats stats() {
        return getCache().stats();
    }

    @Nonnull
    @Override
    public final ConcurrentMap<K, V> asMap() {
        return getCache().asMap();
    }

    @Override
    public final void cleanUp() {
        getCache().cleanUp();
    }

    @Nonnull
    @Override
    public final Policy<K, V> policy() {
        return getCache().policy();
    }

    public static class CacheMessageListener implements MessageListener {

        @Override
        public void onMessage(@Nonnull Message message, byte[] bytes) {
            String channel = new String(message.getChannel());
            AbstractCache<?, ?> abstractCache = cacheMap.get(channel);
            if (abstractCache != null) {
                Object key = Objects.requireNonNull(RedisSerializer.java().deserialize(message.getBody()));
                Cache<?, ?> cache = abstractCache.getCache();
                logger.info("CacheMessageListener listen on message. message: {}, channel: {}", key, channel);
                if (key instanceof String && ((String) key).equalsIgnoreCase(ALL_KEYS)) {
                    cache.invalidateAll();
                } else if (key instanceof Iterable) {
                    cache.invalidateAll((Iterable<?>) key);
                } else {
                    cache.invalidate(key);
                }
            }
        }
    }
}
