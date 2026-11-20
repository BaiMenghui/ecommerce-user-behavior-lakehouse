package com.bmh.lakehouse.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.SimpleCacheErrorHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 缓存配置：app.cache-type=redis|caffeine 双实现。
 * redis 模式下启动时先 ping，失败自动降级 caffeine 并记录；
 * 运行期 Redis 异常由 CacheErrorHandler 吞掉（当作未命中），不阻断业务接口。
 */
@Configuration
public class CacheConfig implements CachingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(CacheConfig.class);

    public static final String[] CACHE_NAMES = {
            "ads_dates", "ads_kpi", "ads_dau_trend", "ads_funnel",
            "ads_topn", "ads_category_share", "ads_hour_heatmap"};

    /** 实际生效的缓存类型（/api/health 展示用） */
    public static final AtomicReference<String> ACTIVE_CACHE_TYPE = new AtomicReference<>("unknown");

    private final RedisConnectionFactory redisConnectionFactory;
    private final String configuredType;

    public CacheConfig(RedisConnectionFactory redisConnectionFactory,
                       @org.springframework.beans.factory.annotation.Value("${app.cache-type}") String configuredType) {
        this.redisConnectionFactory = redisConnectionFactory;
        this.configuredType = configuredType;
    }

    @Override
    public CacheManager cacheManager() {
        String type = configuredType == null ? "redis" : configuredType.toLowerCase();
        if ("redis".equals(type)) {
            try (RedisConnection conn = redisConnectionFactory.getConnection()) {
                conn.ping();
            } catch (Exception e) {
                log.warn("Redis 连接失败（{}），自动降级为 caffeine 本地缓存", e.getMessage());
                type = "caffeine";
            }
        }
        ACTIVE_CACHE_TYPE.set(type);
        log.info("生效缓存类型: {} (配置值: {})", type, configuredType);
        if ("redis".equals(type)) {
            return redisCacheManager();
        }
        return caffeineCacheManager();
    }

    private CacheManager redisCacheManager() {
        RedisCacheConfiguration conf = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(5))
                .disableCachingNullValues()
                .computePrefixWith(name -> "ads:" + name + ":")
                .serializeKeysWith(org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair
                        .fromSerializer(new GenericJackson2JsonRedisSerializer()));
        RedisCacheManager.RedisCacheManagerBuilder builder =
                RedisCacheManager.builder(redisConnectionFactory).cacheDefaults(conf);
        for (String name : CACHE_NAMES) {
            builder.withCacheConfiguration(name, conf);
        }
        return builder.build();
    }

    private CacheManager caffeineCacheManager() {
        CaffeineCacheManager manager = new CaffeineCacheManager(CACHE_NAMES);
        manager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(5))
                .maximumSize(1024));
        manager.setCacheNames(java.util.Arrays.asList(CACHE_NAMES));
        return manager;
    }

    @Override
    public CacheErrorHandler errorHandler() {
        return new SimpleCacheErrorHandler() {
            @Override
            public void handleCacheGetError(RuntimeException e, Cache cache, Object key) {
                log.warn("缓存读取失败(当作未命中): cache={}, key={}, err={}", cache.getName(), key, e.getMessage());
            }

            @Override
            public void handleCachePutError(RuntimeException e, Cache cache, Object key, Object value) {
                log.warn("缓存写入失败: cache={}, key={}, err={}", cache.getName(), key, e.getMessage());
            }
        };
    }
}
