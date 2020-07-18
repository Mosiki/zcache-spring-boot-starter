
# ZCache

一款基于 caffeine 实现的简单分布式缓存。

ZCache 缓存默认采取懒加载的模式，数据变更时直接失效缓存。

ZCache 基于 Redis 的 PubSub 特性，实现集群中其他服务本地缓存失效通知，
从而保证所有服务本地缓存的一致性。

## 使用方法
1. 下载本项目打开后，install 到本地或者 deploy 到 maven 私服
2. 需要使用 ZCache 的项目引入如下依赖，项目需同时依赖 spring-data-redis 
    ```xml
    <dependency>
       <groupId>pw.nullpointer</groupId>
      <artifactId>zcache-spring-boot-starter</artifactId>
      <version>0.0.1</version>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-redis</artifactId>
    </dependency>
    ```
3. 配置文件开启 ZCache
    ```yaml
    zcache:
      enabled: true
    ```
4. 新增 AbstractCache 的实现类，覆写相应方法。
    ```java
    @Component
    public class UserCache extends AbstractCache<Long, User> {
        private Cache<Long, User> cache = Caffeine.newBuilder()
                .initialCapacity(1)
                .maximumSize(500)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
    
        @Resource
        private UserMapper userMapper;
    
        @Nonnull
        @Override
        protected Cache<Long, User> getCache() {
           // 自定义 Cache 实例
            return cache;
        }
    
        @Nonnull
        @Override
        protected Function<? super Long, ? extends User> getLoadFunction() {
            // 自定义 Cache Loader
            return key -> {
                log.info("load cache from db. key: {}", key);
                return userMapper.findById(key);
            };
        }
    }
    ```
5. 使用
    ```java
   @Resource
   private UserCache userCache;
   
   // cache中不存该key时，使用getLoadFunction中的方法加载数据       
   User user = userCache.get(userId);

   // 通过Redis的发布订阅发布通知，接收到通知的服务会自动淘汰本地缓存   
   userCache.invalidate(userId); 
   ```
## example
[https://github.com/Mosiki/ZCache-Example](https://github.com/Mosiki/ZCache-Example)
## 联系方式
QQ群号:967808880  
邮箱：vcmq@foxmail.com