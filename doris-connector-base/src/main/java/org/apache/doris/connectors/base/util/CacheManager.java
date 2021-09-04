package org.apache.doris.connectors.base.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.doris.connectors.base.cfg.DorisConfiguration;
import org.apache.doris.connectors.base.rest.RestService;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CacheManager {

    public static LoadingCache<String, List<String>> buildBackendCache(DorisConfiguration dorisConfig,
                                                                       long duration,
                                                                       TimeUnit timeUnit) {
        return Caffeine
                .newBuilder()
                .maximumSize(10)
                .expireAfterWrite(duration, timeUnit)
                .build(key -> RestService
                        .getBackends(dorisConfig)
                        .stream()
                        .filter(f -> f.getCluster().equalsIgnoreCase(key))
                        .map(m -> m.getIp() + ":" + m.getHttpPort())
                        .collect(Collectors.toList())
                );
    }
}
