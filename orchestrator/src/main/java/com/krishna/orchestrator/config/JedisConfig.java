package com.krishna.orchestrator.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

@Configuration
public class JedisConfig {

    @Value("${upstash.redis.host}")
    private String host;

    @Value("${upstash.redis.port}")
    private int port;

    @Value("${upstash.redis.password}")
    private String password;

    @Bean
    public JedisPooled jedisPooled() {
        DefaultJedisClientConfig config = DefaultJedisClientConfig.builder()
                .user("default")
                .password(password)
                .ssl(true)
                .build();

        return new JedisPooled(new HostAndPort(host, port), config);
    }
}