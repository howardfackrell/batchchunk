package com.hlf.batchchunk;

import javax.sql.DataSource;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.cloud.sleuth.instrument.async.LazyTraceAsyncTaskExecutor;
import org.springframework.cloud.sleuth.instrument.async.LazyTraceThreadPoolTaskExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.security.task.DelegatingSecurityContextAsyncTaskExecutor;
import org.springframework.security.task.DelegatingSecurityContextTaskExecutor;

import java.util.Collection;

@SpringBootApplication
@EnableBatchProcessing
public class BatchchunkApplication {

  public static void main(String[] args) {
    SpringApplication.run(BatchchunkApplication.class, args);
  }

  @Bean
  public NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
    return new NamedParameterJdbcTemplate(dataSource);
  }

  @Bean
  public TaskExecutor taskExecutor(BeanFactory beanFactory) {
    var baseExecutor =
        new TaskExecutorBuilder()
            .corePoolSize(4)
            .threadNamePrefix("batWorker-")
            .awaitTermination(true)
            .build();
    baseExecutor.initialize();
//        return baseExecutor;

//    return new DelegatingSecurityContextAsyncTaskExecutor(baseExecutor);
        return new LazyTraceThreadPoolTaskExecutor(beanFactory, baseExecutor);
  }
}
