package com.hlf.batchchunk;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.context.SecurityContextHolder;

@Configuration
@Slf4j
public class Processors {

  public static ItemProcessor<Integer, Integer> compose(
      ItemProcessor<Integer, Integer>... processors) {
    return new CompositeItemProcessorBuilder().delegates(processors).build();
  }

  public static ItemProcessor<Integer, Integer> failEveryNthProcessor(int n) {
    return new ItemProcessor<Integer, Integer>() {
      @Override
      public Integer process(Integer integer) throws Exception {
        if (integer % n == 0) {
          throw new DontLikeItException(integer);
        }
        return integer;
      }
    };
  }

  public static ItemProcessor<Integer, Integer> consoleLoggingProcessor() {
    return new ItemProcessor<Integer, Integer>() {
      @Override
      public Integer process(Integer integer) throws Exception {
//        var principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        log.info(integer + " currently processing ");
        return integer;
      }
    };
  }

  @Bean
  public ItemProcessor<Integer, Integer> dbLoggingProcessor(LoggingService loggingService) {
    return new ItemProcessor<Integer, Integer>() {
      @Override
      public Integer process(Integer integer) throws Exception {
        loggingService.log(integer);
        return integer;
      }
    };
  }

  @Bean
  public ItemProcessor<Integer, Integer> dbLoggingNewTransactionProcessor(
      LoggingService loggingService) {
    return new ItemProcessor<Integer, Integer>() {
      @Override
      public Integer process(Integer integer) throws Exception {
        loggingService.logInNewTransaction(integer);
        return integer;
      }
    };
  }

  @Bean
  public ItemProcessor<Integer, Integer> markProcessedItemProcessor(LoggingService loggingService) {
    return new ItemProcessor<Integer, Integer>() {
      @Override
      public Integer process(Integer integer) throws Exception {
        loggingService.markWorkItemProcessed(integer);
        return integer;
      }
    };
  }
}
