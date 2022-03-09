package com.hlf.batchchunk;

import java.util.List;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Processors {

  public static ItemProcessor<Integer, Integer> compose(
      ItemProcessor<Integer, Integer>... processors) {
    var compositeProcessor = new CompositeItemProcessor<Integer, Integer>();
    compositeProcessor.setDelegates(List.of(processors));
    return compositeProcessor;
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
        System.out.println(integer + " currently processing");
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
}
