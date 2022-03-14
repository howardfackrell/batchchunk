package com.hlf.batchchunk;

import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class Writers {

  public static ItemWriter<Integer> consoleItemWriter() {
    return new ItemWriter<Integer>() {
      @Override
      public void write(List<? extends Integer> list) throws Exception {
        var str = list.stream().map(i -> i + "").collect(Collectors.joining(",", "[", "]"));
        System.out.println("Now writing w a list of " + list.size());
        System.out.println(str);
      }
    };
  }

  @Bean
  public ItemWriter<Integer> dbItemWriter(LoggingService loggingService) {
    return new ItemWriter<Integer>() {
      @Override
      public void write(List<? extends Integer> list) throws Exception {
        System.out.println("Now writing to the DB a list of " + list.size());
        list.forEach(i -> loggingService.log(i));
      }
    };
  }
}
