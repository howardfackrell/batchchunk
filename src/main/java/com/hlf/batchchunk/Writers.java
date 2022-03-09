package com.hlf.batchchunk;

import java.util.List;
import java.util.stream.Collectors;
import org.springframework.batch.item.ItemWriter;

public class Writers {

  public static ItemWriter<Integer> itemWriter() {
    return new ItemWriter<Integer>() {
      @Override
      public void write(List<? extends Integer> list) throws Exception {
        var str = list.stream().map(i -> i + "").collect(Collectors.joining(",", "[", "]"));
        System.out.println("Now writing w a list of " + list.size());
        System.out.println(str);
      }
    };
  }
}
