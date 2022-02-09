package com.hlf.batchchunk;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableBatchProcessing
public class BatchchunkApplication {

  protected static <I, O> Step buildStep(
          StepBuilderFactory factory,
          String name,
          ItemReader<I> reader,
          ItemProcessor<I, O> processor,
          ItemWriter<O> writer) {
    return factory
            .get(name)
            .<I, O>chunk(5)

            .faultTolerant()
            .skipLimit(50)
            .skip(DontLikeItException.class)
//            .readerIsTransactionalQueue()
            .reader(reader)
            .processor(processor)
            .writer(writer)
            .build();
  }


  public static void main(String[] args) {
    SpringApplication.run(BatchchunkApplication.class, args);
  }

  @Bean
  public NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
    return new NamedParameterJdbcTemplate(dataSource);
  }

  @Bean
  public Job databaseReaderJob(JobBuilderFactory jobBuilderFactory, Step dbStep) {
    return jobBuilderFactory
        .get("databaseReaderJob")
        .start(dbStep)
        .build();
  }

  @Bean
  public Job fileReaderJob(JobBuilderFactory jobBuilderFactory, Step fileStep) {
    return jobBuilderFactory
        .get("fileReaderJob")
        .start(fileStep)
        .build();
  }

  @Bean
  public ItemReader<Integer> databaseReader(NamedParameterJdbcTemplate jdbcTemplate) {
    String sql = "SELECT work_item_id FROM work_item ORDER BY work_item_id";
    List<Integer> work =
        jdbcTemplate.query(
            sql,
            new RowMapper<Integer>() {
              @Override
              public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getInt("WORK_ITEM_ID");
              }
            });

    return new ListItemReader(work);
  }

  @Bean
  public ItemReader<Integer> fileReader() throws IOException {
    var work =
        Files.readAllLines(Paths.get(".", "work_ids.txt")).stream()
            .map(Integer::parseInt)
            .collect(Collectors.toList());

    return new ListItemReader(work);
  }

  @Bean
  public ItemProcessor<Integer, Integer> itemProcessor() {
    return new ItemProcessor<Integer, Integer>() {
      @Override
      public Integer process(Integer integer) throws Exception {
        if (integer % 4 == 0) {
          throw new DontLikeItException(integer);
        }
        System.out.println(integer + " currently processing");
        return integer;
      }
    };
  }

  @Bean
  public ItemWriter<Integer> itemWriter() {
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
  public Step dbStep(
      StepBuilderFactory stepBuilderFactory,
      ItemReader<Integer> databaseReader,
      ItemProcessor<Integer, Integer> itemProcessor,
      ItemWriter<Integer> itemWriter) {
    return buildStep(
            stepBuilderFactory,
        "dbStep",
        databaseReader,
        itemProcessor,
        itemWriter);
  }

  @Bean
  public Step fileStep(
      StepBuilderFactory stepBuilderFactory,
      ItemReader<Integer> fileReader,
      ItemProcessor<Integer, Integer> itemProcessor,
      ItemWriter<Integer> itemWriter) {
    return buildStep(
            stepBuilderFactory,
            "fileStep",
            fileReader,
            itemProcessor,
            itemWriter);
  }

}
