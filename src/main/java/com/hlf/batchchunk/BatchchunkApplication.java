package com.hlf.batchchunk;

import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

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
    return jobBuilderFactory.get("databaseReaderJob").start(dbStep).build();
  }

  @Bean
  public Job fileReaderJob(JobBuilderFactory jobBuilderFactory, Step fileStep) {
    return jobBuilderFactory.get("fileReaderJob").start(fileStep).build();
  }

  @Bean
  public Step dbStep(
      StepBuilderFactory stepBuilderFactory,
      ItemReader<Integer> databaseReader,
      ItemProcessor<Integer, Integer> itemProcessor,
      ItemWriter<Integer> itemWriter) {
    return buildStep(stepBuilderFactory, "dbStep", databaseReader, itemProcessor, itemWriter);
  }

  @Bean
  public Step fileStep(
      StepBuilderFactory stepBuilderFactory,
      ItemReader<Integer> fileReader,
      ItemProcessor<Integer, Integer> itemProcessor,
      ItemWriter<Integer> itemWriter) {
    return buildStep(stepBuilderFactory, "fileStep", fileReader, itemProcessor, itemWriter);
  }
}
