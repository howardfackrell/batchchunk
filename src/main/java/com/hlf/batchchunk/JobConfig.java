package com.hlf.batchchunk;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.cloud.sleuth.instrument.async.LazyTraceThreadPoolTaskExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.backoff.FixedBackOffPolicy;

@Configuration
@Slf4j
public class JobConfig {

  @Bean
  public TaskExecutor batchTaskExecutor(BeanFactory beanFactory) {
    var baseExecutor =
        new TaskExecutorBuilder()
            .corePoolSize(4)
            .threadNamePrefix("batWorker-")
            .awaitTermination(true)
            .build();
    baseExecutor.initialize();
    return new JobScopeExecutorDecorator(new LazyTraceThreadPoolTaskExecutor(beanFactory, baseExecutor));
//    return new LazyTraceThreadPoolTaskExecutor(beanFactory, baseExecutor);
  }

  @Bean
  @JobScope
  Step integerProcessingStep(
          StepBuilderFactory stepBuilderFactory,
          ItemReader<Integer> syncDatabaseListReader,
          ItemReader<Integer> databaseListReader,
          ItemReader<Integer> databaseCursorReader,
          ItemProcessor<Integer, Integer> dbLoggingProcessor,
          ItemProcessor<Integer, Integer> dbLoggingNewTransactionProcessor,
          ItemProcessor<Integer, Integer> markProcessedItemProcessor,
          ItemWriter<Integer> dbItemWriter,
          TaskExecutor batchTaskExecutor,
          @Value("#{jobParameters[runAt]}") String runAt,
          PrintMeItemProcessor<Integer> printMeItemProcessor) {
    log.info("Job Step instantiated, runAt: " + runAt);
    var step =
        stepBuilderFactory
            .get("integerProcessingStep")
            .<Integer, Integer>chunk(1)
            .faultTolerant()
            .skipLimit(1000)
            //            .processorNonTransactional()
            .backOffPolicy(new FixedBackOffPolicy())
            .skip(DontLikeItException.class)
            //            .readerIsTransactionalQueue()
            .reader(syncDatabaseListReader)
            .processor(
                Processors.compose(
                        printMeItemProcessor,
                    Processors.failEveryNthProcessor(4),
                    dbLoggingNewTransactionProcessor,
                    markProcessedItemProcessor,
                    Processors.consoleLoggingProcessor()))
            .writer(Writers.consoleItemWriter())
            .taskExecutor(batchTaskExecutor)
            .build();
    return step;
  }

  @Bean
  @JobScope
  PrintMeItemProcessor<Integer> printMeItemProcessor(@Value("#{jobParameters[runAt]}") String runAt) {
    return new PrintMeItemProcessor<>(runAt);
  }

  @Bean
  Job integerProcessingJob(JobBuilderFactory jobBuilderFactory, Step integerProcessingStep) {
    var job = jobBuilderFactory.get("integerProcessingJob").start(integerProcessingStep).build();
    return job;
  }
}
