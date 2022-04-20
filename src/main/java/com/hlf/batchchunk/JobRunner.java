package com.hlf.batchchunk;

import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobRunner implements CommandLineRunner {

  private final JobLauncher jobLauncher;
  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private final LoggingService loggingService;

  // readers needing spring wiring, there are more static ItemReader factory methods in the Readers
  // class
  private final ItemReader<Integer> databaseListReader;
  private final ItemReader<Integer> syncDatabaseListReader;
  private final ItemReader<Integer> databaseCursorReader;

  // processors needing spring wiring, there are more static ItemProcessor factory methods in the
  // Processors class
  private final ItemProcessor<Integer, Integer> dbLoggingProcessor;
  private final ItemProcessor<Integer, Integer> dbLoggingNewTransactionProcessor;
  private final ItemProcessor<Integer, Integer> markProcessedItemProcessor;

  private final ItemWriter<Integer> dbItemWriter;

  private final TaskExecutor taskExecutor;

  public void run(String... args) throws Exception {
    SecurityContextHolder.setContext(
            new SecurityContextImpl(new UsernamePasswordAuthenticationToken("scott", "tiger")));

    // clean up after any previous run - do it at the beginning not the end so you can query the db
    // at the end
    loggingService.deleteAll();
    loggingService.clearWorkItemsProcessed();

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
                    Processors.failEveryNthProcessor(4),
                    dbLoggingNewTransactionProcessor,
                    markProcessedItemProcessor,
                    Processors.consoleLoggingProcessor()))
            .writer(Writers.consoleItemWriter())
            .taskExecutor(taskExecutor)
            .build();
    var job = jobBuilderFactory.get("integerProcessingJob").start(step).build();

    var jobParameters =
        new JobParameters(Map.of("runAt", new JobParameter(Instant.now().toString(), true)));

    var start = Instant.now();
    jobLauncher.run(job, jobParameters);
    log.info(Instant.now().toEpochMilli() - start.toEpochMilli() + " millis to the the whole job");
  }
}
