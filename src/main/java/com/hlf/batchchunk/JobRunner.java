package com.hlf.batchchunk;

import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JobRunner implements CommandLineRunner {

  private final JobLauncher jobLauncher;
  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private final LoggingService loggingService;

  // readers needing spring wiring, there are more static ItemReader factory methods in the Readers
  // class
  private final ItemReader<Integer> databaseListReader;
  private final ItemReader<Integer> databaseCursorReader;

  // processors needing spring wiring, there are more static ItemProcessor factory methods in the
  // Processors class
  private final ItemProcessor<Integer, Integer> dbLoggingProcessor;
  private final ItemProcessor<Integer, Integer> dbLoggingNewTransactionProcessor;

  private final ItemWriter<Integer> dbItemWriter;

  public void run(String... args) throws Exception {

    // clean up after any previous run - do it at the beginning not the end so you can query the db
    // at the end
    loggingService.deleteAll();

    var step =
        stepBuilderFactory
            .get("integerProcessingStep")
            .<Integer, Integer>chunk(5)
            .faultTolerant()
            .skipLimit(50)
            .processorNonTransactional()
            .backOffPolicy(new FixedBackOffPolicy())
            .skip(DontLikeItException.class)
//            .readerIsTransactionalQueue()
            .reader(databaseCursorReader)
            .processor(
                Processors.compose(
                    Processors.failEveryNthProcessor(4),
                    dbLoggingNewTransactionProcessor,
                    Processors.consoleLoggingProcessor()))
            .writer(Writers.consoleItemWriter())
            .build();
    var job = jobBuilderFactory.get("integerProcessingJob").start(step).build();

    var jobParameters =
        new JobParameters(Map.of("runAt", new JobParameter(Instant.now().toString(), true)));

    jobLauncher.run(job, jobParameters);
  }
}
