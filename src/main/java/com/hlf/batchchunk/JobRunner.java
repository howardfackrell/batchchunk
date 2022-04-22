package com.hlf.batchchunk;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobRunner implements CommandLineRunner {

  private final JobLauncher jobLauncher;
  private final LoggingService loggingService;
  private final Job integerProcessingJob;

  public void run(String... args) throws Exception {
    SecurityContextHolder.setContext(
        new SecurityContextImpl(new UsernamePasswordAuthenticationToken("scott", "tiger")));

    // clean up after any previous run - do it at the beginning not the end so you can query the db
    // at the end
    loggingService.deleteAll();
    loggingService.clearWorkItemsProcessed();

    var jobParameters =
        new JobParameters(Map.of("runAt", new JobParameter(Instant.now().toString(), true)));

    var start = Instant.now();
    jobLauncher.run(integerProcessingJob, jobParameters);
    log.info(Instant.now().toEpochMilli() - start.toEpochMilli() + " millis to the the whole job");
  }
}
