package com.hlf.batchchunk;

import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JobRunner implements CommandLineRunner {

  private final JobLauncher jobLauncher;
  private final Job databaseReaderJob;
  private final Job fileReaderJob;

  public void run(String... args) throws Exception {
    var jobParameters =
        new JobParameters(Map.of("runAt", new JobParameter(Instant.now().toString(), true)));
    jobLauncher.run(databaseReaderJob, jobParameters);
    //    jobLauncher.run(fileReaderJob, jobParameters);
  }
}
