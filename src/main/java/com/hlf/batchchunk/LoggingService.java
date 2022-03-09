package com.hlf.batchchunk;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class LoggingService {

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  @Transactional
  public void log(Integer integer) {
    var params =
        Map.<String, Object>of("id", integer, "message", integer + " currently processing");
    namedParameterJdbcTemplate.update(
        "insert into work_log(work_item_id, work_log_message) values (:id, :message)", params);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void logInNewTransaction(Integer integer) {
    log(integer);
  }
}
