package com.hlf.batchchunk;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
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
  public void deleteAll() {
    namedParameterJdbcTemplate.update("truncate work_log", Map.of());
  }

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

  @Transactional
  public void markWorkItemProcessed(Integer id) {
    var sql = "update work_item set work_item_processed = :processed where work_item_id = :id";
    var params = new HashMap<String, Object>();
    params.put("processed", LocalDateTime.now());
    params.put("id", id);
    namedParameterJdbcTemplate.update(sql, params);
  }

  @Transactional
  public void clearWorkItemsProcessed() {
    var sql = "update work_item set work_item_processed = null";
    namedParameterJdbcTemplate.update(sql, Map.of());
  }
}
