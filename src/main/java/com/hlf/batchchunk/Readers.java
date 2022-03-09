package com.hlf.batchchunk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
public class Readers {

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
}
