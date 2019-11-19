package com.bnpparibas.training.batch.springbatchdemo;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;

@SpringBootConfiguration
@EnableBatchProcessing /* Modular=true */
public class BatchTestConfiguration {
    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() { //instance pour lancer les test specifiquement
        return new JobLauncherTestUtils();
    }
    @Bean
    public JdbcTemplate jdbcTemplate(final DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }
    @Bean
    public DataSource dataSource() {
        return new EmbeddedDatabaseBuilder() //
                .setType(EmbeddedDatabaseType.H2) // EmbeddedDatabaseType.H2
//                .setType(EmbeddedDatabaseType.HSQL) // EmbeddedDatabaseType.HSQL
                .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
//                .addScript("db/sql/create-db.sql")
//                .addScript("db/sql/insert-data.sql")
                .addScripts("schema-all.sql") //
                .build();
    }
}
