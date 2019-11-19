package com.bnpparibas.training.batch.springbatchdemo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.test.jdbc.JdbcTestUtils.countRowsInTable;
import static org.springframework.test.jdbc.JdbcTestUtils.deleteFromTables;

import com.bnpparibas.training.batch.springbatchdemo.config.ImportJobConfig;
import com.bnpparibas.training.batch.springbatchdemo.config.JobCompletionNotificationListener;
import com.bnpparibas.training.batch.springbatchdemo.config.MaClassMetier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;



@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = { /* BatchApplication.class, */ BatchTestConfiguration.class,
		ImportJobConfig.class, JobCompletionNotificationListener.class, MaClassMetier.class})
public class ImportStepConfigTest {

	@Autowired
	private JobLauncherTestUtils testUtils;

	// @Autowired
	// private JobRepositoryTestUtils jobRepositoryTestUtils;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	// @Before
	// public void clearJobExecutions() {
	// jobRepositoryTestUtils.removeJobExecutions();
	// }

	@Before
	public void deleteBooks() {
		deleteFromTables(jdbcTemplate, "book");
	}

	@Test
	public void importJob() throws Exception {
		// Given
		final JobParameters jobParameters = new JobParametersBuilder(testUtils.getUniqueJobParameters()) //
				.addString("input-file", "src/main/resources/sample-data.csv") //
				.toJobParameters();
		// When
		final JobExecution jobExec = testUtils.launchJob(jobParameters);
		// Then
		assertThat(jobExec.getStatus(), equalTo(BatchStatus.COMPLETED));
	}

	@Test
	public void importStepWithFileShouldSuccess() {
		// Given
		final JobParameters jobParameters = new JobParametersBuilder(testUtils.getUniqueJobParameters()) //
				.addString("input-file", "src/main/resources/sample-data.csv") //
				.toJobParameters();
		// When
		final JobExecution jobExec = testUtils.launchStep("import-step", jobParameters);
		// Then
		assertThat(jobExec.getStatus(), equalTo(BatchStatus.COMPLETED));
		assertThat(countRowsInTable(jdbcTemplate, "book"), equalTo(19));
	}

	@Test
	public void importStepWithBadFileShouldFail() {
		// Given
		final JobParameters jobParameters = new JobParametersBuilder(testUtils.getUniqueJobParameters()) //
				.addString("input-file", "src/main/resources/bad-data.csv") //
				.toJobParameters();
		// When
		final JobExecution jobExec = testUtils.launchStep("import-step", jobParameters);
		// Then
		assertThat(jobExec.getStatus(), equalTo(BatchStatus.FAILED));
		assertThat(countRowsInTable(jdbcTemplate, "book"), equalTo(0));
	}

}

