package com.bnpparibas.training.batch.springbatchdemo.config;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport{
    private static final Logger LOGGER = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    //@Override
    public void afterjob(JobExecution jobExecution){
        LOGGER.info("Status" + jobExecution.getStatus());
    }
}
