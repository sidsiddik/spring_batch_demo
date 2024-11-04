package com.example.batch_job_demo.integreation;

import lombok.RequiredArgsConstructor;
import org.aspectj.bridge.MessageHandler;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.DefaultFileNameGenerator;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
@EnableIntegration
@IntegrationComponentScan
@RequiredArgsConstructor
public class BatchIntegrationConfig {

    private final Job job;
    private final JobRepository jobRepository;

    @Value("${batch.int.directory}")
    private String batchIntDirectory;

    public FileReadingMessageSource messageSource(){
        FileReadingMessageSource messageSource =new FileReadingMessageSource();
        messageSource.setDirectory(new File(batchIntDirectory));
        messageSource.setFilter(new SimplePatternFileListFilter("*.csv"));
        return messageSource;
    }

    public DirectChannel channel(){
        DirectChannel channel = new DirectChannel();
        return new DirectChannel();
    }

    public FileWritingMessageHandler handler(){
        FileWritingMessageHandler messageHandler = new FileWritingMessageHandler(new File(batchIntDirectory));
        messageHandler.setFileExistsMode(FileExistsMode.REPLACE);
        messageHandler.setDeleteSourceFiles(Boolean.TRUE);
        messageHandler.setFileNameGenerator(fileNameGenerator());
        messageHandler.setRequiresReply(Boolean.FALSE);
        return messageHandler;
    }

    public DefaultFileNameGenerator fileNameGenerator(){
        DefaultFileNameGenerator generator = new DefaultFileNameGenerator();
        generator.setExpression("payload.name + '.processing'");
        return generator;
    }

    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest() {
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("input.file.name");
        fileMessageToJobRequest.setJob(job);
        return fileMessageToJobRequest;
    }

    @Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SyncTaskExecutor());
        return new JobLaunchingGateway(jobLauncher);
    }

    @Bean
    public IntegrationFlow integrationFlow(JobLaunchingGateway jobLaunchingGateway) {
        return IntegrationFlow.from(messageSource(),
                        c -> c.poller(Pollers.fixedRate(1000).maxMessagesPerPoll(1))).
                transform(fileMessageToJobRequest()).
                handle(jobLaunchingGateway).
                log(LoggingHandler.Level.WARN, "headers.id + ': ' + payload").
                get();
    }

}
