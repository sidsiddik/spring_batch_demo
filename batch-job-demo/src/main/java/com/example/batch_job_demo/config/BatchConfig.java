package com.example.batch_job_demo.config;

import com.example.batch_job_demo.entity.PersonDetails;
import com.example.batch_job_demo.repo.PersonsRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final PersonsRepo repo;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    @StepScope
    public FlatFileItemReader<PersonDetails> itemReader(@Value("#{jobParameters['input.file.name']}") String resource) {
        FlatFileItemReader<PersonDetails> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(resource));
        itemReader.setName("csv-Reader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    @Bean
    public LineMapper<PersonDetails> lineMapper() {
        DefaultLineMapper<PersonDetails> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "age");

        BeanWrapperFieldSetMapper<PersonDetails> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(PersonDetails.class);

        lineMapper.setFieldSetMapper(mapper);
        lineMapper.setLineTokenizer(lineTokenizer);
        return lineMapper;
    }

    @Bean
    public PersonDetProcessor processor() {
        return new PersonDetProcessor();
    }

    @Bean
    public RepositoryItemWriter<PersonDetails> itemWriter() {
        RepositoryItemWriter<PersonDetails> itemWriter = new RepositoryItemWriter<>();
        itemWriter.setRepository(repo);
        itemWriter.setMethodName("save");
        return itemWriter;
    }

    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }

    @Bean
    public Step step(ItemReader<PersonDetails> itemReader) {
        return new StepBuilder("csv-step", jobRepository)
                .<PersonDetails, PersonDetails>chunk(10, platformTransactionManager)
                .reader(itemReader)
                .processor(processor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job job(Step step) {
        return new JobBuilder("person-info", jobRepository)
                .start(step)
                .build();
    }
}
