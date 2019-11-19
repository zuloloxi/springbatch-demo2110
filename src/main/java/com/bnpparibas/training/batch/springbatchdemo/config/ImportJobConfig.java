package com.bnpparibas.training.batch.springbatchdemo.config;

import com.bnpparibas.training.batch.springbatchdemo.dto.BookDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class ImportJobConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportJobConfig.class);
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MaClassMetier maClassMetier;

    @Bean(name = "importJob")
    public Job importBookJob(final JobCompletionNotificationListener listener, final Step importStep){
        return jobBuilderFactory.get("import-Job")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
               .flow(importStep)
//                .step(importStep)
                .end()
                .build();
    }

    @Bean
    public Step importStep(final FlatFileItemReader<BookDto> importReader,
                            final ItemProcessor<BookDto,BookDto> importProcessor,
                            final ItemWriter<BookDto> impotWriter){
        return stepBuilderFactory.get("import-step")
                .<BookDto,BookDto>chunk(10)
                .reader(importReader)
                .processor(importProcessor)
                .writer(impotWriter)
                .build()
                ;
    }
    @StepScope //Mnadatory for using jobParameters
    @Bean
    public FlatFileItemReader<BookDto> importReader( @Value("#{jobParameters['input-file']}") final String inputFile){
        return new FlatFileItemReaderBuilder<BookDto>()
               .name("bookItemReader")
               .resource(new FileSystemResource(inputFile))
                .delimited()
                .delimiter(";")
                .names(new String[]{ "title","author","isbn","publisher","publishedOn"}).linesToSkip(1)
                .fieldSetMapper(new BeanWrapperFieldSetMapper<BookDto>() {
                    {
                    setTargetType(BookDto .class);
                    }
                }).build();
    }
    @Bean
    ItemProcessor<BookDto,BookDto> importProcessor(){
        return new ItemProcessor<BookDto, BookDto>() {
            @Override
            public BookDto process(final BookDto book) throws Exception {
                LOGGER.info("BookDto {}",book );
                return maClassMetier.maMethodMetier(book);
                //return item;
            }
        };
    }
    @Bean
    public JdbcBatchItemWriter<BookDto> importwriter(final DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<BookDto>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("insert into book(title,author,isbn,publisher,publishedOn) "+
                        "values (:title,:author,:isbn,:publisher,:publishedOn)")
                        .dataSource(dataSource)
                .build();

    }

}
