package com.example.batch_job_demo.config;

import com.example.batch_job_demo.entity.PersonDetails;
import org.springframework.batch.item.ItemProcessor;

public class PersonDetProcessor implements ItemProcessor<PersonDetails, PersonDetails> {

    @Override
    public PersonDetails process(PersonDetails item){
        return item;
    }
}
