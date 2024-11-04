package com.example.batch_job_demo.repo;

import com.example.batch_job_demo.entity.PersonDetails;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;


public interface PersonsRepo extends JpaRepository<PersonDetails, Integer> {
}
