package com.example.batch_job_demo.entity;

import jakarta.persistence.*;

import lombok.Getter;

import lombok.Setter;

import java.util.Date;


@Getter
@Setter
@Entity
@Table(name = "person_details")
public class PersonDetails {

    @Id
    @GeneratedValue
    private Integer id;
    private String firstname;
    private String lastname;
    private int age;
}
