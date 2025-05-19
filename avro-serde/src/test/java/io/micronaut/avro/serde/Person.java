package io.micronaut.avro.serde;

import io.micronaut.avro.AvroSchemaSource;

@AvroSchemaSource("classpath:Person-schema.avsc")
public class Person {
    private String name;
    private int age;

    // Required: No-arg constructor
    public Person() {}

    // Optional: Constructor with fields (helpful for testing)
    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    // Getters and Setters (required for ReflectData to work)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    // Optional: toString() for better print output
    @Override
    public String toString() {
        return "Salamander{name='" + name + "', age=" + age + "}";
    }
}
