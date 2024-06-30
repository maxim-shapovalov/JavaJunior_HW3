package example;

import example.Department;

public class Person {
    private  long id;
    private Department department;
    private String name;
    private int age;
    private boolean active;

    public Person(long id, Department department, String name, int age, boolean active) {
        this.id = id;
        this.department = department;
        this.name = name;
        this.age = age;
        this.active = active;
    }

    public long getId() {
        return id;
    }

    public Department getDepartment() {
        return department;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public boolean isActive() {
        return active;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setDepartment(Department department) {
        this.department = department;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}
