package com.haoxin.Parquet;

/**
 * @ClassName: Persion
 * @Author: 郝鑫
 * @Data: 2019/12/14/18:44
 * @Descripition:
 */
public class Persion {
    private String name;
    private int age;
    private String ID;

    public Persion(String name, int age, String ID) {
        this.name = name;
        this.age = age;
        this.ID = ID;
    }

    public Persion(){}

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

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }
}
