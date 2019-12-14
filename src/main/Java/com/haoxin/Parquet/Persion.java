package com.haoxin.Parquet;

/**
 * @ClassName: Persion
 * @Author: 郝鑫
 * @Data: 2019/12/14/18:44
 * @Descripition:
 */
public class Persion {
    public String name;
    public int age;
    public String ID;

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

    @Override
    public String toString() {
        return "Persion{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", ID='" + ID + '\'' +
                '}';
    }
}
