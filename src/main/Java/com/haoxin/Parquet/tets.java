package com.haoxin.Parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * @ClassName: tets
 * @Author: 郝鑫
 * @Data: 2019/12/14/18:45
 * @Descripition:
 */
public class tets {
    public static void main(String[] args) {
        try {
            String name=System.currentTimeMillis()+".parquet";
            String outPutPath="D:\\data\\sparktest\\"+name;

            SaveData2Parquet saveData2Parquet = new SaveData2Parquet("com.haoxin.Parquet.Persion", outPutPath);


            for (int i = 0; i < 100; i++) {
                Persion persion = new Persion("郝鑫",i,i+"");
                saveData2Parquet.write(persion);
            }

            saveData2Parquet.Close();

            File file = new File(outPutPath.replace(name,"")+"."+name+".crc");
            if (file.exists()) {
                boolean delete = file.delete();

            }

            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(outPutPath));

            ParquetReader<Group> build = reader.build();
            Group line=null;
            while ((line=build.read())!=null){
                System.out.println("name:"+line.getString("name",0)+"\tage:"+line.getInteger("age",0)+"\tid:"+line.getString(
                        "ID",0));
            }
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
