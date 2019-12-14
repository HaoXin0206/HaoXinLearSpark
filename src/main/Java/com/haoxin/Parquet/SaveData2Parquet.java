package com.haoxin.Parquet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import scala.annotation.meta.field;


import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;


/**
 * @ClassName: SaveData2Parquet
 * @Author: 郝鑫
 * @Data: 2019/12/14/18:10
 * @Descripition:
 */
public class SaveData2Parquet {


    private HashMap<String, String> typeMap = new HashMap<>();
    private ParquetWriter<Group> build;
    private SimpleGroupFactory simpleGroupFactory;
    private Group group;
    private Field[] fields;
    private Class<?> aClass;

    SaveData2Parquet(String scrme, String savePath) throws IOException, ClassNotFoundException {
        typeMap.put("String", "BINARY");
        typeMap.put("int", "INT32");
        typeMap.put("long", "int64");
        typeMap.put("float", "float");
        typeMap.put("double", "double");
        typeMap.put("boolean", "boolean");


        MessageType messageType = MessageTypeParser.parseMessageType(GetMessageString(scrme));
        simpleGroupFactory = new SimpleGroupFactory(messageType);

        Path path = new Path(savePath);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType, configuration);

        build = ExampleParquetWriter.builder(path)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(configuration)
                .withType(messageType)
                .build();

    }


    public void write(Object object) throws IllegalAccessException, IOException {
        group = simpleGroupFactory.newGroup();
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field fieldd : fields) {
            String name = fieldd.getName();
            if (fieldd.getType().getTypeName().toLowerCase().contains("string")) {
                String s = fieldd.get(object).toString();
                group.append(name,s);
            }
            if (fieldd.getType().getTypeName().toLowerCase().contains("int")) {
                int anInt = fieldd.getInt(object);
                group.append(name,anInt);
            }
            if (fieldd.getType().getTypeName().toLowerCase().contains("double")) {
                Double s = fieldd.getDouble(object);
               group.append(name,s);
            }
            if (fieldd.getType().getTypeName().toLowerCase().contains("float")) {
                Float s = fieldd.getFloat(object);
                group.append(name,s);
            }
            if (fieldd.getType().getTypeName().toLowerCase().contains("long")) {
                Long s = fieldd.getLong(object);
                group.append(name,s);
            }


        }
        build.write(group);
    }

    private String GetMessageString(String scrme) throws ClassNotFoundException {

        aClass = Class.forName(scrme);
        fields = aClass.getDeclaredFields();
        String mess = "message Pair{\n";
        for (Field field : fields) {
            String name = field.getName();
            String typeName = field.getType().getTypeName().replace("java.lang.", "");
            mess = mess + " required " + typeMap.get(typeName) + " " + name + " " + (typeName.toLowerCase().contains("string") ?
                    "(UTF8);\n" : ";\n");
        }

        mess = mess + "}";

        System.out.println(mess);
        return mess;
    }


    public void Close() throws IOException {
        build.close();
    }

}
