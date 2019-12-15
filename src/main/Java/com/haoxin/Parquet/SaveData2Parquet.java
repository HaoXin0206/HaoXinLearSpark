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
import scala.reflect.internal.Trees;


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

    /**
     *
     * @params:  * @param: scrme 元数据信息
     * @param: savePath 文件保存地址
     * @return
     * @Describe: 初始化
     * @Date： 2019/12/14   23:28
     */
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


    /**
     *
     * @params:  * @param: object 待写入对象
     * @return void
     * @Describe: 写入文件
     * @Date： 2019/12/14   23:29
     */
    public void write(Object object) throws Exception {

        Group group = simpleGroupFactory.newGroup();
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field fieldd : fields) {
            fieldd.setAccessible(true);
            String name = fieldd.getName();
            switch (fieldd.getType().getTypeName().toLowerCase()){
                case "double":
                case "java.lang.double":
                    group.append(name,fieldd.getDouble(object));
                    break;
                case "int":
                case "java.lang.integer":
                    group.append(name,fieldd.getInt(object));
                    break;
                case "long":
                case "java.lang.long":
                    group.append(name,fieldd.getLong(object));
                    break;
                case "float":
                case "java.lang.float":
                    group.append(name,fieldd.getFloat(object));
                    break;
                case "boolean":
                case "java.lang.boolean":
                    group.append(name,fieldd.getBoolean(object));
                    break;
                default:
                    group.append(name,fieldd.get(object).toString());
                    break;

            }


        }
        build.write(group);
    }

    /**
     *
     * @params:  * @param: scrme 元数据信息
     * @return java.lang.String
     * @Describe:
     * @Date： 2019/12/14   23:29
     */
    private String GetMessageString(String scrme) throws ClassNotFoundException {


        Class<?> aClass = Class.forName(scrme);
        Field[] fields = aClass.getDeclaredFields();
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
