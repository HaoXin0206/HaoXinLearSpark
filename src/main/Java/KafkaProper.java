import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

/**
 * @ClassName: KafkaProper
 * @Author: 郝鑫
 * @Data: 2019/10/2/10:34
 * @Descripition:
 */
public class KafkaProper {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.65.130:9092,192.168.65.130:9093,192.168.65.130:9094");
        properties.put("request.required.acks","0");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 1; i < 100; i++) {
            Random random = new Random();
            int tt = random.nextInt(10) + 2;
            String day = LocalDate.now().toString();
            String hour = LocalTime.now().toString().substring(0,8);
            String time=day+" "+hour;

            String value=
                    time+"\t"+GetName()+"\t"+GetSex()+"\t"+GetYear()+"\t"+GetCity()+"\t"+GetMoney()+"\t"+GetBirthday();

            kafkaProducer.send(new ProducerRecord<String, String>("test",time,
                    value));

            System.out.println(value);
            Thread.sleep(tt*1000);

        }
        kafkaProducer.close();
    }


    private static String GetName(){

        String all= "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜戚谢邹喻柏水窦章云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任袁柳酆鲍史唐" +
                "费廉岑薛雷贺倪汤滕殷罗毕郝邬安常乐于时傅皮卞齐康伍余元卜顾孟平黄和穆萧尹姚邵湛汪祁毛禹狄米贝明臧计伏成戴谈宋茅庞熊纪舒屈项祝董" +
                "梁杜阮蓝闵席季麻强贾路娄危江童颜郭梅盛林刁钟徐邱骆高夏蔡田樊胡凌霍虞万支柯昝管卢莫经房裘缪干解应宗丁宣贲邓郁单杭洪包诸左石崔吉" +
                "钮龚程嵇邢滑裴陆荣翁荀羊於惠甄曲家封芮羿储靳汲邴糜松井段富巫乌焦巴弓牧隗山谷车侯宓蓬全郗班仰秋仲伊宫宁仇栾暴甘钭厉戎祖武符刘景" +
                "詹束龙叶幸司韶郜黎蓟薄印宿白怀蒲邰从鄂索咸籍赖卓蔺屠蒙池乔阴鬱胥能苍双闻莘党翟谭贡劳逄姬申扶堵冉宰郦雍卻璩桑桂濮牛寿通边扈燕冀" +
                "郏浦尚农温别庄晏柴瞿阎充慕连茹习宦艾鱼容向古易慎戈廖庾终暨居衡步都耿满弘匡国文寇广禄阙东欧殳沃利蔚越夔隆师巩厍聂晁勾敖融冷訾辛阚" +
                "那简饶空曾毋沙乜养鞠须丰巢关蒯相查后荆红游竺权逯盖益桓公万俟司马上官欧阳夏侯诸葛闻人东方赫连皇甫尉迟公羊澹台公冶宗政濮阳淳于单于" +
                "太叔申屠公孙仲孙轩辕令狐钟离宇文长孙慕容鲜于闾丘司徒司空丌官司寇仉督子车颛孙端木巫马公西漆雕乐正壤驷公良拓跋夹谷宰父谷梁晋楚闫法" +
                "汝鄢涂钦段干百里东郭南门呼延归海羊舌微生岳帅缑亢况郈有琴梁丘左丘东门西门商牟佘佴伯赏南宫墨哈谯笪年爱阳佟第五言福百家姓终";


        String tName = "风花雪月山树楼台一奕娜大小淑书静香陌莫鑫沫眸忆安简素未离央墨如凉荼曦兮顾初旧落逆倾颜微锦诺鑫龙佳伟秀周迪峰";

        Random random = new Random();
        int i = random.nextInt(2)+1;
        String name = RandomStringUtils.random(1, all) + RandomStringUtils.random(i, tName);

        return name;
    }

    private static String GetSex(){
        Random random = new Random();
        int i = random.nextInt(100);
        if (i%2==0) return "男" ; else  return "女";
    }

    private static int GetYear(){
        Random random = new Random();
        int i = random.nextInt(50)+15;
        return i;
    }

    private static String GetCity(){
        String cityname="石家庄、唐山、邯郸、秦皇岛、保定、张家口、承德、廊坊、沧州、衡水、邢台、" +
                "辛集市、藁城市、晋州市、新乐市、鹿泉市、遵化市、迁安市、武安市、南宫市、沙河市、涿州市、定州市、安国市、泊头市、任丘市、黄骅市、河间市、霸州市、三河市、冀州市、深州市、" +
                "杭州、嘉兴、湖州、宁波、金华、温州、丽水、绍兴、衢州、舟山、台州、" +
                "建德市、富阳市、临安市、余姚市、慈溪市、奉化市、瑞安市、乐清市、海宁市、平湖市、桐乡市、诸暨市、上虞市、嵊州市、兰溪市、义乌市、东阳市、永康市、江山市、临海市、温岭市、龙泉市、" +
                "太原、大同、忻州、阳泉、长治、晋城、朔州、晋中、运城、临汾、吕梁、" +
                "古交、潞城、高平、介休、永济、河津、原平、侯马、霍州、孝义、汾阳";

        String[] name = cityname.split("、");
        Random random = new Random();
        int i = random.nextInt(name.length - 1);

        return name[i];
    }

    private static int GetMoney(){
        Random random = new Random();
        int i = random.nextInt(18000) + 2000;
        return i;
    }

    private static String GetBirthday(){

        Random random = new Random();
        DecimalFormat decimalFormat = new DecimalFormat("00");
        int year = random.nextInt(65) + 1954;
        String month =decimalFormat.format( random.nextInt(11) + 1);
        String day = decimalFormat.format(random.nextInt(30 + 1));
        String hour = decimalFormat.format(random.nextInt(23));
        String minute = decimalFormat.format(random.nextInt(59));
        String sess =decimalFormat.format( random.nextInt(59));


        return ""+year+"-"+month+"-"+day+" "+hour+":"+minute+":"+sess;
    }
}
