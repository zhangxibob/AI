package com.bigdata.spark.producer;
import com.bigdata.spark.util.DBUtil;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class AdvertiseLogProducer {
    public static int p_counter = 0;
    public static int c_counter = 0;
    public static String[] chars = new String[] { "a", "b", "c", "d", "e", "f",
            "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
            "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5",
            "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I",
            "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
            "W", "X", "Y", "Z" };
    public static void main(String[] args) throws SQLException, InterruptedException {
        //查询sql
        String province_sql = "select provinceCode from distinctcode";
        String city_sql = "select cityCode from distinctcode";
        String advertise_sql = "select aid from advertiseinfo";
        //省份List
        List<String> pcList = DBUtil.getProvinceCodeList(province_sql);
        //城市List
        List<String> ccList = DBUtil.getCityCodeList(city_sql);
        //广告List
        List<String> adList = DBUtil.getAdvertiseList(advertise_sql);
        //指定文件输出路径
        String filepath=args[0];
		//循环产生广告日志
        while (true){
            //当前时间,省份编号,城市编号,用户id，广告id
            String message =getCurrentTime()+","+getProvinceCode(pcList)+","+getCityCode(ccList)+","+getUid()+","+getAid(adList);
            System.out.println(message);
            writeFile(filepath,message);
            Thread.sleep(300);
        }
    }
    /**
     * 生成当前时间
     * @return
     */
    public static String getCurrentTime(){
        Date date = new Date();
        long ts = date.getTime();
        return String.valueOf(ts);
    }

    /**
     * 获取省份编码
     * @param list
     * @return
     */
    public static String getProvinceCode(List<String> list){
        p_counter++;
        String[] arr = new String[]{"BJ","SH","GD","ZJ"};
        Random random = new Random();
        int i = random.nextInt(list.size());
        int j = random.nextInt(arr.length);
        if(p_counter%2==0){
            return arr[j];
        }else{
            return list.get(i);
        }
    }

    /**
     * 获取城市编码
     * @param list
     * @return
     */
    public static String getCityCode(List<String> list){
        c_counter++;
        String[] arr = new String[]{"GD-SZ","BJ-CY","ZJ-HZ","BJ-HD","BJ-TZ","BJ-FS","BJ-FT",
        "SH-SJ","SH-BS","SH-JS","SH-JD","SH-JD"};
        Random random = new Random();
        int i = random.nextInt(list.size());
        int j = random.nextInt(arr.length);
        if(c_counter%2==0){
            return arr[j];
        }else{
            return list.get(i);
        }
    }

    /**
     * 随机产生8位不重复的用户uid
     * @return
     */
    public static String getUid(){
        StringBuffer shortBuffer = new StringBuffer();
        String uuid = UUID.randomUUID().toString().replace("-", "");
        for (int i = 0; i < 8; i++) {
            String str = uuid.substring(i * 4, i * 4 + 4);
            int x = Integer.parseInt(str, 16);
            shortBuffer.append(chars[x % 0x3E]);
        }
        return shortBuffer.toString();
    }

    /**
     *
     * @param list
     * @return
     */
    public static String getAid(List<String> list){
        Random random = new Random();
        int i = random.nextInt(list.size());
        return list.get(i);
    }


    /**
     * 数据落盘
     * @param file
     * @param conent
     */
    public static void writeFile(String file, String conent) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));
            out.write(conent);
            out.write("\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
