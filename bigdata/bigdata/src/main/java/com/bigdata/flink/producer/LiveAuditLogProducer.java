package com.bigdata.flink.producer;
import com.bigdata.flink.util.DBUtil;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class LiveAuditLogProducer {
    public static void main(String[] args) throws SQLException, InterruptedException {
        //查询sql
        String sql = "select * from distinctcode";
        //返回区域编码集合
        List<String> pcList = DBUtil.getProvinceCodeList(sql);
        //指定文件输出路径
        String filepath=args[0];
		//循环产生直播审计日志
        while (true){
            String message ="{\"audit_time\":\""+getCurrentTime()
                    +"\",\"audit_type\":\""+getRandomAuditType()
                    + "\",\"checker\":\""+getRandomChecker()
                    + "\",\"province_code\":\""+getProvinceCode(pcList)
                    +"\"}";
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
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    /**
     * 获取省份编码
     * @param list
     * @return
     */
    public static String getProvinceCode(List<String> list){
        Random random = new Random();
        int i = random.nextInt(list.size());
        return list.get(i);
    }

    /**
     * 直播类别：音乐、舞蹈、聊天互动、户外、文化才艺、美食、知识教学、其他
     * @return
     */
    public static String getRandomAuditType(){
        String[] audittypes = {"music_upper_shelf","dance_upper_shelf","chat_upper_shelf","outdoors_upper_shelf","culturaltalents_upper_shelf","finefood_upper_shelf",
                "knowledgeteaching_upper_shelf","music_lower_shelf","dance_lower_shelf","chat_lower_shelf","outdoors_lower_shelf","culturaltalents_lower_shelf",
                "finefood_lower_shelf","knowledgeteaching_lower_shelf","music_blacklist","dance_blacklist","chat_blacklist","outdoors_blacklist",
                "culturaltalents_blacklist","finefood_blacklist","knowledgeteaching_blacklist"};
        Random random = new Random();
        int i = random.nextInt(audittypes.length);
        return audittypes[i];
    }

    /**
     * 直播或者短视频审核人
     * @return
     */
    public static String getRandomChecker(){
        String[] checkers = {"checker1","checker2","checker3","checker4","checker5",
                        "checker6","checker7","checker8","checker9","checker10"};
        Random random = new Random();
        int i = random.nextInt(checkers.length);
        return checkers[i];
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
