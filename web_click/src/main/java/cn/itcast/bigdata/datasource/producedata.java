package cn.itcast.bigdata.datasource;


import java.io.*;

/**
 * Created by Administrator on 2017/9/3.
 * 每隔10s产生新的数据产生数据
 */
public class producedata {
    public static void main(String args[]) {
        long scheaduleTime = Long.parseLong(args[0]);
//        long scheaduleTime = 1000;
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        try {
            bufferedReader = new BufferedReader(new FileReader("/opt/project/web_click/source/access.log.fensi"));
            bufferedWriter = new BufferedWriter(new FileWriter("/opt/project/web_click/source/test.log"));
            while (bufferedReader.readLine() != null) {
                String line = bufferedReader.readLine();
                System.out.println(line);
                bufferedWriter.write(line);
                bufferedWriter.newLine();
                bufferedWriter.flush();
                Thread.sleep(scheaduleTime);
            }
            bufferedWriter.flush();
            bufferedWriter.close();
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
