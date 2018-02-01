package com.sxw;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.PreparedStatement;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.io.File;

/**
 * Hello world!
 *
 */
public class Analisis {

    @Value(value="classpath:/properties/banks.json")
    private static Resource data;
    private static Map<String, String> jsonObject2Map(JSONObject obj, Map<String, String> extProperties) {
        Map<String, String> ret = new HashMap<String, String>(extProperties);
        for (Map.Entry<String, Object> item : obj.entrySet())
        {
            ret.put(item.getKey(), item.getValue().toString());
        }
        return ret;
    }
    public static void main( String[] args ) throws IOException, SQLException, ClassNotFoundException

    {
         Class<?> aClass = Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("");
        PreparedStatement ps=null;
        File file = data.getFile();
        long t0 = System.nanoTime();
        String jsonData = Analisis.jsonRead(file);
        long t1 = System.nanoTime();
        long millis = TimeUnit.NANOSECONDS.toMillis(t1-t0);
        System.out.println(millis +"ms");
        Object json = JSON.parse(jsonData);
        JSONArray array = (JSONArray) json;
        //begin transaction
        try {
            String currentTime = String.valueOf ( (new Date().getTime()) / 1000);
            Map<String, String> timeProperties = new HashMap<String, String>();
            timeProperties.put("update_time", currentTime);
            timeProperties.put("create_time", currentTime);

            int batchSize = 1000;
            List<Map<String, String>> datas = new ArrayList<>(batchSize);
            for (int i = 0; i < array.size(); i++) {
                if(i % batchSize == 0 && i != 0 )
                {
                    System.out.println("create map finished");

                    String sql = "   INSERT into sai_banks_store(band_id,region_id,name,address,create_time,update_time)values"+
                    "("+datas.get(Integer.parseInt("bankId"))+","+datas.get(Integer.parseInt("cityId"))+","+datas.get(Integer.parseInt("subBranchName"))+","+datas.get(Integer.parseInt("name"))+","+datas.get(Integer.parseInt("create_time"))+","+datas.get(Integer.parseInt("update_time"))+")";
                    ps.executeUpdate(sql);

                    System.out.println("insert finished");
                    datas =  new ArrayList<>(batchSize);
                }
                System.out.println("" + i + "/" + array.size());
                JSONObject item = (JSONObject) array.get(i);
                datas.add(jsonObject2Map(item, timeProperties));
            }

            if (datas.size() != 0) {
                System.out.println("create map finished");

                String sql = "   INSERT into sai_banks_store(band_id,region_id,name,address,create_time,update_time)values"+
                        "("+datas.get(Integer.parseInt("bankId"))+","+datas.get(Integer.parseInt("cityId"))+","+datas.get(Integer.parseInt("subBranchName"))+","+datas.get(Integer.parseInt("name"))+","+datas.get(Integer.parseInt("create_time"))+","+datas.get(Integer.parseInt("update_time"))+")";
                ps.executeUpdate(sql);
                System.out.println("insert finished");
            }
            con.commit();
            con.close();
        }

        catch (Exception e)
        {
            System.out.println("insert exception");
            e.printStackTrace();
            // System.out.print();
        }
        /*
        System.out.print(json.getClass().getName());

        System.out.print(array.get(0).getClass().getName());

        Gson gosn = new Gson();
         List list = new ArrayList();
        List list1 = gosn.fromJson(jsonData, list.getClass());

        System.out.println(list1.size());
        Map map =new HashMap();
        List<Map> maps = new ArrayList<>();

        try {
            for (Object o : list1
                    ) {
               // System.out.println(o);
                Map map1 = gosn.fromJson(o.toString(), map.getClass());
                map1.put("update_time", (int) (new Date().getTime()) / 1000);
                map1.put("create_time", (int) (new Date().getTime()) / 1000);

          /*  String sql = "   INSERT into sai_banks_store(band_id,region_id,name,address,create_time,update_time)values"+
                    "("+map1.get("bankId")+","+map1.get("cityId")+","+map1.get("subBranchName")+","+map1.get("name")+","+map1.get("create_time")+","+map1.get("update_time")+")";

            ps.executeUpdate(sql);
            i++;
            System.out.println(i);
                maps.add(map1);
            }
        }
        catch ( Exception e)
        {
            System.out.print(e);
            int i=0;
            i++;
        }*/

     /*   System.out.println(i);
        con.commit();
        con.close();*/
        // shopManageService.tets(maps);

    }

    private static String jsonRead(File file){
        Scanner scanner = null;
        StringBuilder buffer = new StringBuilder();
        try {
            scanner = new Scanner(file, "utf-8");
            while (scanner.hasNextLine()) {
                buffer.append(scanner.nextLine());
            }
        } catch (Exception e) {

        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return buffer.toString();
    }
    }

