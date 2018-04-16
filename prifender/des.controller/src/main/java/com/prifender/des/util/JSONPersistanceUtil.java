package com.prifender.des.util;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.Problem;

public class JSONPersistanceUtil {

    public static String createDir(String dirName) {

        if (dirName != "") {
            if (!new File(dirName).exists()) {
                new File(dirName).mkdirs();
            }
        }
        return dirName;
    }

    public static void createFile(Path path) throws IOException {
        File file = new File(path.toString());
        if (!file.exists()) {
            file.createNewFile();
        }
    }

    @SuppressWarnings("unchecked")
    public static void writeFile(String path, JSONObject jsonObject) throws IOException {
        // Create a new FileWriter object
        JSONArray jSONArray = readJsonFile(path);
        jSONArray.add(jsonObject);
        FileWriter fileWriter = new FileWriter(path);
        fileWriter.write(jSONArray.toJSONString());
        fileWriter.close();
    }

    public static void writeFile(String path, JSONArray jSONArray) throws IOException {
        // Create a new FileWriter object
        FileWriter fileWriter = new FileWriter(path);
        fileWriter.write(jSONArray.toJSONString());
        fileWriter.close();
    }

    public static JSONArray readJsonFile(String path) {
        JSONParser parser = new JSONParser();
        JSONArray jSONArray = null;
        try {
            Object obj = parser.parse(new FileReader(path));
            jSONArray = (JSONArray) obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jSONArray;
    }

    @SuppressWarnings("unchecked")
    public static DataSource addDataSource(DataSource ds,Path path) throws DataExtractionServiceException {
        try { 
 
            JSONArray  jsonArray = new JSONArray();
            // Create a new JSONObject
            JSONObject jsonObject = new JSONObject();
            // Add the values to the jsonObject
            jsonObject.put("id", ds.getId());
            jsonObject.put("label", ds.getLabel());
            jsonObject.put("type", ds.getType());
            
            // Create a new JSONArray object
            JSONArray subJsonArray = new JSONArray();
           for(ConnectionParam ConnectionParam : ds.getConnectionParams()){
            JSONObject jsonSubObject = new JSONObject();
            jsonSubObject.put("id", ConnectionParam.getId());
            jsonSubObject.put("value", ConnectionParam.getValue());
            subJsonArray.add(jsonSubObject);
           }
            jsonObject.put("connectionParams", subJsonArray);
            jsonArray.add(jsonObject);
            
            //read from the file if the jsonarray is emptyy then add the jsonarray otherwise the jsonobject
            JSONArray existingJsonArray = JSONPersistanceUtil.readJsonFile(path.toString());
                
            if (Files.exists(path) && (existingJsonArray != null && existingJsonArray.size() > 0)) {
              writeFile(path.toString(),jsonObject);
            }else{
              createFile(path);
              writeFile(path.toString(),jsonArray);
            }
        
        }catch (FileNotFoundException e){
            throw new DataExtractionServiceException( new Problem().code("data source error").message(e.getMessage()) );
        }  catch (Exception e) {
            throw new DataExtractionServiceException( new Problem().code("data source error").message(e.getMessage()) );
        }
        return ds;
    }

    @SuppressWarnings({ "rawtypes" })
    public static List<DataSource> getDataSources(List<DataSource> dataSourcesList,Path path) {
        try {
            JSONArray jsonArray = JSONPersistanceUtil.readJsonFile(path.toString());
            if(jsonArray != null){
                Iterator iterator = jsonArray.iterator();
                while (iterator.hasNext()) {
                    JSONObject jSONObject = (JSONObject) iterator.next();
                    ObjectMapper objectMapper = new ObjectMapper();
                    DataSource dataSource = objectMapper.readValue(jSONObject.toString(), DataSource.class);
                    dataSourcesList.add(dataSource);
                }
            }
        }catch (FileNotFoundException e){
             e.printStackTrace();
        }
        catch (Exception e) {
             e.printStackTrace();
        }
        return dataSourcesList;

    }

    public static void deleteDataSource(String id, boolean isDelete,Path path) {
        JSONArray jSONArray = JSONPersistanceUtil.readJsonFile(path.toString());
        if (isDelete) {
            deleteDataSourceById(id, jSONArray);
        }
        try {
            JSONPersistanceUtil.writeFile(path.toString(), jSONArray);
        } catch (IOException e) {
            e.getMessage();
        }
    }
    
    private static void deleteDataSourceById(String idValue, JSONArray jsonArray) {

        if (jsonArray != null && jsonArray.size() > 0) {
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jSONObject = (JSONObject) jsonArray.get(i);
                String id = (String) jSONObject.get("id");
                if (StringUtils.equals(idValue, id)) {
                    jsonArray.remove(i);
                    break;
                }
            }
        }
    }
}
