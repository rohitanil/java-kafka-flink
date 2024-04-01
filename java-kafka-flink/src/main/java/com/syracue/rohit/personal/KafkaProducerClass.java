package com.syracue.rohit.personal;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class KafkaProducerClass {
    protected Properties kafkaProperties;
    protected Properties apiProperties;
    protected StringBuilder response;
    protected final int SLEEP = 5;

    public KafkaProducerClass(){
        try{
            kafkaProperties = new Properties(); 
            apiProperties = new Properties();
            
            InputStream inpStream1 = KafkaProducerClass.class.getClassLoader().getResourceAsStream("kafka.config");
            InputStream inpStream2 = KafkaProducerClass.class.getClassLoader().getResourceAsStream("api.config");
            
            kafkaProperties.load(inpStream1);
            apiProperties.load(inpStream2);
            inpStream1.close();
            inpStream2.close();
        
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
    
    /** 
     * Method to get cryptocurrency details from url provided
     * @param baseURL The base url of the api
     * @param coins Comma seperated coin values
     * @param currency Comma seperated currency values
     * @param apiKey API key
     * @return String Response from API
     */
    public String getCoinPrice(String baseURL, String coins, String currency, String apiKey){
        try {
            String apiUrl = baseURL + coins + "&tsyms=" + currency + "&api_key=" + apiKey;
            StringBuilder response = new StringBuilder();
            String line;
            BufferedReader reader;
            
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();
            
            if (responseCode == HttpURLConnection.HTTP_OK) {
                reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            } else {
                reader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            }
            
            
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();

            // Add timestamp to the response
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String timestamp = dateFormat.format(new Date());
            response.insert(0, "{\"timestamp\":\"" + timestamp + "\",");
            return response.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Method to publish response from crypto API to the Kafka topic
     */
    public void Producer() {
        String apiResponse;
        try{
            while (true) {
                apiResponse = getCoinPrice(
                    apiProperties.getProperty("api.baseurl"),
                    apiProperties.getProperty("crypto.coins"), 
                    apiProperties.getProperty("crypto.currencies"), 
                    apiProperties.getProperty("api.key")
                );
                if(apiResponse != null)
                {
                    System.out.println(apiResponse);
                    /**
                     * TO DO
                     * 1. create kafka cluster and topic -> use docker
                     * 2. Publish records to kafka topic
                     * **/

                }
                TimeUnit.SECONDS.sleep(this.SLEEP);
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
}
}