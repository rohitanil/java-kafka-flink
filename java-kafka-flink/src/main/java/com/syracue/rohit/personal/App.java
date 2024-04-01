package com.syracue.rohit.personal;

import java.io.IOException;

public class App 
{
    
    public static void main( String[] args ) throws IOException, InterruptedException
    {   
        KafkaProducerClass obj = new KafkaProducerClass();
        obj.Producer();
    }
}
