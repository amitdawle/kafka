package com.ad.basic

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

fun main(args: Array<String>) {
    when (args.isEmpty()) {
        true -> println("Enter topic name")
        else -> publishMessage(args[0])
   }
}

fun publishMessage(topicName: String) {

    val props =  Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["acks"] = "all"
    props["retries"] = 0
    props["batch.size"] = 16384
    props["linger.ms"] = 1
    props["buffer.memory"] = 33554432
    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer";

    val producer =  KafkaProducer<String, String>(props)

    for( i in 1..10) {
        producer.send(ProducerRecord<String, String>(topicName,
                "$i", "value is $i"))
    }
    println("Message sent successfully")
    producer.close()
}