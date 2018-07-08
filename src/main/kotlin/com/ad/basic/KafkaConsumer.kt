package com.ad.basic

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*




fun main(args: Array<String>) {
    when (args.isNotEmpty()) {
        false -> println("Enter topic name")
        true -> {
            consumeMessage(args[0])

        }

    }
}

private fun consumeMessage(topicName: String) {

    val props = Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["group.id"] = "test"
    props["enable.auto.commit"] = "true"
    props["auto.commit.interval.ms"] = "1000"
    props["session.timeout.ms"] = "30000"
    props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

    val consumer = KafkaConsumer<String, String>(props)

    consumer.subscribe(Arrays.asList(topicName))

    print("Subscribed to topic : $topicName")
    while (true) {
        val records = consumer.poll(1000)
        records.forEach { r ->
            println("offset = ${r.offset()}, key = ${r.key()}, value = %${r.value()}\n")
        }
    }
}



