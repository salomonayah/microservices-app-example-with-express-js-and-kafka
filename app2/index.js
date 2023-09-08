const express = require("express");
const kafka = require("kafka-node");
const mongoose = require("mongoose");

const app = express();

app.use(express.json());

const dbStatusActive = async () => {
  mongoose.connect(process.env.MONGO_URL);

  const User = new mongoose.model("User", {
    name: String,
    email: String,
    password: String,
  });

  const kafkaClient = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  const consumer = new kafka.Consumer(
    kafkaClient,
    [
      {
        topic: process.env.KAFKA_TOPIC,
      },
    ],
    {
      autoCommit: true,
    }
  );

  consumer.on("message", async (message) => {
    console.log("message received");
    console.log(JSON.parse(message.value));
    const newUser = await new User(JSON.parse(message.value));
    await newUser.save();
  });

  consumer.on("error", (error) => {
    console.log(error);
  });
};

setTimeout(dbStatusActive, 30000);

app.listen(process.env.PORT);
