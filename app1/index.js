const express = require("express");
const kafka = require("kafka-node");
const sequelize = require("sequelize");

const app = express();

app.use(express.json());

const dbStatusActive = async () => {
  const db = new sequelize(process.env.POSTGRES_URL);

  const UserModel = db.define("user", {
    name: sequelize.STRING,
    email: sequelize.STRING,
    password: sequelize.STRING,
  });

  db.sync({ force: true });

  const kafkaClient = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  const producer = new kafka.Producer(kafkaClient);

  producer.on("ready", async () => {
    console.log("producer ready");

    app.post("/", (req, res) => {
      producer.send(
        [
          {
            topic: process.env.KAFKA_TOPIC,
            messages: JSON.stringify(req.body),
          },
        ],
        async (err, data) => {
          if (err) console.log(err);
          else {
            await UserModel.create(req.body);
            res.send(req.body);
          }
        }
      );
    });
  });
};

setTimeout(dbStatusActive, 30000);

app.listen(process.env.PORT);
