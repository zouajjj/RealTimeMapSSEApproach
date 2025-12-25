import { Kafka } from "kafkajs";

const kafka = new Kafka({ brokers: ["localhost:9092"] });
const producer = kafka.producer();

let isConnected = false;

const connectProducer = async () => {
    if (!isConnected) {
        await producer.connect();
        isConnected = true;
    }
};

export default async function handler(req, res) {
    if (req.method !== "POST") return res.status(405).end();

    const { lat, lng, userId = "user_1", accuracy } = req.body;

    try {
        await connectProducer();

        const message = {
            userId,
            lat,
            lng,
            accuracy,
            timestamp: Date.now()
        };

        await producer.send({
            topic: "location_updates",
            messages: [{ value: JSON.stringify(message) }]
        });

        res.status(200).json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ success: false, error: err.message });
    }
}
