import { createClient } from "redis";

export const redisClient = createClient({
    socket:{
        host: process.env.REDIS_HOST,
        port: parseInt(process.env.REDIS_PORT || '6379', 10)
    }
})

redisClient.on('error', (err) => console.log('Redis Client Error', err));

let connected = false;

export const connectRedis = async () => {
    if (!connected) {
        await redisClient.connect();
        connected = true;
        console.log('Redis connected');
    }
}