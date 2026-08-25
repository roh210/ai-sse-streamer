import 'dotenv/config';
import express, { Application, Request, Response } from 'express';
import cors from 'cors';
import sseRouter from './src/routes/sse';
import { connectRedis } from './src/redis/client';

const app: Application = express();
const PORT = 3000;

connectRedis()

app.use((req, res, next) => {
  console.log('[mw] incoming', req.method, req.url)
  next()
})

app.use(cors());

app.use(express.json());


app.use('/api', sseRouter);


app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

app.use((req: Request, res: Response) => {
  res.status(404).send({ errors: [{ message: "Route not found" }] });
});


export default app;