import 'dotenv/config';
import express, { Application, Request, Response } from 'express';
import cors from 'cors';
import sseRouter from './src/routes/sse';

const app: Application = express();
const PORT = 3000;

app.use(cors());

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

app.use(express.json());

app.use('/api', sseRouter);

app.use((req: Request, res: Response) => {
  res.status(404).send({ errors: [{ message: "Route not found" }] });
});


export default app;