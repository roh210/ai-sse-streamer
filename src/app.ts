import 'dotenv/config';
import express, { Application, Request, Response } from 'express';
import cors from 'cors';
import sseRouter from './routes/sse'
import streamsRouter from './routes/streams';


const app: Application = express();

app.use((req, res, next) => {
  console.log('[mw] incoming', req.method, req.url)
  next()
})

app.use(cors());

app.use(express.json());


app.use('/api', sseRouter);
app.use('/api', streamsRouter);

app.use((req: Request, res: Response) => {
  res.status(404).send({ errors: [{ message: "Route not found" }] });
});


export default app;