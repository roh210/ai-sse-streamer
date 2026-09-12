import 'dotenv/config';
import app from './src/app';
import { connectRedis } from './src/redis/client';


const PORT = 3000;

connectRedis()

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

