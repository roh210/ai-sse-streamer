import Anthropic from '@anthropic-ai/sdk';


const client = new Anthropic({
  apiKey: process.env['ANTHROPIC_API_KEY'],
});

export async function* streamTokens(prompt : string, signal: AbortSignal) {
   const stream  = client.messages.stream({
    messages : [{role: 'user', content: prompt}],
   model: 'claude-haiku-4-5-20251001',
    max_tokens: 1024,
   },
   {signal}
  )

   for await (const event of stream) {
    if(event.type === 'content_block_delta' && event.delta.type==='text_delta'){
        yield event.delta.text;
    }
   }
}
