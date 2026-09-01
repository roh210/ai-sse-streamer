import { redisClient } from "../redis/client"
import { streamKey } from "../redis/streamKeys"

export const resolveCursor = async (streamId:string, lastEventIdHeader:string|null):Promise<{type:'resync'}|{type:'cursor',id:string}> => {
   if (!lastEventIdHeader) {
      return {type:'cursor', id:'0-0'} // fresh connection, start from the beginning of the stream
   }

   const isStale = await isRequestedIdStale(streamId, lastEventIdHeader)
   if (isStale) {
      return { type: 'resync' }
   }

   return { type: 'cursor', id: lastEventIdHeader }  // continue from the last known event ID
}         

const isRequestedIdStale = async(streamId:string, requestedId:string):Promise<boolean> => {
   const info = await redisClient.xInfoStream(streamKey(streamId))
   const oldestId = info["first-entry"]?.id

   if (!oldestId) return true // If there is no oldestId, it means the stream is empty or has been trimmed, so the requestedId is considered stale.
   return compareStreamIds(requestedId, oldestId) < 0
}

const compareStreamIds = (id1: string, id2: string): number => {
   const [timestamp1, sequence1] = id1.split('-').map(Number);
   const [timestamp2, sequence2] = id2.split('-').map(Number);

   if(timestamp1 !== timestamp2) {
       return timestamp1 - timestamp2;
   }
   return sequence1 - sequence2;
}