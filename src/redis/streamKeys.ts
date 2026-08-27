export const cutoffId = (hours: number) => {
  const retentionMs = hours * 60 * 60 * 1000;
  const cutoffTimeStamp = Date.now() - retentionMs;
  return `${cutoffTimeStamp}-0`;
};

export const isValidStreamKey = (id: string) : boolean => {
  return /^[a-zA-Z0-9_-]+$/.test(id);
}

export const streamKey = (id: string) : string => {
  if (!isValidStreamKey(id)) {
    throw new Error(`Invalid stream key: ${id}`);
  }
  return `stream:${id}`;
};

export const streamCountKey = (id: string) : string => {
  if (!isValidStreamKey(id)) {
    throw new Error(`Invalid stream count key: ${id}`);
  }
  return `count:${id}`;
}