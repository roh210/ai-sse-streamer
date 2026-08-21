import chalk from "chalk";

type BufferedEvent = {
    id: number;
    event: 'token' | 'done' | 'error';
    data: string;
}

export const createRingBuffer = (capacity: number) => {
    const items: BufferedEvent[] = [];
    let nextId = 0;

    const push = (event: BufferedEvent['event'], data: object): BufferedEvent => {
        const entry: BufferedEvent = { id: nextId++, event, data: JSON.stringify(data) };

        items.push(entry);
        console.log(chalk.magenta('[buffer]', entry.id, entry.data))

        if (items.length > capacity) items.shift();

        return entry;
    }

    const getFrom = (lastEventId: number): BufferedEvent[] | null => {
        const startIndex = items.findIndex(item => item.id === lastEventId);

        if (startIndex === -1) return null;

        return items.slice(startIndex + 1);
    }

    const getAll = () :BufferedEvent [] =>{
        return [...items]
    }

    return { push, getFrom , getAll};
}
