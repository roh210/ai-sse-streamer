import chalk from "chalk";
import type { Response } from "express";

type Generation = {
    clients: Set<Response>
    abortController: AbortController
}

const generation = new Map<string, Generation>()

export const getGeneration = (streamId: string): Generation | undefined => {
    return generation.get(streamId)
}

const requireGeneration = (streamId: string): Generation => {
    const gen = getGeneration(streamId)
    if (!gen) throw new Error(`no generation exists for ${streamId}`)
    return gen
}

export const createGeneration = (streamId: string, firstClient: Response, abortController: AbortController): Generation => {
    const existing = getGeneration(streamId)
    if (existing) {
        throw new Error(`[duplicate] - generation already exists for ${streamId}`)
    }

    const newGen: Generation = {
        clients: new Set([firstClient]),
        abortController,
    }
    generation.set(streamId, newGen)
    return newGen
}

export const addClient = (streamId: string, res: Response): void => {
    const gen = requireGeneration(streamId)
    gen.clients.add(res)
}

export const removeClient = (streamId: string, res: Response): AbortController | undefined => {
    const gen = getGeneration(streamId)
    if (!gen) return undefined
    gen.clients.delete(res)
    const isEmpty = gen.clients.size === 0
    if (isEmpty) generation.delete(streamId)
    return isEmpty ? gen.abortController : undefined
}

export const closeAll = (streamId: string): void => {
    const gen = getGeneration(streamId)
    const clients = gen?.clients
    if (!clients) return

    for (const client of clients) {
        try {
            client.end()
        } catch (error) {
            console.log('error occured whilst writing closing stream', error)
        }
    }
    generation.delete(streamId)
}

export const broadcast = (streamId: string, chunk: string): void => {
    // synchronous fan-out, one write per client, isolated per-client failure
    const gen = requireGeneration(streamId)
    const clients = gen.clients
    console.log(chalk.blue(`broadcasting to ${clients.size} client(s) `))
    for (const client of clients) {
        try {
            client.write(chunk)
        } catch (error) {
            console.log('error occurred whilst writing token', error)
        }
    }
}