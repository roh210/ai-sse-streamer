import chalk from "chalk";
import type { Response } from "express";


const connections = new Map<string, Set<Response>>()

export const register = (streamId: string, client: Response): void => {
    const existing = connections.get(streamId)
    if (existing) {
        existing.add(client)
        return
    }
    connections.set(streamId, new Set([client]))

}

export const deregister = (streamId: string, client: Response): void => {
    const existing = connections.get(streamId)
    if (!existing) return

    existing.delete(client)
    if (existing.size === 0) {
        connections.delete(streamId)
    }
}

export const getConnections = (streamId: string): Set<Response> | undefined => {
    return connections.get(streamId)
}

