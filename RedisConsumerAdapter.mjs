import { Adapter, TextMessage } from 'hubot'

class AckResultWarning extends Error {
    constructor(ackResult) {
        super(`Acknowledgment failed with result: ${ackResult}`)
        this.name = 'AckResultWarning'
    }
}

class CreateGroupError extends Error {
    constructor(cause) {
        super(`Failed to create consumer group: ${cause}`)
        this.name = 'CreateGroupError'
    }
}

class RedisConsumerAdapter extends Adapter {
    #client = null
    #isReading = false
    #readingPromise = null
    #options = null
    constructor(robot, client, { streamName, outboxStreamName, groupName, consumerName }) {
        super(robot)
        this.#client = client
        this.#options = { streamName, outboxStreamName, groupName, consumerName }
    }
    async #read() {
        if (!this.#client || !this.#isReading) {
            return null
        }
        
        let response = await this.#client.xReadGroup(this.#options.groupName, this.#options.consumerName, [
            {
                key: this.#options.streamName,
                id: '>'
            }
        ], {
            COUNT: 1,
            BLOCK: 1000  // Shorter block time for faster cleanup
        })

        if (!response || response.length === 0) {
            return null
        }

        const ackResult = await this.#client.xAck(this.#options.streamName, this.#options.groupName, response[0].messages[0].id)
        if (ackResult != 1) {
            this.emit('warning', new AckResultWarning(ackResult))
        }

        await this.inbox(response[0].messages)

        return response
    }
    async #tryToCreateGroup(key, groupName, startFrom) {
        try {
            return await this.#client.xGroupCreate(key, groupName, startFrom, { MKSTREAM: true })
        } catch(error) {
            this.emit('info', new CreateGroupError(error))
        }
    }
    async run() {
        if (this.#client && this.#client.isConnected) {
            return
        }

        if (this.#client && !this.#client.isOpen) {
            await this.#client.connect()
        }

        await this.#tryToCreateGroup(this.#options.streamName, this.#options.groupName, '$')
        await this.#client.xTrim(this.#options.streamName, 'MAXLEN', 1000, { strategyModifier: '~' })

        this.#isReading = true
        this.#readingPromise = this.#continousRead()
        this.emit('connected', this)
        return Promise.resolve()
    }
    async close() {
        this.#isReading = false
        if (this.#readingPromise) {
            await this.#readingPromise
            this.#readingPromise = null
        }
        
        if (this.#client) {
            if (this.#client.isOpen) {
                await this.#client.disconnect()
            }
            this.#client = null
        }
        return Promise.resolve()
    }
    async #continousRead() {
        while(this.#isReading) {
            try {
                await this.#read()
            } catch (error) {
                if (this.#isReading) {
                    console.error('Error in continuous read:', error)
                    // Small delay to prevent tight error loops
                    await new Promise(resolve => setTimeout(resolve, 1000))
                }
            }
        }
    }
    async inbox(entries){
        for await (const entry of entries) {
            const { sender, room, body, id } = entry.message
            const textMessage = new TextMessage({user: sender, room}, body, id)
            await this.robot.receive(textMessage)
        }
    }

    async send(envelope, ...strings) {
        if (!this.#client) {
            throw new Error('Redis client is not initialized')
        }
        
        await this.#client.xAdd(this.#options.outboxStreamName, '*', {
            kind: 'send',
            recordedAt: new Date().toISOString(),
            occurredAt: new Date().toISOString(),
            id: new Date().getTime().toString(),
            sender: envelope.user.user,
            room: envelope.room,
            body: strings.join(' ')
        })
    }
    async reply(envelope, ...strings) {
        if (!this.#client) {
            throw new Error('Redis client is not initialized')
        }

        await this.#client.xAdd(this.#options.outboxStreamName, '*', {
            kind: 'reply',
            recordedAt: new Date().toISOString(),
            occurredAt: new Date().toISOString(),
            id: new Date().getTime().toString(),
            sender: envelope.user.user,
            room: envelope.room,
            body: strings.join(' ')
        })
        this.emit('reply', envelope, ...strings)
    }
}

export { RedisConsumerAdapter, CreateGroupError, AckResultWarning }