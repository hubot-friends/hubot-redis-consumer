import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Adapter, Robot, TextMessage } from 'hubot/index.mjs'
import { createClient } from 'redis'
import { RedisConsumerAdapter } from './RedisConsumerAdapter.mjs'

const REDIS_URL = 'redis://localhost:6378'
const INBOX_STREAM_NAME = 'hubot-inbox'
const OUTBOX_STREAM_NAME = 'hubot-outbox'

function getRobotWithAdapter(adapter) {
    return new Robot({
        async use(robot) {
            adapter.robot = robot
            return adapter
        }
    }, false, 'Hubot', 't-bot')
}


await test('Redis Inbox Consumer', async (t) => {
    await t.test('Replies to message in INBOX', async () => {
        const client = createClient({
            url: REDIS_URL
        })
        
        let replyReceived = false
        let consumer = null
        let sut = null
        
        try {
            consumer = new RedisConsumerAdapter(null, client, {
                streamName: INBOX_STREAM_NAME,
                outboxStreamName: OUTBOX_STREAM_NAME,
                groupName: 'hubot-group',
                consumerName: 'hubot-consumer'
            })
            sut = getRobotWithAdapter(consumer)

            // Promise that resolves when reply event is emitted
            const replyProcessed = new Promise(resolve => {
                consumer.on('reply', (envelope, ...strings) => {
                    replyReceived = true
                    assert.equal(strings.join(' '), 'yo bruh')
                    resolve()
                })
            })

            // Set up responder that will trigger a reply
            sut.respond(/hey i expect a reply yo/, async resp => {
                assert.equal(resp.message.text, '@t-bot hey i expect a reply yo')
                await resp.reply('yo bruh')
            })

            await sut.loadAdapter()
            await sut.run()

            await client.xAdd(INBOX_STREAM_NAME, '*', {
                kind: 'InboxEnvelope',
                recordedAt: new Date().toISOString(),
                occurredAt: new Date().toISOString(),
                id: new Date().getTime().toString(),
                sender: 'user1',
                room: 'general',
                body: '@t-bot hey i expect a reply yo'
            })

            await Promise.race([
                replyProcessed,
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Reply timeout')), 3000)
                )
            ])

            assert.ok(replyReceived, 'Reply should have been sent')
            
        } catch (error) {
            console.error('Outbox test error:', error)
            throw error
        } finally {
            if (sut) {
                try {
                    await sut.shutdown()
                } catch (e) {
                    console.error('Error shutting down robot in outbox test:', e)
                }
            }
            
            // Give a moment for cleanup to complete
            await new Promise(resolve => setTimeout(resolve, 200))
        }
    })
})
