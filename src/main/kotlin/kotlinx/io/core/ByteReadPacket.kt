package kotlinx.io.core

import kotlinx.io.core.internal.*
import kotlinx.io.pool.*

expect class ByteReadPacket internal constructor(head: IoBuffer, remaining: Long, pool: ObjectPool<IoBuffer>) :
    NonConcurrentByteReadPacketBase {
    constructor(head: IoBuffer, pool: ObjectPool<IoBuffer>)

    companion object {
        val Empty: ByteReadPacket
        val ReservedSize: Int
    }
}

abstract class NonConcurrentByteReadPacketBase constructor(
    head: IoBuffer,
    remaining: Long,
    pool: ObjectPool<IoBuffer>
) :
    ByteReadPacketPlatformBase(pool) {

    final override var headRemaining: Int = head.readRemaining

    final override var tailRemaining: Long = remaining - headRemaining

    final override var head: IoBuffer = head

    final override fun release() {
        val head = head
        val empty = IoBuffer.Empty

        if (head !== empty) {
            this.head = empty
            headRemaining = 0
            tailRemaining = 0
            head.releaseAll(pool)
        }
    }

    final override tailrec fun prepareRead(minSize: Int, head: IoBuffer): IoBuffer? {
        val headSize = headRemaining
        if (headSize >= minSize) return head

        val next = head.next ?: doFill() ?: return null
        next.byteOrder = byteOrder

        if (headSize == 0) {
            if (head !== IoBuffer.Empty) {
                releaseHead(head)
            }

            return prepareRead(minSize, next)
        } else {
            val before = next.readRemaining
            head.writeBufferAppend(next, minSize - headSize)
            val after = next.readRemaining
            headRemaining = head.readRemaining
            tailRemaining -= before - after
            if (after == 0) {
                head.next = next.next
                next.release(pool)
            }
        }

        if (head.readRemaining >= minSize) return head
        if (minSize > ReservedSize) minSizeIsTooBig(minSize)

        return prepareRead(minSize, head)
    }

    private fun minSizeIsTooBig(minSize: Int): Nothing {
        throw IllegalStateException("minSize of $minSize is too big (should be less than $ReservedSize")
    }

    @DangerousInternalIoApi
    final override fun appendChain(chain: IoBuffer) {
        val tail = head.findTail()
        if (tail === IoBuffer.Empty) {
            head = chain
            chain.byteOrder = byteOrder
            require(tailRemaining == 0L) { throw IllegalStateException("It should be no tail remaining bytes if current tail is EmptyBuffer") }
            headRemaining = chain.readRemaining
            tailRemaining = chain.next?.remainingAll() ?: 0L
        } else {
            tail.next = chain
            tailRemaining += chain.remainingAll()
        }
    }

    @DangerousInternalIoApi
    final override fun stealAll(): IoBuffer? {
        val head = head
        val empty = IoBuffer.Empty

        if (head === empty) return null
        this.head = empty
        headRemaining = 0
        tailRemaining = 0
        return head
    }

    @DangerousInternalIoApi
    final override fun steal(): IoBuffer? {
        val head = head
        val next = head.next
        val empty = IoBuffer.Empty
        if (head === empty) return null

        val nextRemaining = next?.readRemaining ?: 0

        this.head = next ?: empty
        this.headRemaining = nextRemaining
        this.tailRemaining -= nextRemaining
        head.next = null

        return head
    }

    final override tailrec fun ensureNext(current: IoBuffer, empty: IoBuffer): IoBuffer? {
        val next = if (current === empty) {
            doFill()
        } else {
            current.next.also { current.release(pool) } ?: doFill()
        }

        if (next == null) {
            if (head !== empty) {
                head = empty
                headRemaining = 0
                tailRemaining = 0L
            }

            return null
        }

        if (next.canRead()) {
            head = next
            next.byteOrder = byteOrder
            val nextRemaining = next.readRemaining
            headRemaining = nextRemaining
            tailRemaining -= nextRemaining
            return next
        } else {
            return ensureNext(next, empty)
        }
    }

    @DangerousInternalIoApi
    @Deprecated("")
    internal fun tryWriteAppend(chain: IoBuffer): Boolean {
        val tail = head.findTail()
        val size = chain.readRemaining

        if (size == 0 || tail.writeRemaining < size) return false
        tail.writeBufferAppend(chain, size)

        if (head === tail) {
            headRemaining += size
        } else {
            tailRemaining += size
        }

        return true
    }

    final override fun releaseHead(head: IoBuffer) {
        val next = head.next ?: IoBuffer.Empty
        this.head = next
        val nextRemaining = next.readRemaining
        this.headRemaining = nextRemaining
        this.tailRemaining -= nextRemaining
        head.release(pool)
    }
}

expect abstract class ByteReadPacketPlatformBase protected constructor(pool: ObjectPool<IoBuffer>) :
    ByteReadPacketBase {
}

expect fun ByteReadPacket(
    array: ByteArray, offset: Int = 0, length: Int = array.size,
    block: (ByteArray) -> Unit
): ByteReadPacket

@Suppress("NOTHING_TO_INLINE")
inline fun ByteReadPacket(array: ByteArray, offset: Int = 0, length: Int = array.size): ByteReadPacket {
    return ByteReadPacket(array, offset, length) {}
}
