package kotlinx.io.core

import kotlinx.atomicfu.*
import kotlinx.io.core.internal.*
import kotlinx.io.pool.*

class ConcurrentPipe(initial: IoBuffer, pool: ObjectPool<IoBuffer>) : ByteReadPacketPlatformBase(pool) {
    private val dummy = IoBuffer.makeDummy().apply { markLocked(); next = this }

    // updated only by reader however writer is checking it when it is under lock
    override var head: IoBuffer = dummy

    // it is updated by writer during append and by reader (CAS to dummy only)
    private val tail: AtomicRef<IoBuffer> = atomic(dummy)

    // updated only by reader with the only exception: tryAppendBytes that does it when head is under lock
    // so reader is unable to touch it
    override var headRemaining = 0

    override val tailRemaining: Long
        get() = tailRemainingAtomic.value

    // updated concurrently by both reader and writer
    private val tailRemainingAtomic: AtomicLong = atomic(0L)

    init {
        if (initial.canRead()) {
            dummy.casNext(dummy, initial)
            tailRemainingAtomic.value = initial.remainingAll()
        } else if (initial !== IoBuffer.Empty) {
            initial.release(pool)
        }
    }

    override fun copy(): Nothing = throw UnsupportedOperationException("It is strictly prohibited to copy a pipe")

    fun readIntExample(): Int {
        if (headRemaining > 4) {
            return head.readInt()
        }

        return readIntSlow()
    }

    private fun readIntSlow(): Int {
        val next = ensureNext(4) ?: throw EOFException("Not enough bytes available to read 4 bytes")
        val value = next.readInt()
        if (next.isEmpty()) {
            releaseHead(next)
        }
        return value
    }

    override fun releaseHead(head: IoBuffer) {
        detachHeadNonDummy(head)
        head.release(pool)
    }

    private fun detachHeadNonDummy(head: IoBuffer) {
        kotlinx.io.core.internal.require(head.locked) { "Only locked instances could be released" }
        kotlinx.io.core.internal.require(this.head === head) {
            "Only head chunks could be released using this function"
        }

        val next = head.getAndSetNext(null) ?: error("It should be never null next")
        if (next === dummy) { // tail
            dummy.next = dummy

            // if it is failed then appendByRef just appended more chunks so ignore it
            // it is very important to try to release tail ONLY if we have next = dummy
            // otherwise we can break tail reference forever
            // however it is safe here because appendByRef does set next first
            // so if appendByRef is in progress already then we will never reach here
            tail.compareAndSet(head, dummy)

            this.head = dummy // dummy is already published so dummy.next could be already pointing to something
            headRemaining = 0 // but we don't care, dummy is always locked so we can modify headRemaining

            // from this point head is not available in writer unless it already holds it
            // then writer will try to:
            //  set next
            //      will fail as we have next = null while writer is trying to CAS dummy -> chain
            //      if appendByRef already did CAS from dummy to chain then we can't reach here (should be non-tail)
            //  try to lock
            //      will fail as it is still locked here
            //      will fail after unlock due to version change
            head.markUnlocked()
        } else { // non-tail
            // we got next chunk but appendByRef could be still in progress and tail could be not set yet
            // and tail remaining is likely not yet increased

            // for sure appender wouldn't try to append to it so it is safe to simply release it because:
            //     it is still locked so tryAppendBytes will fail and cause fallback to appendByRef
            //     appendByRef will try to append to `next` or to just appended it so will update tail and tailRemainingAtomic
            head.markUnlocked()

            // since there is one more chunk then we are sure that tail != dummy
            // so only reader can update dummy.next (nobody else)
            // as far as we know that head !== dummy then we are also sure that dummy.next = dummy
            // otherwise we likely have concurrent read access or something else is broken
            if (!dummy.casNext(dummy, next)) {
                throw AssertionError("Concurrent read access")
            }

            this.head = dummy
            this.headRemaining = 0 // should be safe as tryAppendBytes will only append to tail but never to dummy

            // we will switch head to next only on-demand to get chance to tryAppendBytes to succeed
        }
    }

    private fun releaseChunkInTheMiddle(prev: IoBuffer, chunk: IoBuffer): Boolean {
        // should be only called by reader
        check(prev.locked) // current should be always locked by reader
        check(prev !== dummy) // prev shouldn't be dummy as it is not guaranteed to work properly

        // first try to lock the chunk
        if (!chunk.markLocked()) return false

        // from this point tryAppendBytes is not the case
        // however appendByRef still could compete with us

        if (chunk.canRead()) {
            // chunk became not empty while we tried to release it
            // so we cancel release
            chunk.markUnlocked()
            return false
        }

        // set to null as we are going to recycle it
        val next = chunk.getAndSetNext(null)
            ?: throw AssertionError("We faced a chunk that is already recycled")
        // from this point appendByRef is unable to set next ref
        // but we better to fix tail as fast as possible

        if (!prev.casNext(chunk, next)) {
            throw AssertionError("Prev node is modified concurrently")
        }

        // ensure that tail doesn't point to the chunk
        this.tail.compareAndSet(chunk, prev) // ignore if failed to CAS

        // from this point nobody is pointing to the chunk
        // and all concurrent operations will fail anyway
        // so unlock it (and modify version)
        chunk.markUnlocked()

        // next is null, version is modified, no tail reference to it
        // not it is safe to release it
        chunk.release(pool)

        return true
    }

    override fun release() {
        do {
            stealAll()?.releaseAll(pool)
            if (tail.value === dummy) break
        } while (true)
    }

    override fun steal(): IoBuffer? {
        return ensureNext()?.also { detachHeadNonDummy(it) }
    }

    @DangerousInternalIoApi
    override fun stealAll(): IoBuffer? {
        // should be called by reader only

        val dummy = dummy
        var head = dummy
        var tail = dummy

        do {
            var start = this.head
            val end = this.tail.value

            if (end === dummy) break // empty chain - stop stealing

            if (start !== end && !end.markLocked()) break // steal remaining later, abort stealing
            // otherwise if start = end - the only chunk we have that is already locked

            // from this point only appendByRef may compete with us

            // already locked, could not be dummy (already checked before)
            end.next = null // to prevent appender from appending to the tail

            // failing assertions here with end.next = null could lead to appender infinite spin-loop
            // so we remember error first

            val error: String? = if (start === dummy) {
                val startNext = dummy.next
                when {
                    startNext == null -> "Chunk is already recycled"
                    // it is safe because tail != dummy
                    // only reader could change it
                    !dummy.casNext(start, dummy) -> "Concurrent read access"
                    else -> {
                        start = startNext
                        null
                    }
                }
            } else {
                // otherwise we believe that it is already dummy.next = dummy
                if (dummy.next === dummy) null else "dummy.next should be dummy"
            }

            // we should be sure that dummy.next = dummy otherwise we break the world
            this.tail.compareAndSet(end, dummy)

            // unlock and increment version so end is completely free and we own it exclusively after it
            end.markUnlocked()

            if (end !== start) {
                // unlock head as well, now we can use it with no risk
                start.markUnlocked()
            }

            // it is relatively safe place to crash
            if (error != null) {
                throw AssertionError(error)
            }

            val size = start.remainingAll()
            this.tailRemainingAtomic.addAndGet(-size)

            // concat chains
            tail.next = start
            tail = end
            if (head === dummy) head = start

            // fix head and headRemaining
            this.head = dummy // at this point dummy could be already modified
            this.headRemaining = 0 // but we don't care, we retry instead
        } while (true)

        return if (head === dummy) null else head
    }

    override fun ensureNext(current: IoBuffer, empty: IoBuffer): IoBuffer? {
        return ensureNext()
    }

    override fun prepareRead(minSize: Int, head: IoBuffer): IoBuffer? {
        return ensureNext(minSize)
    }

    override fun fill(): IoBuffer? = null

    override fun closeSource() {
    }


    private fun ensureNext(): IoBuffer? {
        val head = head
        if (head.canRead()) return head

        return ensureNextSlow()
    }

    private fun ensureNext(n: Int): IoBuffer? {
        require(n > 0)

        val head = head
        if (head.readRemaining >= n) return head

        return ensureNextSlow(n)
    }

    private fun ensureNextSlow(): IoBuffer? {
        val dummy = dummy
        do {
            val head = this.head
            if (head.canRead()) {
                check(head.locked) // it should be already locked
                return head
            }
            if (head !== dummy) {
                releaseHead(head)
                continue
            }

            // head is dummy

            val next = head.next
            if (next === dummy) return null
            if (next == null) throw AssertionError("Head points to recycled chunk?")

            if (next.markLocked()) {
                dummy.getAndSetNext(dummy) // dummy head shouldn't point to next anymore

                this.head = next // only reader updates head
                val size = next.readRemaining
                this.headRemaining = size // we can update it as we hold lock on it

                // we potentially could get negative tailRemainingAtomic for a while
                // until appendByRef will increase it soon
                // so we set headRemaining first and then decrease tail
                // so SUM of them will remain at least the same so we will never see negative total remaining
                this.tailRemainingAtomic.addAndGet(-size.toLong())

                return next
            } else { // tryAppendBytes is in progress on next
                // so let's wait
                // and keep head = dummy, dummy.next = next
                return null
            }
        } while (true)
    }

    private fun ensureNextSlow(n: Int): IoBuffer? {
        val dummy = dummy

        do {
            val current = ensureNext() ?: return null
            if (current.readRemaining >= n) return current

            val next = current.next

            if (next === dummy) return null
            if (next == null) throw AssertionError("We faced a chunk that is already recycled")

            // current is always locked and it is not dummy
            // so it will never grow
            // and since we are the reader then current.readRemaining is fixed before writeBufferAppend invocation

            // it is safe to read from the next because
            //      only reader can recycle chunks
            //      tryAppendBytes will only append to the end of the next chunk (only end offset is modified)
            //      only reader is modifying chunk.start offset during read so no concurrency
            // it is important to use chunks that have separate start and end offsets (such as ByteBuffer backed)
            // however using chunks that have start and length will break the world
            // accessing next.readRemaining is safe because it can only grow and only reader can reduce it
            current.writeBufferAppend(next, minOf(next.readRemaining, n - current.readRemaining))

            // since current is under lock then we can modify it
            headRemaining = current.readRemaining

            // after stealing several bytes the next chunk could become empty
            // so here we can try to release it in spite of that it is in the middle
            val released = !next.canRead() && releaseChunkInTheMiddle(current, next)

            if (current.readRemaining >= n) return current
            if (!released && !next.canRead()) {
                // if we failed to release it then most likely appender is in progress
                // so fallback and retry later
                // but we will retry bytes stealing if the next is not empty
                return null
            }
        } while (true)
    }

    override fun appendChain(chain: IoBuffer) {
        val chainSize = chain.remainingAll()
        val chainTail = chain.findTail()

        if (chainSize < PACKET_MAX_COPY_SIZE && chainTail === chain
            && this.tail.value.writeRemaining >= chain.readRemaining
        ) {
            if (tryAppendBytes(chain)) return
        }

        appendByRef(chain, chainTail, chainSize)
    }

    private fun appendByRef(chain: IoBuffer, chainTail: IoBuffer, chainSize: Long) {
        // invoked only by writer

        val dummy = dummy
        chainTail.next = dummy
        chainTail.markLocked() // temporarily lock tail

        do {
            val tail = this.tail.value

            // tail could be a chunk or dummy that is completely safe to set next to
            // if tail is pointing to dummy then there are no chunks available to reader
            // this is why it is safe to update next otherwise it could ruin the chain
            if (tail.casNext(dummy, chain)) {
                tailRemainingAtomic.addAndGet(chainSize)
                if (!this.tail.compareAndSet(tail, chainTail)) {
                    // this should never happen as far we have at least one chunk locked
                    // so reader will never pass it and therefore will never CAS tail to dummy
                    error("Tail disappeared so reader passed locked chain somehow")
                }
                chainTail.markUnlocked() // now we can unlock it since both references are published
                return
            }
        } while (true)
    }

    private fun tryAppendBytes(chunk: IoBuffer): Boolean {
        // invoked only by writer
        kotlinx.io.core.internal.require(chunk.next == null) { "It should be the only chunk" }

        var tail: IoBuffer
        do {
            tail = this.tail.value
            val version = tail.lockVersion

            if (tail.locked) return false
            if (this.tail.value !== tail) continue  // re-check tail to ensure we have valid version
            if (!tail.markLocked(version)) continue // try to lock with the specified version
            // changing tail will always change version as well
            break
        } while (true)

        try {
            val size = chunk.readRemaining
            tail.writeBufferAppend(chunk, size)
            if (tail === head) {
                // we generally can't check head as it is not volatile and modified by reader
                // however head is only moved forward
                // so if head is already pointing to the tail and we are already holding tail
                // then head couldn't be changed since we have it on-hold
                // therefore headRemaining is associated with head and tail
                // as long as we hold tail's lock
                headRemaining += size // this is why it is safe to modify headRemaining here
            }
        } finally {
            tail.markUnlocked()
        }

        kotlinx.io.core.internal.require(chunk.readRemaining == 0) { "chunk should be copied entirely" }

        chunk.release(pool) // chunk is not used concurrently so we can simply release it, no need to lock it
        return true
    }

}