package kotlinx.io.core

import kotlinx.io.core.internal.*
import kotlinx.io.pool.*

/**
 * Read-only immutable byte packet. Could be consumed only once however it does support [copy] that doesn't copy every byte
 * but creates a new view instead. Once packet created it should be either completely read (consumed) or released
 * via [release].
 */
abstract class ByteReadPacketBase constructor(val pool: ObjectPool<IoBuffer>) : Input {
    final override var byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
        set(newOrder) {
            field = newOrder
            head.byteOrder = newOrder
        }

    protected abstract var headRemaining: Int

    protected abstract val tailRemaining: Long

    @PublishedApi
    internal abstract var head: IoBuffer

    /**
     * Number of bytes available for read
     */
    val remaining: Long get() = headRemaining.toLong() + tailRemaining

    @Deprecated("For compatibility purpose", level = DeprecationLevel.HIDDEN)
    final fun getRemaining(): Int = remaining.coerceAtMostMaxInt()

    /**
     * @return `true` if there is at least one byte to read
     */
    fun canRead() = tailRemaining != 0L || head.canRead()

    /**
     * @return `true` if there are at least [n] bytes to read
     */
    fun hasBytes(n: Int) = headRemaining + tailRemaining >= n

    /**
     * @return `true` if there are at least [n] bytes available for concurrent-safe reading
     */
    @DangerousInternalIoApi
    fun hasFastpathBytes(n: Int) = headRemaining > n

    /**
     * `true` if no bytes available for read
     */
    val isEmpty: Boolean
        get() = headRemaining == 0 && tailRemaining == 0L

    val isNotEmpty: Boolean
        get() = headRemaining > 0 || tailRemaining > 0L

    private var noMoreChunksAvailable = false

    override val endOfInput: Boolean
        get() = isEmpty && (noMoreChunksAvailable || doFill() == null)

    /**
     * Returns a copy of the packet. The original packet and the copy could be used concurrently. Both need to be
     * either completely consumed or released via [release]
     */
    open fun copy(): ByteReadPacket = ByteReadPacket(head.copyAll(), remaining, pool)

    /**
     * Release packet. After this function invocation the packet becomes empty. If it has been copied via [copy]
     * then the copy should be released as well.
     */
    abstract fun release()

    override fun close() {
        release()
        if (!noMoreChunksAvailable) {
            noMoreChunksAvailable = true
        }
        closeSource()
    }

    @DangerousInternalIoApi
    internal abstract fun stealAll(): IoBuffer?

    @DangerousInternalIoApi
    internal abstract fun steal(): IoBuffer?

    @DangerousInternalIoApi
    internal fun tryPrepareContinuousRegionAtLeast(n: Int): Boolean {
        val head = prepareRead(minOf(n, 8)) ?: return false
        return head.readRemaining >= n
    }

    @DangerousInternalIoApi
    internal fun tryPrepareContinuousRegionAtLeast(n: Long): Boolean {
        val head = prepareRead(minOf(n, 8L).toInt()) ?: return false
        return head.readRemaining >= n
    }

    @DangerousInternalIoApi
    internal fun tryPrepareContinuousRegionAtMost(n: Int): Boolean {
        val head = prepareRead(minOf(n, 8)) ?: return false
        return head.readRemaining <= n
    }

    @DangerousInternalIoApi
    internal fun tryPrepareContinuousRegionAtMost(n: Long): Boolean {
        val head = prepareRead(minOf(n, 8L).toInt()) ?: return false
        return head.readRemaining <= n
    }

    final override fun readByte(): Byte {
        val headRemaining = headRemaining
        if (headRemaining > 1) {
            this.headRemaining = headRemaining - 1
            return head.readByte()
        }

        return readByteSlow2()
    }

    private fun readByteSlow2(): Byte {
        val head = head
        val headRemaining = headRemaining

        if (headRemaining == 1) {
            this.headRemaining = headRemaining - 1
            return head.readByte().also { ensureNext(head) }
        } else {
            return readByteSlow(head)
        }
    }

    private fun readByteSlow(head: IoBuffer): Byte {
        ensureNext(head) ?: throw EOFException("One more byte required but reached end of input")
        return readByte()
    }

    final override fun readShort() = readN(2) { readShort() }
    final override fun readFloat() = readN(4) { readFloat() }
    final override fun readDouble() = readN(8) { readDouble() }

    final override fun readInt(): Int {
        val headRemaining = headRemaining
        if (headRemaining > 4) {
            this.headRemaining = headRemaining - 4
            return head.readInt()
        }

        return readIntSlow()
    }

    private fun readIntSlow(): Int = readN(4) { readInt() }

    final override fun readLong(): Long {
        val headRemaining = headRemaining
        if (headRemaining > 8) {
            this.headRemaining = headRemaining - 8
            return head.readLong()
        }

        return readLongSlow()
    }

    private fun readLongSlow(): Long = readN(8) { readLong() }

    /**
     * Read as much bytes as possible to [dst] array
     * @return number of bytes copied
     */
    fun readAvailable(dst: ByteArray): Int = readAvailable(dst, 0, dst.size)

    /**
     * Read at most [length] bytes to [dst] array and write them at [offset]
     * @return number of bytes copied to the array
     */
    final override fun readAvailable(dst: ByteArray, offset: Int, length: Int): Int {
        require(offset >= 0) { "offset shouldn't be negative: $offset" }
        require(length >= 0) { "length shouldn't be negative: $length" }
        require(offset + length <= dst.size) { "offset ($offset) + length ($length) > dst.size (${dst.size})" }

        return readAsMuchAsPossible(dst, offset, length, 0)
    }

    /**
     * Read exactly [length] bytes to [dst] array at specified [offset]
     */
    final override fun readFully(dst: ByteArray, offset: Int, length: Int) {
        val rc = readAvailable(dst, offset, length)
        if (rc != length) throw EOFException("Not enough data in packet to fill buffer: ${length - rc} more bytes required")
    }

    /**
     * Discards at most [n] bytes
     * @return number of bytes has been discarded
     */
    fun discard(n: Int) = discardAsMuchAsPossible(n, 0)

    /**
     * Discards exactly [n] bytes or fails with [EOFException]
     */
    fun discardExact(n: Int) {
        if (discard(n) != n) throw EOFException("Unable to discard $n bytes due to end of packet")
    }

    @PublishedApi
    internal inline fun read(block: (IoBuffer) -> Unit) {
        read(1, block)
    }

    @PublishedApi
    internal inline fun read(n: Int, block: (IoBuffer) -> Unit) {
        val head = head
        var before = head.readRemaining
        val buffer = if (before < n) {
            prepareRead(n, head).also { before = it?.readRemaining ?: 0 }
        } else {
            head
        }

        if (buffer != null) {
            block(buffer)
            val after = buffer.readRemaining
            val delta = before - after
            if (delta > 0) {
                `$updateRemaining$`(after)
            }
            if (after == 0) {
                ensureNext(buffer)
            }
        }
    }

    final override fun readFully(dst: ShortArray, offset: Int, length: Int) {
        if (remaining < length * 2) throw IllegalArgumentException("Not enough bytes available ($remaining) to read $length short integers")

        var copied = 0
        takeWhile { buffer ->
            val rc = buffer.readAvailable(dst, offset + copied, length - copied)
            if (rc == -1) throw EOFException("Unexpected EOF while reading $length bytes")
            copied += rc
            copied < length
        }
    }

    final override fun readAvailable(dst: ShortArray, offset: Int, length: Int): Int {
        val remaining = remaining
        if (remaining == 0L) return -1
        val size = minOf(remaining, length.toLong()).toInt()
        readFully(dst, offset, size)
        return size
    }

    final override fun readFully(dst: IntArray, offset: Int, length: Int) {
        if (remaining < length * 4) throw IllegalArgumentException("Not enough bytes available ($remaining) to read $length integers")

        var copied = 0
        takeWhile { buffer ->
            val rc = buffer.readAvailable(dst, offset + copied, length - copied)
            if (rc == -1) throw EOFException("Unexpected EOF while read $length short integers")
            copied += rc
            copied < length
        }
    }

    final override fun readAvailable(dst: IntArray, offset: Int, length: Int): Int {
        val remaining = remaining
        if (remaining == 0L) return -1
        val size = minOf(remaining, length.toLong()).toInt()
        readFully(dst, offset, size)
        return size
    }

    final override fun readFully(dst: LongArray, offset: Int, length: Int) {
        if (remaining < length * 8) throw IllegalArgumentException("Not enough bytes available ($remaining) to read $length long integers")

        var copied = 0
        takeWhile { buffer ->
            val rc = buffer.readAvailable(dst, offset + copied, length - copied)
            if (rc == -1) throw EOFException("Unexpected EOF while reading $length long integers")
            copied += rc
            copied < length
        }
    }

    final override fun readAvailable(dst: LongArray, offset: Int, length: Int): Int {
        val remaining = remaining
        if (remaining == 0L) return -1
        val size = minOf(remaining, length.toLong()).toInt()
        readFully(dst, offset, size)
        return size
    }

    final override fun readFully(dst: FloatArray, offset: Int, length: Int) {
        if (remaining < length * 4) throw IllegalArgumentException("Not enough bytes available ($remaining) to read $length float numbers")

        var copied = 0
        takeWhile { buffer ->
            val rc = buffer.readAvailable(dst, offset + copied, length - copied)
            if (rc == -1) throw EOFException("Unexpected EOF while read $length float number")
            copied += rc
            copied < length
        }
    }

    final override fun readAvailable(dst: FloatArray, offset: Int, length: Int): Int {
        val remaining = remaining
        if (remaining == 0L) return -1
        val size = minOf(remaining, length.toLong()).toInt()
        readFully(dst, offset, size)
        return size
    }

    final override fun readFully(dst: DoubleArray, offset: Int, length: Int) {
        if (remaining < length.toLong() * 8) throw IllegalArgumentException("Not enough bytes available ($remaining) to read $length double float numbers")

        var copied = 0
        takeWhile { buffer ->
            val rc = buffer.readAvailable(dst, offset + copied, length - copied)
            if (rc == -1) throw EOFException("Unexpected EOF while reading $length double float numbers")
            copied += rc
            copied < length
        }
    }

    final override fun readAvailable(dst: DoubleArray, offset: Int, length: Int): Int {
        val remaining = remaining
        if (remaining == 0L) return -1
        val size = minOf(remaining, length.toLong()).toInt()
        readFully(dst, offset, size)
        return size
    }

    final override fun readFully(dst: IoBuffer, length: Int) {
        if (remaining < length) throw IllegalArgumentException("Not enough bytes available ($remaining) to read $length bytes")
        require(length <= dst.writeRemaining) { "Not enough free space in destination buffer to write $length bytes" }

        var copied = 0
        takeWhile { buffer ->
            val rc = buffer.readAvailable(dst, length - copied)
            if (rc > 0) copied += rc
            copied < length
        }
    }

    final override fun readAvailable(dst: IoBuffer, length: Int): Int {
        val remaining = remaining
        if (remaining == 0L) return -1
        val size = minOf(remaining, length.toLong(), dst.writeRemaining.toLong()).toInt()
        readFully(dst, size)
        return size
    }

    /*
     * Returns next byte (unsigned) or `-1` if no more bytes available
     */
    final override fun tryPeek(): Int {
        if (headRemaining > 1) {
            return head.tryPeek()
        }

        return tryPeekSlow()
    }

    private fun tryPeekSlow(): Int {
        if (tailRemaining == 0L && noMoreChunksAvailable) return -1
        val head = prepareRead(1) ?: return -1
        val value = head.tryPeek()
        if (!head.canRead()) {
            releaseHead(head)
        }
        return value
    }

    final override fun discard(n: Long): Long {
        return discardAsMuchAsPossible(minOf(Int.MAX_VALUE.toLong(), n).toInt(), 0).toLong()
    }

    internal fun readCbuf(cbuf: CharArray, off: Int, len: Int): Int {
        if (isEmpty) return -1

        val out = object : Appendable {
            private var idx = off

            override fun append(c: Char): Appendable {
                cbuf[idx++] = c
                return this
            }

            override fun append(csq: CharSequence?): Appendable {
                if (csq is String) {
                    csq.getCharsInternal(cbuf, idx)
                    idx += csq.length
                } else if (csq != null) {
                    for (i in 0 until csq.length) {
                        cbuf[idx++] = csq[i]
                    }
                }

                return this
            }

            override fun append(csq: CharSequence?, start: Int, end: Int): Appendable {
                throw UnsupportedOperationException()
            }
        }

        return readText(out, 0, len)
    }

    /**
     * Read at least [min] and at most [max] characters and append them to [out]
     * @return number of characters appended
     */
    fun readText(out: Appendable, min: Int = 0, max: Int = Int.MAX_VALUE): Int {
        if (max.toLong() >= remaining) {
            val s = readTextExactBytes(bytes = remaining.toInt())
            out.append(s)
            return s.length
        }
        return readASCII(out, min, max)
    }

    /**
     * Read exactly [exactCharacters] characters and append them to [out]
     */
    fun readTextExact(out: Appendable, exactCharacters: Int) {
        readText(out, exactCharacters, exactCharacters)
    }

    /**
     * Read a string at last [min] and at most [max] characters length
     */
    fun readText(min: Int = 0, max: Int = Int.MAX_VALUE): String {
        if (min == 0 && (max == 0 || isEmpty)) return ""
        if (max.toLong() >= remaining) return readTextExactBytes(bytes = remaining.toInt())

        return buildString(min.coerceAtLeast(16).coerceAtMost(max)) {
            readASCII(this, min, max)
        }
    }

    /**
     * Read a string exactly [exactCharacters] length
     */
    fun readTextExact(exactCharacters: Int): String {
        return readText(exactCharacters, exactCharacters)
    }

    private fun readASCII(out: Appendable, min: Int, max: Int): Int {
        when {
            max == 0 && min == 0 -> return 0
            isEmpty -> if (min == 0) return 0 else atLeastMinCharactersRequire(min)
            max < min -> minShouldBeLess(min, max)
        }

        var copied = 0
        var utf8 = false

        takeWhile { buffer ->
            val rc = buffer.decodeASCII {
                if (copied == max) false
                else {
                    out.append(it)
                    copied++
                    true
                }
            }

            when {
                rc -> true
                copied == max -> false
                else -> {
                    utf8 = true
                    false
                }
            }
        }

        if (utf8) {
            return copied + readUtf8(out, min - copied, max - copied)
        }
        if (copied < min) prematureEndOfStreamChars(min, copied)
        return copied
    }

    private fun atLeastMinCharactersRequire(min: Int): Nothing =
        throw EOFException("at least $min characters required but no bytes available")

    private fun minShouldBeLess(min: Int, max: Int): Nothing =
        throw IllegalArgumentException("min should be less or equal to max but min = $min, max = $max")

    private fun prematureEndOfStreamChars(min: Int, copied: Int): Nothing = throw MalformedUTF8InputException(
        "Premature end of stream: expected at least $min chars but had only $copied"
    )

    private fun prematureEndOfStream(size: Int): Nothing =
        throw MalformedUTF8InputException("Premature end of stream: expected $size bytes")

    private fun readUtf8(out: Appendable, min: Int, max: Int): Int {
        var copied = 0

        takeWhileSize { buffer ->
            val size = buffer.decodeUTF8 {
                if (copied == max) false
                else {
                    out.append(it)
                    copied++
                    true
                }
            }

            when {
                size == 0 -> 1
                size > 0 -> size
                else -> 0
            }
        }

        if (copied < min) prematureEndOfStreamChars(min, copied)

        return copied
    }

    private tailrec fun discardAsMuchAsPossible(n: Int, skipped: Int): Int {
        if (n == 0) return skipped
        val current = prepareRead(1) ?: return skipped
        val size = minOf(current.readRemaining, n)
        current.discardExact(size)
        headRemaining -= size
        afterRead()

        return discardAsMuchAsPossible(n - size, skipped + size)
    }

    private tailrec fun readAsMuchAsPossible(array: ByteArray, offset: Int, length: Int, copied: Int): Int {
        if (length == 0) return copied
        val current = prepareRead(1) ?: return copied
        val size = minOf(length, current.readRemaining)

        current.readFully(array, offset, size)
        headRemaining -= size

        return if (size != length || current.readRemaining == 0) {
            afterRead()
            readAsMuchAsPossible(array, offset + size, length - size, copied + size)
        } else {
            copied + size
        }
    }

    private inline fun <R> readN(n: Int, block: IoBuffer.() -> R): R {
        val bb = prepareRead(n) ?: notEnoughBytesAvailable(n)
        val rc = block(bb)

        val after = bb.readRemaining
        if (after == 0) {
            ensureNext(bb)
        } else {
            headRemaining = after
        }

        return rc
    }

    private fun notEnoughBytesAvailable(n: Int): Nothing {
        throw EOFException("Not enough data in packet ($remaining) to read $n byte(s)")
    }

    @DangerousInternalIoApi
    final override fun `$updateRemaining$`(remaining: Int) {
        headRemaining = remaining
    }

    @DangerousInternalIoApi
    final override fun `$prepareRead$`(minSize: Int): IoBuffer? = prepareRead(minSize, head)

    @DangerousInternalIoApi
    final override fun `$ensureNext$`(current: IoBuffer): IoBuffer? = ensureNext(current)

    @PublishedApi
    internal fun ensureNext(current: IoBuffer) = ensureNext(current, IoBuffer.Empty)

    protected abstract fun ensureNext(current: IoBuffer, empty: IoBuffer): IoBuffer?

    /**
     * Reads the next chunk suitable for reading or `null` if no more chunks available
     */
    protected abstract fun fill(): IoBuffer?

    protected abstract fun closeSource()

    protected fun doFill(): IoBuffer? {
        if (noMoreChunksAvailable) return null
        val chunk = fill()
        if (chunk == null) {
            noMoreChunksAvailable = true
            return null
        }
        appendChain(chunk)
        return chunk
    }

    @DangerousInternalIoApi
    abstract fun appendChain(chain: IoBuffer)

    @Suppress("NOTHING_TO_INLINE")
    internal inline fun prepareRead(minSize: Int): IoBuffer? = prepareRead(minSize, head)

    @PublishedApi
    internal abstract fun prepareRead(minSize: Int, head: IoBuffer): IoBuffer?

    private fun minSizeIsTooBig(minSize: Int): Nothing {
        throw IllegalStateException("minSize of $minSize is too big (should be less than $ReservedSize")
    }

    @Deprecated("")
    private fun afterRead() {
        val head = head
        if (head.readRemaining == 0) {
            releaseHead(head)
        }
    }

    internal abstract fun releaseHead(head: IoBuffer)

    companion object {
        val Empty: ByteReadPacket = ByteReadPacket(IoBuffer.Empty, object : NoPoolImpl<IoBuffer>() {
            override fun borrow() = IoBuffer.Empty
        })

        val ReservedSize: Int = 8
    }
}

expect class EOFException(message: String) : Exception
