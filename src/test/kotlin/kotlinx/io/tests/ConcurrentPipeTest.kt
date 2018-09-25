package kotlinx.io.tests

import kotlinx.io.core.*
import kotlinx.io.core.internal.*
import kotlin.test.*

@DangerousInternalIoApi
class ConcurrentPipeTest {
    val pipe = ConcurrentPipe(IoBuffer.Empty, IoBuffer.NoPool)

    @Test
    fun smokeTest1() {
        val p = buildPacket {
            writeInt(777)
        }

        pipe.appendChain(p.copy().stealAll()!!)
        assertEquals(4, pipe.remaining)
        assertEquals(777, pipe.readInt())

        pipe.appendChain(p.copy().stealAll()!!)
        assertEquals(4, pipe.remaining)
        assertEquals(777, pipe.readIntExample())

        p.release()
    }

    @Test
    fun smokeTest1a() {
        val p = buildPacket {
            writeInt(777)
        }

        pipe.appendChain(p.copy().stealAll()!!)
        pipe.appendChain(p.copy().stealAll()!!)

        assertEquals(8, pipe.remaining)
        assertEquals(777, pipe.readInt())
        assertEquals(777, pipe.readInt())

        p.release()
    }

    @Test
    fun smokeTest2() {
        val count = 10000

        val p = buildPacket {
            repeat(count) {
                writeInt(777)
            }
        }

        pipe.appendChain(p.copy().stealAll()!!)
        assertEquals(4L * count, pipe.remaining)
        repeat(count) {
            assertEquals(777, pipe.readInt())
        }

        pipe.appendChain(p.copy().stealAll()!!)
        assertEquals(4L * count, pipe.remaining)
        repeat(count) {
            assertEquals(777, pipe.readIntExample())
        }

        p.release()
    }

    @Test
    fun smokeTest3() {
        val count = 10000

        val p = buildPacket {
            repeat(count) {
                writeInt(it)
            }
        }

        pipe.appendChain(p.copy().stealAll()!!)
        pipe.appendChain(p.stealAll()!!)

        assertEquals(4L * 2 * count, pipe.remaining)

        repeat(count) {
            assertEquals(it, pipe.readInt())
        }
        repeat(count) {
            assertEquals(it, pipe.readInt())
        }
    }

    @Test
    fun testBytesCountReading() {
        try {
            pipe.appendChain(buildPacket { writeInt(111) }.stealAll()!!)

            assertEquals(4, pipe.bytesAppended)

            pipe.appendChain(buildPacket { writeLong(222L) }.stealAll()!!)

            assertEquals(12, pipe.bytesAppended)
            assertEquals(0, pipe.bytesRead)

            assertEquals(111, pipe.readInt())
            assertEquals(4, pipe.bytesRead)
            assertEquals(222L, pipe.readLong())
            assertEquals(12, pipe.bytesRead)
        } finally {
            pipe.release()
        }
    }

    @Test
    fun testBytesCountStealingSingle() {
        try {
            pipe.appendChain(buildPacket { writeInt(111) }.stealAll()!!)

            assertEquals(4, pipe.bytesAppended)

            pipe.appendChain(buildPacket { writeLong(222L) }.stealAll()!!)

            assertEquals(12, pipe.bytesAppended)
            assertEquals(0, pipe.bytesRead)

            val buffer = pipe.steal()
            assertNotNull(buffer)

            assertEquals(12, pipe.bytesRead)
        } finally {
            pipe.release()
        }
    }

    @Test
    fun testBytesCountStealingSingleFromMultiChain() {
        try {
            pipe.appendChain(buildPacket { writeInt(111); writeInt(112) }.stealAll()!!)

            assertEquals(8, pipe.bytesAppended)
            assertEquals(111, pipe.readInt())
            assertEquals(4, pipe.bytesRead)

            pipe.appendChain(buildPacket { writeLong(222L) }.stealAll()!!) // it will append one more chunk

            assertEquals(16, pipe.bytesAppended)
            assertEquals(4, pipe.bytesRead)

            val buffer = pipe.steal()
            assertNotNull(buffer)
            assertEquals(4, buffer.readRemaining)

            assertEquals(8, pipe.bytesRead)

            val buffer2 = pipe.steal()
            assertNotNull(buffer2)
            assertEquals(8, buffer2.readRemaining)

            assertEquals(16, pipe.bytesRead)
        } finally {
            pipe.release()
        }
    }

    @Test
    fun testBytesCountStealingMultiple() {
        try {
            pipe.appendChain(buildPacket { writeInt(111); writeInt(112) }.stealAll()!!)

            assertEquals(8, pipe.bytesAppended)
            assertEquals(111, pipe.readInt())
            assertEquals(4, pipe.bytesRead)

            pipe.appendChain(buildPacket { writeLong(222L) }.stealAll()!!) // it will append one more chunk

            assertEquals(16, pipe.bytesAppended)
            assertEquals(4, pipe.bytesRead)

            val buffer = pipe.stealAll()
            assertNotNull(buffer)

            assertEquals(16, pipe.bytesRead)
        } finally {
            pipe.release()
        }
    }
}