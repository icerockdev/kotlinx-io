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
}