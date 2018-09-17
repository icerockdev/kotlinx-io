package kotlinx.coroutines.experimental.io

import kotlinx.coroutines.*
import kotlinx.io.core.*
import java.nio.*
import kotlin.random.*
import kotlin.test.*

class JvmConcurrentSmokeTest {
    val count = 1_000_000
    val ch = ByteChannelSequentialJVM(IoBuffer.Empty, true)
    val dispatcher = Dispatchers.Default

    @Test
    fun testConcurrentReadWritePacket7(): Unit = runBlocking<Unit> {
        launch(dispatcher + CoroutineName("writer")) {
            val pkt = buildPacket {
                repeat(7) {
                    writeInt(it)
                }
            }

            pkt.use {
                repeat(count) {
                    ch.writePacket(pkt.copy())
                }
            }

            ch.close()
        }

        launch(dispatcher + CoroutineName("reader")) {
            repeat(count) {
                ch.readPacket(7 * 4).use { pkt ->
                    assertEquals(7 * 4, pkt.remaining)
                }
            }

            assertEquals(-1, ch.readAvailable(ByteArray(1)))
        }
    }

    @Test
    fun testConcurrentReadWritePacket900(): Unit = runBlocking<Unit> {
        launch(dispatcher + CoroutineName("writer")) {
            val pkt = buildPacket {
                repeat(900) {
                    writeInt(it)
                }
            }

            pkt.use {
                repeat(count) {
                    ch.writePacket(pkt.copy())
                }
            }

            ch.close()
        }

        launch(dispatcher + CoroutineName("reader")) {
            repeat(count) {
                ch.readPacket(900 * 4).use { pkt ->
                    assertEquals(900 * 4, pkt.remaining)
                }
            }

            assertEquals(-1, ch.readAvailable(ByteArray(1)))
        }
    }

    @Test
    fun testReadFully(): Unit = runBlocking<Unit> {
        val buffer = ByteBuffer.allocate(900 * 4)!!
        Random.nextBytes(buffer.array())

        launch(dispatcher + CoroutineName("writer")) {
            repeat(count) {
                buffer.clear()
                ch.writeFully(buffer)
                ch.flush()
            }

            ch.close()
        }

        launch(dispatcher + CoroutineName("reader")) {
            val bb = ByteBuffer.allocate(8192)
            var remaining = 900L * 4 * count

            while (remaining > 0) {
                bb.clear()
                if (ch.readAvailable(bb) == -1) break
                bb.flip()
                remaining -= bb.remaining()
            }

            assertEquals(-1, ch.readAvailable(ByteArray(1)))
        }
    }

}