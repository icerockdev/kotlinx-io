package kotlinx.coroutines.io

import kotlin.coroutines.*

class DummyCoroutines {
    private var failure: Throwable? = null
    private val queue = ArrayList<Task<*>>()
    private val liveCoroutines = HashSet<CoroutineHead>()

    private inner class Completion(val head: CoroutineHead) : Continuation<Unit> {
        override val context: CoroutineContext
            get() = head

        override fun resumeWith(result: Result<Unit>) {
            liveCoroutines.remove(head)
            failure = result.exceptionOrNull()
            if (result.isSuccess) {
                process()
            }
        }
    }

    private fun launch(c: Continuation<Unit>) {
        ensureNotFailed()
        val name = c.context[CoroutineHead]!!
        liveCoroutines.add(name)

        queue += Task.Resume(c, Unit)
    }

    fun launch(name: String, block: suspend () -> Unit) {
        val head = CoroutineHead(name)
        launch(block.createCoroutine(Completion(head)))
    }

    fun launch(block: suspend () -> Unit) {
        launch(block.toString(), block)
    }

    suspend fun yield() {
        return suspendCoroutine { c ->
            ensureNotFailed()
            queue += Task.Resume(c, Unit)
        }
    }

    fun run() {
        if (liveCoroutines.isEmpty()) throw IllegalStateException("No coroutines has been scheduled")
        ensureNotFailed()

        process()
        failure?.let { throw it }
        if (liveCoroutines.isNotEmpty()) {
            throw IllegalStateException("There are suspended coroutines remaining: ${liveCoroutines.size}:\n" +
                    liveCoroutines.joinToString("\n") { it.name }
            )
        }
    }

    private fun process() {
        ensureNotFailed()

        while (queue.isNotEmpty()) {
            queue.removeAt(0).run()
            ensureNotFailed()
        }
    }

    private fun ensureNotFailed() {
        failure?.let { throw it }
    }

    private class CoroutineHead(val name: String) : CoroutineContext.Element {
        override val key = Key
        companion object Key : CoroutineContext.Key<CoroutineHead>
    }

    private sealed class Task<T>(val c: Continuation<T>) {
        abstract fun run()

        class Resume<T>(c: Continuation<T>, val value: T) : Task<T>(c) {
            override fun run() {
                c.resume(value)
            }
        }
    }
}
