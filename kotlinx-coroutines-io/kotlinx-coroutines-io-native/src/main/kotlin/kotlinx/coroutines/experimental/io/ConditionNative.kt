package kotlinx.coroutines.io

import kotlin.coroutines.*

internal actual class Condition actual constructor(val predicate: () -> Boolean) {
    private var cont: Continuation<Unit>? = null
    private var cancellation: Throwable? = null

    actual fun check(): Boolean {
        return predicate() || cancellation != null
    }

    actual fun signal() {
        val cont = cont
        if (cont != null && check()) {
            this.cont = null
            val cancellation = cancellation
            if (cancellation != null) {
                cont.resumeWith(Result.failure(cancellation))
            } else {
                cont.resume(Unit)
            }
        }
    }

    actual suspend fun await(block: () -> Unit) {
        checkCancelled()
        if (predicate()) return

        return suspendCoroutine { c ->
            cont = c
            block()
        }
    }

    actual suspend fun await() {
        checkCancelled()
        if (predicate()) return

        return suspendCoroutine { c ->
            cont = c
        }
    }

    actual fun cancel(cause: Throwable) {
        if (cancellation == null) {
            cancellation = cause
            signal()
        }
    }

    private fun checkCancelled() {
        cancellation?.let { throw it }
    }
}
