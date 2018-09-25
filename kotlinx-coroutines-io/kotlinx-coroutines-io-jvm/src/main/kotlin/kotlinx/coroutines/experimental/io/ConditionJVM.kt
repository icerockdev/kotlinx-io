package kotlinx.coroutines.io

import java.util.concurrent.atomic.*
import kotlin.coroutines.*
import kotlin.coroutines.intrinsics.*
import kotlin.jvm.*

internal actual class Condition actual constructor(private val predicate: () -> Boolean) {
    @Volatile
    private var cond: Continuation<Unit>? = null

    @Volatile
    private var failure: Throwable? = null

    actual fun check(): Boolean {
        return failure != null || predicate()
    }

    private fun checkOrFail(): Boolean {
        checkFailure()
        return predicate()
    }

    private fun checkFailure() {
        failure?.let { throw it }
    }

    actual fun signal() {
        val cond = cond
        if (cond != null && check()) {
            if (updater.compareAndSet(this, cond, null)) {
                val failure = failure
                if (failure != null) {
                    cond.intercepted().resumeWithException(failure)
                } else {
                    cond.intercepted().resume(Unit)
                }
            }
        }
    }

    actual fun cancel(cause: Throwable) {
        failure = cause
        signal()
    }

    actual suspend inline fun await(crossinline block: () -> Unit) {
        if (checkOrFail()) return

        return suspendCoroutineUninterceptedOrReturn { c ->
            if (!updater.compareAndSet(this, null, c)) concurrentAwaitIsNotSupported()
            if (check() && updater.compareAndSet(this, c, null)) {
                checkFailure()
                return@suspendCoroutineUninterceptedOrReturn Unit
            }
            block()
            COROUTINE_SUSPENDED
        }
    }

    actual suspend inline fun await() {
        return await { }
    }

    private fun concurrentAwaitIsNotSupported(): Nothing {
        throw IllegalStateException("Concurrent await is not supported")
    }

    override fun toString(): String {
        return "Condition(cond=$cond)"
    }

    companion object {
        @Suppress("UNCHECKED_CAST")
        private val updater = AtomicReferenceFieldUpdater.newUpdater<Condition, Continuation<*>>(Condition::class.java,
            Continuation::class.java, "cond") as AtomicReferenceFieldUpdater<Condition, Continuation<Unit>?>
    }
}
