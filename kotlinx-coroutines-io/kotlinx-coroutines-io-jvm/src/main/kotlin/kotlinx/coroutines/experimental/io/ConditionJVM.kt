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

    @PublishedApi
    internal fun checkOrFail(): Boolean {
        checkFailure()
        return predicate()
    }

    @PublishedApi
    internal fun checkFailure() {
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
            if (!compareAndSet(null, c)) concurrentAwaitIsNotSupported()
            if (check() && compareAndSet(c, null)) {
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

    @PublishedApi
    internal fun concurrentAwaitIsNotSupported(): Nothing {
        throw IllegalStateException("Concurrent await is not supported")
    }

    @PublishedApi
    internal fun compareAndSet(expected: Continuation<Unit>?, newValue: Continuation<Unit>?): Boolean {
        return updater.compareAndSet(this, expected, newValue)
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
