package kotlinx.coroutines.io

internal expect class Condition(predicate: () -> Boolean) {
    /**
     * @return `true` if the condition met or cancelled
     */
    fun check(): Boolean

    /**
     * Await for the condition
     */
    suspend fun await()

    /**
     * Await for the condition and execute [block] in case of suspension
     */
    suspend fun await(block: () -> Unit)

    /**
     * Signal that the condition could be `true` and if so resume [await] suspension
     */
    fun signal()

    /**
     * Cancel all current and future [await] function invocations.
     * After this function invocation [check] function will always return `true` and all [await] will
     * always fail with the specified [cause]
     */
    fun cancel(cause: Throwable)
}
