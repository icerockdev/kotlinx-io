package kotlinx.io.core

import java.lang.IllegalStateException
import java.util.concurrent.atomic.*


private class T {
    @Volatile
    private var ic: VersionedLock = VersionedLock()

    val locked: Boolean get() = ic.locked

    fun markLocked() {
        while (true) {
            val ic = ic
            if (ic.locked) throw IllegalStateException("Already locked")
            val newState = ic.locked()
            if (icUpdater.compareAndSet(this, ic.value, newState.value)) break
        }
    }

    fun markUnlocked() {
        while (true) {
            val ic = ic
            if (!ic.locked) throw IllegalStateException("Not locked")
            val newState = ic.unlocked()
            if (icUpdater.compareAndSet(this, ic.value, newState.value)) break
        }
    }

    companion object {
        private val icUpdater = AtomicIntegerFieldUpdater.newUpdater(T::class.java, T::ic.name)!!
    }
}

@Suppress("EXPERIMENTAL_FEATURE_WARNING")
internal inline class VersionedLock(val value: Int) {
    val version: Int get() = value ushr 1
    val locked: Boolean get() = value and 1 != 0

    fun locked(): VersionedLock = VersionedLock(value = (version + 1) shl 1 or 1)
    fun unlocked(): VersionedLock = VersionedLock(value = (version + 1) shl 1)
}

//    constructor() : this(0) // TODO doesn't work due to KT-26558 so we use a function instead
@Suppress("NOTHING_TO_INLINE")
internal inline fun VersionedLock(): VersionedLock = VersionedLock(0)
