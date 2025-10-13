import Either.Left
import Either.Right
import kotlin.also
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

// Directly copied from Arrow
sealed class Either<out A, out B> {
    fun <A, B> isLeft(): Boolean {
        /*
        contract {
            returns(true) implies (this@Either is Left<A>)
            returns(false) implies (this@Either is Right<B>)
        }
        */
        return this@Either is Left<*, *>
    }

    fun isRight(): Boolean {
        /*
        contract {
            returns(true) implies (this@Either is Right<B>)
            returns(false) implies (this@Either is Left<A>)
        }
        */
        return this@Either is Right<*, *>
    }

    data class Left<out A, out B>(val value: A) : Either<A, B>() {
        override fun toString(): String = "Either.Left($value)"
        public companion object
    }

    data class Right<out A, out B>(val value: B) : Either<A, B>() {
        override fun toString(): String = "Either.Right($value)"

        public companion object {
            @PublishedApi
            internal val unit: Either<Nothing, Unit> = Right(Unit)
        }
    }

    inline fun <C> map(f: (right: B) -> C): Either<A, C> {
        return flatMap { Right(f(it)) }
    }

    inline fun <C> mapLeft(f: (A) -> C): Either<C, B> {
        return when (this) {
            is Left -> Left(f(value))
            is Right -> Right(value)
        }
    }

    inline fun <C> fold(ifLeft: (left: A) -> C, ifRight: (right: B) -> C): C {
        return when (this) {
            is Right -> ifRight(value)
            is Left -> ifLeft(value)
        }
    }

    inline fun onLeft(action: (left: A) -> Unit): Either<A, B> {
        if (this is Left<*, *>) {
            @Suppress("UNCHECKED_CAST")
            action(this.value as A)
        }
        return this
    }

    inline fun onRight(action: (right: B) -> Unit): Either<A, B> {
        if (this is Right<*, *>) {
            @Suppress("UNCHECKED_CAST")
            action(this.value as B)
        }
        return this
    }
}

fun <A, B> left(a: A) = Left<A, B>(a)
fun <A, B> right(b: B) = Right<A, B>(b)

@Suppress("UNCHECKED_CAST")
inline fun <A, B, C> Either<A, B>.flatMap(f: (right: B) -> Either<A, C>): Either<A, C> {
    return when (this) {
        is Right<A, B> -> f(this.value) as Right<A, C>
        is Left<A, B> -> this as Left<A, C>
    }
}

private inline fun <T> T.also(block: (T) -> Unit): T {
    block(this)
    return this
}
