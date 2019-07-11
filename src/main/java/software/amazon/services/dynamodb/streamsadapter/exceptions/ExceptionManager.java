package software.amazon.services.dynamodb.streamsadapter.exceptions;

import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 *
 */
public class ExceptionManager {
    private final Map<Class<? extends Throwable>, Function<? extends Throwable, RuntimeException>>
        map =
        new HashMap<>();

    @Setter @Accessors(fluent = true) private Function<Throwable, RuntimeException>
        defaultFunction =
        RuntimeException::new;

    public <T extends Throwable> void add(@NonNull final Class<T> clazz,
        @NonNull final Function<T, RuntimeException> function) {
        map.put(clazz, function);
    }

    @SuppressWarnings("unchecked")
    private Function<? extends Throwable, RuntimeException> handleFor(@NonNull final Throwable t) {
        Class<? extends Throwable> clazz = t.getClass();
        Optional<Function<? extends Throwable, RuntimeException>> toApply = Optional.ofNullable(map.get(clazz));
        while (!toApply.isPresent() && clazz.getSuperclass() != null) {
            clazz = (Class<? extends Throwable>) clazz.getSuperclass();
            toApply = Optional.ofNullable(map.get(clazz));
        }

        return toApply.orElse(defaultFunction);
    }

    @SuppressWarnings("unchecked") public RuntimeException apply(Throwable t) {
        //
        // We know this is safe as the handler guarantees that the function we get will be able to accept the actual
        // ty pe of the throwable.  handlerFor walks up the inheritance chain so we can't get a function more specific
        // than the actual type of the throwable only.
        //
        Function<Throwable, ? extends RuntimeException>
            f =
            (Function<Throwable, ? extends RuntimeException>) handleFor(t);
        return f.apply(t);
    }

}
