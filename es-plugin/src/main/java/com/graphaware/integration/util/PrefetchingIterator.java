package com.graphaware.integration.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class PrefetchingIterator<T> implements Iterator<T>
{

  boolean hasFetchedNext;
  T nextObject;

  /**
   * Tries to fetch the next item and caches it so that consecutive calls (w/o
   * an intermediate call to {@link #next()} will remember it and won't try to
   * fetch it again.
   *
   * @return {@code true} if there was a next item to return in the next call to
   * {@link #next()}.
   */
  @Override
  public boolean hasNext()
  {
    if (hasFetchedNext)
    {
      return getPrefetchedNextOrNull() != null;
    }

    T nextOrNull = fetchNextOrNull();
    hasFetchedNext = true;
    if (nextOrNull != null)
    {
      setPrefetchedNext(nextOrNull);
    }
    return nextOrNull != null;
  }

  /**
   * Uses {@link #hasNext()} to try to fetch the next item and returns it if
   * found, otherwise it throws a {@link NoSuchElementException}.
   *
   * @return the next item in the iteration, or throws
   * {@link NoSuchElementException} if there's no more items to return.
   */
  @Override
  public T next()
  {
    if (!hasNext())
    {
      throw new NoSuchElementException();
    }
    T result = getPrefetchedNextOrNull();
    setPrefetchedNext(null);
    hasFetchedNext = false;
    return result;
  }

  protected abstract T fetchNextOrNull();

  protected void setPrefetchedNext(T nextOrNull)
  {
    this.nextObject = nextOrNull;
  }

  protected T getPrefetchedNextOrNull()
  {
    return nextObject;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
