using System.Runtime.CompilerServices;

namespace HoffmannQueue;

/// <summary>
/// An entry of a <see cref="SequentialQueue{T}"/>: the sequence number and the stored object.
/// </summary>
public readonly record struct SequencedItem<T>(long SequenceNumber, T Item);

/// <summary>
/// Thrown when an operation cannot succeed because <see cref="SequentialQueue{T}.MarkAsComplete"/> was called.
/// </summary>
public sealed class SequentialQueueCompletedException : InvalidOperationException
{
    public SequentialQueueCompletedException()
        : base("The sequential queue has been completed.")
    {
    }
}

public interface ISequentialQueue<T> where T : class
{
    int Capacity { get; }
    int Count { get; }
    bool IsCompleted { get; }
    long NextSequenceNumber { get; }

    ValueTask EnqueueAsync(long sequenceNumber, T item, CancellationToken cancellationToken = default);
    bool TryEnqueue(long sequenceNumber, T item);

    ValueTask<SequencedItem<T>> DequeueAsync(CancellationToken cancellationToken = default);
    bool TryDequeue(out SequencedItem<T> item);

    void MarkAsComplete();

    IAsyncEnumerable<SequencedItem<T>> ReadAllAsync(CancellationToken cancellationToken = default);
}

public sealed class SequentialQueue<T> :
    IDisposable, ISequentialQueue<T> where T : class
{
    private readonly Lock lockObject = new();
    private readonly int capacity;
    private long nextSequence;
    private bool completed = false;

    private readonly Dictionary<long, T> items = [];

    private readonly List<(long SequenceNumber, TaskCompletionSource Signal)> enqueueWaiters = [];
    private readonly List<TaskCompletionSource> dequeueWaiters = [];

    public int Capacity => capacity;

    public int Count { get { lock (lockObject) { return items.Count; } } }

    public bool IsCompleted { get { lock (lockObject) { return completed; } } }

    public long NextSequenceNumber { get { lock (lockObject) { return nextSequence; } } }

    public SequentialQueue(int capacity, long firstSequenceNumber = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(firstSequenceNumber, 1L);

        this.capacity = capacity;
        nextSequence = firstSequenceNumber;
    }

    #region Enqueue
    /// <summary>
    /// Enqueues the item, waiting while the sequence number is beyond the capacity window.
    /// A producer that holds back an earlier sequence number while waiting for a far-ahead one can deadlock itself.
    /// </summary>
    public async ValueTask EnqueueAsync(long sequenceNumber, T item, CancellationToken cancellationToken = default)
    {
        ValidateSequenceItem(sequenceNumber, item);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            TaskCompletionSource signal;
            lock (lockObject)
            {
                if (TryEnqueueCore(sequenceNumber, item))
                {
                    SignalWaiters();
                    return;
                }

                signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                enqueueWaiters.Add((sequenceNumber, signal));
            }

            try
            {
                await signal.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                lock (lockObject)
                {
                    enqueueWaiters.RemoveAll(w => w.Signal == signal);
                }
            }
        }
    }

    public bool TryEnqueue(long sequenceNumber, T item)
    {
        ValidateSequenceItem(sequenceNumber, item);

        lock (lockObject)
        {
            var added = TryEnqueueCore(sequenceNumber, item);
            if (added)
            {
                SignalWaiters();
            }
            return added;
        }
    }

    private bool TryEnqueueCore(long sequenceNumber, T item)
    {
        lock (lockObject)
        {
            ObjectDisposedException.ThrowIf(disposedValue, this);

            if (completed)
            {
                throw new SequentialQueueCompletedException();
            }

            if (sequenceNumber < nextSequence || items.ContainsKey(sequenceNumber))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(sequenceNumber),
                    sequenceNumber,
                    $"Sequence number {sequenceNumber} is/was already in the queue. Next expected sequence number is {nextSequence}.");
            }

            if (!IsSequenceNumberInWindow(sequenceNumber))
            {
                return false;
            }

            if (!items.TryAdd(sequenceNumber, item))
            {
                throw new InvalidOperationException($"Failed to add item with sequence number {sequenceNumber} to the queue.");
            }
            return true;
        }
    }
    #endregion

    #region Dequeue
    /// <summary>
    /// Dequeues the next item in sequence, waiting until it is available.
    /// Throws <see cref="SequentialQueueCompletedException"/> once the queue is completed and the next item is missing.
    /// </summary>
    public async ValueTask<SequencedItem<T>> DequeueAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            TaskCompletionSource signal;
            lock (lockObject)
            {
                if (TryDequeue(out var item))
                {
                    return item;
                }

                if (completed)
                {
                    throw new SequentialQueueCompletedException();
                }

                signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                dequeueWaiters.Add(signal);
            }

            try
            {
                await signal.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                lock (lockObject)
                {
                    dequeueWaiters.Remove(signal);
                }
            }
        }
    }

    public bool TryDequeue(out SequencedItem<T> item)
    {
        lock (lockObject)
        {
            ObjectDisposedException.ThrowIf(disposedValue, this);

            item = default;
            if (items.Remove(nextSequence, out var value))
            {
                item = new SequencedItem<T>(nextSequence++, value);
                SignalWaiters();
                return true;
            }

            return false;
        }
    }
    #endregion

    public void MarkAsComplete()
    {
        lock (lockObject)
        {
            ObjectDisposedException.ThrowIf(disposedValue, this);
            completed = true;
            SignalWaiters();
        }
    }

    public async IAsyncEnumerable<SequencedItem<T>> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (true)
        {
            SequencedItem<T> item;
            try
            {
                item = await DequeueAsync(cancellationToken);
            }
            catch (SequentialQueueCompletedException)
            {
                yield break;
            }
            yield return item;
        }
    }

    private static void ValidateSequenceItem(long sequenceNumber, T item)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(sequenceNumber, 1L);
        ArgumentNullException.ThrowIfNull(item);
    }


    // Must be called while holding lockObject.
    private void SignalWaiters()
    {
        var finished = completed || disposedValue;

        if (finished || items.ContainsKey(nextSequence))
        {
            foreach (var waiter in dequeueWaiters)
            {
                waiter.TrySetResult();
            }
            dequeueWaiters.Clear();
        }

        enqueueWaiters.RemoveAll(w =>
        {
            if (finished || IsSequenceNumberInWindow(w.SequenceNumber))
            {
                w.Signal.TrySetResult();
                return true;
            }
            return false;
        });
    }

    private bool IsSequenceNumberInWindow(long sequenceNumber)
    {
        lock (lockObject)
        {
            return sequenceNumber < nextSequence + capacity;
        }
    }

    #region IDisposable
    private bool disposedValue;

    private void Dispose(bool disposing)
    {
        lock (lockObject)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    items.Clear();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
                SignalWaiters();
            }
        }
    }

    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~SequentialQueue()
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
    #endregion
}
