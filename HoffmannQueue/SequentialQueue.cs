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
    public SequentialQueueCompletedException() : base("The sequential queue has been completed.") { }
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
    private readonly int capacity;
    private long nextSequence;
    private bool completed = false;

    private readonly Lock lockObject = new();

    private readonly Dictionary<long, T> items = [];

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

    public async ValueTask EnqueueAsync(long sequenceNumber, T item, CancellationToken cancellationToken = default)
    {
        ValidateSequenceItem(sequenceNumber, item);
        cancellationToken.ThrowIfCancellationRequested();

        lock (lockObject)
        {
            if (!TryEnqueue(sequenceNumber, item))
            {
                throw new InvalidOperationException($"Failed to enqueue item with sequence number {sequenceNumber}.");
            }
        }
        await ValueTask.CompletedTask;
    }

    public bool TryEnqueue(long sequenceNumber, T item)
    {
        ValidateSequenceItem(sequenceNumber, item);

        lock (lockObject)
        {
            ObjectDisposedException.ThrowIf(disposedValue, this);
        }

        lock (lockObject)
        {
            if (IsCompleted)
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

            if (sequenceNumber >= nextSequence + capacity)
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

    public async ValueTask<SequencedItem<T>> DequeueAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (lockObject)
        {
            if (!TryDequeue(out var item))
            {
                throw new InvalidOperationException($"Failed to dequeue item with sequence number {nextSequence}.");
            }
            return item;
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
                return true;
            }

            return false;
        }
    }

    public void MarkAsComplete()
    {
        lock (lockObject)
        {
            ObjectDisposedException.ThrowIf(disposedValue, this);
            completed = true;
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
            catch (InvalidOperationException)
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
