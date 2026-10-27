namespace HoffmannQueue;

/// <summary>An entry of a <see cref="SequentialQueue{T}"/>: the sequence number and the stored object.</summary>
public readonly record struct SequencedItem<T>(long SequenceNumber, T Item);

/// <summary>
/// Thrown when an operation cannot succeed because <see cref="SequentialQueue{T}.CompleteAdding"/> was called.
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

    void CompleteAdding();

    IAsyncEnumerable<SequencedItem<T>> ReadAllAsync(CancellationToken cancellationToken = default);
}

public sealed class SequentialQueue<T> :
    IDisposable, ISequentialQueue<T> where T : class
{
    private readonly int capacity;
    private long nextSequence;

    private readonly Dictionary<long, T> items = [];

    public int Capacity => capacity;

    public int Count => items.Count;

    public bool IsCompleted => IsCompleted && items.ContainsKey(nextSequence);

    public long NextSequenceNumber => nextSequence;

    public SequentialQueue(int capacity, long firstSequenceNumber = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(firstSequenceNumber, 1L);

        this.capacity = capacity;
        nextSequence = firstSequenceNumber;
    }

    public void CompleteAdding()
    {
        throw new NotImplementedException();
    }

    public ValueTask<SequencedItem<T>> DequeueAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public ValueTask EnqueueAsync(long sequenceNumber, T item, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<SequencedItem<T>> ReadAllAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public bool TryDequeue(out SequencedItem<T> item)
    {
        throw new NotImplementedException();
    }

    public bool TryEnqueue(long sequenceNumber, T item)
    {
        throw new NotImplementedException();
    }

    #region IDisposable
    private bool disposedValue;

    private void Dispose(bool disposing)
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
