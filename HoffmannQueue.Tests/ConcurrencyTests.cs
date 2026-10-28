using AwesomeAssertions;
using System.Collections.Concurrent;

namespace HoffmannQueue.Tests;

public class ConcurrencyTests
{
    private const int itemCount = 2000;
    private const int workerCount = 8;

    private static string Item(long seq) => $"Item-{seq:D5}";

    [Fact]
    public async Task Should_Succeed_When_ProducersEnqueueDistinctSequencesConcurrently()
    {
        using var queue = new SequentialQueue<string>(itemCount);

        await Task.WhenAll(Enumerable.Range(0, workerCount).Select(worker => Task.Run(() =>
        {
            for (long seq = worker + 1; seq <= itemCount; seq += workerCount)
            {
                queue.TryEnqueue(seq, Item(seq)).Should().BeTrue();
            }
        })));

        queue.Count.Should().Be(itemCount);

        var dequeued = new List<long>();
        while (queue.TryDequeue(out var entry))
        {
            dequeued.Add(entry.SequenceNumber);
        }

        dequeued.Should().Equal(Enumerable.Range(1, itemCount).Select(i => (long)i));
    }

    [Fact]
    public async Task Should_Succeed_When_ProducersAndConsumerRunConcurrently()
    {
        using var queue = new SequentialQueue<string>(64);
        var received = new List<long>();

        var consumer = Task.Run(() =>
        {
            while (received.Count < itemCount)
            {
                if (queue.TryDequeue(out var entry))
                {
                    received.Add(entry.SequenceNumber);
                }
                else
                {
                    Thread.Yield();
                }
            }
        }, TestContext.Current.CancellationToken);

        var producers = Enumerable.Range(0, workerCount).Select(worker => Task.Run(() =>
        {
            for (long seq = worker + 1; seq <= itemCount; seq += workerCount)
            {
                while (!queue.TryEnqueue(seq, Item(seq)))
                {
                    Thread.Yield();
                }
            }
        }));

        await Task.WhenAll(producers.Append(consumer)).WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        received.Should().Equal(Enumerable.Range(1, itemCount).Select(i => (long)i));
        queue.Count.Should().Be(0);
        queue.NextSequenceNumber.Should().Be(itemCount + 1);
    }

    [Fact]
    public async Task Should_Succeed_When_CountIsReadDuringConcurrentEnqueue()
    {
        const int capacity = 16;
        using var queue = new SequentialQueue<string>(capacity);
        var maxObserved = 0;
        using var stop = new CancellationTokenSource();

        var observer = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                maxObserved = Math.Max(maxObserved, queue.Count);
            }
        }, TestContext.Current.CancellationToken);

        await Task.WhenAll(Enumerable.Range(0, workerCount).Select(worker => Task.Run(() =>
        {
            for (long seq = worker + 1; seq <= 500; seq += workerCount)
            {
                while (!queue.TryEnqueue(seq, Item(seq)))
                {
                    while (queue.TryDequeue(out _)) { }
                    Thread.Yield();
                }
            }
        })));

        await stop.CancelAsync();
        await observer;

        maxObserved.Should().BeLessThanOrEqualTo(capacity);
    }

    [Fact]
    public async Task Should_Reject_When_SameSequenceIsEnqueuedConcurrently()
    {
        using var queue = new SequentialQueue<string>(8);
        var successes = 0;
        var failures = new ConcurrentBag<Exception>();
        using var barrier = new Barrier(workerCount);

        await Task.WhenAll(Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                if (queue.TryEnqueue(1, Item(1)))
                {
                    Interlocked.Increment(ref successes);
                }
            }
            catch (Exception ex)
            {
                failures.Add(ex);
            }
        })));

        successes.Should().Be(1);
        failures.Should().HaveCount(workerCount - 1);
        failures.Should().AllBeOfType<ArgumentOutOfRangeException>();
        queue.Count.Should().Be(1);
    }

    [Fact]
    public async Task Should_Reject_When_SameItemIsDequeuedByMultipleConsumers()
    {
        using var queue = new SequentialQueue<string>(itemCount);
        for (long seq = 1; seq <= itemCount; seq++)
        {
            queue.TryEnqueue(seq, Item(seq)).Should().BeTrue();
        }

        var dequeued = new ConcurrentBag<long>();

        await Task.WhenAll(Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            while (queue.TryDequeue(out var entry))
            {
                dequeued.Add(entry.SequenceNumber);
            }
        })));

        dequeued.Should().HaveCount(itemCount);
        dequeued.Should().OnlyHaveUniqueItems();
        queue.Count.Should().Be(0);
    }

    [Fact]
    public async Task Should_Throw_When_EnqueueingConcurrentlyWithMarkAsComplete()
    {
        using var queue = new SequentialQueue<string>(itemCount);
        var accepted = 0;
        var rejected = 0;
        using var barrier = new Barrier(workerCount + 1);

        var producers = Enumerable.Range(0, workerCount).Select(worker => Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (long seq = worker + 1; seq <= itemCount; seq += workerCount)
            {
                try
                {
                    queue.TryEnqueue(seq, Item(seq)).Should().BeTrue();
                    Interlocked.Increment(ref accepted);
                }
                catch (SequentialQueueCompletedException)
                {
                    Interlocked.Increment(ref rejected);
                }
            }
        })).ToList();

        var completer = Task.Run(() =>
        {
            barrier.SignalAndWait();
            queue.MarkAsComplete();
        }, TestContext.Current.CancellationToken);

        await Task.WhenAll(producers.Append(completer));

        queue.IsCompleted.Should().BeTrue();
        (accepted + rejected).Should().Be(itemCount);
        queue.Count.Should().Be(accepted);
        queue.Invoking(q => q.TryEnqueue(itemCount + 1, "x")).Should().Throw<SequentialQueueCompletedException>();
    }

    [Fact]
    public async Task Should_Throw_When_UsedConcurrentlyWithDispose()
    {
        var queue = new SequentialQueue<string>(itemCount);
        var unexpected = new ConcurrentBag<Exception>();
        using var barrier = new Barrier(workerCount + 1);

        var producers = Enumerable.Range(0, workerCount).Select(worker => Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (long seq = worker + 1; seq <= itemCount; seq += workerCount)
            {
                try
                {
                    queue.TryEnqueue(seq, Item(seq));
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    unexpected.Add(ex);
                    return;
                }
            }
        })).ToList();

        var disposer = Task.Run(() =>
        {
            barrier.SignalAndWait();
            queue.Dispose();
        }, TestContext.Current.CancellationToken);

        await Task.WhenAll(producers.Append(disposer));

        unexpected.Should().BeEmpty();
        queue.Invoking(q => q.TryEnqueue(itemCount + 1, "x")).Should().Throw<ObjectDisposedException>();
        queue.Invoking(q => q.TryDequeue(out _)).Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task Should_Succeed_When_EnqueueAsyncWaitsForSpace()
    {
        using var queue = new SequentialQueue<string>(2);
        queue.TryEnqueue(1, Item(1)).Should().BeTrue();

        var pending = queue.EnqueueAsync(3, Item(3), TestContext.Current.CancellationToken).AsTask();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        pending.IsCompleted.Should().BeFalse();

        queue.TryDequeue(out _).Should().BeTrue();
        await pending.WithTimeout();

        queue.Count.Should().Be(1);
    }

    [Fact]
    public async Task Should_Succeed_When_DequeueAsyncWaitsForNextItem()
    {
        using var queue = new SequentialQueue<string>(4);

        var pending = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        pending.IsCompleted.Should().BeFalse();

        queue.TryEnqueue(1, Item(1)).Should().BeTrue();
        var entry = await pending.WithTimeout();

        entry.SequenceNumber.Should().Be(1);
    }

    [Fact]
    public async Task Should_Succeed_When_ReadAllAsyncEndsAfterCompleteAndDrain()
    {
        using var queue = new SequentialQueue<string>(8);
        var reader = Task.Run(async () =>
        {
            var seen = new List<long>();
            await foreach (var entry in queue.ReadAllAsync(TestContext.Current.CancellationToken))
            {
                seen.Add(entry.SequenceNumber);
            }
            return seen;
        }, TestContext.Current.CancellationToken);

        queue.TryEnqueue(2, Item(2)).Should().BeTrue();
        queue.TryEnqueue(1, Item(1)).Should().BeTrue();
        queue.TryEnqueue(3, Item(3)).Should().BeTrue();
        queue.MarkAsComplete();

        (await reader.WithTimeout()).Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Should_Succeed_When_AsyncProducersSendBlocksNoLargerThanWindow()
    {
        const int window = 16;
        using var queue = new SequentialQueue<string>(window);

        var consumer = Task.Run(async () =>
        {
            var seen = new List<long>();
            while (seen.Count < itemCount)
            {
                seen.Add((await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber);
            }
            return seen;
        }, TestContext.Current.CancellationToken);

        var producers = Enumerable.Range(0, workerCount).Select(worker => Task.Run(async () =>
        {
            for (long seq = worker + 1; seq <= itemCount; seq += workerCount)
            {
                await queue.EnqueueAsync(seq, Item(seq), TestContext.Current.CancellationToken);
            }
        }, TestContext.Current.CancellationToken)).ToList();

        await Task.WhenAll(producers).WithTimeout();
        (await consumer.WithTimeout()).Should().Equal(Enumerable.Range(1, itemCount).Select(i => (long)i));
    }

    [Fact]
    public async Task Should_Throw_When_EnqueueAsyncCancelledWhileWaiting()
    {
        using var queue = new SequentialQueue<string>(1);
        queue.TryEnqueue(1, Item(1)).Should().BeTrue();
        using var cts = new CancellationTokenSource();

        var pending = queue.EnqueueAsync(2, Item(2), cts.Token).AsTask();
        await cts.CancelAsync();

        await pending.Invoking(p => p).Should().ThrowAsync<OperationCanceledException>();
        queue.Count.Should().Be(1);
    }

    [Fact]
    public async Task Should_Throw_When_DequeueAsyncCancelledWhileWaiting()
    {
        using var queue = new SequentialQueue<string>(4);
        using var cts = new CancellationTokenSource();

        var pending = queue.DequeueAsync(cts.Token).AsTask();
        await cts.CancelAsync();

        await pending.Invoking(p => p).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Should_Throw_When_WaitingEnqueueAsyncAndMarkAsComplete()
    {
        using var queue = new SequentialQueue<string>(1);
        queue.TryEnqueue(1, Item(1)).Should().BeTrue();

        var pending = queue.EnqueueAsync(2, Item(2), TestContext.Current.CancellationToken).AsTask();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        queue.MarkAsComplete();

        await pending.Invoking(p => p.WithTimeout()).Should().ThrowAsync<SequentialQueueCompletedException>();
    }

    [Fact]
    public async Task Should_Throw_When_WaitingDequeueAsyncAndMarkAsComplete()
    {
        using var queue = new SequentialQueue<string>(4);
        queue.TryEnqueue(2, Item(2)).Should().BeTrue();

        var pending = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        queue.MarkAsComplete();

        await pending.Invoking(p => p.WithTimeout()).Should().ThrowAsync<SequentialQueueCompletedException>();
        queue.Count.Should().Be(1);
    }

    [Fact]
    public async Task Should_Throw_When_WaitingDequeueAsyncAndDispose()
    {
        var queue = new SequentialQueue<string>(4);

        var pending = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        queue.Dispose();

        await pending.Invoking(p => p.WithTimeout()).Should().ThrowAsync<ObjectDisposedException>();
    }
}
