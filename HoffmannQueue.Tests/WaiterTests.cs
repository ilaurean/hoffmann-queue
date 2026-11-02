using AwesomeAssertions;

namespace HoffmannQueue.Tests;

public class WaiterTests
{
    private static string Item(long seq) => $"Item-{seq:D5}";

    [Fact]
    public async Task Should_Succeed_When_NextSequenceArrivesForWaitingDequeue()
    {
        using var queue = new SequentialQueue<string>(10);
        var pending = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();

        (await pending.FastComplete()).Should().BeFalse();
        await queue.EnqueueAsync(2, Item(2), TestContext.Current.CancellationToken);
        (await pending.FastComplete()).Should().BeFalse();

        await queue.EnqueueAsync(1, Item(1), TestContext.Current.CancellationToken);
        var first = await pending.WithTimeout();
        first.SequenceNumber.Should().Be(1);
        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber.Should().Be(2);
    }

    [Fact]
    public async Task Should_Succeed_When_WindowOpensForWaitingEnqueue()
    {
        using var queue = new SequentialQueue<string>(2);
        await queue.EnqueueAsync(1, Item(1), TestContext.Current.CancellationToken);
        var pending = queue.EnqueueAsync(3, Item(3), TestContext.Current.CancellationToken).AsTask();

        (await pending.FastComplete()).Should().BeFalse();
        queue.Count.Should().Be(1);

        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber.Should().Be(1);
        await pending.WithTimeout();
        queue.Count.Should().Be(1);

        await queue.EnqueueAsync(2, Item(2), TestContext.Current.CancellationToken);
        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber.Should().Be(2);
        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber.Should().Be(3);
    }

    [Fact]
    public async Task Should_Succeed_When_MultipleConsumersWaitInOrder()
    {
        using var queue = new SequentialQueue<string>(10);
        var first = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();
        var second = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();
        var third = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();

        await queue.EnqueueAsync(3, Item(3), TestContext.Current.CancellationToken);
        await queue.EnqueueAsync(2, Item(2), TestContext.Current.CancellationToken);
        await queue.EnqueueAsync(1, Item(1), TestContext.Current.CancellationToken);

        (await first.WithTimeout()).SequenceNumber.Should().Be(1);
        (await second.WithTimeout()).SequenceNumber.Should().Be(2);
        (await third.WithTimeout()).SequenceNumber.Should().Be(3);
    }

    [Fact]
    public async Task Should_Succeed_When_WindowAdvancesWhileEnqueueing()
    {
        using var queue = new SequentialQueue<string>(2);
        var p3 = queue.EnqueueAsync(3, Item(3), TestContext.Current.CancellationToken).AsTask();
        var p4 = queue.EnqueueAsync(4, Item(4), TestContext.Current.CancellationToken).AsTask();
        var p5 = queue.EnqueueAsync(5, Item(5), TestContext.Current.CancellationToken).AsTask();
        (await p3.FastComplete(50)).Should().BeFalse();

        queue.TryEnqueue(1, Item(1)).Should().BeTrue();
        queue.TryEnqueue(2, Item(2)).Should().BeTrue();
        queue.TryDequeue(out _).Should().BeTrue();
        queue.TryDequeue(out _).Should().BeTrue();

        await p3.WithTimeout();
        await p4.WithTimeout();
        (await p5.FastComplete(50)).Should().BeFalse();

        queue.TryDequeue(out var three).Should().BeTrue();
        three.SequenceNumber.Should().Be(3);
        await p5.WithTimeout();
    }

    [Fact]
    public async Task Should_Succeed_When_ItemIsAvailableForDequeue()
    {
        using var queue = new SequentialQueue<string>(10);
        queue.TryEnqueue(1, Item(1));

        var task = queue.DequeueAsync(TestContext.Current.CancellationToken);
        task.IsCompletedSuccessfully.Should().BeTrue();
        (await task).Item.Should().Be(Item(1));
    }

    [Fact]
    public async Task Should_Reject_When_TryingToDequeueWhileConsumerIsWaiting()
    {
        using var queue = new SequentialQueue<string>(10);
        var waiting = queue.DequeueAsync(TestContext.Current.CancellationToken).AsTask();

        queue.TryEnqueue(1, Item(1));

        (await waiting.WithTimeout()).SequenceNumber.Should().Be(1);
        queue.TryDequeue(out _).Should().BeFalse();
    }
}
