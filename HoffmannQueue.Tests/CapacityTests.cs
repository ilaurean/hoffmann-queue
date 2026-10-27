using AwesomeAssertions;

namespace HoffmannQueue.Tests;

public class CapacityTests
{
    private static string Item(long seq) => $"Item-{seq}";

    [Fact]
    public void Should_Succeed_When_TryingToDequeueAfterGapIsFilled()
    {
        using var queue = new SequentialQueue<string>(16);
        queue.TryEnqueue(2, Item(2));
        queue.TryEnqueue(1, Item(1));

        queue.TryDequeue(out var a).Should().BeTrue();
        a.SequenceNumber.Should().Be(1);

        queue.TryDequeue(out var b).Should().BeTrue();
        b.SequenceNumber.Should().Be(2);

        queue.TryDequeue(out _).Should().BeFalse();
    }

    [Fact]
    public void Should_Succeed_When_SequenceIsInsideWindow()
    {
        using var queue = new SequentialQueue<string>(3);
        queue.TryEnqueue(3, Item(3)).Should().BeTrue();
        queue.TryEnqueue(4, Item(4)).Should().BeFalse();

        queue.Count.Should().Be(1);
    }

    [Fact]
    public void Should_Succeed_When_WindowSlidesAfterDequeue()
    {
        using var queue = new SequentialQueue<string>(2);
        queue.TryEnqueue(1, Item(1)).Should().BeTrue();
        queue.TryEnqueue(3, Item(3)).Should().BeFalse();
        queue.TryDequeue(out _).Should().BeTrue();

        queue.TryEnqueue(3, Item(3)).Should().BeTrue();
    }

    [Fact]
    public void Should_Succeed_When_CapacityIsOne()
    {
        using var queue = new SequentialQueue<string>(1);
        for (long i = 1; i <= 20; i++)
        {
            queue.TryEnqueue(i, Item(i)).Should().BeTrue();
            queue.TryEnqueue(i + 1, Item(i + 1)).Should().BeFalse();

            queue.TryDequeue(out var entry).Should().BeTrue();
            entry.SequenceNumber.Should().Be(i);
        }
    }

    [Fact]
    public void Should_Reject_When_TryingToDequeueFromEmptyQueue()
    {
        using var queue = new SequentialQueue<string>(16);
        queue.TryDequeue(out var entry).Should().BeFalse();
        entry.Should().Be(default(SequencedItem<string>));
    }

    [Fact]
    public void Should_Reject_When_TryingToDequeueWithNextItemMissing()
    {
        using var queue = new SequentialQueue<string>(16);
        queue.TryEnqueue(2, Item(2));
        queue.TryEnqueue(3, Item(3));

        queue.TryDequeue(out _).Should().BeFalse();
        queue.Count.Should().Be(2);
    }

    [Fact]
    public void Should_Reject_When_WindowIsFull()
    {
        using var queue = new SequentialQueue<string>(3);
        queue.TryEnqueue(3, Item(3)).Should().BeTrue();
        queue.TryEnqueue(2, Item(2)).Should().BeTrue();
        queue.TryEnqueue(1, Item(1)).Should().BeTrue();
        queue.Count.Should().Be(3);

        queue.TryEnqueue(4, Item(4)).Should().BeFalse();
        queue.Count.Should().BeLessThanOrEqualTo(queue.Capacity);
    }
}
