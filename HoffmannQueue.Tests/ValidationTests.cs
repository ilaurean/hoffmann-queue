using AwesomeAssertions;

namespace HoffmannQueue.Tests;

public class ValidationTests
{
    [Fact]
    public void Should_Succeed_When_QueueIsNewlyCreated()
    {
        using var queue = new SequentialQueue<string>(8);
        queue.Capacity.Should().Be(8);
        queue.Count.Should().Be(0);
        queue.NextSequenceNumber.Should().Be(1);
        queue.IsCompleted.Should().BeFalse();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Should_Throw_When_CapacityIsNotPositive(int capacity)
    {
        var act = () => new SequentialQueue<string>(capacity);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Should_Throw_When_FirstSequenceNumberIsBelowOne()
    {
        var act = () => new SequentialQueue<string>(4, 0);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public async Task Should_Throw_When_EnqueuingSequenceBelowOne(long sequence)
    {
        using var queue = new SequentialQueue<string>(8);
        queue.Invoking(x => x.TryEnqueue(sequence, "x")).Should().Throw<ArgumentOutOfRangeException>();
        await queue.Invoking(x => x.EnqueueAsync(sequence, "x").AsTask()).Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task Should_Throw_When_EnqueuingAlreadyDequeuedSequence()
    {
        using var queue = new SequentialQueue<string>(8);
        queue.TryEnqueue(1, "a");
        queue.TryDequeue(out _).Should().BeTrue();

        queue.Invoking(x => x.TryEnqueue(1, "again")).Should().Throw<ArgumentOutOfRangeException>();
        await queue.Invoking(x => x.EnqueueAsync(1, "again").AsTask()).Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task Should_Throw_When_EnqueuingDuplicateBufferedSequence()
    {
        using var queue = new SequentialQueue<string>(8);
        queue.TryEnqueue(3, "a");

        queue.Invoking(x => x.TryEnqueue(3, "b")).Should().Throw<ArgumentOutOfRangeException>();
        await queue.Invoking(x => x.EnqueueAsync(3, "b").AsTask()).Should().ThrowAsync<ArgumentOutOfRangeException>();
        queue.Count.Should().Be(1);
    }

    [Fact]
    public async Task Should_Throw_When_EnqueuingNullItem()
    {
        using var queue = new SequentialQueue<string>(8);
        queue.Invoking(x => x.TryEnqueue(1, null!)).Should().Throw<ArgumentNullException>();
        await queue.Invoking(x => x.EnqueueAsync(1, null!).AsTask()).Should().ThrowAsync<ArgumentNullException>();
    }
}
