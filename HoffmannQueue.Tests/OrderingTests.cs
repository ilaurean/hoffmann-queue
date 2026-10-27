using AwesomeAssertions;

namespace HoffmannQueue.Tests;

public class OrderingTests
{
    private static string Item(long seq) => $"Item-{seq}";

    [Fact]
    public async Task Should_Succeed_When_InputIsInOrder()
    {
        using var queue = new SequentialQueue<string>(10);
        for (long i = 1; i <= 10; i++) { queue.TryEnqueue(i, Item(i)).Should().BeTrue(); }
        for (long i = 1; i <= 10; i++) { (await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber.Should().Be(i); }
    }

    [Fact]
    public async Task Should_Succeed_When_InputIsInReverseOrder()
    {
        using var queue = new SequentialQueue<string>(50);
        for (long i = 50; i >= 1; i--) { queue.TryEnqueue(i, Item(i)).Should().BeTrue(); }
        for (long i = 1; i <= 50; i++) { (await queue.DequeueAsync(TestContext.Current.CancellationToken)).Item.Should().Be(Item(i)); }
    }

    [Fact]
    public async Task Should_Succeed_When_EnqueuingSpecificationExampleOutOfOrder()
    {
        using var queue = new SequentialQueue<string>(10);
        foreach (var seq in new long[] { 4, 2, 3, 1, 5 })
        {
            await queue.EnqueueAsync(seq, Item(seq), TestContext.Current.CancellationToken);
        }

        for (long expected = 1; expected <= 5; expected++)
        {
            var entry = await queue.DequeueAsync(TestContext.Current.CancellationToken);
            entry.SequenceNumber.Should().Be(expected);
            entry.Item.Should().Be(Item(expected));
        }
    }

    [Fact]
    public void Should_Succeed_When_EnqueueAndDequeueAreInterleaved()
    {
        using var queue = new SequentialQueue<string>(3);
        queue.TryEnqueue(2, Item(2)).Should().BeTrue();
        queue.TryDequeue(out _).Should().BeFalse();
        queue.TryEnqueue(1, Item(1)).Should().BeTrue();
        queue.TryDequeue(out var first).Should().BeTrue();

        first.SequenceNumber.Should().Be(1);

        queue.TryEnqueue(4, Item(4)).Should().BeTrue();
        queue.TryEnqueue(3, Item(3)).Should().BeTrue();
        queue.TryDequeue(out var second).Should().BeTrue();
        queue.TryDequeue(out var third).Should().BeTrue();
        queue.TryDequeue(out var fourth).Should().BeTrue();

        new[] { second.SequenceNumber, third.SequenceNumber, fourth.SequenceNumber }.Should().Equal(2, 3, 4);
    }

    [Fact]
    public async Task Should_Succeed_When_FirstSequenceNumberIsCustom()
    {
        using var queue = new SequentialQueue<string>(4, firstSequenceNumber: 100);
        queue.TryEnqueue(101, Item(101)).Should().BeTrue();
        queue.TryEnqueue(100, Item(100)).Should().BeTrue();

        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber.Should().Be(100);
        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).SequenceNumber.Should().Be(101);

        queue.NextSequenceNumber.Should().Be(102);
    }

    [Fact]
    public async Task Should_Succeed_When_DequeuingStoredInstance()
    {
        using var queue = new SequentialQueue<object>(2);
        var payload = new object();

        await queue.EnqueueAsync(1, payload, TestContext.Current.CancellationToken);
        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).Item.Should().BeSameAs(payload);
    }

    private static long[] Permutation(int count, int seed, long first = 1)
    {
        var rnd = new Random(seed);
        return [.. Enumerable.Range(0, count).Select(i => first + i).OrderBy(x => rnd.Next())];
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task Should_Succeed_When_InputIsRandomPermutation(int seed)
    {
        const int n = 500;
        using var queue = new SequentialQueue<string>(n);
        foreach (var seq in Permutation(n, seed))
        {
            await queue.EnqueueAsync(seq, Item(seq), TestContext.Current.CancellationToken);
        }

        for (long i = 1; i <= n; i++)
        {
            var entry = await queue.DequeueAsync(TestContext.Current.CancellationToken);
            entry.SequenceNumber.Should().Be(i);
            entry.Item.Should().Be(Item(i));
        }

        queue.Count.Should().Be(0);
    }
}
