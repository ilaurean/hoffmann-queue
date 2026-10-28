using AwesomeAssertions;

namespace HoffmannQueue.Tests;

public class CancellationTests
{
    private static string Item(long seq) => $"Item-{seq:D5}";

    [Fact]
    public async Task Should_Throw_When_EnqueueTokenIsAlreadyCancelled()
    {
        using var queue = new SequentialQueue<string>(16);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await queue.Invoking(q => q.EnqueueAsync(1, "x", cts.Token).AsTask()).Should().ThrowAsync<OperationCanceledException>();
        queue.Count.Should().Be(0);
    }

    [Fact]
    public async Task Should_Throw_When_DequeueTokenIsAlreadyCancelled()
    {
        using var queue = new SequentialQueue<string>(16);
        queue.TryEnqueue(1, Item(1));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await queue.Invoking(q => q.DequeueAsync(cts.Token).AsTask()).Should().ThrowAsync<OperationCanceledException>();
        queue.Count.Should().Be(1);
    }

    [Fact]
    public async Task Should_Throw_When_ReadingAllIsCancelled()
    {
        using var queue = new SequentialQueue<string>(16);
        using var cts = new CancellationTokenSource(1);
        await cts.CancelAsync(); // flaky when using timer, so cancel immediately
        var act = async () => { await foreach (var _ in queue.ReadAllAsync(cts.Token)) { } };
        await act.Should().ThrowAsync<OperationCanceledException>();
    }
}

public static class TaskExtensions
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    public static async Task WithTimeout(this Task task)
    {
        var winner = await Task.WhenAny(task, Task.Delay(Timeout));
        if (winner != task)
        {
            throw new TimeoutException("Operation did not complete in time (possible deadlock).");
        }

        await task;
    }

    public static async Task<T> WithTimeout<T>(this Task<T> task)
    {
        await ((Task)task).WithTimeout();
        return await task;
    }
}
