namespace HoffmannQueue.Tests;

public static class TaskExtensions
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

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

    public static async Task<bool> FastComplete(this Task task, int milliseconds = 200)
        => await Task.WhenAny(task, Task.Delay(milliseconds)) == task;
}