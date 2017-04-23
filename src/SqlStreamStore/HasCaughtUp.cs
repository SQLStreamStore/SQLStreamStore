namespace SqlStreamStore
{
    /// <summary>
    ///     A delegate that is invoked when a subscription has either caught up or fallen behind.
    /// </summary>
    /// <returns>
    ///     True if the subscription has caught up, False otherwise.
    /// </returns>
    public delegate void HasCaughtUp(bool hasCaughtUp);
}