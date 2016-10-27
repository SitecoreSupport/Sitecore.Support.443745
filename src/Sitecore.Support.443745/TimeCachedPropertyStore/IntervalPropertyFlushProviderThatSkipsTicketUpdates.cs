namespace Sitecore.Support.TimeCachedPropertyStore
{
  using System;
  using Sitecore.Abstract.Data.SqlServer;
  using Sitecore.Configuration;
  using Sitecore.Diagnostics;

  using Sitecore.Support.TimeCachedPropertyStore.DataModels;
  using Sitecore.Web.Authentication;

  public class IntervalPropertyFlushProviderThatSkipsTicketUpdates : DatabasePropertyProviderWithAsyncFlush
  {
    [UsedImplicitly]
    public IntervalPropertyFlushProviderThatSkipsTicketUpdates(SqlServerDataProvider provider)
      : base(provider)
    {

    }

    protected override bool ShouldSkipUpdate([NotNull] UpdateDatabasePropertyRowModel updateCandidate, [CanBeNull] DatabasePropertyRowModel cached)
    {
      Assert.ArgumentNotNull(updateCandidate, "updateCandidate");

      //if either nothing is cached, or an update candidate is cached.

      if ((cached == null) || (cached is EmptyCachedPropertyRowModel) || (cached is UpdateDatabasePropertyRowModel))
        return false;

      //Tickets are updated too often when remember me is not checked...IntervalPropertyFlushProviderThatSkipsTicketUpdates
      if (updateCandidate.Key.IndexOf(@"SC_TICKET_", StringComparison.OrdinalIgnoreCase) >= 0)
        return this.SkipTicketUpdate(updateCandidate, cached);

      return false;
    }

    private bool SkipTicketUpdate([NotNull]UpdateDatabasePropertyRowModel updateCandidate, [NotNull] DatabasePropertyRowModel cached)
    {
      Assert.ArgumentNotNull(updateCandidate, "updateCandidate");
      Assert.ArgumentNotNull(cached, "cached");
      Ticket cachedTicket;
      Ticket candidateTicket;
      try
      {
        if ((Database != null) && (!Database.Equals(Client.CoreDatabase)))
          return false;
        cachedTicket = Ticket.Parse(cached.Value);
        candidateTicket = Ticket.Parse(updateCandidate);

        //Could not parse those. Likely not ticket entities are met.
        if ((cachedTicket == null) || (candidateTicket == null))
          return false;

        // if Settings.Authentication.FormsAuthenticationSlidingExpiration is not used,
        // property store would not be spammed with ticket prolongations on every call.
        if (!Settings.Authentication.FormsAuthenticationSlidingExpiration)
          return false;

        if ((cachedTicket.Persist == candidateTicket.Persist)
            && string.Equals(cachedTicket.ClientId, candidateTicket.ClientId, StringComparison.Ordinal)
            && string.Equals(cachedTicket.Id, candidateTicket.Id, StringComparison.Ordinal)
            && string.Equals(cachedTicket.StartUrl, candidateTicket.StartUrl, StringComparison.Ordinal)
            && string.Equals(cachedTicket.UserName, candidateTicket.UserName, StringComparison.Ordinal))
        {
          //only timestamp has changed. Need to make desicion based on that.
          return SkipTicketProlongation(cachedTicket, candidateTicket);
        }

        //some other properties were changed. Must not skip.
        return false;
      }
      catch (Exception)
      {
        //Could not parse those. Likely not ticket entities are met.
        return false;
      }
    }

    private bool SkipTicketProlongation(Ticket cachedTicket, Ticket candidateTicket)
    {
      //Candidate and cached vary only by timeStamp for now.
      //If cached has bigger stamp - no need in prolongation at all..
      if (cachedTicket.TimeStamp > candidateTicket.TimeStamp)
        return true;

      //Problem is TicketManager.IsTicketExpired prolongates tickets on every request.
      //We would prolongate it only in case 50 % of expiration period has passed.
      var updateInNoEarlierThanIn = TimeSpan.FromTicks((long)((cachedTicket.Persist ? Settings.Authentication.ClientPersistentLoginDuration : Settings.Authentication.FormsAuthenticationTimeout).Ticks * 0.5));
      var earliestAcceptableProlongation = cachedTicket.TimeStamp + updateInNoEarlierThanIn;

      return (candidateTicket.TimeStamp < earliestAcceptableProlongation);

    }

  }
}
