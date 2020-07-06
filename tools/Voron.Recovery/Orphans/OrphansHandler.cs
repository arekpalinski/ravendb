namespace Voron.Recovery.Orphans
{
    public class OrphansHandler
    {
        public OrphansHandler(RecoveryStorage recoveryStorage)
        {
            Revisions = new OrphanRevisionsHandler(recoveryStorage);
            Attachments = new OrphanAttachmentsHandler(recoveryStorage);
        }

        public OrphanRevisionsHandler Revisions { get; } 
        
        public OrphanAttachmentsHandler Attachments { get; }
    }
}
