namespace Raven.Server.Utils.Metrics;

public sealed class TimeAgnosticEwma
{
    private const double Alpha = 0.05;
    private double _ewmaErrors;
    private double _ewmaTotalDocs;

    private bool _initialized;

    public void UpdateOnBatchCompletion(long errorsInThisBatch, long totalDocsInThisBatch)
    {
        if (totalDocsInThisBatch == 0)
            return;
        
        if (_initialized == false)
        {
            _ewmaErrors = errorsInThisBatch;
            _ewmaTotalDocs = totalDocsInThisBatch;
            _initialized = true;
        }
        else
        {
            _ewmaErrors = _ewmaErrors * (1 - Alpha) + errorsInThisBatch * Alpha;
            _ewmaTotalDocs = _ewmaTotalDocs * (1 - Alpha) + totalDocsInThisBatch * Alpha;
        }
    }

    public double GetRate()
    {
        if (_ewmaTotalDocs == 0)
            return 0; 
        
        return _ewmaErrors / _ewmaTotalDocs;
    }
}
