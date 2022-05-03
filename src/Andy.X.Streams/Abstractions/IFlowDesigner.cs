using System;

namespace Andy.X.Streams.Abstractions
{
    public interface IFlowDesigner
    {
        public ISinkDesigner Map<TIn, TOut>(Func<TIn, TOut> filterFunction);
    }
}
