using System;

namespace Andy.X.Streams.Abstractions
{
    public interface IFlowDesigner
    {
        //public delegate TOut Func<TIn, TOut>(TIn input);
        public ISinkDesigner Map<TIn, TOut>(Func<TIn, TOut> filterFunction);
    }
}
