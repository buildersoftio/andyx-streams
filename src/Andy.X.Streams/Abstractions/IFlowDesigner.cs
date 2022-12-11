using Andy.X.Client.Models;
using System;

namespace Andy.X.Streams.Abstractions
{
    public interface IFlowDesigner<TIn,TOut>
    {
        public delegate bool Condition(TIn input);

        public ISinkDesigner<TIn, TOut> Map(Func<Message<TIn>, TOut> function);
        public ISinkDesigner<TIn, TOut> MapIf(Condition condition, Func<Message<TIn>, TOut> filterFunction);
        public ISinkDesignerIfElse<TIn, TOut> MapIf(Condition condition, Func<Message<TIn>, TOut> filterFunctionIfTrue, Func<Message<TIn>, TOut> filterFunctionIfFalse);


        public ISinkDesigner<TIn, TOut> Filter(Condition filterCondition);
    }
}
