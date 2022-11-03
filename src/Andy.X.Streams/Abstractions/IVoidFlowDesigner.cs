using Andy.X.Client.Models;
using System;

namespace Andy.X.Streams.Abstractions
{
    public interface IVoidFlowDesigner<TIn>
    {
        public delegate bool Condition(TIn input);

        public IVoidSinkDesigner<TIn> Map(Action<Message<TIn>> action);
        public IVoidSinkDesigner<TIn> MapIf(Condition condition, Action<Message<TIn>> action);
        public IVoidSinkDesigner<TIn> MapIf(Condition condition, Action<Message<TIn>> actionIfTrue, Action<Message<TIn>> actionIfFalse);
    }
}
