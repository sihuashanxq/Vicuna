using System;
using System.Collections.Generic;

namespace Vicuna.Engine.Collections
{
    public static class CollectionExtensions
    {
        public static void Push<T>(this Stack<T> stack, Stack<T> other)
        {
            if (stack == null)
            {
                throw new ArgumentNullException(nameof(stack));
            }

            if (other == null)
            {
                return;
            }

            while (other?.Count != 0)
            {
                stack.Push(other.Pop());
            }
        }

        public static void Push<T>(this Stack<T> stack, IEnumerable<T> collections)
        {
            if (stack == null)
            {
                throw new ArgumentNullException(nameof(stack));
            }

            if (collections == null)
            {
                return;
            }

            foreach (var item in collections)
            {
                stack.Push(item);
            }
        }

        public static IEnumerable<T> PopWhen<T>(this Stack<T> stack, Func<T, bool> when)
        {
            if (stack == null)
            {
                throw new ArgumentNullException(nameof(stack));
            }

            if (when == null)
            {
                throw new ArgumentNullException(nameof(when));
            }

            var list = new List<T>();
            var items = new Stack<T>();

            while (stack.Count != 0)
            {
                var item = stack.Pop();
                if (!when(item))
                {
                    items.Push(item);
                }
                else
                {
                    list.Add(item);
                }
            }

            Push(stack, items);

            return list;
        }

        public static IEnumerable<T> PopUntil<T>(this Stack<T> stack, Func<T, bool> when)
        {
            if (stack == null)
            {
                throw new ArgumentNullException(nameof(stack));
            }

            if (when == null)
            {
                throw new ArgumentNullException(nameof(when));
            }

            var list = new List<T>();

            while (stack.Count != 0)
            {
                if (!when(stack.Peek()))
                {
                    break;
                }

                list.Add(stack.Pop());
            }

            return list;
        }
    }
}
