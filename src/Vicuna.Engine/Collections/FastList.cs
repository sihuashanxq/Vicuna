using System;

namespace Vicuna.Storage.Collections
{
    public class FastList<T>
    {
        private T[] _items;

        private int _count;

        private const int DefaultCapcity = 8;

        private static readonly T[] _empty = new T[0];

        public int Count => _count;

        public int Capcity => _items.Length;

        public Span<T> ToSpan()
        {
            return _items.AsSpan().Slice(0, _count);
        }

        public FastList()
        {
            _items = _empty;
        }

        public FastList(int capcity)
        {
            if (capcity < 0)
            {
                throw new ArgumentOutOfRangeException($"the argument of capcity must be non-negtive!");
            }

            _items = new T[capcity];
        }

        public void Add(T item)
        {
            var count = _count;
            if (count >= _items.Length)
            {
                EnsureCapcity(_count + 1);
            }

            _count++;
            _items[count] = item;
        }

        public void AddRange(T[] items)
        {
            if (items == null || items.Length == 0)
            {
                return;
            }

            var count = _count;
            if (count + items.Length >= _items.Length)
            {
                EnsureCapcity(count + items.Length);
            }

            _count += items.Length;

            Array.Copy(items, 0, _items, count, items.Length);
        }

        public void AddRange(Span<T> items)
        {
            if (items == null || items.Length == 0)
            {
                return;
            }

            var count = _count;
            if (count + items.Length >= _items.Length)
            {
                EnsureCapcity(count + items.Length);
            }

            _count += items.Length;
            _items.AsSpan().Slice(count, items.Length);
        }

        public void CopyTo(T[] array)
        {
            CopyTo(array, 0, 0, _count);
        }

        public void CopyTo(T[] array, int index, int arrayIndex, int count)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException($"the index:{index} was out of the list's bound!");
            }

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException($"the count:{count} was out of the list's bound!");
            }

            if (index + count > _count)
            {
                throw new ArgumentOutOfRangeException($"the index:{index} and count:{count} was out of the list's bound:0-{Capcity}!");
            }

            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException($"the index:{index} was out of the array's bound!");
            }

            if (arrayIndex + count > array.Length)
            {
                throw new ArgumentOutOfRangeException($"the arrayIndex:{arrayIndex} and count:{count} was out of the array's bound:0-{array.Length}!");
            }

            Array.Copy(_items, index, array, arrayIndex, count);
        }

        public void CopyTo(Span<T> array, int index, int count)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException($"the index:{index} was out of the list's bound!");
            }

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException($"the count:{count} was out of the list's bound!");
            }

            if (index + count > _count)
            {
                throw new ArgumentOutOfRangeException($"the index:{index} and count:{count} was out of the list's bound:0-{Capcity}!");
            }

            if (count > array.Length)
            {
                throw new ArgumentOutOfRangeException($"the count:{count} was out of the array's bound:0-{array.Length}!");
            }

            _items.AsSpan().Slice(index, count).CopyTo(array);
        }

        public void Clear()
        {
            _count = 0;
            _items = new T[DefaultCapcity];
        }

        private void EnsureCapcity(int size)
        {
            if (_items.Length < size)
            {
                var capcity = _items.Length == 0 ? DefaultCapcity : (long)_items.Length * 2;
                if (capcity > int.MaxValue)
                {
                    capcity = int.MaxValue;
                }

                if (capcity < size)
                {
                    capcity = size;
                }

                if (_items.Length != capcity)
                {
                    var items = new T[capcity];

                    Array.Copy(_items, items, _items.Length);

                    _items = items;
                }
            }
        }
    }
}