using System;

namespace Vicuna.Engine.Buffers
{
    public class BufferEntryLinkedList
    {
        private int _count;

        private int _version;

        private BufferEntry _first;

        public int Count => _count;

        public BufferEntry First => _first;

        public BufferEntry Last => _first?.Prev;

        public void AddFirst(BufferEntry entry)
        {
            if (_first == null)
            {
                _first = entry;
                _version++;
                _count++;
                entry.Prev = entry;
                entry.Next = entry;
            }
            else
            {
                InsertBefore(_first, entry);
            }
        }

        public void AddLast(BufferEntry entry)
        {
            if (_first == null)
            {
                _first = entry;
                _version++;
                _count++;
                entry.Prev = entry;
                entry.Next = entry;
            }
            else
            {
                InsertBefore(_first, entry);
            }
        }

        public void RemoveFirst()
        {
            if (_first != null)
            {
                Remove(_first.Prev);
            }
        }

        public void RemoveLast()
        {
            if (_first != null)
            {
                Remove(_first.Prev);
            }
        }

        public void MoveToFirst(BufferEntry entry)
        {
            if (entry == null)
            {
                throw new ArgumentNullException(nameof(entry));
            }

            Remove(entry);
            AddFirst(entry);
        }

        public void MoveToLast(BufferEntry entry)
        {
            if (entry == null)
            {
                throw new ArgumentNullException(nameof(entry));
            }

            Remove(entry);
            AddLast(entry);
        }

        private void InsertBefore(BufferEntry beforeEntry, BufferEntry entry)
        {
            if (entry == null)
            {
                throw new ArgumentNullException(nameof(entry));
            }

            if (beforeEntry == null)
            {
                throw new ArgumentNullException(nameof(beforeEntry));
            }

            _count++;
            _version++;

            entry.Next = beforeEntry;
            entry.Prev = beforeEntry.Prev;
            beforeEntry.Prev.Next = entry;
            beforeEntry.Prev = entry;
        }

        private void Remove(BufferEntry entry)
        {
            if (entry.Prev == null &&
                entry.Next == null &&
                entry != _first)
            {
                return;
            }

            if (entry.Prev == entry)
            {
                _first = null;
            }
            else if (_first == entry)
            {
                entry.Next.Prev = entry.Prev;
                entry.Prev.Next = entry.Next;

                _first = entry.Next;
            }
            else
            {
                entry.Next.Prev = entry.Prev;
                entry.Prev.Next = entry.Next;
            }

            _count--;
            _version++;
        }

        public void Clear()
        {
            _first = null;
            _count = 0;
            _version++;
        }
    }
}
