using System;
using System.Collections.Generic;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Storages;

namespace Vicuna.Engine.Transactions
{
    public class LowLevelTransaction : IDisposable
    {
        internal long Id { get; }

        internal BufferPool Buffers { get; }

        internal PageManager PageManager { get; }

        internal Transaction Transaction { get; }

        internal Stack<LatchScope> LatchScopes { get; }

        internal Dictionary<object, LatchScope> LatchScopeMaps { get; }

        internal Dictionary<PagePosition, Page> Modifies { get; }

        public LowLevelTransaction(long id, BufferPool buffers)
        {
            Id = id;
            LatchScopes = new Stack<LatchScope>();
            Buffers = buffers ?? throw new ArgumentNullException(nameof(buffers));
            Modifies = new Dictionary<PagePosition, Page>();
            LatchScopeMaps = new Dictionary<object, LatchScope>();
        }

        public Page GetPage(PagePosition pos)
        {
            if (Modifies.TryGetValue(pos, out var cache))
            {
                return cache;
            }

            if (LatchScopeMaps.TryGetValue(pos, out var entry))
            {
                return ((BufferEntry)entry?.Latch?.Target)?.Page;
            }

            var buffer = Buffers.GetEntry(pos);
            if (buffer == null)
            {
                return null;
            }

            return GetPage(buffer);
        }

        public Page GetPage(BufferEntry buffer)
        {
            if (Modifies.TryGetValue(buffer.Page.Position, out var cache))
            {
                return cache;
            }

            if (!LatchScopeMaps.ContainsKey(buffer.Page.Position))
            {
                AddLatchScope(buffer.Latch.EnterReadScope());
            }

            return buffer.Page;
        }

        public Page ModifyPage(int fileId, long pageNumber)
        {
            return ModifyPage(new PagePosition(fileId, pageNumber));
        }

        public Page ModifyPage(PagePosition pos)
        {
            if (!Modifies.TryGetValue(pos, out var entry))
            {
                var buffer = Buffers.GetEntry(pos);
                if (buffer == null)
                {
                    return null;
                }

                AddLatchScope(buffer.Latch.EnterWriteScope());
                Modifies[buffer.Position] = buffer.Page;

                return buffer.Page;
            }

            return entry;
        }

        public Page ModifyPage(BufferEntry buffer)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            if (!Modifies.TryGetValue(buffer.Page.Position, out var entry))
            {
                AddLatchScope(buffer.Latch.EnterWriteScope());
                Modifies[buffer.Position] = buffer.Page;

                return buffer.Page;
            }

            return entry;
        }

        public void ModifyFile(File file)
        {
            AddLatchScope(file.Latch.EnterWriteScope());
        }

        public void FreePage(Page page)
        {

        }

        public IEnumerable<Page> AllocPage(int fileId, uint count)
        {
            var list = PageManager.AllocPageAtFree(this, fileId, count);

            for (var i = 0; i < list.Count; i++)
            {
                var pos = list[i];
                var buffer = Buffers.AllocEntry(pos);

                yield return ModifyPage(buffer);
            }
        }

        public LowLevelTransaction StartNew()
        {
            return new LowLevelTransaction(Id, Buffers);
        }

        protected void AddLatchScope(LatchScope scope)
        {
            if (LatchScopeMaps.TryGetValue(scope.Latch.Target, out var old))
            {
                if (old.Flags != scope.Flags)
                {
                    throw new InvalidOperationException($"latch at target:{old.Latch.Target} has already exists!");
                }

                return;
            }

            LatchScopes.Push(scope);
            LatchScopeMaps.Add(scope.Latch.Target, scope);
        }

        public void Commit()
        {
            ReleaseResources();

            Modifies.Clear();
            LatchScopes.Clear();
            LatchScopeMaps.Clear();
        }

        public void Dispose()
        {
            Commit();
        }

        private void ReleaseResources()
        {
            lock (Buffers.SyncRoot)
            {
                while (LatchScopes.Count != 0)
                {
                    var latch = LatchScopes.Pop();
                    if (latch.Latch.Target is BufferEntry entry)
                    {
                        entry.Count--;
                    }

                    latch.Dispose();
                }
            }
        }
    }
}
