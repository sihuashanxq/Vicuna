using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Buffers
{
    public class BufferPool
    {
        public object SyncRoot { get; }

        public PageManager PageManager { get; }

        public BufferPoolOptions Options { get; }

        public BufferEntryLinkedList LRU { get; }

        public BufferEntryLinkedList Flush { get; }

        public Dictionary<PagePosition, BufferEntry> Buffers { get; }

        public BufferPool(BufferPoolOptions options)
        {
            Options = options;
            SyncRoot = new object();
            LRU = new BufferEntryLinkedList();
            Flush = new BufferEntryLinkedList();
            Buffers = new Dictionary<PagePosition, BufferEntry>();
        }

        public BufferEntry AllocEntry(PagePosition pos)
        {
            lock (SyncRoot)
            {
                var buffer = CreateEntry(pos, BufferState.Clean);

                buffer.Count++;
                buffer.Page = new Page(new byte[Constants.PageSize]);
                Buffers[pos] = buffer;

                MoveLRUEntry(buffer);

                return buffer;
            }
        }

        public BufferEntry GetEntry(int fileId, long pageNumber)
        {
            return GetEntry(new PagePosition(fileId, pageNumber));
        }

        /// <summary>
        /// 获取缓冲项
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public BufferEntry GetEntry(PagePosition pos, BufferSeekFlags flags = BufferSeekFlags.None)
        {
            //获取或者新分配(创建)一个Buffer
            var buffer = GetOrCreateEntry(pos, flags);
            if (buffer == null)
            {
                return buffer;
            }

            if (buffer.State == BufferState.NoneLoading)
            {
                //新buffer,未加载,加载页面,此时一定是自己获取了写锁
                LoadBufferPage(buffer);

                lock (SyncRoot)
                {
                    AddLRUEntry(buffer);

                    return buffer;
                }
            }

            return buffer;
        }

        /// <summary>
        /// 获取或创建页面缓冲
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public BufferEntry GetOrCreateEntry(PagePosition pos, BufferSeekFlags flags)
        {
            //缓冲区锁
            Monitor.Enter(SyncRoot);

            //对应页的buffer不存在,创建,获取写锁(很快)阻塞读,释放缓冲区锁,返回去读取
            if (!Buffers.TryGetValue(pos, out var buffer))
            {
                buffer = CreateEntry(pos);

                buffer.Count++;
                buffer.Latch.EnterWriteScope();
                Buffers[pos] = buffer;

                Monitor.Exit(SyncRoot);
                return buffer;
            }

            //buffer对应的页是无须加载的状态,增加引用计数,移动LRU,返回
            if (buffer.State != BufferState.NoneLoading)
            {
                buffer.Count++;

                if (!flags.HasFlag(BufferSeekFlags.NoLRU))
                {
                    MoveLRUEntry(buffer);
                }

                Monitor.Exit(SyncRoot);
                return buffer;
            }

            //不等待页面加载,不增加引用计数
            if (flags.HasFlag(BufferSeekFlags.NoWait))
            {
                Monitor.Exit(SyncRoot);
                return null;
            }

            //页面未加载,释放缓冲区锁
            buffer.Count++;
            Monitor.Exit(SyncRoot);

            //等待加载完成
            buffer.Latch.EnterReadScope();
            buffer.Latch.ExitReadScope();

            //加缓冲区锁,移动LRUList
            Monitor.Enter(SyncRoot);

            if (!flags.HasFlag(BufferSeekFlags.NoLRU))
            {
                MoveLRUEntry(buffer);
            }

            Monitor.Exit(SyncRoot);

            return buffer;
        }

        private BufferEntry CreateEntry(PagePosition pos, BufferState state = BufferState.NoneLoading)
        {
            if (Buffers.Count >= Options.LRULimit)
            {
                //Flush
            }

            return new BufferEntry(state, pos);
        }

        private void LoadBufferPage(BufferEntry buffer)
        {
            buffer.Page = new Page(new byte[Constants.PageSize]);
            //buffer.Page = PageManager.ReadPage(buffer.Position);
            buffer.State = BufferState.Clean;
            buffer.Latch.ExitWriteScope();
        }

        public void AddFlushEntry(BufferEntry buffer)
        {
            Flush.AddFirst(buffer);
        }

        public void AddFlushEntry(PagePosition pos)
        {
            lock (SyncRoot)
            {
                var buffer = GetEntry(pos);

                AddFlushEntry(buffer);
            }
        }

        private void AddLRUEntry(BufferEntry buffer)
        {
            if (LRU.Count >= Options.LRULimit)
            {
                //FLUSH
            }

            LRU.AddFirst(buffer);
        }

        private void MoveLRUEntry(BufferEntry buffer)
        {
            LRU.MoveToFirst(buffer);
        }
    }
}
