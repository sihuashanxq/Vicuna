using System.Collections.Generic;
using System.Threading;
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

        /// <summary>
        /// 获取缓冲项
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public BufferEntry GetBuffer(PagePosition pos, BufferPeekFlags flags = BufferPeekFlags.None)
        {
            //获取或者新分配(创建)一个Buffer
            var buffer = GetOrCreateBuffer(pos, flags);
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
                    AddLRUBuffer(buffer);

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
        private BufferEntry GetOrCreateBuffer(PagePosition pos, BufferPeekFlags flags)
        {
            //缓冲区锁
            Monitor.Enter(SyncRoot);

            //对应页的buffer不存在,创建,获取写锁(很快)阻塞读,释放缓冲区锁,返回去读取
            if (!Buffers.TryGetValue(pos, out var buffer))
            {
                buffer = CreateBuffer(pos);

                buffer.Count++;
                buffer.Latch.EnterWrite();
                Buffers[pos] = buffer;

                Monitor.Exit(SyncRoot);
                return buffer;
            }

            //buffer对应的页是无须加载的状态,增加引用计数,移动LRU,返回
            if (buffer.State != BufferState.NoneLoading)
            {
                buffer.Count++;

                MoveLRUBuffer(buffer, flags);

                Monitor.Exit(SyncRoot);
                return buffer;
            }

            //不等待页面加载,不增加引用计数
            if (flags.HasFlag(BufferPeekFlags.NoneWait))
            {
                Monitor.Exit(SyncRoot);
                return null;
            }

            //页面未加载,释放缓冲区锁
            buffer.Count++;
            Monitor.Exit(SyncRoot);

            //等待加载完成
            buffer.Latch.ExitRead();
            buffer.Latch.ExitRead();

            //加缓冲区锁,移动LRUList
            Monitor.Enter(SyncRoot);

            MoveLRUBuffer(buffer, flags);

            Monitor.Exit(SyncRoot);

            return buffer;
        }

        private void LoadBufferPage(BufferEntry buffer)
        {
            buffer.Page = PageManager.ReadPage(buffer.Position);
            buffer.State = BufferState.Clean;
            buffer.Latch.ExitWrite();
        }

        private void AddLRUBuffer(BufferEntry buffer)
        {
            if (LRU.Count >= Options.LRULimit)
            {
                //FLUSH
            }

            LRU.AddFirst(buffer);
        }

        public void AddFlushBuffer(BufferEntry entry)
        {
            Flush.AddFirst(entry);
        }

        private void MoveLRUBuffer(BufferEntry buffer, BufferPeekFlags flags)
        {
            if (!flags.HasFlag(BufferPeekFlags.KeepLRU))
            {
                LRU.MoveToFirst(buffer);
            }
        }

        private BufferEntry CreateBuffer(PagePosition pos)
        {
            if (Buffers.Count >= Options.LRULimit)
            {
                //Flush
            }

            return new BufferEntry(BufferState.NoneLoading, pos);
        }
    }
}
