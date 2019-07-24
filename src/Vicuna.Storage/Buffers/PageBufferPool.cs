using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Buffers
{
    public class PageBufferPool
    {
        public object SyncRoot { get; }

        public PageBufferPoolOptions Options { get; }

        public PageBufferEntryLinkedList LRU { get; }

        public PageBufferEntryLinkedList Flush { get; }

        public Dictionary<PagePosition, PageBufferEntry> Buffers { get; }

        public PageManager PageManager { get; }

        public PageBufferPool(PageBufferPoolOptions options)
        {
            Options = options;
            SyncRoot = new object();
            LRU = new PageBufferEntryLinkedList();
            Flush = new PageBufferEntryLinkedList();
            Buffers = new Dictionary<PagePosition, PageBufferEntry>();
        }

        public void AddFlushBufferEntry(PageBufferEntry entry)
        {
            Flush.AddFirst(entry);
        }

        /// <summary>
        /// 获取缓冲项
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public PageBufferEntry GetEntry(PagePosition pos, PageBufferPeekFlags flags = PageBufferPeekFlags.None)
        {
            //获取或者新分配(创建)一个Buffer
            var buffer = GetOrCreateBufferEntry(pos, flags);
            if (buffer == null)
            {
                return buffer;
            }

            if (buffer.State == PageBufferState.NoneLoading)
            {
                //新buffer,未加载,加载页面,此时一定是自己获取了写锁
                LoadBufferEntry(buffer);

                lock (SyncRoot)
                {
                    AddLRUBufferEntry(buffer);

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
        private PageBufferEntry GetOrCreateBufferEntry(PagePosition pos, PageBufferPeekFlags flags)
        {
            //缓冲区锁
            Monitor.Enter(SyncRoot);

            //对应页的buffer不存在,创建,获取写锁(很快)阻塞读,释放缓冲区锁,返回去读取
            if (!Buffers.TryGetValue(pos, out var buffer))
            {
                buffer = CreateBufferEntry(pos);

                buffer.Count++;
                buffer.Lock.EnterWriteLock();
                Buffers[pos] = buffer;

                Monitor.Exit(SyncRoot);
                return buffer;
            }

            //buffer对应的页是无须加载的状态,增加引用计数,移动LRU,返回
            if (buffer.State != PageBufferState.NoneLoading)
            {
                buffer.Count++;

                MoveLRUBufferEntry(buffer, flags);

                Monitor.Exit(SyncRoot);
                return buffer;
            }

            //不等待页面加载,不增加引用计数
            if (flags.HasFlag(PageBufferPeekFlags.NoneWaitReading))
            {
                Monitor.Exit(SyncRoot);
                return null;
            }

            //页面未加载,释放缓冲区锁
            buffer.Count++;
            Monitor.Exit(SyncRoot);

            //等待加载完成
            buffer.Lock.EnterReadLock();
            buffer.Lock.ExitReadLock();

            //加缓冲区锁,移动LRUList
            Monitor.Enter(SyncRoot);

            MoveLRUBufferEntry(buffer, flags);

            Monitor.Exit(SyncRoot);

            return buffer;
        }

        private void LoadBufferEntry(PageBufferEntry buffer)
        {
            Debug.Assert(buffer.Lock.IsWriteLockHeld);

            buffer.Page = PageManager.GetPage(buffer.Position);
            buffer.State = PageBufferState.Clean;
            buffer.Lock.ExitWriteLock();
        }

        private void AddLRUBufferEntry(PageBufferEntry buffer)
        {
            if (LRU.Count >= Options.LRULimit)
            {
                //FLUSH
            }

            LRU.AddFirst(buffer);
        }

        private void MoveLRUBufferEntry(PageBufferEntry buffer, PageBufferPeekFlags flags)
        {
            if (flags.HasFlag(PageBufferPeekFlags.NoneMoveLRU))
            {
                return;
            }

            LRU.MoveToFirst(buffer);
        }

        private PageBufferEntry CreateBufferEntry(PagePosition pos)
        {
            if (Buffers.Count >= Options.Limit)
            {
                //Flush
            }

            return new PageBufferEntry(PageBufferState.NoneLoading)
            {
                Position = pos
            };
        }
    }
}
