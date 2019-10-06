using System;
using System.Collections.Generic;
using Vicuna.Engine.Transactions;

using TableIndex = Vicuna.Engine.Data.Tables.TableIndex;

namespace Vicuna.Engine.Data.Trees
{
    public class Program
    {
        public unsafe static void Main()
        {
            var tree = new Tree(new TableIndex(), new TreeRootHeader()
            {
                FileId = 0,
                PageNumber = 0
            }, null);

            var buffer = new Buffers.BufferPool(new Buffers.BufferPoolOptions()
            {
                LRULimit = int.MaxValue
            });

            var root = buffer.AllocEntry(new Paging.PagePosition(0, 0)).Page.AsTree();
            root.TreeHeader.Depth = Constants.BTreeLeafPageDepth;
            root.TreeHeader.UsedSize = Constants.PageHeaderSize + Constants.PageFooterSize;
            root.TreeHeader.Low = Constants.PageHeaderSize;
            root.TreeHeader.Upper = Constants.PageSize - Constants.PageFooterSize;
            root.TreeHeader.PageNumber = 0;
            root.TreeHeader.FileId = 0;
            root.TreeHeader.PrevPageNumber = -1;
            root.TreeHeader.NextPageNumber = -1;
            root.TreeHeader.NodeFlags = TreeNodeFlags.Leaf | TreeNodeFlags.Root;
            var st = new System.Diagnostics.Stopwatch();
            st.Start();

            //System.Threading.Tasks.Parallel.For(1, 750000, n =>
            //{
            //    //for (var n = 1; n < 125000; n++)
            //    {
            //        Span<byte> bytes = new byte[5];
            //        bytes[0] = (byte)DataType.Int32;
            //        BitConverter.TryWriteBytes(bytes.Slice(1), n);
            //        DBOperationFlags op = default;
            //        using (var tx = new LowLevelTransaction(0, buffer))
            //        {
            //            op = tree.AddOptimisticClusterEntry(tx, new KVTuple()
            //            {
            //                Key = bytes,
            //                Value = new byte[128]
            //            });
            //        }

            //        if (op == DBOperationFlags.Split)
            //        {
            //            using (var tx = new LowLevelTransaction(0, buffer))
            //            {
            //                op = tree.AddPessimisticClusterEntry(tx, new KVTuple()
            //                {
            //                    Key = bytes,
            //                    Value = new byte[128]
            //                });
            //            }
            //        }
            //    }
            //});

            //for (var n = 1; n < 125000; n++)
            var k = 500000;
            var task1 = new System.Threading.Tasks.Task(() =>
            {
                var st1 = new System.Diagnostics.Stopwatch();
                st1.Start();
                for (var n = 1; n < k; n++)
                {
                    Span<byte> bytes = new byte[5];
                    bytes[0] = (byte)DataType.Int32;
                    BitConverter.TryWriteBytes(bytes.Slice(1), n);
                    DBOperationFlags op = default;
                    using (var tx = new LowLevelTransaction(0, buffer))
                    {
                        op = tree.AddOpmtClusterEntry(tx, new KVTuple()
                        {
                            Key = bytes,
                            Value = new byte[128]
                        }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                    }

                    if (op == DBOperationFlags.Split)
                    {
                        using (var tx = new LowLevelTransaction(0, buffer))
                        {
                            op = tree.AddPsmtClusterEntry(tx, new KVTuple()
                            {
                                Key = bytes,
                                Value = new byte[128]
                            }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                        }
                    }
                }

                st1.Stop();
                Console.WriteLine("st1:" + st1.ElapsedMilliseconds);
            });

            var task2 = new System.Threading.Tasks.Task(() =>
            {
                var st2 = new System.Diagnostics.Stopwatch();
                st2.Start();

                for (var n = 1 * k; n < 2 * k; n++)
                {
                    Span<byte> bytes = new byte[5];
                    bytes[0] = (byte)DataType.Int32;
                    BitConverter.TryWriteBytes(bytes.Slice(1), n);
                    DBOperationFlags op = default;
                    using (var tx = new LowLevelTransaction(0, buffer))
                    {
                        op = tree.AddOpmtClusterEntry(tx, new KVTuple()
                        {
                            Key = bytes,
                            Value = new byte[128]
                        }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                    }

                    if (op == DBOperationFlags.Split)
                    {
                        using (var tx = new LowLevelTransaction(0, buffer))
                        {
                            op = tree.AddPsmtClusterEntry(tx, new KVTuple()
                            {
                                Key = bytes,
                                Value = new byte[128]
                            }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                        }
                    }
                }

                st2.Stop();
                Console.WriteLine("st2:" + st2.ElapsedMilliseconds);
            });

            var task3 = new System.Threading.Tasks.Task(() =>
            {
                var st2 = new System.Diagnostics.Stopwatch();
                st2.Start();

                for (var n = 2 * k; n < 3 * k; n++)
                {
                    Span<byte> bytes = new byte[5];
                    bytes[0] = (byte)DataType.Int32;
                    BitConverter.TryWriteBytes(bytes.Slice(1), n);
                    DBOperationFlags op = default;
                    using (var tx = new LowLevelTransaction(0, buffer))
                    {
                        op = tree.AddOpmtClusterEntry(tx, new KVTuple()
                        {
                            Key = bytes,
                            Value = new byte[128]
                        }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                    }

                    if (op == DBOperationFlags.Split)
                    {
                        using (var tx = new LowLevelTransaction(0, buffer))
                        {
                            op = tree.AddPsmtClusterEntry(tx, new KVTuple()
                            {
                                Key = bytes,
                                Value = new byte[128]
                            }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                        }
                    }
                }

                st2.Stop();
                Console.WriteLine("st2:" + st2.ElapsedMilliseconds);
            });
            var task4 = new System.Threading.Tasks.Task(() =>
            {
                var st2 = new System.Diagnostics.Stopwatch();
                st2.Start();

                for (var n = 3 * k; n < 4 * k; n++)
                {
                    Span<byte> bytes = new byte[5];
                    bytes[0] = (byte)DataType.Int32;
                    BitConverter.TryWriteBytes(bytes.Slice(1), n);
                    DBOperationFlags op = default;
                    using (var tx = new LowLevelTransaction(0, buffer))
                    {
                        op = tree.AddOpmtClusterEntry(tx, new KVTuple()
                        {
                            Key = bytes,
                            Value = new byte[128]
                        }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                    }

                    if (op == DBOperationFlags.Split)
                    {
                        using (var tx = new LowLevelTransaction(0, buffer))
                        {
                            op = tree.AddPsmtClusterEntry(tx, new KVTuple()
                            {
                                Key = bytes,
                                Value = new byte[128]
                            }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                        }
                    }
                }

                st2.Stop();
                Console.WriteLine("st2:" + st2.ElapsedMilliseconds);
            });

            //task1.Start();
            //task2.Start();
            //task3.Start();
            //task4.Start();

            //System.Threading.Tasks.Task.WaitAll(task1, task2, task3, task4);
            st.Stop();
            st.Reset();
            st.Start();
            System.Threading.Tasks.Parallel.For(1, 2000000, n =>
            {
                //for (var n = 750000; n < 1000000; n++)
                {
                    Span<byte> bytes = new byte[5];
                    bytes[0] = (byte)DataType.Int32;
                    BitConverter.TryWriteBytes(bytes.Slice(1), n);
                    DBOperationFlags op = default;
                    using (var tx = new LowLevelTransaction(0, buffer))
                    {
                        op = tree.AddOpmtClusterEntry(tx, new KVTuple()
                        {
                            Key = bytes,
                            Value = new byte[128]
                        },TreeNodeHeaderFlags.Primary|TreeNodeHeaderFlags.Data);
                    }

                    if (op == DBOperationFlags.Split)
                    {
                        using (var tx = new LowLevelTransaction(0, buffer))
                        {
                            op = tree.AddPsmtClusterEntry(tx, new KVTuple()
                            {
                                Key = bytes,
                                Value = new byte[128]
                            }, TreeNodeHeaderFlags.Primary | TreeNodeHeaderFlags.Data);
                        }
                    }
                }
            });

            st.Stop();
            Console.WriteLine("st4:" + st.ElapsedMilliseconds);
            st.Reset();
            st.Start();
            for (var n = 3000000; n < 5000000; n++)
            {
                Span<byte> bytes = new byte[5];
                bytes[0] = (byte)DataType.Int32;
                BitConverter.TryWriteBytes(bytes.Slice(1), n);
                DBOperationFlags op = default;
                using (var tx = new LowLevelTransaction(0, buffer))
                {
                    op = tree.AddOpmtClusterEntry(tx, new KVTuple()
                    {
                        Key = bytes,
                        Value = new byte[128]
                    }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                }

                if (op == DBOperationFlags.Split)
                {
                    using (var tx = new LowLevelTransaction(0, buffer))
                    {
                        op = tree.AddPsmtClusterEntry(tx, new KVTuple()
                        {
                            Key = bytes,
                            Value = new byte[128]
                        }, TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary);
                    }
                }
            }

            st.Stop();
            Console.WriteLine("st43333:" + st.ElapsedMilliseconds);
            st.Reset();
            st.Start();

            var list = new List<int>();

            System.Threading.Tasks.Parallel.For(1, 1500000, n =>
            {
                Span<byte> bytes = new byte[5];
                bytes[0] = (byte)DataType.Int32;
                BitConverter.TryWriteBytes(bytes.Slice(1), n);

                using (var tx = new LowLevelTransaction(0, buffer))
                {
                    if (tree.TryGetEntry(tx, bytes, out var n2))
                        if (n2 != n)
                        {
                            list.Add(n);
                        }
                }
            });

            Console.WriteLine("read:" + st.ElapsedMilliseconds);
            Console.WriteLine("read:" + list.Count);

            Console.WriteLine(buffer.Buffers.Count);
            Console.ReadLine();
        }
    }
}
