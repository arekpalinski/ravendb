using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Voron.Impl;

namespace Voron
{
    public class TransactionPersistentContext
    {
        private bool _longLivedTransaction;
        private int _cacheSize;

        public bool LongLivedTransactions
        {
            get { return _longLivedTransaction; }
            set
            {
                _longLivedTransaction = value;
                _cacheSize = _longLivedTransaction ? 512 : 256;
            }
        }

        private readonly Stack<PageLocator> _pageLocators = new Stack<PageLocator>();

        public TransactionPersistentContext(bool longLivedTransactions = false)
        {
            LongLivedTransactions = longLivedTransactions;
        }

        public Guid ID = Guid.NewGuid();

        private object locker = new object();

        public int CurrentThreadIdHoldingLock;

        public string CurrentThreadNamHoldingLock;

        public int PreviousThreadIdHoldingLock;

        public string PreviousThreadNamHoldingLock;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageLocator AllocatePageLocator(LowLevelTransaction tx)
        {
            if (Monitor.TryEnter(locker, TimeSpan.Zero) == false)
            {
                string currentThreadNamHoldingLock = CurrentThreadNamHoldingLock;
                int currentThreadIdHoldingLock = CurrentThreadIdHoldingLock;
                string previousThreadNamHoldingLock = PreviousThreadNamHoldingLock;
                int previousThreadIdHoldingLock = PreviousThreadIdHoldingLock;

                Debugger.Break();

                Console.WriteLine(
                    $"Could not get lock in thread {Thread.CurrentThread}." +
                    $" Current holder: {currentThreadNamHoldingLock} - id: {currentThreadIdHoldingLock}, " +
                    $"Previous holder: {previousThreadNamHoldingLock} - id: {previousThreadIdHoldingLock}");
                Debugger.Launch();
                Debugger.Break();
            }

            CurrentThreadIdHoldingLock = Thread.CurrentThread.ManagedThreadId;
            CurrentThreadNamHoldingLock = Thread.CurrentThread.Name;

            try
            {
                PageLocator locator = null;
                if (_pageLocators.Count != 0)
                {
                    try
                    {
                        locator = _pageLocators.Pop();
                        locator.Renew(tx, _cacheSize);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Voron NRE debug - AllocatePageLocator 1: {IsNull(locator, nameof(locator))}", e);
                    }
                }
                else
                {
                    try
                    {
                        locator = new PageLocator(tx, _cacheSize);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Voron NRE debug - AllocatePageLocator 2:", e);
                    }
                }
                return locator;
            }
            finally
            {
                PreviousThreadNamHoldingLock = CurrentThreadNamHoldingLock;
                PreviousThreadIdHoldingLock = CurrentThreadIdHoldingLock;

                CurrentThreadIdHoldingLock = -1;
                CurrentThreadNamHoldingLock = null;

                //Thread.Sleep(100);

                Monitor.Exit(locker);
            }
        }

        private static string IsNull(object obj, string name)
        {
            if (obj == null)
                return $"{name} IS NULL";

            return $"{name} is NOT null";
        }

        public void FreePageLocator(PageLocator locator)
        {
            if (locator == null)
                throw new NullReferenceException($"Voron NRE debug - Locator is NULL. Stack trace: {Environment.StackTrace}");

            Debug.Assert(locator != null);
            locator.Release();
            if (_pageLocators.Count < 1024)
                _pageLocators.Push(locator);
        }

    }
}
