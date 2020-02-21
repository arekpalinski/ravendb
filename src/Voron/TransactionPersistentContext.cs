using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageLocator AllocatePageLocator(LowLevelTransaction tx)
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

        private static string IsNull(object obj, string name)
        {
            if (obj == null)
                return $"{name} IS NULL";

            return $"{name} is NOT null";
        }

        public void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            locator.Release();
            if (_pageLocators.Count < 1024)
                _pageLocators.Push(locator);
        }

    }
}
