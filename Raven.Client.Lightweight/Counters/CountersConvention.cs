﻿using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;

namespace Raven.Client.Counters
{
	/// <summary>
	/// The set of conventions used by the <see cref="CountersConvention"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class CountersConvention: ConventionBase
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="CountersConvention"/> class.
		/// </summary>
		public CountersConvention()
		{
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			AllowMultipuleAsyncOperations = true;
			ShouldCacheRequest = url => true;
		}

		/// <summary>
		/// Begins handling of unauthenticated responses, usually by authenticating against the oauth server
		/// in async manner
		/// </summary>
		public Func<HttpResponseMessage, OperationCredentials, Task<Action<HttpClient>>> HandleUnauthorizedResponseAsync { get; set; }

		/// <summary>
		/// Begins handling of forbidden responses
		/// in async manner
		/// </summary>
		public Func<HttpResponseMessage, OperationCredentials, Task<Action<HttpClient>>> HandleForbiddenResponseAsync { get; set; }
	}
}