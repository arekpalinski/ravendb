using System;
using System.Globalization;
using System.Net.Http;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.TimeSeries.Actions
{
	/// <summary>
	/// implements administration level time series functionality
	/// </summary>
	public abstract class TimeSeriesOperationsBase 
 	{
		private readonly OperationCredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly TimeSeriesConvention timeSeriesConvention;
		protected readonly string ServerUrl;
		protected readonly JsonSerializer JsonSerializer;
		protected readonly string TimeSeriesUrl;
		protected readonly TimeSeriesStore Parent;
		protected readonly string TimeSeriesName;

		protected TimeSeriesOperationsBase(TimeSeriesStore parent, string timeSeriesName)
		{
			credentials = parent.Credentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			ServerUrl = parent.Url;
			Parent = parent;
			TimeSeriesName = timeSeriesName;
			TimeSeriesUrl = string.Format(CultureInfo.InvariantCulture, "{0}/ts/{1}", ServerUrl, timeSeriesName);
			JsonSerializer = parent.JsonSerializer;
			timeSeriesConvention = parent.TimeSeriesConvention;
		}

		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
		{
			CreateHttpJsonRequestParams @params;
			if (timeout.HasValue)
			{
				@params = new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, credentials, timeSeriesConvention.ShouldCacheRequest)
				{
					DisableRequestCompression = disableRequestCompression,
					DisableAuthentication = disableAuthentication,
					Timeout = timeout.Value
				};
			}
			else
			{
				@params = new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, credentials, timeSeriesConvention.ShouldCacheRequest)
				{
					DisableRequestCompression = disableRequestCompression,
					DisableAuthentication = disableAuthentication,
				};				
			}
			var request = jsonRequestFactory.CreateHttpJsonRequest(@params);
		
			return request;
		}
	}

}