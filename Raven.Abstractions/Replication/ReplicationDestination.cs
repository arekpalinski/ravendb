//-----------------------------------------------------------------------
// <copyright file="ReplicationDestination.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.ComponentModel;
using Raven.Imports.Newtonsoft.Json;

using Raven.Abstractions.Cluster;

namespace Raven.Abstractions.Replication
{
	/// <summary>
	/// Data class for replication destination documents
	/// </summary>
	public class ReplicationDestination
	{
		/// <summary>
		/// The name of the connection string specified in the 
		/// server configuration file. 
		/// Override all other properties of the destination
		/// </summary>

		private string url;
		private string[] _sourceCollections;

		/// <summary>
		/// Gets or sets the URL of the replication destination
		/// </summary>
		/// <value>The URL.</value>
		public string Url
		{
			get { return url; }
			set
			{
				url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
			}
		}

		[JsonIgnore]
		public bool ShouldReplicateFromSpecificCollections
		{
			get
			{
				return SourceCollections != null &&
				       SourceCollections.Length > 0;
			}
		}

		/// <summary>
		/// If an option to replicate only from specific collections is selected, 
		/// replicate documents only from the specified collections
		/// </summary>
		public string[] SourceCollections
		{
			get { return _sourceCollections ?? (_sourceCollections = new string[0]); }
			set
			{
				_sourceCollections = value;

				//precaution - make sure to remove the destination from possible failover targets 
				//if collection specific replication is enabled
				if (_sourceCollections.Length > 0)
					IgnoredClient = true;
			}
		}

		/// <summary>
		/// The replication server username to use
		/// </summary>
		public string Username { get; set; }

		/// <summary>
		/// The replication server password to use
		/// </summary>
		public string Password { get; set; }

		/// <summary>
		/// The replication server domain to use
		/// </summary>
		public string Domain { get; set; }

		/// <summary>
		/// The replication server api key to use
		/// </summary>
		public string ApiKey { get; set; }

		/// <summary>
		/// The database to use
		/// </summary>
		public string Database { get; set; }

		/// <summary>
		/// How should the replication bundle behave with respect to replicated documents.
		/// If a document was replicated to us from another node, should we replicate that to
		/// this destination, or should we replicate only documents that were locally modified.
		/// </summary>
		public TransitiveReplicationOptions TransitiveReplicationBehavior { get; set; }

		/// <summary>
		/// Gets or sets a flag that controls whether index is replicated to the node or not
		/// </summary>
		public bool SkipIndexReplication { get; set; }

		/// <summary>
		/// Gets or sets if the replication will ignore this destination in the client
		/// </summary>
		public bool IgnoredClient { get; set; }

		/// <summary>
		/// Gets or sets if replication to this destination is disabled in both client and server.
		/// </summary>
		public bool Disabled { get; set; }

		/// <summary>
		/// Gets or sets the Client URL of the replication destination
		/// </summary>
		public string ClientVisibleUrl { get; set; }

		public string Humane
		{
			get
			{
				if (string.IsNullOrEmpty(url))
					return null;
				return url + " " + Database;
			}
		}

		protected bool Equals(ReplicationDestination other)
		{
			return IsEqualTo(other);
		}

		public bool IsEqualTo(ReplicationDestination other)
		{
			return string.Equals(Username, other.Username) && string.Equals(Password, other.Password) &&
				   string.Equals(Domain, other.Domain) && string.Equals(ApiKey, other.ApiKey) &&
				   string.Equals(Database, other.Database, StringComparison.InvariantCultureIgnoreCase) &&
				   TransitiveReplicationBehavior == other.TransitiveReplicationBehavior &&
				   IgnoredClient.Equals(other.IgnoredClient) && Disabled.Equals(other.Disabled) &&
				   ((string.Equals(Url, other.Url, StringComparison.InvariantCultureIgnoreCase) && string.IsNullOrWhiteSpace(ClientVisibleUrl)) ||
				   (!string.IsNullOrWhiteSpace(ClientVisibleUrl) && string.Equals(ClientVisibleUrl, other.ClientVisibleUrl, StringComparison.InvariantCultureIgnoreCase)));
		}


		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((ReplicationDestination)obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (Username != null ? Username.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (Password != null ? Password.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (Domain != null ? Domain.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (ApiKey != null ? ApiKey.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (Database != null ? Database.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (int)TransitiveReplicationBehavior;
				hashCode = (hashCode * 397) ^ IgnoredClient.GetHashCode();
				hashCode = (hashCode * 397) ^ Disabled.GetHashCode();
				hashCode = (hashCode * 397) ^ (ClientVisibleUrl != null ? ClientVisibleUrl.GetHashCode() : 0);
				return hashCode;
			}
		}

		public class ReplicationDestinationWithConfigurationOrigin : ReplicationDestination
		{
			public bool HasGlobal { get; set; }

			public bool HasLocal { get; set; }
		}

		public class ReplicationDestinationWithClusterInformation : ReplicationDestination
		{
			public ClusterInformation ClusterInformation { get; set; }

			public static ReplicationDestinationWithClusterInformation Create(ReplicationDestinationWithConfigurationOrigin source, bool isInCluster, bool isLeader)
			{
				return new ReplicationDestinationWithClusterInformation
					   {
						   ApiKey = source.ApiKey,
						   ClientVisibleUrl = source.ClientVisibleUrl,
						   Database = source.Database,
						   Disabled = source.Disabled,
						   Domain = source.Domain,
						   IgnoredClient = source.IgnoredClient,
						   ClusterInformation = new ClusterInformation(isInCluster, isLeader),
						   Password = source.Password,
						   SkipIndexReplication = source.SkipIndexReplication,
						   TransitiveReplicationBehavior = source.TransitiveReplicationBehavior,
						   Url = source.Url,
						   Username = source.Username,
						   SourceCollections = source.SourceCollections
					   };
			}
		}
	}

	/// <summary>
	/// Options for how to replicate replicated documents
	/// </summary>
	public enum TransitiveReplicationOptions
	{
		/// <summary>
		/// Don't replicate replicated documents
		/// </summary>
		[Description("Changed only")]
		None,
		/// <summary>
		/// Replicate replicated documents
		/// </summary>
		[Description("Changed and replicated")]
		Replicate
	}
}