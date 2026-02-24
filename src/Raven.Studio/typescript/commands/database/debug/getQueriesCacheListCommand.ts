import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

interface QueriesCacheListResponse {
    TotalCachedQueries: number;
    Results: QueryCacheEntry[];
}

type QueryCacheEntry = {
    QueryHash?: number | string;
    QueryText: string;
    CreatedAt: string;
    LastQueriedAt: string;
    IsDynamic?: boolean;
    CollectionName?: string;
    AutoIndexName?: string;
    IndexName?: string;
};

class getQueriesCacheListCommand extends commandBase {
    private readonly db: database | string;
    private readonly queryHash?: string;

    constructor(db: database | string, queryHash?: string) {
        super();
        this.db = db;
        this.queryHash = queryHash;
    }

    execute(): JQueryPromise<QueriesCacheListResponse> {
        const url = endpoints.databases.queriesDebug.debugQueriesCacheList;
        const args = this.queryHash ? { queryHash: this.queryHash } : null;

        return this.query<QueriesCacheListResponse>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to load queries cache", response.responseText, response.statusText));
    }
}

export = getQueriesCacheListCommand;
