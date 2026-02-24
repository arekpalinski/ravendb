import viewModelBase = require("viewmodels/viewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import awesomeMultiselect = require("common/awesomeMultiselect");
import databasesManager = require("common/shell/databasesManager");
import getQueriesCacheListCommand = require("commands/database/debug/getQueriesCacheListCommand");
import messagePublisher = require("common/messagePublisher");
import generalUtils = require("common/generalUtils");
import typeUtils = require("common/typeUtils");

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

type QueryCacheEntryWithDb = QueryCacheEntry & {
    DatabaseName: string;
};

type QueriesCacheListResponse = {
    TotalCachedQueries: number;
    Results: QueryCacheEntry[];
};

class queryMetadataCache extends viewModelBase {
    view = require("views/manage/queryMetadataCache.html");

    queryHash = ko.observable<string>();

    private data = ko.observableArray<QueryCacheEntryWithDb>([]);

    private gridController = ko.observable<virtualGridController<QueryCacheEntryWithDb>>();
    private columnPreview = new columnPreviewPlugin<QueryCacheEntryWithDb>();

    private allDbNames = ko.observableArray<string>();
    private selectedDbNames = ko.observableArray<string>();

    private currentLoadOperation = 0;

    resultsCount: KnockoutComputed<number>;

    constructor() {
        super();

        this.resultsCount = ko.pureComputed(() => this.data().length);

        const throttledQueryHash = this.queryHash.extend({ rateLimit: { timeout: 300, method: "notifyWhenChangesStop" } });
        this.registerDisposable(throttledQueryHash.subscribe(() => this.loadUsingCurrentCriteria()));

        this.registerDisposable(this.selectedDbNames.subscribe(() => this.loadUsingCurrentCriteria()));
    }

    activate(args: { database?: string; queryHash?: string }) {
        super.activate(args);

        this.allDbNames(databasesManager.default.databases().map(x => x.name));
        this.selectedDbNames(args?.database ? [args.database] : this.allDbNames().slice(0));

        if (args?.queryHash) {
            this.queryHash(args.queryHash);
        }
    }

    attached() {
        super.attached();

        awesomeMultiselect.build($("#visibleDbsSelector"), opts => {
            opts.includeSelectAllOption = true;
            opts.nSelectedText = " databased selected";
            opts.allSelectedText = "All databases selected";
        });
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.setDefaultSortBy(5, "desc");
        grid.init(() => this.fetchData(), () =>
            [
                new textColumn<QueryCacheEntryWithDb>(grid, x => x.DatabaseName, "Database", "12%", {
                    sortable: "string"
                }),
                new textColumn<QueryCacheEntryWithDb>(grid, x => x.QueryText, "Query", "26%", {
                    sortable: "string"
                }),
                new textColumn<QueryCacheEntryWithDb>(grid, x => this.formatQueryHash(x), "Query Hash", "15%", {
                    sortable: x => this.formatQueryHash(x)
                }),
                new textColumn<QueryCacheEntryWithDb>(grid, x => this.getIndexName(x), "Index", "18%", {
                    sortable: "string"
                }),
                new textColumn<QueryCacheEntryWithDb>(grid, x => x.CollectionName || "-", "Collection", "12%", {
                    sortable: "string"
                }),
                new textColumn<QueryCacheEntryWithDb>(grid, x => x.LastQueriedAt, "Last Queried At", "17%", {
                    sortable: "string"
                })
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-query-hash-lookup-tooltip",
            (item: QueryCacheEntryWithDb, column: textColumn<QueryCacheEntryWithDb>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(item);
                if (value !== undefined) {
                    const html = generalUtils.escapeHtml(String(value)).replace(/\r?\n/g, "<br/>");
                    onValue(html, String(value));
                }
            });

        if (this.currentLoadOperation === 0 && this.data().length === 0) {
            this.loadUsingCurrentCriteria();
        }
    }

    private loadUsingCurrentCriteria() {
        const queryHash = this.queryHash()?.trim();

        if (queryHash && /^-?\d+$/.test(queryHash) === false) {
            return;
        }

        this.load(queryHash ? queryHash : undefined);
    }

    private load(queryHash?: string) {
        const dbNames = this.selectedDbNames().slice(0);
        if (!dbNames.length) {
            messagePublisher.reportWarning("No databases selected.");
            return;
        }

        const loadToken = ++this.currentLoadOperation;

        const requests = dbNames.map(dbName => {
            const db = databasesManager.default.getDatabaseByName(dbName);
            return new getQueriesCacheListCommand(db, queryHash).execute();
        });

        const handleResponses = (responses: QueriesCacheListResponse[]) => {
            if (loadToken !== this.currentLoadOperation) {
                return;
            }

            this.onData(dbNames, responses);
        };

        if (requests.length === 1) {
            return requests[0]
                .done(response => handleResponses([response]));
        }

        return $.when.apply($, requests)
            .done((...responses: [QueriesCacheListResponse, string, JQueryXHR][]) => {
                const results = responses.map(x => x[0]);
                handleResponses(results);
            });
    }

    private onData(dbNames: string[], responses: QueriesCacheListResponse[]) {
        const entries = responses.flatMap((response, index) => {
            const dbName = dbNames[index];
            return (response.Results || []).map(item => ({
                ...item,
                DatabaseName: dbName
            }));
        });

        this.data(typeUtils.sortBy(entries, x => `${x.DatabaseName}|${x.LastQueriedAt}|${this.formatQueryHash(x)}`));

        if (this.gridController()) {
            this.gridController().reset(true);
        }
    }

    private fetchData(): JQueryPromise<pagedResult<QueryCacheEntryWithDb>> {
        return $.when<pagedResult<QueryCacheEntryWithDb>>({
            items: this.data(),
            totalResultCount: this.data().length,
            resultEtag: null,
            additionalResultInfo: undefined
        });
    }

    private getIndexName(item: QueryCacheEntryWithDb) {
        return item.IndexName || item.AutoIndexName || "-";
    }

    private formatQueryHash(item: QueryCacheEntryWithDb) {
        if (item.QueryHash === undefined || item.QueryHash === null)
            return "-";

        return String(item.QueryHash);
    }
}

export = queryMetadataCache;
