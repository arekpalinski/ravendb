import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class compactDatabaseCommand extends commandBase {

    private readonly databaseName: string;

    private readonly compactDocuments: boolean;

    private readonly indexesToCompact: Array<string>;

    private readonly skipOptimizeIndexes: boolean = false;

    private readonly tempPath: string = null;

    private readonly deleteOriginalBeforeCopyBack: boolean = false;

    constructor(databaseName: string, compactDocuments: boolean, indexesToCompact: Array<string>, skipOptimizeIndexes = false,
                tempPath: string = null, deleteOriginalBeforeCopyBack = false) {
        super();
        this.skipOptimizeIndexes = skipOptimizeIndexes;
        this.indexesToCompact = indexesToCompact;
        this.compactDocuments = compactDocuments;
        this.databaseName = databaseName;
        this.tempPath = tempPath;
        this.deleteOriginalBeforeCopyBack = deleteOriginalBeforeCopyBack;
    }

    execute(): JQueryPromise<operationIdDto> {
        const payload: Raven.Client.ServerWide.CompactSettings = {
            DatabaseName: this.databaseName,
            Documents: this.compactDocuments,
            Indexes: this.indexesToCompact,
            SkipOptimizeIndexes: this.skipOptimizeIndexes,
            TempPath: this.tempPath,
            DeleteOriginalBeforeCopyBack: this.deleteOriginalBeforeCopyBack
        };

        const url = endpoints.global.adminDatabases.adminCompact;
        
        return this.post<operationIdDto>(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError("Failed to compact database", response.responseText, response.statusText));
    }


} 

export = compactDatabaseCommand;
