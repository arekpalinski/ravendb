﻿import database from "models/resources/database";
import React, { useCallback, useEffect, useReducer, useState } from "react";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { useServices } from "hooks/useServices";
import { ongoingTasksReducer, ongoingTasksReducerInitializer } from "./OngoingTasksReducer";
import { useAccessManager } from "hooks/useAccessManager";
import createOngoingTask from "viewmodels/database/tasks/createOngoingTask";
import app from "durandal/app";
import useTimeout from "hooks/useTimeout";
import appUrl from "common/appUrl";
import { ExternalReplicationPanel } from "../panels/ExternalReplicationPanel";
import {
    OngoingTaskElasticSearchEtlInfo,
    OngoingTaskExternalReplicationInfo,
    OngoingTaskInfo,
    OngoingTaskKafkaEtlInfo,
    OngoingTaskOlapEtlInfo,
    OngoingTaskPeriodicBackupInfo,
    OngoingTaskRavenEtlInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationSinkInfo,
    OngoingTaskSharedInfo,
    OngoingTaskSqlEtlInfo,
    OngoingTaskSubscriptionInfo,
} from "../../../../models/tasks";
import { RavenEtlPanel } from "../panels/RavenEtlPanel";
import { SqlEtlPanel } from "../panels/SqlEtlPanel";
import { OlapEtlPanel } from "../panels/OlapEtlPanel";
import { ElasticSearchEtlPanel } from "../panels/ElasticSearchEtlPanel";
import { PeriodicBackupPanel } from "../panels/PeriodicBackupPanel";
import { SubscriptionPanel } from "../panels/SubscriptionPanel";
import { ReplicationSinkPanel } from "../panels/ReplicationSinkPanel";
import { ReplicationHubDefinitionPanel } from "../panels/ReplicationHubDefinitionPanel";
import useBoolean from "hooks/useBoolean";
import { OngoingTaskProgressProvider } from "./OngoingTaskProgressProvider";
import { BaseOngoingTaskPanelProps, taskKey } from "../shared";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;

import "./OngoingTaskPage.scss";
import etlScriptDefinitionCache from "models/database/stats/etlScriptDefinitionCache";
import TaskUtils from "../../../../utils/TaskUtils";
import { KafkaEtlPanel } from "../panels/KafkaEtlPanel";
import { RabbitMqEtlPanel } from "../panels/RabbitMqEtlPanel";
import useInterval from "hooks/useInterval";

interface OngoingTasksPageProps {
    database: database;
}

export function OngoingTasksPage(props: OngoingTasksPageProps) {
    const { database } = props;

    const locations = database.getLocations();

    const { canReadWriteDatabase, isClusterAdminOrClusterNode } = useAccessManager();

    const { value: progressEnabled, setTrue: startTrackingProgress } = useBoolean(false);

    const { tasksService } = useServices();

    const [definitionCache] = useState(() => new etlScriptDefinitionCache(database));

    const [tasks, dispatch] = useReducer(ongoingTasksReducer, locations, ongoingTasksReducerInitializer);

    const fetchTasks = useCallback(
        async (location: databaseLocationSpecifier) => {
            try {
                const tasks = await tasksService.getOngoingTasks(database, location);
                dispatch({
                    type: "TasksLoaded",
                    location,
                    tasks,
                });
            } catch (e) {
                dispatch({
                    type: "TasksLoadError",
                    location,
                    error: e,
                });
            }
        },
        [database, tasksService, dispatch]
    );

    const reload = useCallback(async () => {
        const loadTasks = tasks.locations.map((location) => fetchTasks(location));
        await Promise.all(loadTasks);
    }, [tasks, fetchTasks]);

    useInterval(reload, 10_000);

    const loadMissing = async () => {
        if (tasks.tasks.length > 0) {
            const loadTasks = tasks.tasks[0].nodesInfo.map(async (nodeInfo) => {
                if (nodeInfo.status === "notLoaded") {
                    await fetchTasks(nodeInfo.location);
                }
            });

            await Promise.all(loadTasks);
        }
    };

    useTimeout(loadMissing, 3_000);

    useEffect(() => {
        const nodeTag = clusterTopologyManager.default.localNodeTag();
        const initialLocation = database.getFirstLocation(nodeTag);

        fetchTasks(initialLocation);
    }, [fetchTasks, database]);

    const addNewOngoingTask = useCallback(() => {
        const addOngoingTaskView = new createOngoingTask(database);
        app.showBootstrapDialog(addOngoingTaskView);
    }, [database]);

    const deleteTask = useCallback(
        async (task: OngoingTaskSharedInfo) => {
            await tasksService.deleteOngoingTask(database, task);
            await reload();
        },
        [tasksService, database, reload]
    );

    const toggleOngoingTask = useCallback(
        async (task: OngoingTaskSharedInfo, enable: boolean) => {
            await tasksService.toggleOngoingTask(database, task, enable);
            await reload();
        },
        [database, reload, tasksService]
    );

    const onEtlProgress = useCallback(
        (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => {
            dispatch({
                type: "ProgressLoaded",
                progress,
                location,
            });
        },
        [dispatch]
    );

    const showItemPreview = useCallback(
        (task: OngoingTaskInfo, scriptName: string) => {
            const taskType = TaskUtils.studioTaskTypeToTaskType(task.shared.taskType);
            const etlType = TaskUtils.taskTypeToEtlType(taskType);
            definitionCache.showDefinitionFor(etlType, task.shared.taskId, scriptName);
        },
        [definitionCache]
    );

    const canNavigateToServerWideTasks = isClusterAdminOrClusterNode();
    const serverWideTasksUrl = appUrl.forServerWideTasks();

    const externalReplications = tasks.tasks.filter(
        (x) => x.shared.taskType === "Replication"
    ) as OngoingTaskExternalReplicationInfo[];
    const ravenEtls = tasks.tasks.filter((x) => x.shared.taskType === "RavenEtl") as OngoingTaskRavenEtlInfo[];
    const sqlEtls = tasks.tasks.filter((x) => x.shared.taskType === "SqlEtl") as OngoingTaskSqlEtlInfo[];
    const olapEtls = tasks.tasks.filter((x) => x.shared.taskType === "OlapEtl") as OngoingTaskOlapEtlInfo[];
    const kafkaEtls = tasks.tasks.filter((x) => x.shared.taskType === "KafkaQueueEtl") as OngoingTaskKafkaEtlInfo[];
    const rabbitMqEtls = tasks.tasks.filter((x) => x.shared.taskType === "RabbitQueueEtl") as OngoingTaskKafkaEtlInfo[];
    const elasticSearchEtls = tasks.tasks.filter(
        (x) => x.shared.taskType === "ElasticSearchEtl"
    ) as OngoingTaskElasticSearchEtlInfo[];
    const backups = tasks.tasks.filter((x) => x.shared.taskType === "Backup") as OngoingTaskPeriodicBackupInfo[];
    const subscriptions = tasks.tasks.filter(
        (x) => x.shared.taskType === "Subscription"
    ) as OngoingTaskSubscriptionInfo[];
    const replicationHubs = tasks.tasks.filter(
        (x) => x.shared.taskType === "PullReplicationAsHub"
    ) as OngoingTaskReplicationHubInfo[];
    const replicationSinks = tasks.tasks.filter(
        (x) => x.shared.taskType === "PullReplicationAsSink"
    ) as OngoingTaskReplicationSinkInfo[];

    const hubDefinitions = tasks.replicationHubs;

    const sharedPanelProps: Omit<BaseOngoingTaskPanelProps<OngoingTaskInfo>, "data"> = {
        db: database,
        onDelete: deleteTask,
        toggleState: toggleOngoingTask,
    };

    return (
        <div>
            {progressEnabled && <OngoingTaskProgressProvider db={database} onEtlProgress={onEtlProgress} />}
            <div className="flex-vertical">
                <div className="flex-header flex-horizontal">
                    {canReadWriteDatabase(database) && (
                        <button onClick={addNewOngoingTask} className="btn btn-primary">
                            <i className="icon-plus"></i>
                            <span>Add a Database Task</span>
                        </button>
                    )}
                    <div className="flex-separator"></div>
                    {canNavigateToServerWideTasks && (
                        <small className="padding padding-xs margin-left" title="Go to the Server-Wide Tasks view">
                            <a target="_blank" href={serverWideTasksUrl}>
                                <i className="icon-link"></i>Server-Wide Tasks
                            </a>
                        </small>
                    )}
                </div>
                <div className="scroll flex-grow">
                    {tasks.tasks.length === 0 && tasks.replicationHubs.length === 0 && (
                        <div className="row">
                            <div className="col-sm-8 col-sm-offset-2 col-lg-6 col-lg-offset-3">
                                <i className="icon-xl icon-empty-set text-muted"></i>
                                <h2 className="text-center text-muted">
                                    No tasks have been created for this Database Group.
                                </h2>
                            </div>
                        </div>
                    )}

                    {externalReplications.length > 0 && (
                        <div key="external-replications">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item external-replication no-text-transform">
                                    <i className="icon-external-replication"></i>
                                    <span>External Replication ({externalReplications.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {externalReplications.map((x) => (
                                    <ExternalReplicationPanel {...sharedPanelProps} key={taskKey(x.shared)} data={x} />
                                ))}
                            </div>
                        </div>
                    )}

                    {ravenEtls.length > 0 && (
                        <div key="raven-etls">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item ravendb-etl no-text-transform">
                                    <i className="icon-etl"></i>
                                    <span>RavenDB ETL ({ravenEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {ravenEtls.map((x) => (
                                    <RavenEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {sqlEtls.length > 0 && (
                        <div key="sql-etls">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item sql-etl no-text-transform">
                                    <i className="icon-sql-etl"></i>
                                    <span>SQL ETL ({sqlEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {sqlEtls.map((x) => (
                                    <SqlEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {olapEtls.length > 0 && (
                        <div key="olap-etls">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item olap-etl no-text-transform">
                                    <i className="icon-olap-etl"></i>
                                    <span>OLAP ETL ({olapEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {olapEtls.map((x) => (
                                    <OlapEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {kafkaEtls.length > 0 && (
                        <div key="kafka-etls">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item kafka-etl no-text-transform">
                                    <i className="icon-kafka-etl"></i>
                                    <span>KAFKA ETL ({kafkaEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {kafkaEtls.map((x) => (
                                    <KafkaEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {rabbitMqEtls.length > 0 && (
                        <div key="rabbitmq-etls">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item rabbitmq-etl no-text-transform">
                                    <i className="icon-rabbitmq-etl"></i>
                                    <span>RABBITMQ ETL ({rabbitMqEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {rabbitMqEtls.map((x) => (
                                    <RabbitMqEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {elasticSearchEtls.length > 0 && (
                        <div key="elastic-search-etls">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item elastic-etl no-text-transform">
                                    <i className="icon-elastic-search-etl"></i>
                                    <span>Elasticsearch ETL ({elasticSearchEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {elasticSearchEtls.map((x) => (
                                    <ElasticSearchEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {backups.length > 0 && (
                        <div key="backups">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item periodic-backup no-text-transform">
                                    <i className="icon-backups"></i>
                                    <span>Periodic Backup ({backups.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {backups.map((x) => (
                                    <PeriodicBackupPanel
                                        forceReload={reload}
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {subscriptions.length > 0 && (
                        <div key="subscriptions">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item subscription no-text-transform">
                                    <i className="icon-subscription"></i>
                                    <span>Subscription ({subscriptions.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {subscriptions.map((x) => (
                                    <SubscriptionPanel {...sharedPanelProps} key={taskKey(x.shared)} data={x} />
                                ))}
                            </div>
                        </div>
                    )}

                    {hubDefinitions.length > 0 && (
                        <div key="replication-hubs">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item pull-replication-hub no-text-transform">
                                    <i className="icon-pull-replication-hub"></i>
                                    <span>Replication Hub ({hubDefinitions.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {hubDefinitions.map((def) => (
                                    <ReplicationHubDefinitionPanel
                                        {...sharedPanelProps}
                                        key={taskKey(def.shared)}
                                        data={def}
                                        connectedSinks={replicationHubs.filter(
                                            (x) => x.shared.taskId === def.shared.taskId
                                        )}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {replicationSinks.length > 0 && (
                        <div key="replication-sinks">
                            <div className="hr-title margin-top-xs">
                                <h5 className="tasks-list-item pull-replication-sink no-text-transform">
                                    <i className="icon-pull-replication-agent"></i>
                                    <span>Replication Sink ({replicationSinks.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {replicationSinks.map((x) => (
                                    <ReplicationSinkPanel {...sharedPanelProps} key={taskKey(x.shared)} data={x} />
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}