﻿import React from "react";
import { ReactNode } from "react";

import "./NamedProgress.scss";
import classNames from "classnames";

export function NamedProgress(props: { name: string | ReactNode; children: ReactNode | ReactNode[] }) {
    const { name, children } = props;
    return (
        <div className="named-progress-container">
            <div className="progress-name">{name}</div>
            <div className="named-progress">{children}</div>
        </div>
    );
}

export function NamedProgressItem(props: {
    children: ReactNode | ReactNode[];
    progress: Progress;
    incomplete?: boolean;
}) {
    const { children, progress, incomplete } = props;
    const progressFormatted = formatPercentage(progress, incomplete);
    const completed = !incomplete && progress.total === progress.processed;
    return (
        <div className="progress-item">
            <strong className="progress-percentage">{progressFormatted}%</strong> {children}
            <div className="progress">
                <div className={classNames("progress-bar", { completed })} style={{ width: progressFormatted + "%" }} />
            </div>
        </div>
    );
}

function formatPercentage(progress: Progress, incomplete: boolean) {
    const processed = progress.processed;
    const total = progress.total;
    if (total === 0) {
        return incomplete ? 99.9 : 100;
    }

    const result = Math.floor((processed * 100.0) / total);

    return result === 100 && incomplete ? 99.9 : result;
}
