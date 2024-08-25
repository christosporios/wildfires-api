import express from 'express';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { isValidWildfire, loadWildfireConfigs } from './utils';
import { TimedEvent, Wildfire } from './types';
import DataSource from './dataSources/dataSource';
import Metars from './dataSources/metars';
import Fires from './dataSources/fires';
import Announcements from './dataSources/announcements';
import FlightPings from './dataSources/flightPings';

dotenv.config({ path: '.env.local' });
const app = express();
const port = process.env.PORT || 3000;
const dataDir = process.env.DATA_DIR || './data';

// Create the data directory if it doesn't exist
if (!fs.existsSync(dataDir)) {
    try {
        fs.mkdirSync(dataDir, { recursive: true });
        console.log(`Created data directory: ${dataDir}`);
    } catch (error) {
        console.error(`Error creating data directory: ${error}`);
    }
}

const wildfires: Map<Wildfire['id'], Map<string, DataSource>> = new Map();

const dataSources: { [key: string]: new (wildfireConfig: Wildfire) => DataSource } = {
    "metars": Metars,
    "fires": Fires,
    "announcements": Announcements,
    "flightPings": FlightPings
}

const initDataSources = async () => {
    const wildfireConfigs = await loadWildfireConfigs();

    for (const [wildfireId, wildfireConfig] of wildfireConfigs.entries()) {
        let wildfireDataSources = new Map<string, DataSource>();
        for (const dataSourceName of wildfireConfig.dataSources) {
            if (dataSourceName in dataSources) {
                const dataSourceClass = dataSources[dataSourceName];
                wildfireDataSources.set(dataSourceName, new dataSourceClass(wildfireConfig));
            } else {
                console.warn(`Data source "${dataSourceName}" is configured but not implemented for wildfire ${wildfireId}`);
            }
        }
        wildfires.set(wildfireId, wildfireDataSources);
    }
}

let wildfireConfigs = new Map<string, Wildfire>();
const updateData = async () => {
    wildfireConfigs = await loadWildfireConfigs();
    for (const [wildfireId, wildfireConfig] of wildfireConfigs.entries()) {
        const wildfireDataSources = wildfires.get(wildfireId);
        if (wildfireDataSources) {
            for (const [dataSourceId, dataSource] of wildfireDataSources.entries()) {
                if (wildfireConfig.dataSources.includes(dataSourceId)) {
                    dataSource.updateData(wildfireConfig);
                }
            }
        }
    }
}

const init = async () => {
    await initDataSources();
    await updateData();
    setInterval(updateData, 1000 * 60);
}

init();

app.get('/', (req, res) => {
    res.send('Hello, World!');
});

app.get('/wildfires', (req, res) => {
    res.json(Array.from(wildfires.values()));
});

const mergeSortedEvents = (a: TimedEvent[], b: TimedEvent[]) => {
    const merged: TimedEvent[] = [];
    let i = 0, j = 0;
    while (i < a.length && j < b.length) {
        if (a[i].timestamp > b[j].timestamp) {
            merged.push(a[i]);
            i++;
        } else {
            merged.push(b[j]);
            j++;
        }
    }
    // Add remaining elements from a, if any
    while (i < a.length) {
        merged.push(a[i]);
        i++;
    }
    // Add remaining elements from b, if any
    while (j < b.length) {
        merged.push(b[j]);
        j++;
    }
    return merged;
}

const parseDate = (value: string | undefined, defaultDate: Date): Date => {
    if (!value) return defaultDate;
    if (/^\d+$/.test(value)) {
        // Unix timestamp
        return new Date(parseInt(value) * 1000);
    }
    // ISO string
    const date = new Date(value);
    return isNaN(date.getTime()) ? defaultDate : date;
};

app.get('/wildfires/:id', async (req, res) => {
    const wildfireId = req.params.id;
    const from = req.query.from;
    const to = req.query.to;

    const dataSources = wildfires.get(wildfireId);
    if (!dataSources) {
        res.status(404).send('Wildfire not found');
        return;
    }

    const wildfireConfig = wildfireConfigs.get(wildfireId);
    if (!wildfireConfig) {
        res.status(404).send('Wildfire configuration not found');
        return;
    }

    // Check if 'only' parameter is provided and valid
    const onlyString = req.query.only as string | undefined;
    let only: string[] | undefined;
    if (onlyString) {
        only = onlyString.split(',').map(s => s.trim());
        const validSources = Array.from(dataSources.keys());
        const invalidSources = only.filter(s => !validSources.includes(s));

        if (invalidSources.length > 0) {
            res.status(400).send(`Invalid data source(s): ${invalidSources.join(', ')}`);
            return;
        }
    }

    const fromDate = parseDate(from as string | undefined, new Date(wildfireConfig.start));
    const toDate = parseDate(to as string | undefined, new Date());

    let events: TimedEvent[] = [];
    let dataSourceTimeIntervals: { [key: string]: { from: Date | null, to: Date | null } } = {};
    for (const [dataSourceId, dataSource] of dataSources.entries()) {
        if (!wildfireConfig.dataSources.includes(dataSourceId)) continue;

        if (!only || only.includes(dataSourceId)) {
            const sourceEvents = await dataSource.getData(fromDate, toDate);
            console.log(`Got ${sourceEvents.length} events from ${dataSourceId}`);
            events = mergeSortedEvents(events, sourceEvents);
            dataSourceTimeIntervals[dataSourceId] = dataSource.getCurrentInterval();
        }
    }

    res.json({
        events,
        recency: dataSourceTimeIntervals
    });
});

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});