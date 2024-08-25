import { TimedEvent, Wildfire } from "../types";
import { DataSourceData } from "../types";
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';

dotenv.config({ path: '.env.local' });
const dataDir = process.env.DATA_DIR || './data';

export default abstract class DataSource {
    private data: TimedEvent[] = [];
    private wildfireId: string;
    private dataSource: string;
    // the period of time for which we have already fetched data
    private dataPeriod: { from: Date | null, to: Date | null } = { from: null, to: null };
    private isFetching: boolean = false; // Add a flag to track fetching status

    abstract fetchInterval(from: Date, to: Date): Promise<TimedEvent[]>;
    abstract initFromWildfire(wildfire: Wildfire): void;
    abstract getMeta(): any;
    abstract initFromSavedData(data: DataSourceData): void;
    abstract isFullFetchNeeded(wildfire: Wildfire): boolean;

    log(message: string): void {
        console.log(`[${this.wildfireId}-${this.dataSource}] ${message}`);
    }

    constructor(wildfireId: string, dataSource: string) {
        this.wildfireId = wildfireId;
        this.dataSource = dataSource;
        this.load();
    }

    load(): boolean {
        const fileName = `${this.wildfireId}-${this.dataSource}.json`;
        const filePath = path.join(dataDir, fileName);

        try {
            const data = fs.readFileSync(filePath, 'utf8');
            const parsedData = JSON.parse(data);
            this.data = parsedData.data;
            this.dataPeriod = {
                from: new Date(parsedData.period.from),
                to: new Date(parsedData.period.to)
            };
            this.initFromSavedData(parsedData);
            this.log(`Loaded data from ${filePath}`);
            return true;
        } catch (error: any) {
            if (error.code === 'ENOENT') {
                this.log(`File does not exist: ${filePath}`);
                return false;
            }
            throw error; // Re-throw other errors
        }
    }

    save(): void {
        const data: DataSourceData = {
            period: { from: this.dataPeriod.from!, to: this.dataPeriod.to! },
            data: this.data,
            meta: this.getMeta()
        };

        const fileName = `${this.wildfireId}-${this.dataSource}.json`;
        const filePath = path.join(dataDir, fileName);

        try {
            fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
            this.log(`Data saved to ${filePath}`);
        } catch (error) {
            this.log(`Error saving data to ${filePath}: ${error}`);
        }
    }

    async updateData(wildfire: Wildfire): Promise<void> {
        if (this.isFetching) {
            this.log('Warning: Fetch in progress, skipping update');
            return;
        }
        const from = new Date(wildfire.start);
        const to = wildfire.end ? new Date(wildfire.end) : new Date();

        // Check if a full fetch is needed
        const fullFetchNeeded = this.isFullFetchNeeded(wildfire);
        if (fullFetchNeeded) {
            this.log(`Full fetch needed`);
            this.initFromWildfire(wildfire);
            // Reset the data period
            this.dataPeriod = { from: null, to: null };
        }

        this.log(`Updating data for ${this.wildfireId}-${this.dataSource} from ${from} to ${to}`);
        // Check if we need to fetch new data
        const epsilon = 1000; // 1 second in milliseconds
        if (!this.dataPeriod.from || !this.dataPeriod.to ||
            from.getTime() < this.dataPeriod.from.getTime() - epsilon ||
            to.getTime() > this.dataPeriod.to.getTime() + epsilon) {
            this.log(`Previous data period: ${this.dataPeriod.from} to ${this.dataPeriod.to}
                (wildfire config period ${from} to ${to})`);
            this.isFetching = true;

            // Calculate the periods we need to fetch
            const fetchFrom = this.dataPeriod.from ? new Date(Math.min(from.getTime(), this.dataPeriod.from.getTime())) : from;
            const fetchTo = this.dataPeriod.to ? new Date(Math.max(to.getTime(), this.dataPeriod.to.getTime())) : to;

            this.log(`Fetching data from ${fetchFrom} to ${fetchTo}`);

            let mostRecentFirst = (a: TimedEvent, b: TimedEvent) => b.timestamp - a.timestamp;
            try {
                const newData = await this.fetchInterval(fetchFrom, fetchTo);
                // Replace data if it's a full fetch, otherwise merge
                if (fullFetchNeeded) {
                    this.data = newData.sort(mostRecentFirst); // Replace data
                } else {
                    this.data = [...this.data, ...newData].sort(mostRecentFirst); // Merge new data
                }
                this.log(`Fetched ${newData.length} events, now have ${this.data.length} events`);

                // Update the data period
                this.dataPeriod.from = this.dataPeriod.from ? new Date(Math.min(this.dataPeriod.from.getTime(), from.getTime())) : from;
                this.dataPeriod.to = this.dataPeriod.to ? new Date(Math.max(this.dataPeriod.to.getTime(), to.getTime())) : to;
                await this.save();
            } catch (error) {
                this.log(`Error fetching interval: ${error}`);
                throw error;
            } finally {
                this.isFetching = false;
            }
        } else {
            this.log('Already have the data for the requested period');
            return Promise.resolve();
        }
    }

    async getData(from: Date, to: Date): Promise<TimedEvent[]> {
        this.log(`Getting data from ${from} to ${to} (have ${this.data.length} events)`);
        if (this.isFetching) {
            this.log('Warning: Fetch in progress, returning existing data');
        }
        const events = this.data.filter(event => event.timestamp >= from.getTime() / 1000 && event.timestamp <= to.getTime() / 1000);
        this.log(`Returning ${events.length} events`);
        return events;
    }

    getCurrentInterval(): { from: Date | null, to: Date | null } {
        return { from: this.dataPeriod.from, to: this.dataPeriod.to };
    }
}