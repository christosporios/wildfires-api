import { TimedEvent, Fire, Wildfire, Coordinates, DataSourceData } from "../types";
import DataSource from "./dataSource";
import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config({ path: '.env.local' });
const FIRMS_API_KEY = process.env.FIRMS_API_KEY!;

export default class Fires extends DataSource {
    private bounds: [Coordinates, Coordinates];

    constructor(wildfire: Wildfire) {
        super(wildfire.id, "fires");
        this.bounds = wildfire.boundingBox;
    }

    initFromWildfire(wildfire: Wildfire): void {
        this.bounds = wildfire.boundingBox;
    }

    getMeta(): any {
        return {
            bounds: this.bounds
        };
    }

    initFromSavedData(data: DataSourceData): void {
        this.bounds = data.meta.bounds;
    }

    async fetchInterval(from: Date, to: Date): Promise<TimedEvent[]> {
        const sources = ['VIIRS_NOAA20_NRT', 'VIIRS_NOAA21_NRT', 'MODIS_NRT'];
        const [[north, west], [south, east]] = this.bounds;
        const areaCoordinates = `${west},${south},${east},${north}`;

        let allFires: Fire[] = [];

        this.log(`Fetching fires from ${from.toISOString()} to ${to.toISOString()}`);

        const currentDate = new Date(from);
        while (currentDate <= to) {
            const dateString = currentDate.toISOString().split('T')[0];
            this.log(`Fetching fires for ${dateString}`);

            for (const source of sources) {
                const url = `https://firms.modaps.eosdis.nasa.gov/api/area/csv/${FIRMS_API_KEY}/${source}/${areaCoordinates}/1/${dateString}`;
                this.log(`Fetching fires for ${dateString} from ${source}: ${url}`);
                try {
                    const response = await axios.get(url);
                    const csvData = response.data;
                    const rows = csvData.trim().split('\n');

                    if (rows.length < 2) {
                        this.log(`Error in API response: ${csvData.trim()}`);
                        continue; // Skip to the next source
                    }

                    const headers = rows[0].split(',');
                    const headerMap = {
                        latitude: headers.indexOf('latitude'),
                        longitude: headers.indexOf('longitude'),
                        brightness: headers.indexOf('brightness'),
                        acq_date: headers.indexOf('acq_date'),
                        acq_time: headers.indexOf('acq_time'),
                        satellite: headers.indexOf('satellite'),
                        instrument: headers.indexOf('instrument'),
                    };
                    const requiredColumns = ['latitude', 'longitude', 'acq_date', 'acq_time', 'satellite', 'instrument'];
                    const missingColumns = requiredColumns
                        .filter(column => headerMap[column as keyof typeof headerMap] === -1);

                    if (missingColumns.length > 0) {
                        this.log(`Error: Missing required columns: ${missingColumns.join(', ')}`);
                        continue; // Skip to the next source
                    }

                    // Check if brightness column is missing and log a warning
                    if (headerMap.brightness === -1) {
                        this.log('Warning: Brightness column is missing. Fires will be created without brightness data.');
                    }

                    for (let i = 1; i < rows.length; i++) {
                        const row = rows[i];
                        if (!row.trim()) continue;
                        const columns = row.split(',');

                        const timestamp = new Date(`${columns[headerMap.acq_date]} ${columns[headerMap.acq_time].substring(0, 2)}:${columns[headerMap.acq_time].substring(2, 4)}`).getTime() / 1000;

                        if (timestamp >= from.getTime() / 1000 && timestamp <= to.getTime() / 1000) {
                            const fire: Fire = {
                                event: "fire",
                                position: [parseFloat(columns[headerMap.latitude]), parseFloat(columns[headerMap.longitude])],
                                timestamp,
                                instrument: columns[headerMap.instrument],
                                satellite: columns[headerMap.satellite],
                                brightness: parseFloat(columns[headerMap.brightness])
                            };
                            allFires.push(fire);
                        }
                    }
                } catch (error) {
                    this.log(`Error fetching fire data for ${dateString} from ${source}: ${error}`);
                }

                this.log(`Fetched ${allFires.length} fires for ${dateString} from ${source}`);

                // Wait for 1 second before the next request to avoid rate limiting
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

            currentDate.setDate(currentDate.getDate() + 1);
        }

        return allFires;
    }

    isFullFetchNeeded(wildfire: Wildfire): boolean {
        // Compare the current bounds with the wildfire's bounding box
        return this.bounds.some((currentBound, index) =>
            currentBound.some((coord, coordIndex) =>
                Math.abs(coord - wildfire.boundingBox[index][coordIndex]) > Number.EPSILON
            )
        );
    }

}