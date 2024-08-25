import { TimedEvent, Fire, Wildfire, Coordinates, DataSourceData, FlightPing } from "../types";
import DataSource from "./dataSource";
import dotenv from 'dotenv';
import { BasicAuth, Iterator, QueryResult, Trino } from 'trino-client';
import axios from 'axios';
import jwt from 'jsonwebtoken';

dotenv.config({ path: '.env.local' });
const TRINO_USERNAME = process.env.OPENSKY_TRINO_USERNAME!;
const TRINO_PASSWORD = process.env.OPENSKY_TRINO_PASSWORD!;


type AircraftStats = {
    icao24: string;
    callsign: string;
    lowestAltitude: number;
    squawks: string[];
}

const callsignPrefixBlacklist: string[] = [
    "SEH", "RYR", "AEE", "AFR", "DLH", "OAL", "QTR", "DAL", "UAL",
    "EJU", "VOE", "THY", "WZZ", "AAL", "UAL", "KLM", "ITY", "IBE",
    "QAR"
];


const trino: Trino = Trino.create({
    server: 'https://trino.opensky-network.org',
    catalog: 'minio',
    schema: 'osky',
});


export default class FlightPings extends DataSource {
    private bounds: [Coordinates, Coordinates];
    private token: { access_token: string, exp: number } | null = null;

    constructor(wildfire: Wildfire) {
        super(wildfire.id, "flightPings");
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

    isFullFetchNeeded(wildfire: Wildfire): boolean {
        // Compare the current bounds with the wildfire's bounding box
        return this.bounds.some((currentBound, index) =>
            currentBound.some((coord, coordIndex) =>
                Math.abs(coord - wildfire.boundingBox[index][coordIndex]) > Number.EPSILON
            )
        );
    }

    private getHourStartUnix(date: Date): number {
        return Math.floor(date.getTime() / 1000 / 3600) * 3600;
    }

    private getHourEndUnix(date: Date): number {
        return Math.floor(date.getTime() / 1000 / 3600) * 3600 + 3600;
    }

    private getAircraftStats(allResults: any[], columns: string[]): Map<string, AircraftStats> {
        const aircraftStats = new Map<string, AircraftStats>();
        for (const result of allResults) {
            const existingStats = aircraftStats.get(result[columns.indexOf("icao24")]);
            let squawk = result[columns.indexOf("squawk")];
            if (existingStats) {
                if (result[columns.indexOf("baroaltitude")] < existingStats.lowestAltitude || existingStats.lowestAltitude === null) {
                    existingStats.lowestAltitude = result[columns.indexOf("baroaltitude")];
                    console.log(`Updating lowest altitude for ${result[columns.indexOf("icao24")]}: ${result[columns.indexOf("baroaltitude")]}`);
                }
                if (result[columns.indexOf("callsign")] !== existingStats.callsign && existingStats.callsign !== null) {
                    this.log(`Warning: multiple callsigns for ${result[columns.indexOf("icao24")]}. Ignoring ${result[columns.indexOf("callsign")]}, keeping ${existingStats.callsign}`);
                }
                if (existingStats.callsign === null) {
                    existingStats.callsign = result[columns.indexOf("callsign")];
                }
                if (squawk && !existingStats.squawks.includes(squawk)) {
                    existingStats.squawks.push(squawk);
                }
            } else {
                aircraftStats.set(result[columns.indexOf("icao24")], {
                    icao24: result[columns.indexOf("icao24")],
                    callsign: result[columns.indexOf("callsign")],
                    lowestAltitude: result[columns.indexOf("baroaltitude")],
                    squawks: squawk ? [squawk] : []
                });
            }
        }

        return aircraftStats;
    }

    async fetchInterval(fetchFrom: Date, fetchTo: Date): Promise<TimedEvent[]> {
        this.log(`Fetching flight pings from ${fetchFrom} to ${fetchTo}`);
        let minLat = Math.min(this.bounds[0][0], this.bounds[1][0]);
        let maxLat = Math.max(this.bounds[0][0], this.bounds[1][0]);
        let minLon = Math.min(this.bounds[0][1], this.bounds[1][1]);
        let maxLon = Math.max(this.bounds[0][1], this.bounds[1][1]);
        const query = `select * from state_vectors_data4
            where
            hour >= ${this.getHourStartUnix(fetchFrom)} AND
            hour <= ${this.getHourEndUnix(fetchTo)}
            and lat between ${minLat} and ${maxLat}
            and lon between ${minLon} and ${maxLon}
            and time % 30 = 0
            and baroaltitude < 3000`; // 3000 METERS
        this.log(query);
        const startTime = Date.now();
        const result = await this.trinoQuery(query);
        const allResults: any[] = [];
        let columns: string[] = [];

        for await (const queryResult of result) {
            if (queryResult.data) {
                this.log(`Partial result: ${queryResult.data.length} rows`);
                allResults.push(...queryResult.data);
            }

            if (queryResult.columns) {
                columns = queryResult.columns.map((column: any) => column.name);
            }
        }
        this.log(`Trino query took ${Math.round((Date.now() - startTime) / 1000)} seconds`);

        const aircraftStats = this.getAircraftStats(allResults, columns);
        this.log(`Got ${allResults.length} rows and ${columns.length} columns, for ${aircraftStats.size} aircraft`);

        const potentialFirefighterPings = allResults.filter((row: any) => { // filter out aircraft that remained above 1000 meters
            const stats = aircraftStats.get(row[columns.indexOf("icao24")]);
            return stats && stats.lowestAltitude < 1000; // meters
        }).filter((row: any) => { // filter out common airline callsigns
            const callsign = row[columns.indexOf("callsign")];
            return !callsign || !callsignPrefixBlacklist.some(prefix => callsign.startsWith(prefix));
        });




        this.log(`Discarding ${allResults.length - potentialFirefighterPings.length} rows of aircraft that don't look like firefighters`);

        let relevantAircraftStats = this.getAircraftStats(potentialFirefighterPings, columns);
        this.log(`Got ${relevantAircraftStats.size} relevant aircraft from potential firefighter pings`);

        // Print all stats for each aircraft
        for (const [icao24, stats] of relevantAircraftStats.entries()) {
            this.log(`Aircraft ${icao24}:`);
            this.log(`  Callsign: ${stats.callsign}`);
            this.log(`  Lowest Altitude: ${stats.lowestAltitude} meters`);
            this.log(`  Squawks: ${stats.squawks.join(", ")}`);
        }

        const events: FlightPing[] = potentialFirefighterPings.map((row: any) => ({
            timestamp: row[columns.indexOf("time")],
            event: "flightPing",
            icao24: row[columns.indexOf("icao24")],
            callsign: row[columns.indexOf("callsign")],
            position: [row[columns.indexOf("lat")], row[columns.indexOf("lon")]],
            altitude: row[columns.indexOf("baroaltitude")],
            altitudeGeometric: row[columns.indexOf("geoaltitude")],
            heading: row[columns.indexOf("heading")],
            squawk: row[columns.indexOf("squawk")],
            velocity: row[columns.indexOf("velocity")],
            verticalSpeed: row[columns.indexOf("vertrate")],
        } as FlightPing));

        this.log(`Got ${events.length} flight pings`);

        return events;
    }

    async trinoQuery(query: string): Promise<Iterator<QueryResult>> {
        let token = await this.getToken();
        try {
            return await trino.query({
                query,
                extraHeaders: {
                    Authorization: `Bearer ${token}`,
                    "X-Trino-User": TRINO_USERNAME
                }
            });
        } catch (error) {
            console.error("Error executing Trino query:", error);
            throw error;
        }
    }

    private async getToken(): Promise<string | null> {
        const now = Math.floor(Date.now() / 1000) - 60; // Current time minus 1 minute

        if (this.token && this.token.exp > now) {
            console.log(`Token still valid until ${new Date(this.token.exp * 1000).toISOString()}`);
            return this.token.access_token;
        }

        console.log("Requesting authentication token");
        try {
            const response = await axios.post(
                "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
                new URLSearchParams({
                    client_id: "trino-client",
                    grant_type: "password",
                    username: TRINO_USERNAME,
                    password: TRINO_PASSWORD,
                }),
                {
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded'
                    }
                }
            );

            const payload = response.data;
            const decodedToken = jwt.decode(payload.access_token) as { exp: number } | null;

            if (decodedToken) {
                this.token = {
                    access_token: payload.access_token,
                    exp: decodedToken.exp
                };
                console.log(`Got token expiring at ${new Date(this.token.exp * 1000).toISOString()}`);
                return payload.access_token;
            } else {
                console.warn("Failed to decode JWT token");
                return null;
            }
        } catch (error) {
            if (axios.isAxiosError(error) && error.response) {
                if (error.response.status === 400 || error.response.status === 401) {
                    console.warn("Authentication failing on trino");
                    return null;
                }
            }
            console.error("Error fetching token:", error);
            return null;
        }
    }

}