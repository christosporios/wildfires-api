import { DataSourceData, Metar, TimedEvent, Wildfire } from "../types";
import DataSource from "./dataSource";
import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config({ path: '.env.local' });

export default class Metars extends DataSource {
    private airportIcao: string;
    private wildfireStart: Date;

    constructor(wildfire: Wildfire) {
        super(wildfire.id, "metars");
        this.airportIcao = wildfire.metarAirport;
        this.wildfireStart = new Date(wildfire.start);
    }

    getMeta(): any {
        return {
            airportIcao: this.airportIcao,
            wildfireStart: this.wildfireStart
        };
    }

    initFromWildfire(wildfire: Wildfire): void {
        this.airportIcao = wildfire.metarAirport;
        this.wildfireStart = new Date(wildfire.start);
    }

    initFromSavedData(data: DataSourceData): void {
        this.airportIcao = data.meta.airportIcao;
        this.wildfireStart = data.meta.wildfireStart;
    }
    async fetchInterval(from: Date, to: Date): Promise<TimedEvent[]> {
        const allMetars: TimedEvent[] = [];
        let currentDate = new Date(from);

        while (currentDate <= to) {
            const dateString = currentDate.toISOString().slice(0, 10); // Format: YYYY-MM-DD
            const url = `https://api.metar-taf.com/metar-archive?api_key=${process.env.METAR_API_KEY}&v=2.3&locale=en-US&id=${this.airportIcao}&date=${dateString}`;
            this.log(`Fetching metars for ${dateString}: ${url}`);

            try {
                const response = await axios.get(url);
                const metars = response.data.metars;

                metars.forEach((metar: { raw: string }) => {
                    const rawMetar = metar.raw.startsWith("METAR ") ? metar.raw.slice(6) : metar.raw;
                    const parsed = this.parseMetar(rawMetar, this.wildfireStart);
                    if (parsed) allMetars.push(parsed);
                    else {
                        this.log(`Warning: Failed to parse METAR: ${metar.raw}`);
                    }
                });

                this.log(`Fetched ${metars.length} metars for ${dateString}`);
            } catch (error) {
                this.log(`Error fetching metars for ${dateString}: ${error}`);
            }

            // Move to the next day
            currentDate.setDate(currentDate.getDate() + 1);

            // Wait for 2 seconds before the next request
            await new Promise(resolve => setTimeout(resolve, 2000));
        }

        return allMetars;
    }

    private parseMetar(raw: string, wildfireStartDate: Date): TimedEvent | null {
        const parts = raw.split(' ');
        if (parts.length < 5) return null;

        const icaoId = parts[0];
        const timestamp = parts[1].endsWith('Z') ? parts[1].slice(0, -1) : parts[1];
        const day = parseInt(timestamp.slice(0, 2));
        const hour = parseInt(timestamp.slice(2, 4));
        const minute = parseInt(timestamp.slice(4, 6));

        const year = wildfireStartDate.getUTCFullYear();
        const month = wildfireStartDate.getUTCMonth();

        const metarDate = new Date(Date.UTC(year, month, day, hour, minute));

        if (metarDate < wildfireStartDate) {
            metarDate.setUTCMonth(metarDate.getUTCMonth() + 1);
        }

        const unixTimestamp = Math.floor(metarDate.getTime() / 1000);

        let windIndex = 2;
        let wind: Metar['wind'] = {
            direction: 0,
            speed: 0,
            variable: false
        };

        if (parts[windIndex] === 'AUTO') windIndex++;

        if (parts[windIndex].includes('KT')) {
            const windPart = parts[windIndex];
            wind.direction = windPart.startsWith('VRB') ? 'VRB' : parseInt(windPart.slice(0, 3));
            wind.speed = parseInt(windPart.slice(3, 5));
            if (windPart.includes('G')) {
                wind.gusting = parseInt(windPart.slice(windPart.indexOf('G') + 1, -2));
            }
            wind.variable = windPart.startsWith('VRB');
        }

        let tempDewIndex = parts.findIndex(p => p.includes('/'));
        let temperature: number | undefined = undefined;
        let dewPoint: number | undefined = undefined;
        if (tempDewIndex !== -1) {
            const tempDewParts = parts[tempDewIndex].split('/');
            temperature = parseInt(tempDewParts[0]);
            dewPoint = parseInt(tempDewParts[1]);
        }

        let qnhIndex = parts.findIndex(p => p.startsWith('Q'));
        let qnh: number | undefined = qnhIndex !== -1 ? parseInt(parts[qnhIndex].slice(1)) : undefined;

        if (temperature === undefined || dewPoint === undefined || qnh === undefined) {
            return null;
        }

        return {
            event: "metar",
            type: "metar",
            icaoId,
            raw,
            timestamp: unixTimestamp,
            wind,
            temperature,
            dewPoint,
            qnh
        } as Metar;
    }

    isFullFetchNeeded(wildfire: Wildfire): boolean {
        return this.airportIcao !== wildfire.metarAirport;
    }

}