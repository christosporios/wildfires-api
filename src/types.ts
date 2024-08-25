/* top level structures */

export type DataSourceData = {
    period: { from: Date, to: Date };
    data: TimedEvent[];
    meta: any;
}

export type Wildfire = {
    id: string;
    name: string;
    boundingBox: [Coordinates, Coordinates];
    position: [number, number];
    zoom: number;
    start: string;
    end?: string; // undefined for live wildfires
    timezone: string;
    metarAirport: string;
    dataSources: string[];
};

export interface WildfireData {
    events: TimedEvent[];
    wildfire: Wildfire;
}

export interface WildfireSummary {
    fires: Fire[];
    wildfire: Wildfire;
}

/* events */

export interface TimedEvent {
    timestamp: number;
    event: "flightPing" | "metar" | "fire" | "announcement";
}

export interface FlightPing extends TimedEvent {
    event: "flightPing"
    callsign: string;
    icao24: string;
    position: Coordinates;
    altitude: number;
    altitudeGeometric: number;
    velocity: number;
    verticalSpeed: number;
    heading: number;
    squawk: string;
    timestamp: number;
}

export interface Metar extends TimedEvent {
    event: "metar";
    type: string;
    icaoId: string;
    raw: string;
    wind: {
        direction: number | 'VRB';
        speed: number;
        gusting?: number;
        variable: boolean;
    };
    temperature: number;
    dewPoint: number;
    qnh: number;
}

export interface Fire extends TimedEvent {
    event: "fire";
    position: Coordinates;
    timestamp: number;
    instrument: string;
    satellite: string;
    brightness?: number;
}

export interface Announcement extends TimedEvent {
    event: "announcement";
    tweetUrl: string;
    type: "alert" | "evacuate";
    timestamp: number;

    from: {
        name: string;
        position: Coordinates;
    }[];

    to: {
        name: string
        position: Coordinates
    }[];
}


// [Latitude, Longitude]
export type Coordinates = [number, number];

/* deprecated */

export type ThermalAnomaly = {
    latitude: number;
    longitude: number;
    acq_date: string;
    acq_time: string;
    version: string;
    bright_t31: number;
    daynight: "D" | "N";
    brightness: number;
    confidence: string | number;
    instrument: "VIIRS" | "MODIS";
    track: number;
    satellite: string;
    scan: number;
    frp: number;
};
